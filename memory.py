# -*- coding: utf-8 -*-
# memory.py — 记忆处理（全局/群/私聊/机器人记忆、聊天记录、媒体缓存、记忆整理）
import json
import time
import re
import asyncio
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone

from log import info, warn, error, debug, LogCtx, spawn_background
from config import (
    safe_load_json, BASE_DIR, MEMORY_FILE, MIRROR_FILE, USER_MAP_FILE,
    HISTORY_DIR, MEDIA_CACHE_DIR, QUN_MEMORY_DIR, C2C_MEMORY_DIR, BOT_MEMORY_DIR,
    get_bot_isolate_flag, get_compress_threshold, get_context_limit,
)
from utils import atomic_write_json


# ==================== 全局记忆 ====================
def get_global_memory_file(app_id: Optional[str] = None) -> Path:
    if app_id:
        # app_id 会拼进文件名（memory_{app_id}.json），同样必须净化，
        # 否则 identifier="../../config" 之类的值可越出 BASE_DIR。
        # 详见 _validate_memory_identifier 处的说明。
        safe_app_id = _validate_memory_identifier(app_id)
        isolate = get_bot_isolate_flag(safe_app_id)
        if isolate:
            return BASE_DIR / f"memory_{safe_app_id}.json"
    return MEMORY_FILE


def load_memory(app_id: Optional[str] = None) -> Dict:
    file_path = get_global_memory_file(app_id)
    # 全局记忆按 app_id 分文件，归属明确；未指定 app_id 时才是真正的全局
    return safe_load_json(file_path, {"global_memory": [], "enabled": 1},
                          ctx=LogCtx(app_id=app_id) if app_id else None)


def save_memory(data: Dict, app_id: Optional[str] = None):
    file_path = get_global_memory_file(app_id)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_global_memory(app_id: Optional[str] = None) -> List[str]:
    return load_memory(app_id).get("global_memory", [])


def set_global_memory(memory_list: List[str], app_id: Optional[str] = None):
    data = load_memory(app_id)
    data["global_memory"] = memory_list
    save_memory(data, app_id)


def add_global_memory(text: str, app_id: Optional[str] = None):
    mem = get_global_memory(app_id)
    mem.append(text)
    set_global_memory(mem, app_id)


def remove_global_memory(index: int, app_id: Optional[str] = None) -> bool:
    mem = get_global_memory(app_id)
    if 0 <= index < len(mem):
        mem.pop(index)
        set_global_memory(mem, app_id)
        return True
    return False


def replace_global_memory(index: int, new_text: str, app_id: Optional[str] = None) -> bool:
    mem = get_global_memory(app_id)
    if 0 <= index < len(mem):
        mem[index] = new_text
        set_global_memory(mem, app_id)
        return True
    return False


def clear_global_memory(app_id: Optional[str] = None):
    set_global_memory([], app_id)


def get_global_memory_enabled(app_id: Optional[str] = None) -> bool:
    return load_memory(app_id).get("enabled", 1) == 1


def set_global_memory_enabled(enabled: bool, app_id: Optional[str] = None):
    data = load_memory(app_id)
    data["enabled"] = 1 if enabled else 0
    save_memory(data, app_id)


# ==================== 用户映射表（mirror.json） ====================
# 新格式: {"users": {"bot_appid": {"user_id": "username", ...}}}
# 旧格式（自动备份为 old_version_mirror.json）:
#   v1: {user_id: username}
#   v2: {bot_appid: [{user_id: username}, ...]}


# ==================== 用户/群映射表（user_map.json） ====================
# 新格式: {"app_id": {"user": ["user_openid1", ...], "group": {"group_openid1": ["member_id1", ...], ...}}}
# 旧格式（自动备份为 old_version_user_map.json）:
#   旧格式: {"app_id": {"user": [...], "group": ["group_openid1", ...]}}
# 记录所有私聊过的用户ID、机器人加入过的群ID及群内成员

def load_user_map() -> dict:
    """加载 user_map.json，返回 {app_id: {"user": [...], "group": [...]}}"""
    return safe_load_json(USER_MAP_FILE, {})


def save_user_map(data: dict):
    """保存 user_map.json（原子写）。

    ★ M6：原先是 open(...,"w") 直接覆盖。该项目里 mirror/user_map 的更新
      都是「读全表 → 改一条 → 写回全表」，而调用方（msg.py）在读写之间
      存在 await 窗口。两个协程交错时，后写的那个拿的是旧快照，会把对方
      刚加的用户/群整片抹掉。
      这里做两件事：
        1. 原子写（utils.atomic_write_json）—— 进程被杀不会留下半个文件；
        2. 写前把磁盘现状并回内存副本（_merge_map_tables）—— 消除丢失更新。
    """
    _write_map_table(USER_MAP_FILE, data, "user_map.json")


def _merge_map_tables(disk: dict, mem: dict) -> dict:
    """把磁盘上的现状并回内存副本，避免「读-改-写」丢更新（M6）。

    合并规则（按结构层级，不做过度推断）：
      - 顶层键：两边取并集；同一个 app_id 下递归合并。
      - 值为 list：取并集（保持磁盘顺序在前，内存新增项追加在后）。
      - 值为 dict：递归合并（如 group → {群id: [成员]}）。
      - 其它标量：以内存（本次要写的值）为准 —— 它代表调用方的明确意图。
    这样「另一个协程刚加进去的条目」不会被本次写回抹掉。
    """
    if not isinstance(disk, dict) or not isinstance(mem, dict):
        return mem
    out = dict(disk)
    for k, mv in mem.items():
        dv = out.get(k)
        if isinstance(dv, list) and isinstance(mv, list):
            merged = list(dv)
            for item in mv:
                if item not in merged:
                    merged.append(item)
            out[k] = merged
        elif isinstance(dv, dict) and isinstance(mv, dict):
            out[k] = _merge_map_tables(dv, mv)
        else:
            out[k] = mv
    return out


def _write_map_table(path, data: dict, label: str):
    """带合并与原子写的表写入（mirror.json / user_map.json 共用）。"""
    try:
        disk = safe_load_json(path, {})
    except Exception:
        disk = {}
    merged = _merge_map_tables(disk if isinstance(disk, dict) else {}, data)
    atomic_write_json(path, merged)


# ==================== openid → app_id 反查（仅供日志归属使用） ====================
# 为什么需要它：qun_memory / c2c_memory / history 的文件名只有 openid，
# 文件内容里也不存 app_id（这是既有的存储格式，不改）。于是这些模块内部
# 打日志时手头只有 openid，只能打出 appid=? 的无主记录 —— 排查时看不出
# 是哪个机器人的数据出了问题。
#
# 关键在于 openid 与 app_id 是 1:1 的：QQ 的 openid 由平台按「应用」签发，
# 同一个群/用户在不同机器人下拿到的 openid **完全不同**，因此
#   · 不存在两个机器人共用同一个 openid 的情况
#   · 同一个 openid 也绝不会对应多个 app_id
# 所以反查的结果是唯一且确定的，不存在"归属有歧义"的场景。
# （实测：本工作区 17 个机器人、25 个群 openid、16 个用户 openid，
#   全部互不重复，跨机器人重合数为 0。）
#
# user_map.json 是"哪个机器人拥有哪些群/用户"的权威来源
# （api_server 的归属校验也用同一份数据，口径一致）。纯只读，不改存储格式。

def find_app_id_by_group(group_id: str) -> str:
    """反查拥有该群的 app_id；查不到返回空串。

    由于 openid 与 app_id 是 1:1 的，一旦命中就是唯一答案。
    理论上的"多个机器人共有同一 openid"不会发生；真出现了说明 user_map
    被手工改坏，此时返回空串（不猜归属）比随便挑一个更安全。
    """
    if not group_id:
        return ""
    try:
        data = load_user_map()
    except Exception:
        return ""
    owners = []
    for app_id, v in data.items():
        if not isinstance(v, dict):
            continue
        raw = v.get("group")
        if isinstance(raw, (list, dict)) and group_id in raw:
            owners.append(app_id)
    return owners[0] if len(owners) == 1 else ""


def find_app_id_by_user(user_id: str) -> str:
    """反查拥有该私聊用户的 app_id；查不到返回空串（同样 1:1，命中即唯一）。"""
    if not user_id:
        return ""
    try:
        data = load_user_map()
    except Exception:
        return ""
    owners = []
    for app_id, v in data.items():
        if not isinstance(v, dict):
            continue
        raw = v.get("user")
        if isinstance(raw, (list, dict)) and user_id in raw:
            owners.append(app_id)
    return owners[0] if len(owners) == 1 else ""


def find_app_id_by_thread(thread_key: str) -> str:
    """按 thread_key（group_xxx / c2c_xxx）反查 app_id。

    history 的键是 thread_key，这里拆出 openid 后复用上面的反查逻辑。
    """
    if not thread_key:
        return ""
    if thread_key.startswith("group_"):
        return find_app_id_by_group(thread_key[len("group_"):])
    if thread_key.startswith("c2c_"):
        return find_app_id_by_user(thread_key[len("c2c_"):])
    return ""


def ctx_for_thread(thread_key: str) -> Optional[LogCtx]:
    """为 thread_key 构造带归属的 LogCtx（查不到 app_id 时至少保留 thread）。"""
    app_id = find_app_id_by_thread(thread_key)
    return LogCtx(app_id=app_id, thread_key=thread_key) if thread_key else None


def ctx_for_group(group_id: str) -> Optional[LogCtx]:
    """为群 openid 构造带归属的 LogCtx。"""
    if not group_id:
        return None
    return LogCtx(app_id=find_app_id_by_group(group_id), thread_key=f"group_{group_id}")


def ctx_for_user(user_id: str) -> Optional[LogCtx]:
    """为私聊用户 openid 构造带归属的 LogCtx。"""
    if not user_id:
        return None
    return LogCtx(app_id=find_app_id_by_user(user_id), thread_key=f"c2c_{user_id}")


def _ensure_user_map_app_entry(app_id: str) -> dict:
    """确保 user_map 中有 app_id 条目，返回完整数据"""
    data = load_user_map()
    if app_id not in data:
        data[app_id] = {"user": [], "group": {}}
    else:
        if "user" not in data[app_id]:
            data[app_id]["user"] = []
        if "group" not in data[app_id]:
            data[app_id]["group"] = {}
    return data


def record_user_id(app_id: str, user_openid: str):
    """记录私聊用户ID（C2C）"""
    if not app_id or not user_openid:
        return
    data = _ensure_user_map_app_entry(app_id)
    if user_openid not in data[app_id]["user"]:
        data[app_id]["user"].append(user_openid)
        save_user_map(data)
        info(f"[UserMap] 记录私聊用户 {user_openid} (app={app_id})", ctx=app_id)


def record_group_id(app_id: str, group_openid: str, member_id: str = None):
    """记录群ID（Group），可选同时记录该群的一个成员"""
    if not app_id or not group_openid:
        return
    data = _ensure_user_map_app_entry(app_id)
    if group_openid not in data[app_id]["group"]:
        data[app_id]["group"][group_openid] = []
        if member_id:
            data[app_id]["group"][group_openid].append(member_id)
        save_user_map(data)
        info(f"[UserMap] 记录群 {group_openid} (app={app_id})", ctx=app_id)
    elif member_id and member_id not in data[app_id]["group"][group_openid]:
        data[app_id]["group"][group_openid].append(member_id)
        save_user_map(data)
        info(f"[UserMap] 记录群成员 {member_id} -> 群 {group_openid} (app={app_id})", ctx=app_id)


def record_group_member(app_id: str, group_openid: str, member_id: str):
    """记录群内的一个成员，群不存在时会自动创建"""
    if not app_id or not group_openid or not member_id:
        return
    data = _ensure_user_map_app_entry(app_id)
    if group_openid not in data[app_id]["group"]:
        data[app_id]["group"][group_openid] = [member_id]
        save_user_map(data)
        info(f"[UserMap] 记录群 {group_openid} 及其成员 {member_id} (app={app_id})", ctx=app_id)
    elif member_id not in data[app_id]["group"][group_openid]:
        data[app_id]["group"][group_openid].append(member_id)
        save_user_map(data)
        info(f"[UserMap] 记录群成员 {member_id} -> 群 {group_openid} (app={app_id})", ctx=app_id)

def _mirror_is_new_format(data: dict) -> bool:
    """检测是否为最新格式：顶层含 'users' 键"""
    return isinstance(data, dict) and "users" in data


def mirror_migrate_old():
    """
    检测旧版 mirror.json，若有则备份为 old_version_mirror.json，
    并创建一个空的新格式文件。
    """
    if not MIRROR_FILE.exists():
        return
    try:
        with open(MIRROR_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return

    # 空文件或已是最新格式 → 跳过
    if not data or _mirror_is_new_format(data):
        return

    # 备份旧文件
    backup_name = BASE_DIR / "old_version_mirror.json"
    if backup_name.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = BASE_DIR / f"old_version_mirror_{timestamp}.json"
    try:
        MIRROR_FILE.rename(backup_name)
        info(f"旧版 mirror.json 已备份为 {backup_name.name}", ctx=None)
    except Exception as e:
        warn(f"备份旧版 mirror.json 失败: {e}", ctx=None)
        return

    # 创建新格式的空文件
    new_empty = {"users": {}, "groups": {}}
    with open(MIRROR_FILE, "w", encoding="utf-8") as f:
        json.dump(new_empty, f, ensure_ascii=False, indent=2)
    info("已创建新版 mirror.json（格式: {\"users\": {...}, \"groups\": {...}}）", ctx=None)


def user_map_migrate_old():
    """
    检测旧版 user_map.json 的 group 格式（列表），转换为新版格式（字典）。
    旧: {"app_id": {"group": ["gid1", "gid2"]}}
    新: {"app_id": {"group": {"gid1": [], "gid2": []}}}
    """
    if not USER_MAP_FILE.exists():
        return
    try:
        data = safe_load_json(USER_MAP_FILE, {})
        if not data:
            return
    except Exception:
        return

    changed = False
    for app_id, entry in data.items():
        if not isinstance(entry, dict):
            continue
        group_val = entry.get("group")
        if isinstance(group_val, list):
            # 旧格式: [gid1, gid2] -> 新格式: {gid1: [], gid2: []}
            entry["group"] = {gid: [] for gid in group_val}
            changed = True

    if not changed:
        # 已经是新格式（dict），但确保所有 group 值都是列表
        for entry in data.values():
            if isinstance(entry, dict) and isinstance(entry.get("group"), dict):
                for gid, members in entry["group"].items():
                    if not isinstance(members, list):
                        entry["group"][gid] = []
                        changed = True
        if not changed:
            return

    # 备份旧文件
    backup_name = BASE_DIR / "old_version_user_map.json"
    if backup_name.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = BASE_DIR / f"old_version_user_map_{timestamp}.json"
    try:
        USER_MAP_FILE.rename(backup_name)
        info(f"旧版 user_map.json 已备份为 {backup_name.name}", ctx=None)
    except Exception as e:
        warn(f"备份旧版 user_map.json 失败: {e}", ctx=None)
        return

    save_user_map(data)
    info("user_map.json 已迁移为新格式（group 列表 -> 字典，含成员列表）", ctx=None)


def load_mirror() -> Dict:
    return safe_load_json(MIRROR_FILE, {"users": {}, "groups": {}})


def save_mirror(data: Dict):
    """保存 mirror.json（原子写 + 写前合并，见 _write_map_table 说明）。

    ★ M6：与 save_user_map 同因同治 —— 全表重写 + 调用方存在 await 窗口。
    """
    _write_map_table(MIRROR_FILE, data, "mirror.json")


def _ensure_bot_entry(bot_appid: str) -> Dict:
    """确保 mirror data 中有 users→bot_appid 条目，返回完整的 mirror data"""
    data = load_mirror()
    if "users" not in data:
        data["users"] = {}
    if bot_appid not in data["users"]:
        data["users"][bot_appid] = {}
    return data


def get_user_mapping(bot_appid: str) -> dict:
    """获取指定机器人的用户映射 dict: {user_id: username, ...}"""
    data = load_mirror()
    return data.get("users", {}).get(bot_appid, {})


def set_user_mapping(bot_appid: str, mapping_dict: dict):
    """设置指定机器人的用户映射 dict（整表覆盖）。

    注意：全项目无调用点，属遗留接口，保留仅为兼容。日志按与
    update_user_mapping 相同的口径补上，避免将来被启用时又是静默写盘。
    """
    data = load_mirror()
    if "users" not in data:
        data["users"] = {}
    old_map = data["users"].get(bot_appid) or {}
    data["users"][bot_appid] = mapping_dict
    save_mirror(data)
    old_n = len(old_map)
    new_n = len(mapping_dict or {})
    info(f"[Mirror] 用户映射已覆盖写入: {old_n} -> {new_n} 条 (app={bot_appid})",
         ctx=bot_appid)


def get_user_name(qq_id: str, bot_appid: str) -> Optional[str]:
    """在指定机器人的映射中查找用户名称"""
    mapping = get_user_mapping(bot_appid)
    return mapping.get(qq_id)


def update_user_mapping(qq_id: str, username: str, bot_appid: str):
    """更新指定机器人的用户映射，不存在则添加。

    ★ 日志区分「首次记录」与「昵称变更」两种情况 —— 与群名的
      save_group_to_mirror 保持一致，否则用户改名是完全静默的，
      排查时只能去比对 mirror.json 才能发现改过。
      用户名依赖 QQ 每次消息随包下发的 author.username，因此用户改名后
      会在其下一次发言时自动刷新。
    """
    if not qq_id or not username:
        return
    data = _ensure_bot_entry(bot_appid)
    old_name = data["users"][bot_appid].get(qq_id)
    if old_name != username:
        data["users"][bot_appid][qq_id] = username
        save_mirror(data)
        if old_name:
            info(f"[Mirror] 用户昵称已更新: {old_name} -> {username} "
                 f"({qq_id}) (app={bot_appid})", ctx=bot_appid)
        else:
            info(f"[Mirror] 记录用户: {username} ({qq_id}) (app={bot_appid})",
                 ctx=bot_appid)


# ==================== mirror.json 群记录 ====================
def _ensure_mirror_groups_entry(app_id: str) -> Dict:
    """确保 mirror data 中有 groups→app_id 条目，返回完整 mirror data"""
    data = load_mirror()
    if "groups" not in data:
        data["groups"] = {}
    if app_id not in data["groups"]:
        data["groups"][app_id] = {}
    return data


def save_group_to_mirror(app_id: str, group_openid: str, group_name: str):
    """记录群ID和群名称到 mirror.json groups 字段。

    日志与 update_user_mapping 保持同一风格：区分「首次记录」与「群名称已更新」，
    否则改名前后的日志长得一样，翻日志分不出到底改没改。
    """
    if not app_id or not group_openid or not group_name:
        return
    data = _ensure_mirror_groups_entry(app_id)
    old_name = data["groups"][app_id].get(group_openid)
    if old_name != group_name:
        data["groups"][app_id][group_openid] = group_name
        save_mirror(data)
        if old_name:
            info(f"[Mirror] 群名称已更新: {old_name} -> {group_name} "
                 f"({group_openid}) (app={app_id})", ctx=app_id)
        else:
            info(f"[Mirror] 记录群: {group_name} ({group_openid}) (app={app_id})",
                 ctx=app_id)


def get_group_name_from_mirror(app_id: str, group_openid: str) -> Optional[str]:
    """从 mirror.json 查询群名称，返回 None 表示不存在"""
    data = load_mirror()
    return data.get("groups", {}).get(app_id, {}).get(group_openid)


# ==================== 记忆 identifier 净化 ====================
# 记忆标识符（app_id / group_openid / user_openid）最终会被拼成文件名：
#   QUN_MEMORY_DIR / f"{group_id}.json"   等
# 因此它必须是**纯标识符**，绝不能含路径分隔符或 ".."。
#
# 为什么必须在这里拦：模型可以直接调 view_memory/add_memory 等工具，
# identifier 完全由模型（或 HTTP 调用方）控制。若不净化，传
# identifier="../config" 就会离开 qun_memory/ 目录、命中项目根目录的
# config.json（该文件含各机器人 APP_SECRET 与 LLM api_key 明文）；
# 写入方向（add/modify/delete/enable/disable）更会以 open(...,"w")
# 覆盖该文件。这是一条「一个字符串参数即可窃取或破坏全部凭证」的链路。
#
# 采用白名单而非黑名单：只放行字母/数字/下划线/连字符/点。
# QQ 的 openid 为十六进制大写串，APP_ID 为纯数字，均在此集合内；
# 同时禁止 "." 与 ".." 作为整体，避免目录自引用。
_MEMORY_IDENT_RE = re.compile(r'^[A-Za-z0-9_.-]{1,128}$')


class MemoryIdentifierError(ValueError):
    """identifier 不是合法标识符（含路径分隔符/穿越片段/超长等）。

    直接继承 ValueError（而非文件后部的 MemoryError_）——本类定义在模块
    前部、早于 MemoryError_ 的声明，继承它会在 import 时直接 NameError。
    而 ValueError 也能让工具/API 层现有的 except 分支照常捕获。
    """


def _validate_memory_identifier(identifier: str) -> str:
    """校验并归一 identifier，非法则抛 MemoryIdentifierError。

    返回 str(identifier).strip()，供调用方直接使用，避免各处重复 strip。
    """
    ident = str(identifier or "").strip()
    if not ident:
        raise MemoryIdentifierError("identifier 不能为空")
    if not _MEMORY_IDENT_RE.match(ident) or ident in (".", ".."):
        raise MemoryIdentifierError(
            f"identifier 非法：{ident!r}（只允许字母、数字、下划线、连字符和点）")
    return ident


# ==================== thread_key 净化 ====================
# thread_key 形如 "group_<openid>" / "c2c_<openid>"，最终被拼成
#   HISTORY_DIR / f"{thread_key}.json"
# 与记忆 identifier 是**两条独立**的拼接链路，因此必须各自校验 —— 早先只给
# identifier 加校验时，history 这条路仍是敞开的。
#
# 危险来源：API 的 /api/history 直接把 query 里的 target_id 交给
# append_message/save_history（只做过 .strip()），构造
# target_id="x/../../config" 会让路径解析到项目根目录的 config.json ——
# 该文件含全部机器人 APP_SECRET 与 LLM api_key 明文；写方向更会直接覆盖它。
#
# 这里同样用白名单：只放行字母/数字/下划线/连字符/点，并额外要求必须先有
# "group_" 或 "c2c_" 前缀，保证 key 的语义不会被伪造。
_THREAD_KEY_RE = re.compile(r'^(?:group|c2c)_[A-Za-z0-9_.-]{1,128}$')


def _validate_thread_key(thread_key: str) -> str:
    """校验并归一 thread_key，非法则抛 MemoryIdentifierError。

    合法形式：group_<id> / c2c_<id>，id 只含字母数字下划线连字符点。
    返回值可直接用于拼接文件名。
    """
    key = str(thread_key or "").strip()
    if not key:
        raise MemoryIdentifierError("thread_key 不能为空")
    if not _THREAD_KEY_RE.match(key):
        raise MemoryIdentifierError(
            f"thread_key 非法：{key!r}（应形如 group_<id> 或 c2c_<id>）")
    # 前缀已由正则保证，但仍排除 "group_." / "group_.." 这类目录自引用
    if key.split("_", 1)[1] in (".", ".."):
        raise MemoryIdentifierError(f"thread_key 非法：{key!r}")
    return key


# ==================== 整理结果的并发安全写回（M5） ====================
# 四个整理函数（群/私聊/机器人/全局）都是这个形状：
#     1. 读出 old_list
#     2. await call_ai_with_tools(...)      ← LLM 往返，可达数秒到数十秒
#     3. set_xxx_memory_list(new_list)      ← 整表覆盖
#
# 第 2 步是个长 await 窗口。期间若有人 add_memory（AI 工具或 API），
# 新条目会落盘；第 3 步却拿「整理前的快照 + LLM 结果」整表盖回去 ——
# 新增的记忆**静默消失**，而调用方早已收到「添加成功」。
#
# 这里不改「整理」的语义（整理本来就是要重写整个列表），只做一件事：
# 把「窗口期内新出现、且不在 old_list 里」的条目补回到结果末尾。
# 这样既不丢新记忆，也不影响 LLM 的精简效果。
def _merge_organized_result(old_list: List[str], new_list: List[str],
                            current_list: List[str]) -> List[str]:
    """把 await 窗口内新增的条目并回整理结果。

    old_list     : 整理开始时的快照（发给 LLM 的基准）
    new_list     : LLM 返回的整理结果
    current_list : 此刻磁盘上的真实内容

    返回：new_list + 窗口内新增项（按原顺序、去重）。
    """
    if not isinstance(new_list, list):
        return new_list
    old_set = set(old_list or [])
    new_set = set(new_list)
    added = []
    for c in (current_list or []):
        # 只收「窗口期内新出现」且「LLM 结果里没有」的条目，
        # 并按首次出现去重（current_list 自身可能含重复项）
        if c not in old_set and c not in new_set and c not in added:
            added.append(c)
    if not added:
        return new_list
    info(f"[记忆整理] await 窗口内新增 {len(added)} 条记忆，已并回整理结果", ctx=None)
    return list(new_list) + added


# ==================== 群记忆 ====================
def get_qun_memory(group_id: str) -> Dict:
    file_path = QUN_MEMORY_DIR / f"{_validate_memory_identifier(group_id)}.json"
    # 群记忆文件名只有群 openid，不含 app_id —— 反查一次，让日志能看出归属
    return safe_load_json(file_path, {"enabled": 1, "memory": []},
                          ctx=ctx_for_group(group_id))


def set_qun_memory(group_id: str, data: Dict):
    file_path = QUN_MEMORY_DIR / f"{_validate_memory_identifier(group_id)}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_qun_memory_list(group_id: str) -> List[str]:
    return get_qun_memory(group_id).get("memory", [])


def set_qun_memory_list(group_id: str, memory_list: List[str]):
    data = get_qun_memory(group_id)
    data["memory"] = memory_list
    set_qun_memory(group_id, data)


def add_qun_memory(group_id: str, text: str):
    mem = get_qun_memory_list(group_id)
    mem.append(text)
    set_qun_memory_list(group_id, mem)


def remove_qun_memory(group_id: str, index: int) -> bool:
    mem = get_qun_memory_list(group_id)
    if 0 <= index < len(mem):
        mem.pop(index)
        set_qun_memory_list(group_id, mem)
        return True
    return False


def replace_qun_memory(group_id: str, index: int, new_text: str) -> bool:
    mem = get_qun_memory_list(group_id)
    if 0 <= index < len(mem):
        mem[index] = new_text
        set_qun_memory_list(group_id, mem)
        return True
    return False


def clear_qun_memory(group_id: str):
    set_qun_memory_list(group_id, [])


def get_qun_memory_enabled(group_id: str) -> bool:
    return get_qun_memory(group_id).get("enabled", 1) == 1


def set_qun_memory_enabled(group_id: str, enabled: bool):
    data = get_qun_memory(group_id)
    data["enabled"] = 1 if enabled else 0
    set_qun_memory(group_id, data)


def transfer_memory_to_global(group_id: str, index: int, app_id: Optional[str] = None) -> bool:
    qun_mem = get_qun_memory_list(group_id)
    if 0 <= index < len(qun_mem):
        text = qun_mem.pop(index)
        set_qun_memory_list(group_id, qun_mem)
        add_global_memory(text, app_id)
        return True
    return False


def transfer_memory_to_qun(group_id: str, index: int, app_id: Optional[str] = None) -> bool:
    global_mem = get_global_memory(app_id)
    if 0 <= index < len(global_mem):
        text = global_mem.pop(index)
        set_global_memory(global_mem, app_id)
        add_qun_memory(group_id, text)
        return True
    return False


# ==================== 私聊记忆 ====================
def get_c2c_memory(user_id: str) -> Dict:
    file_path = C2C_MEMORY_DIR / f"{_validate_memory_identifier(user_id)}.json"
    # 同群记忆：文件名只有用户 openid，反查 app_id 补上归属
    return safe_load_json(file_path, {"enabled": 1, "memory": []},
                          ctx=ctx_for_user(user_id))


def set_c2c_memory(user_id: str, data: Dict):
    file_path = C2C_MEMORY_DIR / f"{_validate_memory_identifier(user_id)}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_c2c_memory_list(user_id: str) -> List[str]:
    return get_c2c_memory(user_id).get("memory", [])


def set_c2c_memory_list(user_id: str, memory_list: List[str]):
    data = get_c2c_memory(user_id)
    data["memory"] = memory_list
    set_c2c_memory(user_id, data)


def add_c2c_memory(user_id: str, text: str):
    mem = get_c2c_memory_list(user_id)
    mem.append(text)
    set_c2c_memory_list(user_id, mem)


def remove_c2c_memory(user_id: str, index: int) -> bool:
    mem = get_c2c_memory_list(user_id)
    if 0 <= index < len(mem):
        mem.pop(index)
        set_c2c_memory_list(user_id, mem)
        return True
    return False


def replace_c2c_memory(user_id: str, index: int, new_text: str) -> bool:
    mem = get_c2c_memory_list(user_id)
    if 0 <= index < len(mem):
        mem[index] = new_text
        set_c2c_memory_list(user_id, mem)
        return True
    return False


def clear_c2c_memory(user_id: str):
    set_c2c_memory_list(user_id, [])


def get_c2c_memory_enabled(user_id: str) -> bool:
    return get_c2c_memory(user_id).get("enabled", 1) == 1


def set_c2c_memory_enabled(user_id: str, enabled: bool):
    data = get_c2c_memory(user_id)
    data["enabled"] = 1 if enabled else 0
    set_c2c_memory(user_id, data)


# ==================== 机器人记忆 ====================
def get_bot_memory(app_id: str) -> Dict:
    file_path = BOT_MEMORY_DIR / f"{_validate_memory_identifier(app_id)}.json"
    return safe_load_json(file_path, {"enabled": 1, "memory": []},
                          ctx=LogCtx(app_id=app_id) if app_id else None)


def set_bot_memory(app_id: str, data: Dict):
    file_path = BOT_MEMORY_DIR / f"{_validate_memory_identifier(app_id)}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_bot_memory_list(app_id: str) -> List[str]:
    return get_bot_memory(app_id).get("memory", [])


def set_bot_memory_list(app_id: str, memory_list: List[str]):
    data = get_bot_memory(app_id)
    data["memory"] = memory_list
    set_bot_memory(app_id, data)


def add_bot_memory(app_id: str, text: str):
    mem = get_bot_memory_list(app_id)
    mem.append(text)
    set_bot_memory_list(app_id, mem)


def remove_bot_memory(app_id: str, index: int) -> bool:
    mem = get_bot_memory_list(app_id)
    if 0 <= index < len(mem):
        mem.pop(index)
        set_bot_memory_list(app_id, mem)
        return True
    return False


def replace_bot_memory(app_id: str, index: int, new_text: str) -> bool:
    mem = get_bot_memory_list(app_id)
    if 0 <= index < len(mem):
        mem[index] = new_text
        set_bot_memory_list(app_id, mem)
        return True
    return False


def clear_bot_memory(app_id: str):
    set_bot_memory_list(app_id, [])


def get_bot_memory_enabled(app_id: str) -> bool:
    return get_bot_memory(app_id).get("enabled", 1) == 1


def set_bot_memory_enabled(app_id: str, enabled: bool):
    data = get_bot_memory(app_id)
    data["enabled"] = 1 if enabled else 0
    set_bot_memory(app_id, data)


# ==================== 聊天记录 ====================
# 记录结构（结构化存储，展示用字段独立保存）：
#   {
#     "role":     "user" | "assistant" | "system" | "tool",
#     "content":  正文（不含时间戳、不含 "昵称(id): " 前缀）,
#     "ts":       "2026-09-15 22:09"（北京时间，独立字段）,
#     "username": 发送者昵称（user 消息才有）,
#     "user_id":  发送者 openid（user 消息才有）,
#     "revoked":  true 表示已撤回（正文保持不变，状态独立记录）,
#     "is_hide":  1 表示该条已隐藏、不参与「发给 AI」的上下文（默认不写，视为 0）,
#     ... 以及 msg_id / msg_idx / ref_msg_idx / is_summary / is_markdown / is_wakeup / media_url
#   }
#
# is_hide 与 revoked 完全独立，两者互不影响：
#   revoked —— 「对方撤回了这条消息」这一客观事实，由撤回 API/工具写入；
#              发给 AI 时保留该条并加 [已撤回] 前缀，让模型知道发生过什么。
#   is_hide —— 「不要把这条喂给模型」这一人工筛选意图（界面上的「隐藏」）；
#              发给 AI 时整条剔除，模型完全看不到它。
#   一条消息可以已撤回但仍参与上下文，也可以没撤回却被隐藏，反之亦然。
#   因此不要把它们合并成一个字段，也不要在任一处互相推导。
#
# 为什么默认不写 is_hide（缺省视为 0）：
#   历史文件里已有大量记录，若在写入时统一补 0 需要迁移全部文件；
#   读侧一律用 .get("is_hide") 判真，缺字段即等于 0，旧数据天然兼容。
#   取消隐藏时同样把字段删掉而不是写 0，保持「没隐藏过」的记录与初始状态一致。
#
# 为什么不把时间戳和昵称拼进 content：
#   旧实现把三者拼成一个字符串（"[ts]昵称(id): 正文"），导致任何一处想取
#   单独的字段都只能靠正则反解 —— 前端要剥 [ts]、后端要按 ": " 切昵称、
#   撤回时还要在时间戳前面插 "[已撤回]" 把格式搞得更乱。
#   现在三者各占一个字段，只有「发给 AI」这一步才拼回可读行
#   （见 render_history_for_ai），API 原样返回 json，前端直接取字段。
_TS_FMT = "%Y-%m-%d %H:%M"
# 旧格式的行首时间戳/用户前缀：(?s) 让 . 匹配换行，保证多行正文也能整体处理
_LEGACY_TS_RE = re.compile(r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?)\]')
_LEGACY_USER_RE = re.compile(r'^([^:\n]{1,60}?)\(([^)\n]{4,})\):\s?')


def _now_ts() -> str:
    """当前北京时间字符串（YYYY-MM-DD HH:MM）。"""
    return datetime.now(timezone(timedelta(hours=8))).strftime(_TS_FMT)


def split_legacy_content(content: str) -> Dict[str, str]:
    """把旧格式 content（"[ts]昵称(id): 正文"）拆成结构化字段。

    仅用于兼容改造之前写入的历史文件 —— 新写入的记录一律结构化保存，
    不需要也不应该走这里。拆不出的部分原样留在正文里，绝不丢内容。
    """
    text = str(content or "")
    ts = ""
    m = _LEGACY_TS_RE.match(text)
    if m:
        ts = m.group(1)[:16]          # 统一截到分钟，去掉可能的秒
        text = text[m.end():]
    username = ""
    user_id = ""
    um = _LEGACY_USER_RE.match(text)
    if um:
        username = um.group(1).strip()
        user_id = um.group(2).strip()
        text = text[um.end():]
    return {"ts": ts, "username": username, "user_id": user_id, "content": text}


def normalize_history_msg(msg: Dict) -> Dict:
    """把一条历史记录归一为结构化格式（就地不改原对象，返回新 dict）。

    已经是新格式（带 ts 字段）的原样返回；旧格式则拆出 ts/username/user_id。
    读取入口统一调用它，这样新旧历史混存时上层代码只需处理一种结构。
    """
    if not isinstance(msg, dict):
        return msg
    if "ts" in msg or "username" in msg or "user_id" in msg:
        return msg
    raw = msg.get("content", "")
    # 只有 user 消息才可能带 "昵称(id): " 前缀；assistant/system/tool 不拆
    if msg.get("role") != "user":
        m = _LEGACY_TS_RE.match(str(raw or ""))
        if not m:
            return msg
        out = dict(msg)
        out["ts"] = m.group(1)[:16]
        out["content"] = str(raw)[m.end():]
        return out
    parts = split_legacy_content(raw)
    out = dict(msg)
    out["content"] = parts["content"]
    if parts["ts"]:
        out["ts"] = parts["ts"]
    if parts["username"]:
        out["username"] = parts["username"]
    if parts["user_id"]:
        out["user_id"] = parts["user_id"]
    return out


def is_message_hidden(msg: Dict) -> bool:
    """该条是否已隐藏（is_hide=1，即不发给 AI）。缺字段视为 0（未隐藏）。"""
    if not isinstance(msg, dict):
        return False
    return bool(msg.get("is_hide"))


def render_history_for_ai(msg: Dict) -> Dict:
    """把一条历史记录渲染成「发给 AI」的 messages 条目。

    这里的拼接是**唯一**把 ts / username / user_id 重新合成文本的地方：
    模型需要的是可读的一行 "[时间]昵称(id): 正文"，而存储层保持结构化。
    同时剥离只服务于 API/撤回的元数据字段。

    被标记 is_hide=1 的记录返回 None，表示「这条不应出现在上下文里」，
    由调用方过滤掉（见 strip_message_meta / get_history）。
    不在这里直接抛异常或返回空对象：那会让模型看到一条空消息，
    而正确语义是「这条根本不存在」。
    """
    if not isinstance(msg, dict):
        return msg
    if is_message_hidden(msg):
        return None
    m = normalize_history_msg(msg)
    role = m.get("role", "")
    body = m.get("content", "")

    header = ""
    if m.get("ts"):
        header = f"[{m['ts']}]"
    if role == "user":
        name = m.get("username") or ""
        uid = m.get("user_id") or ""
        if uid:
            header += f"{name or '用户'}({uid}): "
        elif name:
            header += f"{name}: "

    # 撤回标记放在整行最前面（头部之前），避免被读成正文的一部分
    prefix = ""
    if m.get("revoked") and not str(body).startswith("[已撤回]"):
        prefix = "[已撤回]"

    out = {"role": role, "content": f"{prefix}{header}{body}"}
    # tool 消息的 tool_call_id 必须保留，否则回放上下文的工具调用链会断
    if m.get("tool_call_id"):
        out["tool_call_id"] = m["tool_call_id"]
    if m.get("tool_calls"):
        out["tool_calls"] = m["tool_calls"]
    return out


def load_history(thread_key: str) -> List[Dict]:
    hist_file = HISTORY_DIR / f"{_validate_thread_key(thread_key)}.json"
    # history 文件名是 thread_key（c2c_xxx / group_xxx），本身不含 app_id，
    # 反查一次补上归属，避免历史上出问题时看日志不知道是谁的会话
    raw = safe_load_json(hist_file, [], ctx=ctx_for_thread(thread_key))
    if not isinstance(raw, list):
        return []
    # 读取即归一：旧格式记录在此拆出 ts/username/user_id，上层只见一种结构
    return [normalize_history_msg(m) for m in raw]


def save_history(thread_key: str, hist: List[Dict]):
    hist_file = HISTORY_DIR / f"{_validate_thread_key(thread_key)}.json"
    with open(hist_file, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)


def append_message(thread_key: str, role: str, content: str, is_summary: bool = False,
                   msg_id: Optional[str] = None, msg_idx: Optional[str] = None,
                   ref_msg_idx: Optional[str] = None, is_markdown: bool = False,
                   is_wakeup: bool = False, media_url: Optional[str] = None,
                   username: Optional[str] = None, user_id: Optional[str] = None,
                   ts: Optional[str] = None):
    """追加一条聊天记录。

    时间戳/用户名/用户ID 各自作为独立字段保存，不再拼进 content；
    发给 AI 时由 render_history_for_ai 统一拼回可读行。
    ts 不传则取当前北京时间（历史回放/导入时可显式指定）。
    """
    hist = load_history(thread_key)
    msg = {"role": role, "content": content, "ts": ts or _now_ts()}
    if username:
        msg["username"] = username
    if user_id:
        msg["user_id"] = user_id
    if msg_id:
        msg["msg_id"] = msg_id
    if msg_idx:
        msg["msg_idx"] = msg_idx
    if ref_msg_idx:
        msg["ref_msg_idx"] = ref_msg_idx
    if is_summary:
        msg["is_summary"] = True
    if is_markdown:
        msg["is_markdown"] = 1      # Markdown 消息标记（API 返回用；发给 AI 前会剥离）
    if is_wakeup:
        msg["is_wakeup"] = 1        # 唤醒消息标记（API 返回用；发给 AI 前会剥离）
    if media_url:
        msg["media_url"] = media_url  # 媒体原始 URL（API 返回用；发给 AI 前会剥离）
    hist.append(msg)
    save_history(thread_key, hist)

    last_summary_idx = -1
    for i, m in enumerate(hist):
        if m.get("is_summary"):
            last_summary_idx = i
    msg_count = len(hist) - (last_summary_idx + 1)
    threshold = get_compress_threshold()
    if msg_count > threshold and not hist[-1].get("is_summary"):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 延迟导入避免循环依赖
                from ai import generate_and_insert_summary
                # 用 spawn_background 而不是裸 create_task：
                #   1) 裸调用不持有返回值，任务可能被 GC 回收；
                #   2) 失败时异常只落 stderr，不进日志。
                # 摘要生成失败会表现为「记忆压缩永远不发生」，正是需要
                # 在日志里一眼看到的那类问题（参见 group 记忆整理曾 100% 静默失效）。
                spawn_background(generate_and_insert_summary(thread_key),
                                 f"summary-{thread_key}", ctx_for_thread(thread_key))
        except Exception as e:
            # 原先这里是裸 `except Exception: pass` —— 全项目唯一的静默 pass。
            # 事件循环不可用 / 延迟导入失败都属于「摘要不会生成」的确定性故障，
            # 必须留下痕迹，否则无从判断记忆为何不再压缩。
            warn(f"[摘要] 无法派发摘要生成任务（{thread_key}）：{type(e).__name__}: {e}",
                 ctx=ctx_for_thread(thread_key))


def strip_message_meta(msg: Dict) -> Dict:
    """把一条历史记录渲染为「发给 AI」的 messages 条目。

    保留此函数名以兼容既有调用点；实际工作已交给 render_history_for_ai
    （它会剥离 msg_id / msg_idx / ref_msg_idx / is_markdown / is_wakeup /
    media_url / username / user_id / ts 等仅服务于 API 与展示的字段，
    并把 ts 与用户名拼回 content）。

    对 is_hide=1 的记录返回 None（filter_hidden_for_ai 会据此剔除）。
    """
    return render_history_for_ai(msg)


def filter_hidden_for_ai(msgs: List[Dict]) -> List[Dict]:
    """渲染一批历史记录并剔除 is_hide=1（已隐藏）的条目。

    这是「发给 AI」的统一出口：调用方不必自己判断 is_hide，
    拿到的一定是可直接塞进 messages 的列表。
    """
    out: List[Dict] = []
    for m in msgs or []:
        item = render_history_for_ai(m)
        # render 返回 None 有两种可能：本条已隐藏；或输入本身是 None
        if item is not None:
            out.append(item)
    return out


def parse_history_ts(msg: Dict) -> Optional[datetime]:
    """从记录中解析发送时间（北京时间），解析不出返回 None。

    优先用结构化的 ts 字段；旧记录（ts 缺失）由 normalize_history_msg 在
    读取时补上，因此这里基本只需处理结构化路径 —— 不再从正文里正则抠时间戳。
    """
    m = normalize_history_msg(msg)
    raw = m.get("ts")
    if not raw:
        return None
    bj_tz = timezone(timedelta(hours=8))
    s = str(raw).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=bj_tz)
        except ValueError:
            continue
    # 兜底：ISO 形式（含 T 分隔、带时区）
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=bj_tz)
    except ValueError:
        return None


def mark_message_revoked(thread_key: str, msg_id_to_revoke: str) -> bool:
    """
    在聊天记录中标记指定 msg_id 的消息为已撤回。

    撤回是独立的状态字段（revoked=true），不再修改 content ——
    旧实现往正文前面插 "[已撤回]" 会把时间戳挤到中间，
    导致前端/后端所有按 "[ts]" 行首解析的逻辑失配。
    返回 True 表示找到并标记成功，False 表示未找到。
    """
    if not thread_key or not msg_id_to_revoke:
        return False
    try:
        hist = load_history(thread_key)
        found = False
        for msg in hist:
            if msg.get("msg_id") == msg_id_to_revoke and not msg.get("revoked"):
                msg["revoked"] = True
                found = True
                # 不 break，继续标记所有匹配的（可能多条相同 msg_id 的情况）
        if found:
            save_history(thread_key, hist)
        return found
    except Exception:
        return False


def set_message_hidden(thread_key: str, msg_id: str, is_hide: bool = True) -> int:
    """把指定 msg_id 的消息隐藏 / 取消隐藏（is_hide=1 / 0）。

    返回被改动的条数（0 表示没找到该 msg_id，或值本来就已经是目标值）。

    与 mark_message_revoked 的关系：**完全独立**。本函数只动 is_hide，
    不碰 revoked；撤回状态由撤回 API/工具单独维护，两者可任意组合。
    这里也不像撤回那样跳过「已隐藏」的条目 —— 隐藏是可反复开关的。

    写入约定：只写 1，取消隐藏时**移除字段**。这样「未隐藏」的记录与其
    从没被点过的记录在文件里完全一致，不会因为一次误点就永久多出一个
    is_hide:0。
    """
    if not thread_key or not msg_id:
        return 0
    try:
        hist = load_history(thread_key)
        changed = 0
        for msg in hist:
            if msg.get("msg_id") == msg_id:
                # 与 mark_message_revoked 一样不 break：同一 msg_id 可能有多条
                if is_hide:
                    if not msg.get("is_hide"):
                        changed += 1
                    msg["is_hide"] = 1
                else:
                    # 取消隐藏时**删掉**字段而不是写 0：
                    # 与 append_message 的「默认不写」保持一致，否则一次误点
                    # 就会给这条记录永久留下 is_hide:0，白白膨胀历史文件
                    # （含长 msg_idx 的记录尤其明显）。
                    if "is_hide" in msg:
                        if msg.get("is_hide"):
                            changed += 1
                        del msg["is_hide"]
        if changed:
            save_history(thread_key, hist)
        return changed
    except Exception:
        return 0


def get_history(thread_key: str, limit: int = None) -> List[Dict]:
    """
    获取聊天历史，只按条数限制截取，不进行字符数截断。
    limit 默认为 CONTEXT_LIMIT（来自 config）。

    返回**已剔除 is_hide=1 条目**的列表：本函数的调用方都是「要发给 AI」的场景
    （见 ai.py / msg.py），在源头就过滤掉，避免每个调用点各自记得处理。
    需要含已隐藏条目的原始记录时用 load_history()。
    """
    if limit is None:
        limit = get_context_limit()
    hist = load_history(thread_key)
    if not hist:
        return []
    # is_hide 的条目在截断前就剔除：否则 limit 条里可能有一半是被隐藏的，
    # 真正进上下文的条数会少于预期（模型看到的上下文被无声缩短）。
    hist = [m for m in hist if not is_message_hidden(m)]
    if not hist:
        return []
    # 定位最后一个摘要的位置，从摘要之后开始取
    last_summary_idx = -1
    for i, msg in enumerate(hist):
        if msg.get("is_summary"):
            last_summary_idx = i
    start_idx = last_summary_idx if last_summary_idx != -1 else 0
    result = hist[start_idx:]
    if len(result) > limit:
        result = result[-limit:]
    return result


def get_recent_history(thread_key: str, limit: int) -> List[Dict]:
    """取最近若干条非摘要历史，同样剔除 is_hide=1 的条目（用于群聊判定上下文）。"""
    hist = load_history(thread_key)
    non_summary = [msg for msg in hist
                   if not msg.get("is_summary") and not is_message_hidden(msg)]
    return non_summary[-limit:]


def history_len(thread_key: str) -> int:
    """当前历史条数，用作「本轮任务起点」的标记（见 get_sent_messages_since）。

    用条数而不是时间戳：同一秒内可能写入多条，时间戳无法定位边界；
    条数天然单调递增，只要期间没有裁剪历史就足够可靠。

    ★ 读取失败时**不能返回 0**：0 是合法且极小的一条数，会被下游当作
      「本轮起点在第 0 条」，于是 get_sent_messages_since(thread_key, 0)
      把整段历史里所有带 msg_id 的助手消息都当成「本轮已发送」灌进系统提示，
      模型会以为自己已经说过大量内容而不再回复。
      这里改为记一条 error 并返回当前已知的最大安全值（历史读不出时用 0
      仍有同样风险，故退化为「不注入已发送清单」——由调用方按 None 处理）。
      返回 None 表示「未知」，调用方 generate_reply 会跳过该清单。
    """
    try:
        return len(load_history(thread_key))
    except Exception as e:
        error(f"[历史长度] 读取 {thread_key} 失败，本轮不注入「已发送消息」清单: {e}",
              ctx=ctx_for_thread(thread_key) if thread_key else None)
        return None


def get_sent_messages_since(thread_key: str, since: int) -> List[Dict]:
    """取 since 之后**真正发出去过**的机器人消息（供打断恢复时告知模型）。

    只认 msg_id 非空的 assistant 记录 —— 这是「已发送」的唯一可靠凭证：
      - 工具循环里模型产出的 content 只是「打算说的话」，本轮结束时才真正发送
        （见 ai.py 的发送段），所以打断时那些文本根本没发出去；
      - 只有经 send_message / 发送工具 发出去的消息才会带 msg_id 写入历史。
    因此按 msg_id 判断，才能避免把「没发出去的话」错报成已发送，
    否则模型会以为自己已经说过，反而漏掉该说的内容。

    ★ M11：since 是**历史下标**，而下标会被「摘要插入」改变 ——
      ai.py 生成摘要时会在历史中间 insert 一条 is_summary 消息，其后所有
      消息下标 +1。若本轮的 task_start 是在摘要插入前记下的，摘要一插入，
      这个下标就指向了**更早**的位置，于是 get_sent_messages_since 会把
      上一轮甚至更早的机器人消息也算进「本轮已发送」→ 模型误以为说过，
      从而漏回复。
      这里加一层锚点校正：若 since 处（或其紧邻）正好是摘要消息，说明
      下标已因插入而偏移，向下跳过摘要带来的位移，取到真正的边界。
    """
    out: List[Dict] = []
    try:
        hist = load_history(thread_key)
    except Exception:
        return out
    idx = _adjust_index_after_summary_insert(hist, int(since))
    for m in hist[max(0, idx):]:
        if m.get("role") != "assistant":
            continue
        if not m.get("msg_id"):
            continue                      # 没发出去的（含工具循环中间产物）
        if m.get("is_hide"):
            continue                      # 已隐藏的：模型本来就不该看到
        content = str(m.get("content") or "").strip()
        if not content:
            continue
        out.append({"content": content, "ts": m.get("ts") or ""})
    return out


def _adjust_index_after_summary_insert(hist: List[Dict], idx: int) -> int:
    """校正因「摘要插入到中间」而偏移的历史下标（M11）。

    摘要生成会把一条 is_summary 消息 insert 到历史中部（ai.py 的
    insert_pos = len(hist) - 10 附近），导致其后的消息下标整体 +1。
    而 task_start 是在摘要出现**之前**记下的旧下标，插入后它指向的位置
    会往前偏，把更早的消息纳入范围。

    校正方法：统计 [0, idx) 区间内、且位于「当前最后一个摘要之后」的
    摘要条数 —— 这些都是插入点在本轮起点之前、把起点往后推的位移量，
    补偿回去即可。

    这是保守估计：宁可比真实起点略早（多带出几条旧消息）也不漏掉
    本轮真正发过的消息吗？—— 不，反过来。多带出旧消息会让模型误以为
    已经说过（漏回复），所以这里选择**对齐到最近的摘要边界之后**，
    确保不会把摘要之前的内容算进本轮。
    """
    if idx <= 0:
        return idx
    n = len(hist)
    if n == 0:
        return 0
    if idx >= n:
        return n
    # 摘要插入位移的检测要**双向**看：
    #   - 向后看（idx 本身及紧邻之前）：起点正好落在摘要上，或摘要就在
    #     起点前一条 —— 这是「插入点刚好压在起点上」的形态。
    #   - 向前看（idx 紧邻之后）：摘要就在起点后一条 —— 摘要被 insert 到
    #     起点的位置，把原来的起点内容往后挤了一位（ai.py 的 insert_pos
    #     常取 len(hist)-10，很容易正好等于本轮 task_start）。
    # 两种形态都会让起点相对真实边界前移，统一推到该摘要之后。
    summary_idx = -1
    for i in (idx, idx - 1, idx + 1):
        if 0 <= i < n and hist[i].get("is_summary"):
            summary_idx = i
            break
    if summary_idx != -1:
        return min(summary_idx + 1, n)
    return idx


# ==================== 媒体缓存 ====================
def get_media_cache_path(cache_key: str) -> Path:
    return MEDIA_CACHE_DIR / f"{cache_key}.json"


def get_cached_media_summary(cache_key: str) -> Optional[str]:
    path = get_media_cache_path(cache_key)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            return data.get("summary")
        return None
    except Exception:
        return None


def set_cached_media(
    cache_key: str,
    summary: str,
    media_type: str,
    filename: str,
    url: str,
    height: int = 0,
    width: int = 0
):
    data = {
        "md5": cache_key,
        "media_type": media_type,
        "filename": filename,
        "url": url,
        "height": height,
        "width": width,
        "summary": summary
    }
    path = get_media_cache_path(cache_key)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_media_cache_key(media_type: str, filename: str, height: int = 0, width: int = 0) -> str:
    raw = f"{media_type}_{filename}_{height}x{width}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def get_url_cache_key(url: str) -> str:
    return hashlib.md5(url.encode('utf-8')).hexdigest()


# ==================== 记忆相关性计算 ====================
def compute_similarity(text1: str, text2: str) -> float:
    words1 = set(re.findall(r'\w+', text1.lower()))
    words2 = set(re.findall(r'\w+', text2.lower()))
    if not words1 or not words2:
        return 0.0
    intersection = len(words1 & words2)
    union = len(words1 | words2)
    return intersection / union if union > 0 else 0.0


# ==================== 记忆整理（公共：工具调用结果提取） ====================
# 四处记忆整理（全局 / 群 / 机器人 / 私聊）原先各自写着同一段：
#     tool_calls = ai_msg.get("tool_calls", [])
#     if not tool_calls: raise ValueError("AI 未调用 organize_memory 工具")
#     args = json.loads(tool_calls[0]["function"]["arguments"])
#     new_list = args.get("memories", [])
# 这段写法把三个假设焊死在调用点：
#   1. 只取第 0 个工具调用 —— 模型一次返回多个调用（或把 organize_memory
#      放在后面）时会被静默丢弃；
#   2. 不校验工具名 —— 模型调了别的工具也照吞它的 arguments；
#   3. arguments 一定是 JSON 字符串 —— 模型直接给对象、或带尾逗号时抛异常。
# 统一收敛到这里：按名字查找、兼容字符串/对象两种 arguments、失败给出明确原因。
def _extract_memories_from_ai_msg(ai_msg: Dict, tool_name: str = "organize_memory") -> List[str]:
    """从 AI 返回中取出 organize_memory 工具提交的记忆列表。

    成功返回 List[str]（可能为空列表）；未按约定调用工具、arguments 非法、
    或 memories 字段缺失/类型不对时抛 ValueError，由调用方统一重试。
    """
    tool_calls = (ai_msg or {}).get("tool_calls") or []

    # 按工具名挑选，而不是盲目取 [0]：模型可能一次返回多个调用，
    # 也可能先调别的工具再调 organize_memory。
    target = None
    for tc in tool_calls:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") or {}
        if fn.get("name") == tool_name:
            target = fn
            break
    if target is None:
        raise ValueError(f"AI 未调用 {tool_name} 工具")

    raw_args = target.get("arguments")
    # arguments 规范上是 JSON 字符串，但模型有时直接给对象/给空值。
    if isinstance(raw_args, dict):
        args = raw_args
    else:
        try:
            args = json.loads(raw_args) if raw_args else {}
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"{tool_name} 的 arguments 不是合法 JSON: {str(e)[:100]}")
    if not isinstance(args, dict):
        raise ValueError(f"{tool_name} 的 arguments 不是 JSON 对象（{type(args).__name__}）")

    memories = args.get("memories")
    if not isinstance(memories, list) or not memories:
        raise ValueError("整理后无有效记忆")
    # 逐条归一为字符串：模型偶尔会把数字/对象塞进数组，
    # 落到记忆文件里会变成非字符串条目，后续拼接提示词时报错。
    out = []
    for m in memories:
        if m is None:
            continue
        text = m if isinstance(m, str) else str(m)
        if text.strip():
            out.append(text)
    if not out:
        raise ValueError("整理后无有效记忆")
    return out


# ==================== 记忆整理（全局） ====================
_is_organizing_global: Dict[str, bool] = {}


async def check_and_organize_global_memory(app_id: Optional[str] = None):
    key = app_id if app_id else "shared"
    if _is_organizing_global.get(key, False):
        return
    mem = get_global_memory(app_id)
    if len(mem) > 15:
        _is_organizing_global[key] = True
        try:
            await organize_global_memory(app_id)
        finally:
            _is_organizing_global[key] = False


async def organize_global_memory(app_id: Optional[str] = None, retries: int = 3):
    from ai import call_ai_with_tools  # 延迟导入避免循环依赖
    organize_tool = {
        "type": "function",
        "function": {
            "name": "organize_memory",
            "description": "提交整理精简后的记忆列表",
            "parameters": {
                "type": "object",
                "properties": {
                    "memories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "整理精简后的记忆列表，每一条记忆是一个字符串。如果涉及用户，必须包含用户名(QQ号)的格式"
                    }
                },
                "required": ["memories"]
            }
        }
    }
    for attempt in range(retries):
        try:
            old_list = get_global_memory(app_id)
            if len(old_list) <= 15:
                return
            if len(old_list) > 50:
                old_list = old_list[:50] + [f"... 还有 {len(old_list)-50} 条记忆未显示"]
            system_msg = "你是一个记忆整理助手，负责精简和合并全局记忆列表。"
            user_msg = (
                "当前记忆列表如下（每条记忆是一个字符串）：\n"
                f"{json.dumps(old_list, ensure_ascii=False, indent=2)}\n\n"
                "请调用 organize_memory 工具提交整理精简后的记忆列表。\n"
                "要求：保留重要信息，合并相似内容，删除过期/无关信息。\n"
                "如果记忆涉及用户，必须包含用户名(QQ号)的格式。"
            )
            ai_msg = await call_ai_with_tools(
                [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
                "judge",
                [organize_tool],
                temperature=0.3
            , ctx=app_id)
            new_list = _extract_memories_from_ai_msg(ai_msg)
            timestamp = time.strftime("%Y%m%d_%H%M")
            suffix = f"_{app_id}" if app_id else ""
            old_file = BASE_DIR / f"old_memory{suffix}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
            # 并回 await 窗口内新增的记忆（M5）
            new_list = _merge_organized_result(
                old_list, new_list, get_global_memory(app_id))
            set_global_memory(new_list, app_id)
            info(f"[记忆整理] 完成（工具调用），原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}", ctx=app_id)
            return
        except Exception as e:
            error(f"[记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}", ctx=app_id)
            await asyncio.sleep(2)
    error("[记忆整理] 最终失败，保留原记忆", ctx=app_id)


# ==================== 记忆整理（群） ====================
_is_organizing_qun: Dict[str, bool] = {}


async def check_and_organize_qun_memory(group_id: str, app_id: Optional[str] = None):
    """检查群记忆是否超阈值，超了就触发整理。

    app_id: 所属机器人 APP_ID，由调用方（auto_manage_memory）显式传入，
            用于日志标识；缺失时日志显示 appid=? 而非猜测。
    """
    if _is_organizing_qun.get(group_id, False):
        return
    if not get_qun_memory_enabled(group_id):
        return
    mem = get_qun_memory_list(group_id)
    if len(mem) > 15:
        _is_organizing_qun[group_id] = True
        try:
            await organize_qun_memory(group_id, app_id=app_id)
        finally:
            _is_organizing_qun[group_id] = False


async def organize_qun_memory(group_id: str, retries: int = 3,
                             app_id: Optional[str] = None):
    """整理（压缩）群记忆。

    app_id: 所属机器人 APP_ID，由调用链显式传入，用于日志标识。
            不传时按群 openid 反查（查不到就留空，至少 thread 仍可定位）。
    """
    # 本函数所有日志统一使用的标识（thread_key = 群会话）
    ctx = LogCtx(app_id=app_id or find_app_id_by_group(group_id),
                 thread_key=f"group_{group_id}")
    from ai import call_ai_with_tools  # 延迟导入避免循环依赖
    organize_tool = {
        "type": "function",
        "function": {
            "name": "organize_memory",
            "description": "提交整理精简后的记忆列表",
            "parameters": {
                "type": "object",
                "properties": {
                    "memories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "整理精简后的记忆列表，每一条记忆是一个字符串。如果涉及用户，必须包含用户名(QQ号)的格式"
                    }
                },
                "required": ["memories"]
            }
        }
    }
    for attempt in range(retries):
        try:
            qun_data = get_qun_memory(group_id)
            old_list = qun_data.get("memory", [])
            if len(old_list) <= 15:
                return
            if len(old_list) > 50:
                old_list = old_list[:50] + [f"... 还有 {len(old_list)-50} 条记忆未显示"]
            system_msg = "你是一个记忆整理助手，负责精简和合并群聊长期记忆列表。"
            user_msg = (
                "当前群记忆列表如下（每条记忆是一个字符串）：\n"
                f"{json.dumps(old_list, ensure_ascii=False, indent=2)}\n\n"
                "请调用 organize_memory 工具提交整理精简后的记忆列表。\n"
                "要求：保留重要信息，合并相似内容，删除过期/无关信息。\n"
                "如果记忆涉及用户，必须包含用户名(QQ号)的格式。"
            )
            ai_msg = await call_ai_with_tools(
                [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
                "judge",
                [organize_tool],
                temperature=0.3
            , ctx=ctx)
            new_list = _extract_memories_from_ai_msg(ai_msg)
            timestamp = time.strftime("%Y%m%d_%H%M")
            backup_dir = BASE_DIR / "qun_memory_backup"
            backup_dir.mkdir(exist_ok=True)
            old_file = backup_dir / f"{group_id}_{timestamp}.json"
            # 备份的是「整理前的记忆列表」——与全局/机器人/私聊三条同级路径
            # （本文件内另外三处 json.dump(old_list) 调用）保持一致。
            # 原先此处误写 current_data（本函数内根本不存在的变量），NameError
            # 被下方 except 吞掉并重试 3 次后放弃，导致群记忆整理 100% 静默失效。
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
            # 写回前并回 await 窗口内新增的记忆（M5），否则会被整表覆盖丢掉
            new_list = _merge_organized_result(
                old_list, new_list, get_qun_memory_list(group_id))
            set_qun_memory_list(group_id, new_list)
            info(f"[群记忆整理] 群 {group_id} 整理完成（工具调用），原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}", ctx=ctx)
            return
        except Exception as e:
            error(f"[群记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}", ctx=ctx)
            await asyncio.sleep(2)
    error(f"[群记忆整理] 群 {group_id} 最终失败，保留原记忆", ctx=ctx)


# ==================== 记忆整理（机器人） ====================
_is_organizing_bot: Dict[str, bool] = {}


async def check_and_organize_bot_memory(app_id: str):
    if _is_organizing_bot.get(app_id, False):
        return
    if not get_bot_memory_enabled(app_id):
        return
    mem = get_bot_memory_list(app_id)
    if len(mem) > 15:
        _is_organizing_bot[app_id] = True
        try:
            await organize_bot_memory(app_id)
        finally:
            _is_organizing_bot[app_id] = False


async def organize_bot_memory(app_id: str, retries: int = 3):
    from ai import call_ai_with_tools
    organize_tool = {
        "type": "function",
        "function": {
            "name": "organize_memory",
            "description": "提交整理精简后的记忆列表",
            "parameters": {
                "type": "object",
                "properties": {
                    "memories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "整理精简后的记忆列表，每一条记忆是一个字符串。如果涉及用户，必须包含用户名(QQ号)的格式"
                    }
                },
                "required": ["memories"]
            }
        }
    }
    for attempt in range(retries):
        try:
            old_list = get_bot_memory_list(app_id)
            if len(old_list) <= 15:
                return
            if len(old_list) > 50:
                old_list = old_list[:50] + [f"... 还有 {len(old_list)-50} 条记忆未显示"]
            system_msg = "你是一个记忆整理助手，负责精简和合并机器人专属记忆列表。"
            user_msg = (
                "当前机器人记忆列表如下（每条记忆是一个字符串）：\n"
                f"{json.dumps(old_list, ensure_ascii=False, indent=2)}\n\n"
                "请调用 organize_memory 工具提交整理精简后的记忆列表。\n"
                "要求：保留重要信息，合并相似内容，删除过期/无关信息。\n"
                "如果记忆涉及用户，必须包含用户名(QQ号)的格式。"
            )
            ai_msg = await call_ai_with_tools(
                [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
                "judge",
                [organize_tool],
                temperature=0.3
            , ctx=app_id)
            new_list = _extract_memories_from_ai_msg(ai_msg)
            timestamp = time.strftime("%Y%m%d_%H%M")
            old_file = BASE_DIR / f"bot_memory_backup_{app_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
            # 并回 await 窗口内新增的记忆（M5）
            new_list = _merge_organized_result(
                old_list, new_list, get_bot_memory_list(app_id))
            set_bot_memory_list(app_id, new_list)
            info(f"[机器人记忆整理] 完成（工具调用），原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}", ctx=app_id)
            return
        except Exception as e:
            error(f"[机器人记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}", ctx=app_id)
            await asyncio.sleep(2)
    error(f"[机器人记忆整理] 最终失败，保留原记忆", ctx=app_id)


# ==================== 记忆整理（私聊） ====================
_is_organizing_c2c: Dict[str, bool] = {}


async def check_and_organize_c2c_memory(user_id: str, app_id: Optional[str] = None):
    """检查私聊记忆是否超阈值，超了就触发整理。

    app_id: 所属机器人 APP_ID，由调用方（auto_manage_memory）显式传入，
            用于日志标识；缺失时日志显示 appid=? 而非猜测。
    """
    if _is_organizing_c2c.get(user_id, False):
        return
    if not get_c2c_memory_enabled(user_id):
        return
    mem = get_c2c_memory_list(user_id)
    if len(mem) > 15:
        _is_organizing_c2c[user_id] = True
        try:
            await organize_c2c_memory(user_id, app_id=app_id)
        finally:
            _is_organizing_c2c[user_id] = False


async def organize_c2c_memory(user_id: str, retries: int = 3,
                             app_id: Optional[str] = None):
    """整理（压缩）私聊记忆。

    app_id: 所属机器人 APP_ID，由调用链显式传入，用于日志标识。
            不传时按用户 openid 反查（查不到就留空，至少 thread 仍可定位）。
    """
    # 本函数所有日志统一使用的标识（thread_key = 私聊会话）
    ctx = LogCtx(app_id=app_id or find_app_id_by_user(user_id),
                 thread_key=f"c2c_{user_id}")
    from ai import call_ai_with_tools
    organize_tool = {
        "type": "function",
        "function": {
            "name": "organize_memory",
            "description": "提交整理精简后的记忆列表",
            "parameters": {
                "type": "object",
                "properties": {
                    "memories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "整理精简后的记忆列表，每一条记忆是一个字符串。如果涉及用户，必须包含用户名(QQ号)的格式"
                    }
                },
                "required": ["memories"]
            }
        }
    }
    for attempt in range(retries):
        try:
            old_list = get_c2c_memory_list(user_id)
            if len(old_list) <= 15:
                return
            if len(old_list) > 50:
                old_list = old_list[:50] + [f"... 还有 {len(old_list)-50} 条记忆未显示"]
            system_msg = "你是一个记忆整理助手，负责精简和合并私聊长期记忆列表。"
            user_msg = (
                "当前私聊记忆列表如下（每条记忆是一个字符串）：\n"
                f"{json.dumps(old_list, ensure_ascii=False, indent=2)}\n\n"
                "请调用 organize_memory 工具提交整理精简后的记忆列表。\n"
                "要求：保留重要信息，合并相似内容，删除过期/无关信息。\n"
                "如果记忆涉及用户，必须包含用户名(QQ号)的格式。"
            )
            ai_msg = await call_ai_with_tools(
                [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
                "judge",
                [organize_tool],
                temperature=0.3
            , ctx=ctx)
            new_list = _extract_memories_from_ai_msg(ai_msg)
            timestamp = time.strftime("%Y%m%d_%H%M")
            old_file = BASE_DIR / f"c2c_memory_backup_{user_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
            # 并回 await 窗口内新增的记忆（M5）
            new_list = _merge_organized_result(
                old_list, new_list, get_c2c_memory_list(user_id))
            set_c2c_memory_list(user_id, new_list)
            info(f"[私聊记忆整理] 完成（工具调用），原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}", ctx=ctx)
            return
        except Exception as e:
            error(f"[私聊记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}", ctx=ctx)
            await asyncio.sleep(2)
    error(f"[私聊记忆整理] 最终失败，保留原记忆", ctx=ctx)


# ==================== 自动记忆管理 ====================
async def auto_manage_memory(thread_key: str, bot_client, **kwargs):
    """
    后台自动整理记忆（不涉及添加/修改/删除，这些由 AI 工具调用完成）。
    检查全局、机器人、群/私聊所有四个层级的记忆，超过阈值时触发压缩整理。
    """
    app_id = bot_client.app_id
    is_group = thread_key.startswith("group_")

    # —— 全局记忆 ——
    await check_and_organize_global_memory(app_id)
    # —— 机器人专属记忆 ——
    await check_and_organize_bot_memory(app_id)

    if is_group:
        group_id = thread_key.replace("group_", "")
        await check_and_organize_qun_memory(group_id, app_id=app_id)
    else:
        c2c_user_id = thread_key.replace("c2c_", "")
        await check_and_organize_c2c_memory(c2c_user_id, app_id=app_id)


# ==================== 记忆搜索 ====================
def search_memory(keywords: List[str], layer: str = "all",
                  app_id: Optional[str] = None,
                  group_id: Optional[str] = None,
                  c2c_user_id: Optional[str] = None) -> str:
    """
    跨层搜索记忆，返回格式化结果。
    keywords: 搜索关键词列表（多个关键词取并集，只要匹配任意一个即返回）
    layer:    "global"=全局记忆, "bot"=机器人记忆, "group"=群聊记忆,
              "c2c"=私聊记忆, "all"=全部（默认）
    app_id:   机器人 APP_ID（global/bot 层需要）
    group_id: 群 openid（group 层需要）
    c2c_user_id: 用户 openid（c2c 层需要）
    """
    results = []

    def _matches(text: str) -> bool:
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in keywords)

    # --- 全局记忆 ---
    if layer in ("global", "all"):
        if app_id is not None:
            mem_list = get_global_memory(app_id)
        else:
            mem_list = get_global_memory(None)
        matched = [(idx, m) for idx, m in enumerate(mem_list) if _matches(m)]
        if matched:
            results.append(f"【全局记忆】（共 {len(matched)} 条匹配）")
            for idx, m in matched:
                results.append(f"  [{idx}] {m}")
        else:
            results.append("【全局记忆】无匹配")

    # --- 机器人专属记忆 ---
    if layer in ("bot", "all") and app_id is not None:
        mem_list = get_bot_memory_list(app_id)
        matched = [(idx, m) for idx, m in enumerate(mem_list) if _matches(m)]
        if matched:
            results.append(f"【机器人专属记忆】（共 {len(matched)} 条匹配）")
            for idx, m in matched:
                results.append(f"  [{idx}] {m}")
        else:
            results.append("【机器人专属记忆】无匹配")

    # --- 群聊记忆 ---
    if layer in ("group", "all") and group_id is not None:
        mem_list = get_qun_memory_list(group_id)
        matched = [(idx, m) for idx, m in enumerate(mem_list) if _matches(m)]
        if matched:
            results.append(f"【群聊记忆】（共 {len(matched)} 条匹配）")
            for idx, m in matched:
                results.append(f"  [{idx}] {m}")
        else:
            results.append("【群聊记忆】无匹配")

    # --- 私聊记忆 ---
    if layer in ("c2c", "all") and c2c_user_id is not None:
        mem_list = get_c2c_memory_list(c2c_user_id)
        matched = [(idx, m) for idx, m in enumerate(mem_list) if _matches(m)]
        if matched:
            results.append(f"【私聊记忆】（共 {len(matched)} 条匹配）")
            for idx, m in matched:
                results.append(f"  [{idx}] {m}")
        else:
            results.append("【私聊记忆】无匹配")

    return "\n".join(results) if results else "（未指定任何记忆层级或标识符，无搜索结果）"


# ==================== 记忆管理统一入口（API / AI 工具共用） ====================
# 本段是「按级别操作记忆」的唯一业务实现，两端共用同一份函数对象：
#  1. AI 工具 execute_tool_call 的 view_memory / add_memory / modify_memory /
#     delete_memory / enable_memory / disable_memory（tool.py 导入）
#  2. api_server.py 的 /api/memory/* 接口
#
# 它垫在上面那些原子读写函数之上：
#      HTTP /api/memory/*  ──┐
#                            ├──> 本段（级别派发 / 禁用检查 / 批量语义）──> 原子读写 ──> json
#      AI 工具 *_memory    ──┘
#
# 为什么放在 memory.py 而不是 tool.py：这两端共用的是「记忆」这件事本身的语义
#   （级别怎么派发、禁用算不算错误、批量失败要不要回滚），属于本文件的职责；
#   放进 tool.py 会让 api_server 为了用记忆而反向依赖整个工具定义模块。
#   此前这段逻辑以 6 份副本的形式摊在工具执行分支里，现收敛到此处。
#
# 设计约束：本段**只返回结构化数据 / 抛异常，不产出任何中文文案**。
#   文案属于展示层，两端要求不同：
#     工具侧要说给模型听（"错误：群聊记忆当前已禁用，请先使用 enable_memory 启用"）
#     API 侧要放进 JSON（结构化 failed 数组 + 准确的 HTTP 状态码）
#   把文案焊死在底层，两边就无法共用逻辑了。

MEMORY_LEVELS = ("global", "bot", "group", "c2c")


class MemoryError_(Exception):
    """记忆操作的通用错误基类，便于调用方一并捕获。"""


class MemoryDisabledError(MemoryError_):
    """该级别记忆已被禁用 —— 添加操作应被拒绝。

    修改/删除**不检查**禁用状态：禁用只表示「不再注入系统提示」，
    记忆内容仍然保留，此时仍应允许整理（与改造前行为一致）。
    """

    def __init__(self, level: str, identifier: str):
        self.level = level
        self.identifier = identifier
        super().__init__(f"{level}({identifier}) 记忆已禁用")


class MemoryLevelError(MemoryError_):
    """未知的记忆级别。"""

    def __init__(self, level: str):
        self.level = level
        super().__init__(f"未知记忆级别 {level}")


def _norm_memory_level(level: Any) -> str:
    """归一记忆级别；未知级别抛 MemoryLevelError。"""
    lv = str(level or "").strip().lower()
    if lv not in MEMORY_LEVELS:
        raise MemoryLevelError(lv)
    return lv


def _memory_get(level: str, identifier: str) -> Tuple[List[str], bool]:
    """读取某级记忆，返回 (记忆列表, 是否启用)。

    派发表：把「级别 -> 本文件里那组读写函数」的映射收在一处。
    改造前这张表在 view_memory / add_memory / modify_memory / delete_memory /
    enable_memory / disable_memory 里各抄了一遍，共 6 份 —— 新增一个级别要改 6 处，
    漏改一处就是「A 工具能用、B 工具报未知级别」。
    注意 global 的函数把 app_id 放在参数末尾，与其余三级的顺序相反，
    由本函数统一抹平，调用方不必再记这个差异。
    """
    lv = _norm_memory_level(level)
    if lv == "global":
        return get_global_memory(identifier), get_global_memory_enabled(identifier)
    if lv == "bot":
        return get_bot_memory_list(identifier), get_bot_memory_enabled(identifier)
    if lv == "group":
        return get_qun_memory_list(identifier), get_qun_memory_enabled(identifier)
    return get_c2c_memory_list(identifier), get_c2c_memory_enabled(identifier)


def _memory_add(level: str, identifier: str, content: str) -> None:
    """向某级记忆追加一条。该级已禁用时抛 MemoryDisabledError。"""
    lv = _norm_memory_level(level)
    if not str(content or "").strip():
        raise MemoryError_("记忆内容不能为空")
    _, enabled = _memory_get(lv, identifier)
    if not enabled:
        raise MemoryDisabledError(lv, identifier)
    if lv == "global":
        add_global_memory(content, identifier)
    elif lv == "bot":
        add_bot_memory(identifier, content)
    elif lv == "group":
        add_qun_memory(identifier, content)
    else:
        add_c2c_memory(identifier, content)


def _memory_replace(level: str, identifier: str, index: Any, content: str) -> bool:
    """替换某级记忆的第 index 条，返回 False 表示索引越界。"""
    lv = _norm_memory_level(level)
    idx = _coerce_memory_index(index)
    if idx is None:
        return False
    if lv == "global":
        return replace_global_memory(idx, content, identifier)
    if lv == "bot":
        return replace_bot_memory(identifier, idx, content)
    if lv == "group":
        return replace_qun_memory(identifier, idx, content)
    return replace_c2c_memory(identifier, idx, content)


def _memory_remove(level: str, identifier: str, index: Any) -> bool:
    """删除某级记忆的第 index 条，返回 False 表示索引越界。"""
    lv = _norm_memory_level(level)
    idx = _coerce_memory_index(index)
    if idx is None:
        return False
    if lv == "global":
        return remove_global_memory(idx, identifier)
    if lv == "bot":
        return remove_bot_memory(identifier, idx)
    if lv == "group":
        return remove_qun_memory(identifier, idx)
    return remove_c2c_memory(identifier, idx)


def _memory_set_enabled(level: str, identifier: str, enabled: bool) -> None:
    """启用/禁用某级记忆。"""
    lv = _norm_memory_level(level)
    if lv == "global":
        set_global_memory_enabled(enabled, identifier)
    elif lv == "bot":
        set_bot_memory_enabled(identifier, enabled)
    elif lv == "group":
        set_qun_memory_enabled(identifier, enabled)
    else:
        set_c2c_memory_enabled(identifier, enabled)


def _coerce_memory_index(index: Any) -> Optional[int]:
    """把 index 归一为非负整数，非法返回 None（调用方据此判越界/参数错）。

    模型与 HTTP 调用方都可能把索引给成字符串（"2"）或负数；
    负索引在 list.pop 里是「从后往前数」的合法写法，但会静默删掉尾部的条目，
    与调用方「删除第 2 条」的意图不符，因此一律拒绝负数。
    """
    if index is None or isinstance(index, bool):
        return None
    try:
        idx = int(index)
    except (TypeError, ValueError):
        return None
    return idx if idx >= 0 else None


def _memory_batch(op: str, items: Any) -> Dict[str, Any]:
    """批量执行记忆操作 —— 批量的唯一实现，工具与 API 共用。

    op:    "add" | "update" | "delete"
    items: [{"level":..., "identifier":..., "content":..., "index":...}, ...]

    返回 {"success": [...], "failed": [{"index":i, "error":...}, ...]}，
    每项带原始下标，便于调用方把失败定位到具体是哪一条。

    语义：**逐条独立、部分失败不回滚**（与改造前工具侧行为一致）——
    批量里一条禁用/越界不应让其余几条白做。
    """
    success: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    if not isinstance(items, list):
        return {"success": success,
                "failed": [{"index": None, "error": "items 必须是数组"}]}

    for i, item in enumerate(items):
        if not isinstance(item, dict):
            failed.append({"index": i, "error": "条目必须是对象"})
            continue
        level = item.get("level", "")
        identifier = item.get("identifier", "")
        try:
            if not level or not identifier:
                raise MemoryError_("缺少 level 或 identifier")
            if op == "add":
                content = item.get("content", "")
                if not str(content or "").strip():
                    raise MemoryError_("缺少 content")
                _memory_add(level, identifier, content)
            elif op == "update":
                content = item.get("content", "")
                if not str(content or "").strip():
                    raise MemoryError_("缺少 content")
                if item.get("index") is None:
                    raise MemoryError_("缺少 index")
                if not _memory_replace(level, identifier, item.get("index"), content):
                    raise MemoryError_(f"索引 {item.get('index')} 越界或无效")
            elif op == "delete":
                if item.get("index") is None:
                    raise MemoryError_("缺少 index")
                if not _memory_remove(level, identifier, item.get("index")):
                    raise MemoryError_(f"索引 {item.get('index')} 越界或无效")
            else:
                raise MemoryError_(f"未知批量操作 {op}")
        except MemoryError_ as e:
            failed.append({"index": i, "level": str(level), "identifier": str(identifier),
                           "error": str(e)})
            continue
        except Exception as e:                      # 落盘失败等意外
            failed.append({"index": i, "level": str(level), "identifier": str(identifier),
                           "error": f"{type(e).__name__}: {e}"})
            continue
        success.append({"index": i, "level": str(level), "identifier": str(identifier)})

    return {"success": success, "failed": failed}


# ==================== 初始化：旧版 mirror / user_map 迁移 ====================
mirror_migrate_old()
user_map_migrate_old()