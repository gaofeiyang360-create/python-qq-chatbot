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

from log import info, warn, error, debug, LogCtx
from config import (
    safe_load_json, BASE_DIR, MEMORY_FILE, MIRROR_FILE, USER_MAP_FILE,
    HISTORY_DIR, MEDIA_CACHE_DIR, QUN_MEMORY_DIR, C2C_MEMORY_DIR, BOT_MEMORY_DIR,
    get_bot_isolate_flag, get_compress_threshold, get_context_limit,
)


# ==================== 全局记忆 ====================
def get_global_memory_file(app_id: Optional[str] = None) -> Path:
    if app_id:
        isolate = get_bot_isolate_flag(app_id)
        if isolate:
            return BASE_DIR / f"memory_{app_id}.json"
    return MEMORY_FILE


def load_memory(app_id: Optional[str] = None) -> Dict:
    file_path = get_global_memory_file(app_id)
    return safe_load_json(file_path, {"global_memory": [], "enabled": 1})


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
    """保存 user_map.json"""
    with open(USER_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
    with open(MIRROR_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


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
    """设置指定机器人的用户映射 dict"""
    data = load_mirror()
    if "users" not in data:
        data["users"] = {}
    data["users"][bot_appid] = mapping_dict
    save_mirror(data)


def get_user_name(qq_id: str, bot_appid: str) -> Optional[str]:
    """在指定机器人的映射中查找用户名称"""
    mapping = get_user_mapping(bot_appid)
    return mapping.get(qq_id)


def update_user_mapping(qq_id: str, username: str, bot_appid: str):
    """更新指定机器人的用户映射，不存在则添加"""
    if not qq_id or not username:
        return
    data = _ensure_bot_entry(bot_appid)
    if data["users"][bot_appid].get(qq_id) != username:
        data["users"][bot_appid][qq_id] = username
        save_mirror(data)


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
    """记录群ID和群名称到 mirror.json groups 字段"""
    if not app_id or not group_openid or not group_name:
        return
    data = _ensure_mirror_groups_entry(app_id)
    if data["groups"][app_id].get(group_openid) != group_name:
        data["groups"][app_id][group_openid] = group_name
        save_mirror(data)
        info(f"[Mirror] 记录群信息: {group_name} ({group_openid}) (app={app_id})", ctx=app_id)


def get_group_name_from_mirror(app_id: str, group_openid: str) -> Optional[str]:
    """从 mirror.json 查询群名称，返回 None 表示不存在"""
    data = load_mirror()
    return data.get("groups", {}).get(app_id, {}).get(group_openid)


# ==================== 群记忆 ====================
def get_qun_memory(group_id: str) -> Dict:
    file_path = QUN_MEMORY_DIR / f"{group_id}.json"
    return safe_load_json(file_path, {"enabled": 1, "memory": []})


def set_qun_memory(group_id: str, data: Dict):
    file_path = QUN_MEMORY_DIR / f"{group_id}.json"
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
    file_path = C2C_MEMORY_DIR / f"{user_id}.json"
    return safe_load_json(file_path, {"enabled": 1, "memory": []})


def set_c2c_memory(user_id: str, data: Dict):
    file_path = C2C_MEMORY_DIR / f"{user_id}.json"
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
    file_path = BOT_MEMORY_DIR / f"{app_id}.json"
    return safe_load_json(file_path, {"enabled": 1, "memory": []})


def set_bot_memory(app_id: str, data: Dict):
    file_path = BOT_MEMORY_DIR / f"{app_id}.json"
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
def load_history(thread_key: str) -> List[Dict]:
    hist_file = HISTORY_DIR / f"{thread_key}.json"
    return safe_load_json(hist_file, [])


def save_history(thread_key: str, hist: List[Dict]):
    hist_file = HISTORY_DIR / f"{thread_key}.json"
    with open(hist_file, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)


def append_message(thread_key: str, role: str, content: str, is_summary: bool = False,
                   msg_id: Optional[str] = None, msg_idx: Optional[str] = None,
                   ref_msg_idx: Optional[str] = None, is_markdown: bool = False,
                   is_wakeup: bool = False, media_url: Optional[str] = None):
    hist = load_history(thread_key)
    # 自动添加北京时间时间戳 [YYYY-MM-DD HH:MM]
    bj_tz = timezone(timedelta(hours=8))
    ts = datetime.now(bj_tz).strftime("%Y-%m-%d %H:%M")
    msg = {"role": role, "content": f"[{ts}]{content}"}
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
                asyncio.create_task(generate_and_insert_summary(thread_key))
        except Exception:
            pass


def strip_message_meta(msg: Dict) -> Dict:
    """返回移除了内部元数据字段的消息副本，用于发送给 AI。

    剥离的字段：msg_id / msg_idx / ref_msg_idx / revoked / is_markdown / is_wakeup / media_url
    （这些只服务于 API 查询与撤回逻辑，不该出现在喂给模型的上下文里）
    """
    cleaned = {k: v for k, v in msg.items()
               if k not in ("msg_id", "msg_idx", "ref_msg_idx", "revoked",
                            "is_markdown", "is_wakeup", "media_url")}
    return cleaned


def mark_message_revoked(thread_key: str, msg_id_to_revoke: str) -> bool:
    """
    在聊天记录中标记指定 msg_id 的消息为已撤回。
    在 content 中添加 "已撤回" 前缀，并设置 revoked=true。
    返回 True 表示找到并标记成功，False 表示未找到。
    """
    if not thread_key or not msg_id_to_revoke:
        return False
    try:
        hist = load_history(thread_key)
        found = False
        for msg in hist:
            if msg.get("msg_id") == msg_id_to_revoke and not msg.get("revoked"):
                raw = msg.get("content", "")
                if not raw.startswith("[已撤回]"):
                    # 格式: [2026-09-15 22:09]内容 → [已撤回][2026-09-15 22:09]内容
                    msg["content"] = "[已撤回]" + raw
                msg["revoked"] = True
                found = True
                # 不 break，继续标记所有匹配的（可能多条相同 msg_id 的情况）
        if found:
            save_history(thread_key, hist)
        return found
    except Exception:
        return False


def get_history(thread_key: str, limit: int = None) -> List[Dict]:
    """
    获取聊天历史，只按条数限制截取，不进行字符数截断。
    limit 默认为 CONTEXT_LIMIT（来自 config）。
    """
    if limit is None:
        limit = get_context_limit()
    hist = load_history(thread_key)
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
    hist = load_history(thread_key)
    non_summary = [msg for msg in hist if not msg.get("is_summary")]
    return non_summary[-limit:]


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
            tool_calls = ai_msg.get("tool_calls", [])
            if not tool_calls:
                raise ValueError("AI 未调用 organize_memory 工具")
            args = json.loads(tool_calls[0]["function"]["arguments"])
            new_list = args.get("memories", [])
            if not new_list or not isinstance(new_list, list):
                raise ValueError("整理后无有效记忆")
            timestamp = time.strftime("%Y%m%d_%H%M")
            suffix = f"_{app_id}" if app_id else ""
            old_file = BASE_DIR / f"old_memory{suffix}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
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
    """
    # 本函数所有日志统一使用的标识（thread_key = 群会话）
    ctx = LogCtx(app_id=app_id or "", thread_key=f"group_{group_id}")
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
            tool_calls = ai_msg.get("tool_calls", [])
            if not tool_calls:
                raise ValueError("AI 未调用 organize_memory 工具")
            args = json.loads(tool_calls[0]["function"]["arguments"])
            new_list = args.get("memories", [])
            if not new_list or not isinstance(new_list, list):
                raise ValueError("整理后无有效记忆")
            timestamp = time.strftime("%Y%m%d_%H%M")
            backup_dir = BASE_DIR / "qun_memory_backup"
            backup_dir.mkdir(exist_ok=True)
            old_file = backup_dir / f"{group_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
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
            tool_calls = ai_msg.get("tool_calls", [])
            if not tool_calls:
                raise ValueError("AI 未调用 organize_memory 工具")
            args = json.loads(tool_calls[0]["function"]["arguments"])
            new_list = args.get("memories", [])
            if not new_list or not isinstance(new_list, list):
                raise ValueError("整理后无有效记忆")
            timestamp = time.strftime("%Y%m%d_%H%M")
            old_file = BASE_DIR / f"bot_memory_backup_{app_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
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
    """
    # 本函数所有日志统一使用的标识（thread_key = 私聊会话）
    ctx = LogCtx(app_id=app_id or "", thread_key=f"c2c_{user_id}")
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
            tool_calls = ai_msg.get("tool_calls", [])
            if not tool_calls:
                raise ValueError("AI 未调用 organize_memory 工具")
            args = json.loads(tool_calls[0]["function"]["arguments"])
            new_list = args.get("memories", [])
            if not new_list or not isinstance(new_list, list):
                raise ValueError("整理后无有效记忆")
            timestamp = time.strftime("%Y%m%d_%H%M")
            old_file = BASE_DIR / f"c2c_memory_backup_{user_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
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


# ==================== 初始化：旧版 mirror / user_map 迁移 ====================
mirror_migrate_old()
user_map_migrate_old()