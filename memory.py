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

from log import info, warn, error, debug
from config import (
    safe_load_json, BASE_DIR, MEMORY_FILE, MIRROR_FILE, USER_MAP_FILE,
    HISTORY_DIR, MEDIA_CACHE_DIR, QUN_MEMORY_DIR, C2C_MEMORY_DIR, BOT_MEMORY_DIR,
    get_bot_isolate_flag, get_compress_threshold, get_context_limit,
    get_global_system_prompt, get_bot_system_prompt, get_bot_name,
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
# 格式: {"app_id": {"user": ["user_openid1", ...], "group": ["group_openid1", ...]}}
# 记录所有私聊过的用户ID和机器人加入过的群ID

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
        data[app_id] = {"user": [], "group": []}
    else:
        if "user" not in data[app_id]:
            data[app_id]["user"] = []
        if "group" not in data[app_id]:
            data[app_id]["group"] = []
    return data


def record_user_id(app_id: str, user_openid: str):
    """记录私聊用户ID（C2C）"""
    if not app_id or not user_openid:
        return
    data = _ensure_user_map_app_entry(app_id)
    if user_openid not in data[app_id]["user"]:
        data[app_id]["user"].append(user_openid)
        save_user_map(data)
        info(f"[UserMap] 记录私聊用户 {user_openid} (app={app_id})")


def record_group_id(app_id: str, group_openid: str):
    """记录群ID（Group）"""
    if not app_id or not group_openid:
        return
    data = _ensure_user_map_app_entry(app_id)
    if group_openid not in data[app_id]["group"]:
        data[app_id]["group"].append(group_openid)
        save_user_map(data)
        info(f"[UserMap] 记录群 {group_openid} (app={app_id})")

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
        info(f"旧版 mirror.json 已备份为 {backup_name.name}")
    except Exception as e:
        warn(f"备份旧版 mirror.json 失败: {e}")
        return

    # 创建新格式的空文件
    new_empty = {"users": {}}
    with open(MIRROR_FILE, "w", encoding="utf-8") as f:
        json.dump(new_empty, f, ensure_ascii=False, indent=2)
    info("已创建新版 mirror.json（格式: {\"users\": {\"bot_appid\": {...}}}）")


def load_mirror() -> Dict:
    return safe_load_json(MIRROR_FILE, {"users": {}})


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


def append_message(thread_key: str, role: str, content: str, is_summary: bool = False):
    hist = load_history(thread_key)
    # 自动添加北京时间时间戳 [YYYY-MM-DD HH:MM]
    bj_tz = timezone(timedelta(hours=8))
    ts = datetime.now(bj_tz).strftime("%Y-%m-%d %H:%M")
    msg = {"role": role, "content": f"[{ts}]{content}"}
    if is_summary:
        msg["is_summary"] = True
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
    from ai import call_ai  # 延迟导入避免循环依赖
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
                "请将上述记忆列表精简、合并，输出多行文本，每行一条精简后的记忆。\n"
                "要求：如果记忆涉及用户，必须包含用户名(QQ号)的格式。\n"
                "格式：第一行以【开头，最后一行以】结尾，中间每一行是一条记忆。\n"
                "只输出这种格式的文本，不要有其他内容。"
            )
            result = await call_ai(
                [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
                "main",
                temperature=0.3
            )
            lines = result.strip().split('\n')
            if not lines:
                raise ValueError("返回为空")
            first = lines[0].strip()
            last = lines[-1].strip()
            if not first.startswith('【') or not last.endswith('】'):
                raise ValueError("首行不以【开头或末行不以】结尾")
            new_list = []
            for line in lines[1:-1]:
                line = line.strip()
                if line:
                    new_list.append(line)
            if not new_list:
                raise ValueError("整理后无有效记忆")
            current_list = get_global_memory(app_id)
            if len(current_list) > len(old_list):
                warn("[记忆整理] 整理期间有新记忆添加，放弃本次整理")
                return
            timestamp = time.strftime("%Y%m%d_%H%M")
            suffix = f"_{app_id}" if app_id else ""
            old_file = BASE_DIR / f"old_memory{suffix}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
            set_global_memory(new_list, app_id)
            info(f"[记忆整理] 完成，原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}")
            return
        except Exception as e:
            error(f"[记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}")
            await asyncio.sleep(2)
    error("[记忆整理] 最终失败，保留原记忆")


# ==================== 记忆整理（群） ====================
_is_organizing_qun: Dict[str, bool] = {}


async def check_and_organize_qun_memory(group_id: str):
    if _is_organizing_qun.get(group_id, False):
        return
    if not get_qun_memory_enabled(group_id):
        return
    mem = get_qun_memory_list(group_id)
    if len(mem) > 15:
        _is_organizing_qun[group_id] = True
        try:
            await organize_qun_memory(group_id)
        finally:
            _is_organizing_qun[group_id] = False


async def organize_qun_memory(group_id: str, retries: int = 3):
    from ai import call_ai  # 延迟导入避免循环依赖
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
                "请将上述记忆列表精简、合并，输出多行文本，每行一条精简后的记忆。\n"
                "要求：如果记忆涉及用户，必须包含用户名(QQ号)的格式。\n"
                "格式：第一行以【开头，最后一行以】结尾，中间每一行是一条记忆。\n"
                "只输出这种格式的文本，不要有其他内容。"
            )
            result = await call_ai(
                [{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
                "main",
                temperature=0.3
            )
            lines = result.strip().split('\n')
            if not lines:
                raise ValueError("返回为空")
            first = lines[0].strip()
            last = lines[-1].strip()
            if not first.startswith('【') or not last.endswith('】'):
                raise ValueError("首行不以【开头或末行不以】结尾")
            new_list = []
            for line in lines[1:-1]:
                line = line.strip()
                if line:
                    new_list.append(line)
            if not new_list:
                raise ValueError("整理后无有效记忆")
            current_data = get_qun_memory(group_id)
            current_list = current_data.get("memory", [])
            if len(current_list) > len(old_list):
                warn(f"[群记忆整理] 整理期间有新记忆添加，放弃本次整理")
                return
            timestamp = time.strftime("%Y%m%d_%H%M")
            backup_dir = BASE_DIR / "qun_memory_backup"
            backup_dir.mkdir(exist_ok=True)
            old_file = backup_dir / f"{group_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
            set_qun_memory_list(group_id, new_list)
            info(f"[群记忆整理] 群 {group_id} 整理完成，原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}")
            return
        except Exception as e:
            error(f"[群记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}")
            await asyncio.sleep(2)
    error(f"[群记忆整理] 群 {group_id} 最终失败，保留原记忆")


# ==================== 自动记忆管理 ====================
async def auto_manage_memory(thread_key: str, user_message: str, reply: str,
                             context_hist: List[Dict], raw_json: str, bot_client):
    app_id = bot_client.app_id
    is_group = thread_key.startswith("group_")
    is_c2c = thread_key.startswith("c2c_")

    if is_group:
        group_id = thread_key.replace("group_", "")
        await update_memory_by_ai(
            app_id=app_id, identifier=group_id,
            user_message=user_message, reply=reply, context_hist=context_hist, raw_json=raw_json,
            mem_type="群", mem_label="群长期记忆",
            add_func=add_qun_memory, remove_func=remove_qun_memory,
            replace_func=replace_qun_memory, clear_func=clear_qun_memory,
            get_mem_func=get_qun_memory_list, set_mem_func=set_qun_memory_list,
            enable_func=lambda: set_qun_memory_enabled(group_id, True),
            disable_func=lambda: set_qun_memory_enabled(group_id, False),
            get_enabled_func=lambda: get_qun_memory_enabled(group_id)
        )
    elif is_c2c:
        user_id = thread_key.replace("c2c_", "")
        await update_memory_by_ai(
            app_id=app_id, identifier=user_id,
            user_message=user_message, reply=reply, context_hist=context_hist, raw_json=raw_json,
            mem_type="私聊", mem_label="私聊长期记忆",
            add_func=add_c2c_memory, remove_func=remove_c2c_memory,
            replace_func=replace_c2c_memory, clear_func=clear_c2c_memory,
            get_mem_func=get_c2c_memory_list, set_mem_func=set_c2c_memory_list,
            enable_func=lambda: set_c2c_memory_enabled(user_id, True),
            disable_func=lambda: set_c2c_memory_enabled(user_id, False),
            get_enabled_func=lambda: get_c2c_memory_enabled(user_id)
        )

    await update_memory_by_ai(
        app_id=app_id, identifier=app_id,
        user_message=user_message, reply=reply, context_hist=context_hist, raw_json=raw_json,
        mem_type="机器人", mem_label=f"机器人({app_id})专属记忆",
        add_func=add_bot_memory, remove_func=remove_bot_memory,
        replace_func=replace_bot_memory, clear_func=clear_bot_memory,
        get_mem_func=get_bot_memory_list, set_mem_func=set_bot_memory_list,
        enable_func=lambda: set_bot_memory_enabled(app_id, True),
        disable_func=lambda: set_bot_memory_enabled(app_id, False),
        get_enabled_func=lambda: get_bot_memory_enabled(app_id),
        is_bot=True
    )

    await update_memory_by_ai(
        app_id=app_id, identifier=app_id,
        user_message=user_message, reply=reply, context_hist=context_hist, raw_json=raw_json,
        mem_type="全局", mem_label="全局长期记忆",
        add_func=add_global_memory, remove_func=remove_global_memory,
        replace_func=replace_global_memory, clear_func=clear_global_memory,
        get_mem_func=get_global_memory, set_mem_func=set_global_memory,
        enable_func=lambda: set_global_memory_enabled(True, app_id),
        disable_func=lambda: set_global_memory_enabled(False, app_id),
        get_enabled_func=lambda: get_global_memory_enabled(app_id),
        is_global=True,
        extra_args=(app_id,)
    )

    if is_group:
        await check_and_organize_qun_memory(group_id)
    else:
        await check_and_organize_global_memory(app_id)


async def update_memory_by_ai(app_id: str, identifier, user_message, reply, context_hist, raw_json,
                              mem_type, mem_label,
                              add_func, remove_func, replace_func, clear_func,
                              get_mem_func, set_mem_func,
                              enable_func, disable_func, get_enabled_func,
                              is_global=False, is_bot=False, extra_args=()):
    from ai import call_ai  # 延迟导入避免循环依赖

    if is_global:
        app_id_for_mem = extra_args[0] if extra_args else None
        current_mem = get_mem_func(app_id_for_mem)
    elif is_bot:
        current_mem = get_mem_func(identifier)
    else:
        current_mem = get_mem_func(identifier)

    enabled_status = "启用" if get_enabled_func() else "禁用"
    mem_text = "\n".join([f"- {item}" for item in current_mem]) if current_mem else "（无）"

    above_text = ""
    for msg in context_hist[-5:]:
        role = msg.get("role", "")
        content = msg.get("content", "")
        if role == "user":
            above_text += f"用户: {content}\n"
        elif role == "assistant":
            above_text += f"机器人: {content}\n"
        elif msg.get("is_summary"):
            above_text += f"摘要: {content}\n"
        elif role == "system":
            above_text += f"系统: {content}\n"
        else:
            above_text += f"{role}: {content}\n"

    global_sys = get_global_system_prompt()
    bot_sys = get_bot_system_prompt(app_id)
    combined_sys = f"{global_sys}\n{bot_sys}" if bot_sys else global_sys

    prompt = (
        f"你是一个记忆管理助手，负责根据对话内容更新机器人的{mem_label}。\n"
        f"机器人的人设和系统提示如下：\n{combined_sys}\n\n"
        f"机器人的名字是 {get_bot_name()}。\n"
        f"当前{mem_label}的启用状态是：{enabled_status}。\n"
        f"当前{mem_label}列表如下：\n"
        f"{mem_text}\n\n"
        "最新的用户消息是：\n"
        f"{user_message}\n\n"
        "机器人的回复是：\n"
        f"{reply}\n\n"
        "对话上文（最近5条）：\n"
        f"{above_text}\n\n"
        "重要规则：如果记忆内容涉及某位用户，必须在该用户的用户名后附上其QQ号（从消息的author.id或member_openid中获取），格式如“用户名(QQ号) 是...”。\n"
        "例如：\"张三(1234567) 是管理员\" 而不是 \"张三是管理员\"。\n"
        "请分析上述内容，判断是否需要更新记忆或调整启用状态。如果需要，输出一个JSON指令，格式如下：\n"
        "- 添加记忆：{ \"action\": \"add\", \"content\": \"要添加的记忆内容（必须包含用户QQ号）\" }\n"
        "- 删除记忆（按索引）：{ \"action\": \"delete\", \"index\": 0 }  （索引从0开始）\n"
        "- 替换记忆：{ \"action\": \"replace\", \"index\": 0, \"content\": \"新内容（必须包含用户QQ号）\" }\n"
        "- 清空所有记忆：{ \"action\": \"clear\" }\n"
        "- 启用记忆：{ \"action\": \"enable\" }\n"
        "- 禁用记忆：{ \"action\": \"disable\" }\n"
        "- 不操作：{ \"action\": \"none\" }\n"
        "注意：禁用记忆会使其在后续回复中不被使用，但记忆内容仍会保留。启用记忆会恢复使用。只有在确实必要时才禁用，避免影响正常对话。\n"
        "只输出JSON，不要有其他内容。"
    )

    try:
        result = await call_ai([{"role": "user", "content": prompt}], "judge", temperature=0.2)
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', result, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = result.strip()
        json_str = re.sub(r'{{', '{', json_str)
        json_str = re.sub(r'}}', '}', json_str)
        json_str = re.sub(r',\s*\}', '}', json_str)
        json_str = re.sub(r',\s*\]', ']', json_str)
        data = json.loads(json_str)
        action = data.get("action")

        if action == "add":
            content = data.get("content")
            if content:
                if is_global:
                    app_id_for_mem = extra_args[0] if extra_args else None
                    add_func(content, app_id_for_mem)
                elif is_bot:
                    add_func(identifier, content)
                else:
                    add_func(identifier, content)
                info(f"[记忆] {mem_type} 添加: {content}")
        elif action == "delete":
            idx = data.get("index")
            if idx is not None:
                if is_global:
                    app_id_for_mem = extra_args[0] if extra_args else None
                    removed = remove_func(idx, app_id_for_mem)
                elif is_bot:
                    removed = remove_func(identifier, idx)
                else:
                    removed = remove_func(identifier, idx)
                if removed:
                    info(f"[记忆] {mem_type} 删除索引 {idx}")
        elif action == "replace":
            idx = data.get("index")
            content = data.get("content")
            if idx is not None and content:
                if is_global:
                    app_id_for_mem = extra_args[0] if extra_args else None
                    replaced = replace_func(idx, content, app_id_for_mem)
                elif is_bot:
                    replaced = replace_func(identifier, idx, content)
                else:
                    replaced = replace_func(identifier, idx, content)
                if replaced:
                    info(f"[记忆] {mem_type} 替换索引 {idx} 为: {content}")
        elif action == "clear":
            if is_global:
                app_id_for_mem = extra_args[0] if extra_args else None
                clear_func(app_id_for_mem)
            elif is_bot:
                clear_func(identifier)
            else:
                clear_func(identifier)
            info(f"[记忆] {mem_type} 清空所有记忆")
        elif action == "enable":
            enable_func()
            info(f"[记忆] {mem_type} 已启用")
        elif action == "disable":
            disable_func()
            info(f"[记忆] {mem_type} 已禁用")
        else:
            info(f"[记忆] {mem_type} 无操作")
    except json.JSONDecodeError as e:
        error(f"[记忆管理 JSON解析错误] {e}, 原始内容: {result[:200]}")
    except Exception as e:
        info(f"[记忆管理错误] {e}")


# ==================== 初始化：旧版 mirror 迁移 ====================
mirror_migrate_old()