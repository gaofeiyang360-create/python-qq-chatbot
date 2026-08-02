#CODE BY 飞扬nb
import sys
import asyncio
import json
import time
import re
import base64
import hashlib
import requests
import websockets
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

# ==================== 配置文件加载 ====================
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

CONFIG_FILE = BASE_DIR / "config.json"
DEFAULT_CONFIG = {
    "bots": [
        {
            "APP_ID": "YOUAPPID",
            "APP_SECRET": "YOUSECRET"
        }
    ],
    "AI_API_URL": "YOUBASEURL",
    "AI_API_KEY": "YOUKEY",
    "AI_MAX_MSG_LEN": 1500,
    "MODEL_JUDGE": "deepseek-v4-flash",
    "MODEL_MAIN": "deepseek-v4-pro",
    "MODEL_VISION": "qwen-vl-max",
    "BOT_NAME": "AI缺省名称",
    "CONTEXT_LIMIT": 15,
    "JUDGE_CONTEXT_LIMIT": 10,
    "COOLDOWN_SECONDS": 2,
    "COMPRESS_THRESHOLD": 25,
    "MAX_WORKERS": 20,
    "SYSTEM_PROMPT": "自定义提示词"  # 新增：自定义系统提示词字段
}

def load_config():
    if not CONFIG_FILE.exists():
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        print("配置文件 config.json 已生成，请编辑后重新运行。")
        sys.exit(0)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()

# ==================== 配置赋值 ====================
BOTS = config.get("bots", [])
if not BOTS:
    print("错误：配置文件中没有机器人信息，请添加 'bots' 数组。")
    sys.exit(1)

AI_API_URL = config["AI_API_URL"]
AI_API_KEY = config["AI_API_KEY"]
AI_MAX_MSG_LEN = config["AI_MAX_MSG_LEN"]
MODEL_JUDGE = config["MODEL_JUDGE"]
MODEL_MAIN = config["MODEL_MAIN"]
MODEL_VISION = config["MODEL_VISION"]
BOT_NAME = config.get("BOT_NAME", "灵泽集AI")
CONTEXT_LIMIT = config["CONTEXT_LIMIT"]
JUDGE_CONTEXT_LIMIT = config["JUDGE_CONTEXT_LIMIT"]
COOLDOWN_SECONDS = config["COOLDOWN_SECONDS"]
COMPRESS_THRESHOLD = config["COMPRESS_THRESHOLD"]
MAX_WORKERS = config.get("MAX_WORKERS", 20)
SYSTEM_PROMPT = config.get("SYSTEM_PROMPT", "you are a helpful ai")  # 读取自定义提示词

# ==================== 自定义线程池 ====================
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

# ==================== 数据存储路径 ====================
MEMORY_FILE = BASE_DIR / "memory.json"
HISTORY_DIR = BASE_DIR / "history"
MEDIA_CACHE_DIR = BASE_DIR / "media_cache"
QUN_MEMORY_DIR = BASE_DIR / "qun_memory"
HISTORY_DIR.mkdir(exist_ok=True)
MEDIA_CACHE_DIR.mkdir(exist_ok=True)
QUN_MEMORY_DIR.mkdir(exist_ok=True)

# ==================== 迁移旧数据 ====================
def migrate_old_data():
    old_file = BASE_DIR / "threads.json"
    if not old_file.exists():
        return
    if MEMORY_FILE.exists() and any(HISTORY_DIR.glob("*.json")):
        return
    print("[迁移] 检测到旧的 threads.json，正在迁移...")
    with open(old_file, "r", encoding="utf-8") as f:
        old = json.load(f)
    memory_data = {
        "global_memory": old.get("global_memory", []),
        "user_mapping": old.get("user_mapping", {})
    }
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(memory_data, f, ensure_ascii=False, indent=2)
    for key, val in old.items():
        if key in ("global_memory", "user_mapping"):
            continue
        if isinstance(val, dict) and "history" in val:
            hist_file = HISTORY_DIR / f"{key}.json"
            with open(hist_file, "w", encoding="utf-8") as f:
                json.dump(val["history"], f, ensure_ascii=False, indent=2)
    backup = old_file.with_suffix(".json.bak")
    old_file.rename(backup)
    print(f"[迁移] 完成，旧文件备份为 {backup}")

migrate_old_data()

# ==================== 全局数据读写 ====================
def load_memory() -> Dict:
    if MEMORY_FILE.exists():
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"global_memory": [], "user_mapping": {}}

def save_memory(data: Dict):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_global_memory() -> List[str]:
    return load_memory().get("global_memory", [])

def set_global_memory(memory_list: List[str]):
    data = load_memory()
    data["global_memory"] = memory_list
    save_memory(data)

def add_global_memory(text: str):
    mem = get_global_memory()
    mem.append(text)
    set_global_memory(mem)

def remove_global_memory(index: int) -> bool:
    mem = get_global_memory()
    if 0 <= index < len(mem):
        mem.pop(index)
        set_global_memory(mem)
        return True
    return False

def replace_global_memory(index: int, new_text: str) -> bool:
    mem = get_global_memory()
    if 0 <= index < len(mem):
        mem[index] = new_text
        set_global_memory(mem)
        return True
    return False

def clear_global_memory():
    set_global_memory([])

def get_user_mapping() -> Dict:
    return load_memory().get("user_mapping", {})

def set_user_mapping(mapping: Dict):
    data = load_memory()
    data["user_mapping"] = mapping
    save_memory(data)

def get_user_name(qq_id: str) -> Optional[str]:
    return get_user_mapping().get(qq_id)

def update_user_mapping(qq_id: str, username: str):
    if not qq_id or not username:
        return
    mapping = get_user_mapping()
    if mapping.get(qq_id) != username:
        mapping[qq_id] = username
        set_user_mapping(mapping)

# ---------- 群记忆 ----------
def get_qun_memory(group_id: str) -> Dict:
    file_path = QUN_MEMORY_DIR / f"{group_id}.json"
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"enabled": 1, "memory": []}

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

def enable_qun_memory(group_id: str):
    data = get_qun_memory(group_id)
    data["enabled"] = 1
    set_qun_memory(group_id, data)

def disable_qun_memory(group_id: str):
    data = get_qun_memory(group_id)
    data["enabled"] = 0
    set_qun_memory(group_id, data)

def transfer_memory_to_global(group_id: str, index: int) -> bool:
    qun_mem = get_qun_memory_list(group_id)
    if 0 <= index < len(qun_mem):
        text = qun_mem.pop(index)
        set_qun_memory_list(group_id, qun_mem)
        add_global_memory(text)
        return True
    return False

def transfer_memory_to_qun(group_id: str, index: int) -> bool:
    global_mem = get_global_memory()
    if 0 <= index < len(global_mem):
        text = global_mem.pop(index)
        set_global_memory(global_mem)
        add_qun_memory(group_id, text)
        return True
    return False

# ---------- 聊天记录 ----------
def load_history(thread_key: str) -> List[Dict]:
    hist_file = HISTORY_DIR / f"{thread_key}.json"
    if hist_file.exists():
        with open(hist_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_history(thread_key: str, hist: List[Dict]):
    hist_file = HISTORY_DIR / f"{thread_key}.json"
    with open(hist_file, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)

def append_message(thread_key: str, role: str, content: str, is_summary: bool = False):
    hist = load_history(thread_key)
    msg = {"role": role, "content": content}
    if is_summary:
        msg["is_summary"] = True
    hist.append(msg)
    save_history(thread_key, hist)
    last_summary_idx = -1
    for i, m in enumerate(hist):
        if m.get("is_summary"):
            last_summary_idx = i
    msg_count = len(hist) - (last_summary_idx + 1)
    if msg_count > COMPRESS_THRESHOLD and not hist[-1].get("is_summary"):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(generate_and_insert_summary(thread_key))
        except:
            pass

def get_history(thread_key: str, max_len: int = AI_MAX_MSG_LEN, limit: int = CONTEXT_LIMIT) -> List[Dict]:
    hist = load_history(thread_key)
    if not hist:
        return []
    last_summary_idx = -1
    for i, msg in enumerate(hist):
        if msg.get("is_summary"):
            last_summary_idx = i
    start_idx = last_summary_idx if last_summary_idx != -1 else 0
    result = hist[start_idx:]
    if len(result) > limit:
        result = result[-limit:]
    total_len = sum(len(msg.get("content", "")) for msg in result)
    while total_len > max_len and len(result) > 2:
        if result and result[0].get("is_summary"):
            if len(result) > 2:
                removed = result.pop(1)
                total_len -= len(removed.get("content", ""))
            else:
                break
        else:
            removed = result.pop(0)
            total_len -= len(removed.get("content", ""))
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
    except:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except:
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

def media_has_cache(media_type: str, filename: str, height: int = 0, width: int = 0) -> bool:
    cache_key = get_media_cache_key(media_type, filename, height, width)
    return get_media_cache_path(cache_key).exists()

def get_media_cache_key(media_type: str, filename: str, height: int = 0, width: int = 0) -> str:
    raw = f"{media_type}_{filename}_{height}x{width}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def get_url_cache_key(url: str) -> str:
    return hashlib.md5(url.encode('utf-8')).hexdigest()

# ==================== 识别函数 ====================
async def recognize_media_by_url(media_url: str, filename: str = "媒体", media_type: str = None) -> str:
    cache_key = get_url_cache_key(media_url)
    cached = get_cached_media_summary(cache_key)
    if cached:
        print(f"[媒体缓存] URL 命中: {media_url[:50]}...")
        return cached

    if not media_type:
        ext = Path(filename).suffix.lower()
        if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
            media_type = "image"
        elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
            media_type = "video"
        else:
            url_path = urlparse(media_url).path
            ext2 = Path(url_path).suffix.lower()
            if ext2 in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
                media_type = "image"
            elif ext2 in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
                media_type = "video"

    if not media_type:
        error_msg = "（内容无法获取/为未知文件格式）"
        set_cached_media(cache_key, error_msg, "unknown", filename, media_url)
        return error_msg

    print(f"[媒体识别] 通过 URL 识别: {media_type}, {filename}")

    try:
        result = await recognize_media(media_type, media_url, filename)
        set_cached_media(cache_key, result, media_type, filename, media_url, 0, 0)
        return result
    except Exception as e:
        print(f"[媒体识别] URL 识别失败: {e}")
        error_msg = f"（媒体识别失败: {e}）"
        set_cached_media(cache_key, error_msg, media_type, filename, media_url)
        return error_msg

async def recognize_media(media_type: str, media_url: str, filename: str = "媒体", height: int = 0, width: int = 0) -> str:
    cache_key = get_media_cache_key(media_type, filename, height, width)
    cached = get_cached_media_summary(cache_key)
    if cached:
        print(f"[媒体缓存] 命中: {filename}")
        return cached

    if media_type == "image":
        content_parts = [
            {"type": "text", "text": "请描述这张图片的内容，生成一段100-400字的摘要，重点描述图片中的主要对象、场景、颜色、构图或可能表达的情感。"},
            {"type": "image_url", "image_url": {"url": media_url}}
        ]
    elif media_type == "video":
        content_parts = [
            {"type": "text", "text": "请描述这个视频的内容，生成一段100-400字的摘要，重点描述视频中的主要场景、动作、颜色或可能表达的情感。"},
            {"type": "video_url", "video_url": {"url": media_url}}
        ]
    else:
        return "（不支持的媒体类型）"

    try:
        messages = [{"role": "user", "content": content_parts}]
        result = await call_ai(messages, MODEL_VISION, temperature=0.3)
        if result and "（AI 未返回有效内容）" not in result:
            set_cached_media(cache_key, result, media_type, filename, media_url, height, width)
            return result
        else:
            error_msg = "（媒体识别失败，模型未返回有效内容）"
            set_cached_media(cache_key, error_msg, media_type, filename, media_url, height, width)
            return error_msg
    except Exception as e:
        print(f"[媒体识别] 识别失败 {media_url}: {e}")
        error_msg = f"（媒体识别失败: {e}）"
        set_cached_media(cache_key, error_msg, media_type, filename, media_url, height, width)
        return error_msg

# ==================== 聊天记录转发媒体解析 ====================
def parse_forwarded_chatlog(text: str) -> Tuple[str, List[Dict]]:
    lines = text.split('\n')
    result_lines = []
    media_list = []
    placeholder_index = 0

    for line in lines:
        if '[附件' in line and ('类型:' in line or 'URL:' in line):
            filename_match = re.search(r'文件名[:：]\s*([^\s]+)', line)
            url_match = re.search(r'URL[:：]\s*(https?://[^\s]+)', line)
            type_match = re.search(r'类型[:：]\s*([^\s,，]+)', line)
            size_match = re.search(r'大小[:：]\s*([^\s]+)', line)
            dimension_match = re.search(r'尺寸[:：]\s*(\d+)x(\d+)', line)

            if url_match:
                url = url_match.group(1)
                filename = filename_match.group(1) if filename_match else "未知文件"
                raw_type = type_match.group(1) if type_match else ""
                media_type = "unknown"
                # 优先根据文件扩展名判断是否为图片或视频
                ext = Path(filename).suffix.lower()
                if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
                    media_type = "image"
                elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
                    media_type = "video"
                else:
                    # 若扩展名无法判断，再根据文本描述
                    if "图片" in raw_type:
                        media_type = "image"
                    elif "视频" in raw_type:
                        media_type = "video"
                    elif "文件" in raw_type:
                        # 文本文件判断
                        if ext in TEXT_FILE_EXTS:
                            media_type = "text_file"
                        else:
                            media_type = "binary_file"
                # 注意：如果 raw_type 本身就写了“图片”或“视频”，上面已经覆盖了

                size_info = size_match.group(1) if size_match else ""
                height = int(dimension_match.group(2)) if dimension_match else 0
                width = int(dimension_match.group(1)) if dimension_match else 0

                media_list.append({
                    'url': url,
                    'filename': filename,
                    'type': media_type,
                    'size': size_info,
                    'height': height,
                    'width': width,
                    'raw_type': raw_type
                })
                placeholder = f"[MEDIA_PLACEHOLDER_{placeholder_index}]"
                result_lines.append(placeholder)
                placeholder_index += 1
            else:
                result_lines.append(line)
        else:
            result_lines.append(line)

    return '\n'.join(result_lines), media_list

# ---------- 工具函数 ----------
TEXT_FILE_EXTS = {
    '.txt', '.py', '.html', '.htm', '.css', '.js', '.json', '.xml', '.md', '.csv',
    '.log', '.sh', '.bat', '.ps1', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
    '.c', '.cpp', '.java', '.go', '.rs', '.swift', '.kt', '.rb', '.php', '.lua', '.pl',
    '.r', '.m', '.vbs', '.jsx', '.tsx', '.ts', '.svelte', '.vue', '.sql'
}

def is_text_file(filename: str) -> bool:
    ext = Path(filename).suffix.lower()
    return ext in TEXT_FILE_EXTS

# 判断是否为图片或视频扩展名
def is_image_or_video_file(filename: str) -> bool:
    ext = Path(filename).suffix.lower()
    return ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']

def get_media_type_from_filename(filename: str) -> Optional[str]:
    ext = Path(filename).suffix.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
        return "image"
    elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
        return "video"
    return None

def get_thread_key(msg_type: str, user_id: str = None, group_id: str = None) -> str:
    if msg_type == "c2c":
        return f"c2c_{user_id}"
    else:
        return f"group_{group_id}"

def get_display_name(author_id: str, username: str = "") -> str:
    if username and username.strip():
        return username.strip()
    mapped = get_user_name(author_id)
    if mapped:
        return mapped
    return "用户"

def decode_face_tags(text: str) -> str:
    pattern1 = r'<faceType=(\d+),faceId="[^"]*",ext="([^"]*)"\s*/?>'
    pattern2 = r'<faceType=(\d+),ext="([^"]*)"\s*/?>'
    def replacer(match):
        ext_b64 = match.group(2)
        try:
            json_str = base64.b64decode(ext_b64).decode('utf-8')
            data = json.loads(json_str)
            text = data.get("text", "")
            if text:
                return f"[表情包：{text}]"
            else:
                return match.group(0)
        except:
            return match.group(0)
    result = re.sub(pattern1, replacer, text)
    result = re.sub(pattern2, replacer, result)
    return result

def replace_mentions_with_names(text: str, mentions: List[Dict]) -> str:
    def replacer(match):
        id_str = match.group(1).lstrip('!')
        for m in mentions:
            if m.get("id") == id_str:
                return f"@{m.get('username', '用户')}"
        return match.group(0)
    return re.sub(r'<@([^>]+)>', replacer, text)

# ---------- 网页内容获取 ----------
def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme in ('http', 'https'), result.netloc])
    except:
        return False

async def fetch_webpage_content(url: str) -> Optional[str]:
    try:
        loop = asyncio.get_event_loop()
        print(f"[网页] 开始获取 {url[:80]}...")
        resp = await loop.run_in_executor(
            _executor,
            lambda: requests.get(
                url,
                timeout=15,
                stream=True,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            )
        )
        if resp.status_code != 200:
            print(f"[网页] GET 失败，状态码: {resp.status_code}")
            return None

        content_type = resp.headers.get('Content-Type', '').lower()
        print(f"[网页] Content-Type: {content_type}")

        try:
            chunk = resp.raw.read(512)
        except:
            chunk = b''
        finally:
            resp.close()

        media_type = None
        if content_type.startswith('image/'):
            media_type = "image"
        elif content_type.startswith('video/'):
            media_type = "video"
        else:
            ext = Path(url).suffix.lower()
            if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
                media_type = "image"
            elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
                media_type = "video"

        if media_type:
            print(f"[网页] 检测到媒体类型: {media_type}，返回 __MEDIA_URL__")
            return f"__MEDIA_URL__:{media_type}:{url}"

        # 非媒体，获取完整文本
        response = await loop.run_in_executor(
            _executor,
            lambda: requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        )
        if response.status_code != 200:
            return None
        text = re.sub(r'<[^>]+>', ' ', response.text)
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) < 50:
            return None
        return text
    except Exception as e:
        print(f"[网页获取] 失败 {url}: {e}")
        return None

async def summarize_content_if_needed(content: str, max_len: int = 5000, summary_len: int = 400) -> str:
    if len(content) <= max_len:
        return content
    prompt = f"请将以下网页内容压缩为一篇摘要，字数控制在{summary_len}字以内：\n{content[:3000]}"
    try:
        summary = await call_ai([{"role": "user", "content": prompt}], MODEL_JUDGE, temperature=0.3)
        return summary if summary else content[:200] + "...（摘要生成失败）"
    except:
        return content[:200] + "...（摘要生成失败）"

# ---------- AI 调用 ----------
async def call_ai(messages: List[Dict], model: str, stream: bool = False, temperature: float = 0.7) -> str:
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": messages,
        "stream": stream,
        "temperature": temperature,
        "top_p": 0.9,
        "max_tokens": 3000,
    }
    loop = asyncio.get_event_loop()
    try:
        start_time = time.time()
        print(f"[AI调用] 开始请求，模型: {model}")
        response = await loop.run_in_executor(
            _executor,
            lambda: requests.post(AI_API_URL, json=payload, headers=headers, timeout=360)
        )
        elapsed = time.time() - start_time
        print(f"[AI调用] 请求完成，耗时 {elapsed:.2f} 秒")
        response.raise_for_status()
        data = response.json()
        if data.get("choices") and len(data["choices"]) > 0:
            content = data["choices"][0].get("message", {}).get("content", "")
            return content.strip()
        return "（AI 未返回有效内容）"
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            print(f"[AI调用错误] 状态码: {e.response.status_code}, 响应: {e.response.text}")
        else:
            print(f"[AI调用错误] {e}")
        raise

# ---------- 记忆相关性计算 ----------
def compute_similarity(text1: str, text2: str) -> float:
    words1 = set(re.findall(r'\w+', text1.lower()))
    words2 = set(re.findall(r'\w+', text2.lower()))
    if not words1 or not words2:
        return 0.0
    intersection = len(words1 & words2)
    union = len(words1 | words2)
    return intersection / union if union > 0 else 0.0

# ---------- 判断回复 ----------
async def should_reply_in_group(history: List[Dict], current_message: str, mentions: List[Dict]) -> bool:
    if not history:
        return False
    history_lines = []
    for msg in history[:-1]:
        role = msg.get("role", "")
        content = msg.get("content", "")
        if role == "user":
            if ": " in content:
                parts = content.split(": ", 1)
                user_name = parts[0]
                text = parts[1] if len(parts) > 1 else ""
            else:
                user_name = "用户"
                text = content
            history_lines.append(f"[{user_name}]: {text}")
        elif role == "assistant":
            history_lines.append(f"[机器人]: {content}")
        elif msg.get("is_summary"):
            history_lines.append(f"[摘要]: {content}")
        elif role == "system":
            history_lines.append(f"[系统]: {content}")
        else:
            history_lines.append(f"[{role}]: {content}")
    history_text = "\n".join(history_lines) if history_lines else "（无上文）"
    current_text = current_message or "（非文本内容）"

    judge_prompt = (
        "你是一个群聊助手，需要判断机器人是否应该介入回复当前这条消息。\n"
        "请先阅读以下【上文】（之前的对话历史），然后重点关注【当前消息】。\n"
        f"机器人的名字是 {BOT_NAME}。如果当前消息或上文中明确提到这个机器人名字，或者请求机器人帮助，则应当回复。\n"
        "判断标准：\n"
        "1. 如果当前消息或上文明确提到机器人、请求机器人帮助，或者话题与机器人有关，回复“是”。\n"
        "2. 如果当前消息中 @ 了某人（包括机器人），且@的是机器人，或者@了之前与机器人互动过的人，则很可能需要回复。\n"
        "3. 如果上文中有机器人参与对话，且当前消息是后续跟进，回复“是”。\n"
        "4. 即使当前消息只是表情包、语音消息或简短情感表达（如“哈哈哈”、“好气啊”等），也请结合上下文判断：如果这些消息是用户在主动与机器人或群友互动，则倾向于回复“是”；如果完全无关（如单方面发泄情绪且未指向任何人），可回复“否”。\n"
        "重要提示：请更加重视新消息，当新消息涉及情感表达、语音消息或与机器人有一定关联时，优先考虑回复以延续对话氛围。\n"
        "只回答“是”或“否”，不要有其他内容。\n\n"
        f"【上文】\n{history_text}\n\n"
        f"【当前消息】\n{current_text}\n\n"
        "请回答：是否需要机器人回复？（是/否）"
    )

    judge_messages = [
        {"role": "system", "content": "你是一个精准的判断助手，只回答'是'或'否'。"},
        {"role": "user", "content": judge_prompt}
    ]

    try:
        result = await call_ai(judge_messages, MODEL_JUDGE, stream=False, temperature=0.2)
        result_clean = result.strip().lower()
        print(f"[AI Judge 结果] {result_clean}")
        return "是" in result_clean or "yes" in result_clean
    except Exception as e:
        print(f"[AI Judge Error] {e}")
        return True

# ---------- 生成摘要 ----------
async def generate_and_insert_summary(thread_key: str, retries: int = 3):
    for attempt in range(retries):
        try:
            hist = load_history(thread_key)
            if not hist:
                return
            last_summary_idx = -1
            for i, msg in enumerate(hist):
                if msg.get("is_summary"):
                    last_summary_idx = i
            start_idx = last_summary_idx + 1
            if len(hist) - start_idx <= COMPRESS_THRESHOLD:
                return
            end_idx = len(hist) - 10
            if end_idx <= start_idx:
                return
            msgs_to_summarize = hist[start_idx:end_idx]
            if not msgs_to_summarize:
                return
            content_for_summary = ""
            for msg in msgs_to_summarize:
                role = msg.get("role", "")
                content = msg.get("content", "")
                if role == "user":
                    content_for_summary += f"用户: {content}\n"
                elif role == "assistant":
                    content_for_summary += f"机器人: {content}\n"
                elif msg.get("is_summary"):
                    content_for_summary += f"摘要: {content}\n"
                else:
                    content_for_summary += f"{role}: {content}\n"
            summary_text = None
            try:
                summary = await call_ai(
                    [{"role": "user", "content": f"请将以下对话历史压缩为一篇摘要，字数控制在200-400字之间：\n{content_for_summary}"}],
                    MODEL_JUDGE,
                    temperature=0.3
                )
                if summary and "（AI 未返回有效内容）" not in summary:
                    summary_text = summary
            except Exception as e:
                print(f"[摘要生成] 尝试 {attempt+1}/{retries} 失败: {e}")
            if summary_text is None:
                summary_text = "（摘要生成失败，请稍后重试）"
            insert_pos = len(hist) - 10
            if insert_pos < 0:
                insert_pos = 0
            if last_summary_idx != -1 and insert_pos <= last_summary_idx:
                insert_pos = last_summary_idx + 1
            summary_msg = {"role": "system", "content": summary_text, "is_summary": True}
            hist = load_history(thread_key)
            last_summary_idx = -1
            for i, msg in enumerate(hist):
                if msg.get("is_summary"):
                    last_summary_idx = i
            if len(hist) - (last_summary_idx + 1) <= COMPRESS_THRESHOLD:
                return
            if hist and hist[-1].get("is_summary"):
                return
            insert_pos = len(hist) - 10
            if insert_pos < 0:
                insert_pos = 0
            if last_summary_idx != -1 and insert_pos <= last_summary_idx:
                insert_pos = last_summary_idx + 1
            hist.insert(insert_pos, summary_msg)
            save_history(thread_key, hist)
            print(f"[摘要] 线程 {thread_key} 已插入摘要，位置 {insert_pos}，长度 {len(summary_text)} 字")
            return
        except Exception as e:
            print(f"[摘要生成] 尝试 {attempt+1}/{retries} 异常: {e}")
            await asyncio.sleep(2)
    print(f"[摘要生成] 线程 {thread_key} 最终失败，已放弃")

# ---------- 生成回复（包含群记忆） ----------
async def generate_reply(thread_key: str, user_message: str, username: str, msg_type: str, raw_message_json: str) -> str:
    # 全局记忆
    global_mem = get_global_memory()
    if global_mem:
        scored = []
        for mem in global_mem:
            score = compute_similarity(user_message, mem)
            scored.append((score, mem))
        scored.sort(key=lambda x: x[0], reverse=True)
        global_memory_lines = []
        for i, (score, mem) in enumerate(scored):
            if i == 0:
                global_memory_lines.append(f"- {mem} （最相关！！！）")
            elif i == 1:
                global_memory_lines.append(f"- {mem} （最相关！！！）")
            else:
                global_memory_lines.append(f"- {mem}")
        global_memory_text = "\n".join(global_memory_lines)
    else:
        global_memory_text = "（无）"

    # 群记忆（仅群聊）
    qun_memory_text = ""
    if msg_type == "group":
        group_id = thread_key.replace("group_", "")
        qun_data = get_qun_memory(group_id)
        if qun_data.get("enabled", 1):
            qun_mem = qun_data.get("memory", [])
            if qun_mem:
                scored_qun = []
                for mem in qun_mem:
                    score = compute_similarity(user_message, mem)
                    scored_qun.append((score, mem))
                scored_qun.sort(key=lambda x: x[0], reverse=True)
                qun_lines = []
                for i, (score, mem) in enumerate(scored_qun):
                    if i == 0:
                        qun_lines.append(f"- {mem} （最相关！！！）")
                    elif i == 1:
                        qun_lines.append(f"- {mem} （最相关！！！）")
                    else:
                        qun_lines.append(f"- {mem}")
                qun_memory_text = "\n".join(qun_lines)
            else:
                qun_memory_text = "（无）"
        else:
            qun_memory_text = "（未启用）"

    # 构建 system prompt
    if msg_type == "group":
        memory_text = f"【全局长期记忆】\n{global_memory_text}\n\n【本群长期记忆】\n{qun_memory_text}"
    else:
        memory_text = f"【全局长期记忆】\n{global_memory_text}"

    # 使用配置文件中的 SYSTEM_PROMPT 作为基础提示词
    system_prompt = (
        f"{SYSTEM_PROMPT}\n"  # 配置文件中的自定义提示词
        f"你的名字是 {BOT_NAME}，用户可能会用这个名字称呼你。\n"
        f"{memory_text}\n"
        "在群聊中，如果需要提及某位用户，请直接使用“@用户名”的形式，例如“@张三”。\n"
        "重要：在回复内容中提及用户时，请仅使用“@用户名”的格式，严禁显示用户的ID（即不要在用户名后面添加括号和ID序列）。\n"
        "用户可能会发送语音消息、文本文件、图片、视频或包含网页链接的消息。语音消息已被自动转写成文字，并显示为 [语音：转文字内容]。文本文件内容会被自动读取并嵌入消息中，格式为 [文件：文件名] 后跟文件内容块。网页内容会被自动获取并嵌入消息中，格式为 [网页内容已自动获取] 或 [网页内容摘要]。图片和视频会被自动识别并生成摘要，格式为 [收到图片：文件名] 或 [收到视频：文件名] 后跟摘要。你可以根据这些内容进行回复。\n"
        "请尽量在回复中适当提及相关用户。"
    )

    print(f"[System Prompt] {system_prompt}")

    history = get_history(thread_key)
    messages = [{"role": "system", "content": system_prompt}]
    json_context = f"以下是当前消息的原始 JSON 数据，你可以从中获取发送者ID等信息以便使用 @ 功能：\n```json\n{raw_message_json}\n```"
    messages.append({"role": "user", "content": json_context})
    messages.extend(history)
    if not messages or messages[-1]["role"] != "user":
        messages.append({"role": "user", "content": user_message})
    try:
        reply = await call_ai(messages, MODEL_MAIN, stream=False)
        return reply
    except Exception as e:
        print(f"[AI Reply Error] {e}")
        return "抱歉，我暂时无法回复，请稍后再试。"

# ---------- 记忆整理（全局） ----------
_is_organizing_global = False

async def check_and_organize_global_memory():
    global _is_organizing_global
    if _is_organizing_global:
        return
    mem = get_global_memory()
    if len(mem) > 15:
        _is_organizing_global = True
        try:
            await organize_global_memory()
        finally:
            _is_organizing_global = False

async def organize_global_memory(retries: int = 3):
    for attempt in range(retries):
        try:
            old_list = get_global_memory()
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
                MODEL_MAIN,
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
            current_list = get_global_memory()
            if len(current_list) > len(old_list):
                print("[记忆整理] 整理期间有新记忆添加，放弃本次整理")
                return
            timestamp = time.strftime("%Y%m%d_%H%M")
            old_file = BASE_DIR / f"old_memory_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(old_list, f, ensure_ascii=False, indent=2)
            set_global_memory(new_list)
            print(f"[记忆整理] 整理完成，原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}")
            return
        except Exception as e:
            print(f"[记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}")
            await asyncio.sleep(2)
    print("[记忆整理] 最终失败，保留原记忆")

# ---------- 记忆整理（群） ----------
_is_organizing_qun = {}

async def check_and_organize_qun_memory(group_id: str):
    if _is_organizing_qun.get(group_id, False):
        return
    qun_data = get_qun_memory(group_id)
    if not qun_data.get("enabled", 1):
        return
    mem = qun_data.get("memory", [])
    if len(mem) > 15:
        _is_organizing_qun[group_id] = True
        try:
            await organize_qun_memory(group_id)
        finally:
            _is_organizing_qun[group_id] = False

async def organize_qun_memory(group_id: str, retries: int = 3):
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
                MODEL_MAIN,
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
                print(f"[群记忆整理] 整理期间有新记忆添加，放弃本次整理")
                return
            timestamp = time.strftime("%Y%m%d_%H%M")
            backup_dir = BASE_DIR / "qun_memory_backup"
            backup_dir.mkdir(exist_ok=True)
            old_file = backup_dir / f"{group_id}_{timestamp}.json"
            with open(old_file, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
            set_qun_memory_list(group_id, new_list)
            print(f"[群记忆整理] 群 {group_id} 整理完成，原{len(old_list)}条精简为{len(new_list)}条，旧记忆保存至 {old_file}")
            return
        except Exception as e:
            print(f"[群记忆整理] 尝试 {attempt+1}/{retries} 失败: {e}")
            await asyncio.sleep(2)
    print(f"[群记忆整理] 群 {group_id} 最终失败，保留原记忆")

# ---------- 自动记忆管理 ----------
async def auto_manage_memory(thread_key: str, user_message: str, reply: str, context_hist: List[Dict], raw_json: str):
    is_group = thread_key.startswith("group_")
    if is_group:
        group_id = thread_key.replace("group_", "")
        qun_data = get_qun_memory(group_id)
        if not qun_data.get("enabled", 1):
            print("[记忆管理] 群记忆未启用，跳过")
            return
        current_mem = get_qun_memory_list(group_id)
        mem_type = "群"
        mem_label = "群长期记忆"
    else:
        current_mem = get_global_memory()
        mem_type = "全局"
        mem_label = "全局长期记忆"

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

    prompt = (
        f"你是一个记忆管理助手，负责根据对话内容更新机器人的{mem_label}。\n"
        f"机器人的名字是 {BOT_NAME}。\n"
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
        "请分析上述内容，判断是否需要更新记忆。如果需要，输出一个JSON指令，格式如下：\n"
        "- 添加记忆：{ \"action\": \"add\", \"content\": \"要添加的记忆内容（必须包含用户QQ号）\" }\n"
        "- 删除记忆（按索引）：{ \"action\": \"delete\", \"index\": 0 }  （索引从0开始）\n"
        "- 替换记忆：{ \"action\": \"replace\", \"index\": 0, \"content\": \"新内容（必须包含用户QQ号）\" }\n"
        "- 清空所有记忆：{ \"action\": \"clear\" }\n"
        "- 不操作：{ \"action\": \"none\" }\n"
        "只输出JSON，不要有其他内容。"
    )

    try:
        result = await call_ai([{"role": "user", "content": prompt}], MODEL_JUDGE, temperature=0.2)
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
                if is_group:
                    add_qun_memory(group_id, content)
                else:
                    add_global_memory(content)
                print(f"[记忆] {mem_type} 添加: {content}")
        elif action == "delete":
            idx = data.get("index")
            if idx is not None:
                if is_group:
                    if remove_qun_memory(group_id, idx):
                        print(f"[记忆] {mem_type} 删除索引 {idx}")
                else:
                    if remove_global_memory(idx):
                        print(f"[记忆] {mem_type} 删除索引 {idx}")
        elif action == "replace":
            idx = data.get("index")
            content = data.get("content")
            if idx is not None and content:
                if is_group:
                    if replace_qun_memory(group_id, idx, content):
                        print(f"[记忆] {mem_type} 替换索引 {idx} 为: {content}")
                else:
                    if replace_global_memory(idx, content):
                        print(f"[记忆] {mem_type} 替换索引 {idx} 为: {content}")
        elif action == "clear":
            if is_group:
                clear_qun_memory(group_id)
            else:
                clear_global_memory()
            print(f"[记忆] {mem_type} 清空所有记忆")
        else:
            print(f"[记忆] {mem_type} 无操作")
    except json.JSONDecodeError as e:
        print(f"[记忆管理 JSON解析错误] {e}, 原始内容: {result[:200]}")
    except Exception as e:
        print(f"[记忆管理错误] {e}")

    # 触发整理
    if is_group:
        await check_and_organize_qun_memory(group_id)
    else:
        await check_and_organize_global_memory()

# ---------- Token 管理（每个机器人独立） ----------
class BotClient:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_info = {"access_token": None, "expires_at": 0}
        self.bot_name = "灵泽集AI"

    def get_access_token(self, force_refresh: bool = False) -> str:
        if force_refresh or not self.token_info["access_token"] or time.time() >= self.token_info["expires_at"] - 60:
            url = "https://bots.qq.com/app/getAppAccessToken"
            headers = {"Content-Type": "application/json"}
            payload = {"appId": self.app_id, "clientSecret": self.app_secret}
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            self.token_info["access_token"] = data["access_token"]
            self.token_info["expires_at"] = time.time() + int(data["expires_in"])
            print(f"[Token] {self.app_id} 获取成功，有效期 {data['expires_in']} 秒")
        return self.token_info["access_token"]

    def get_websocket_url(self):
        token = self.get_access_token()
        url = "https://api.sgroup.qq.com/gateway"
        headers = {"Authorization": f"QQBot {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()["url"]

    async def send_message(self, msg_type: str, recipient_id: str, content: Optional[str] = None,
                           msg_id: Optional[str] = None, media: Optional[str] = None,
                           embed: Optional[Dict] = None, max_retries: int = 3) -> bool:
        for attempt in range(max_retries):
            token = self.get_access_token()
            if msg_type == "c2c":
                url = f"https://api.sgroup.qq.com/v2/users/{recipient_id}/messages"
            else:
                url = f"https://api.sgroup.qq.com/v2/groups/{recipient_id}/messages"
            headers = {
                "Authorization": f"QQBot {token}",
                "Content-Type": "application/json"
            }
            payload: Dict[str, Any] = {}
            if content:
                payload["content"] = content
            if msg_id:
                payload["msg_id"] = msg_id
            if media:
                payload["media"] = media
            if embed:
                payload["embed"] = embed

            loop = asyncio.get_event_loop()
            try:
                resp = await loop.run_in_executor(
                    _executor,
                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                )
                if resp.status_code == 200:
                    print(f"[Reply] {msg_type} 发送成功: {content[:50] if content else media or embed}")
                    return True
                else:
                    print(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}")
                    if resp.status_code in (401, 500):
                        print("[Reply] 强制刷新 Token")
                        self.get_access_token(force_refresh=True)
                        continue
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        print("[Reply] 发送最终失败，放弃消息")
                        return False
            except Exception as e:
                print(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 异常: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    print("[Reply] 发送最终失败，放弃消息")
                    return False
        return False

# ---------- 消息解析 ----------
def parse_message(data: Dict) -> Dict:
    event_type = data.get("t")
    payload = data.get("d", {})
    result = {
        "event_type": event_type,
        "raw": data,
        "msg_type": None,
        "msg_id": payload.get("id"),
        "content": payload.get("content", ""),
        "timestamp": payload.get("timestamp"),
        "author_id": None,
        "username": None,
        "recipient_id": None,
        "attachments": payload.get("attachments", []),
        "mentions": payload.get("mentions", []),
        "is_at_me": False,
        "reply_info": None,
        "voice_text": None,
        "voice_url": None,
        "is_voice": False,
        "ref_media": [],
    }

    attachments = payload.get("attachments", [])
    voice_text = None
    voice_url = None
    is_voice = False
    for att in attachments:
        if att.get("content_type") == "voice":
            is_voice = True
            voice_text = att.get("asr_refer_text", "")
            voice_url = att.get("voice_wav_url") or att.get("url", "")
            break

    result["is_voice"] = is_voice
    result["voice_text"] = voice_text
    result["voice_url"] = voice_url

    if is_voice and voice_text and not result["content"]:
        result["content"] = voice_text

    reply_info = None
    ref_media = []

    reply = payload.get("reply")
    if reply:
        ref_content = reply.get("content", "")
        ref_content_decoded = decode_face_tags(ref_content) if ref_content else ""
        ref_attachments = reply.get("attachments", [])
        for att in ref_attachments:
            if att.get("content_type") and not att.get("content_type").startswith("voice"):
                ref_media.append({
                    "content_type": att.get("content_type"),
                    "filename": att.get("filename", "未知文件"),
                    "url": att.get("url"),
                    "height": att.get("height", 0),
                    "width": att.get("width", 0),
                    "size": att.get("size", 0),
                })
        reply_info = {
            "msg_id": reply.get("msg_id"),
            "content": ref_content_decoded,
            "summary": f"引用了消息: {ref_content_decoded}" if ref_content_decoded else "引用了某条消息"
        }
        if ref_media:
            reply_info["has_media"] = True
    else:
        msg_elements = payload.get("msg_elements", [])
        ref_contents = []
        for elem in msg_elements:
            if elem.get("message_type") == 103:
                ref_attachments = elem.get("attachments", [])
                for att in ref_attachments:
                    content_type = att.get("content_type", "")
                    if content_type and not content_type.startswith("voice"):
                        ref_media.append({
                            "content_type": content_type,
                            "filename": att.get("filename", "未知文件"),
                            "url": att.get("url"),
                            "height": att.get("height", 0),
                            "width": att.get("width", 0),
                            "size": att.get("size", 0),
                        })
                ref_content = elem.get("content", "")
                if ref_content:
                    ref_content_decoded = decode_face_tags(ref_content)
                    ref_contents.append(ref_content_decoded)
        if ref_contents:
            combined_content = "\n".join(ref_contents)
            reply_info = {
                "summary": f"引用了消息: {combined_content[:200] + '...' if len(combined_content) > 200 else combined_content}",
                "content": combined_content,
                "attachments": ref_attachments if ref_attachments else [],
                "has_media": bool(ref_media)
            }

    result["reply_info"] = reply_info
    result["ref_media"] = ref_media

    if event_type == "C2C_MESSAGE_CREATE":
        author = payload.get("author", {})
        result["msg_type"] = "c2c"
        author_id = author.get("user_openid") or author.get("id")
        username = author.get("username", "")
        result["author_id"] = author_id
        result["username"] = username
        result["recipient_id"] = author_id
        if username:
            update_user_mapping(author_id, username)
    elif event_type in ("GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
        result["msg_type"] = "group"
        author = payload.get("author", {})
        author_id = author.get("member_openid") or author.get("id")
        username = author.get("username", "")
        result["author_id"] = author_id
        result["username"] = username
        result["recipient_id"] = payload.get("group_openid")
        if username:
            update_user_mapping(author_id, username)
        for mention in result["mentions"]:
            if mention.get("bot") and mention.get("is_you"):
                result["is_at_me"] = True
                break

    if result["content"]:
        result["clean_content"] = re.sub(r'<@[^>]+>\s*', '', result["content"]).strip()
    else:
        result["clean_content"] = ""

    return result

# ---------- 冷却队列与中断 ----------
pending_queues: Dict[str, List[Dict]] = {}
pending_timers: Dict[str, asyncio.Task] = {}
pending_process_tasks: Dict[str, asyncio.Task] = {}

async def process_queue(thread_key: str, bot_client: BotClient):
    queue = None
    try:
        queue = pending_queues.pop(thread_key, [])
        if not queue:
            return

        merged_content = ""
        for msg in queue:
            content = msg.get("decoded_content", "")
            user_identifier = msg.get("user_identifier", "用户(未知)")
            if content:
                merged_content += f"{user_identifier}: {content}\n"
            else:
                merged_content += f"{user_identifier}: 发送了非文本内容\n"
        merged_content = merged_content.strip()

        last = queue[-1]
        parsed = last["parsed"].copy()
        parsed["clean_content"] = merged_content
        parsed["content"] = merged_content

        await handle_processed_message(parsed, thread_key, merged_content, queue, bot_client)
    except asyncio.CancelledError:
        print(f"[处理取消] 线程 {thread_key} 的处理被中断")
        if queue is not None and queue:
            if thread_key not in pending_queues:
                pending_queues[thread_key] = []
            pending_queues[thread_key] = queue + pending_queues[thread_key]
            print(f"[处理取消] 已放回 {len(queue)} 条消息到队列")
        raise
    except Exception as e:
        print(f"[处理异常] 线程 {thread_key} 处理消息时发生错误: {e}")

# ---------- 处理合并消息 ----------
async def handle_processed_message(parsed: Dict, thread_key: str, merged_content: str, queue: List[Dict], bot_client: BotClient):
    msg_type = parsed["msg_type"]
    recipient_id = parsed["recipient_id"]
    msg_id = parsed["msg_id"]
    username = parsed["username"] or "用户"
    mentions = parsed.get("mentions", [])
    is_voice = parsed.get("is_voice", False)
    voice_text = parsed.get("voice_text", "")

    # ========== 强制回复检查 ==========
    force_reply = False
    for q in queue:
        q_mentions = q["parsed"].get("mentions", [])
        for m in q_mentions:
            if m.get("bot") and m.get("is_you"):
                force_reply = True
                break
        if force_reply:
            break

    should_reply = False

    if force_reply:
        should_reply = True
        print("[强制回复] 队列中包含@机器人的消息，强制回复")
    elif msg_type == "c2c":
        should_reply = True
    elif msg_type == "group" and parsed.get("is_at_me", False):
        should_reply = True
    elif msg_type == "group" and not parsed.get("is_at_me", False):
        recent_history = get_recent_history(thread_key, JUDGE_CONTEXT_LIMIT)
        should_reply = await should_reply_in_group(recent_history, merged_content, mentions)
        print(f"[AI Judge] 判定结果: {should_reply}")

    if not should_reply:
        print("[忽略] 不回复")
        return

    if asyncio.current_task().cancelled():
        print("[处理取消] 判断完成但任务已取消，放弃回复")
        return

    extra_info = ""
    refs = []
    atts = []
    for q in queue:
        if q["parsed"].get("reply_info"):
            refs.append(str(q["parsed"]["reply_info"].get("summary", "")))
        if q["parsed"].get("attachments"):
            atts.extend([att.get("filename", "文件") for att in q["parsed"]["attachments"]])
    if refs:
        extra_info += f" [用户引用了消息: {'; '.join(refs)}]"
    if atts:
        extra_info += f" [用户发送了附件: {', '.join(atts)}]"
    if is_voice and voice_text:
        extra_info += f" [语音转文字: {voice_text}]"

    full_user_input = merged_content + extra_info
    raw_json_str = json.dumps(parsed, ensure_ascii=False, default=str)

    reply = await generate_reply(thread_key, full_user_input, username, msg_type, raw_json_str)

    if asyncio.current_task().cancelled():
        print("[处理取消] 生成回复完成但任务已取消，放弃发送")
        return

    if len(reply) > 2000:
        reply = reply[:1997] + "..."

    success = await bot_client.send_message(msg_type, recipient_id, reply, msg_id)
    if success:
        append_message(thread_key, "assistant", reply)
        full_hist = get_recent_history(thread_key, 6)
        if full_hist and full_hist[-1].get("role") == "assistant":
            above_hist = full_hist[:-1]
        else:
            above_hist = full_hist
        user_msg_for_memory = merged_content
        asyncio.create_task(auto_manage_memory(thread_key, user_msg_for_memory, reply, above_hist, raw_json_str))
    else:
        print("[发送失败] 消息未送达，不保存到历史")

# ---------- 消息处理入口 ----------
async def handle_message(data: Dict, bot_client: BotClient):
    parsed = parse_message(data)
    if not parsed["msg_type"]:
        return

    raw_content = parsed.get("content", "")
    decoded_content = decode_face_tags(raw_content)

    # 强制从原始 JSON 提取引用内容
    raw_data = data.get("d", {})
    msg_elements = raw_data.get("msg_elements", [])
    ref_contents = []
    if msg_elements:
        for elem in msg_elements:
            content = elem.get("content", "")
            if content:
                decoded = decode_face_tags(content)
                ref_contents.append(decoded)
    if ref_contents:
        ref_text = "\n".join(ref_contents)
        if decoded_content:
            decoded_content = ref_text + "\n" + decoded_content
        else:
            decoded_content = ref_text

    # ==================== 解析聊天记录转发中的媒体附件 ====================
    if '[群聊的聊天记录]' in decoded_content or '=== 消息' in decoded_content:
        print("[转发解析] 检测到聊天记录转发格式，开始解析...")
        parsed_text, media_list = parse_forwarded_chatlog(decoded_content)
        decoded_content = parsed_text

        media_to_process = []
        for media in media_list:
            has_cache = False
            if media['type'] in ('image', 'video'):
                cache_key = get_media_cache_key(media['type'], media['filename'], media['height'], media['width'])
                if get_media_cache_path(cache_key).exists():
                    has_cache = True
            if not has_cache:
                media_to_process.append(media)
            else:
                media['_cached'] = True

        if len(media_to_process) > 5:
            media_to_process = media_to_process[:5]
            print("[转发解析] 仅处理前5个无缓存媒体，其余忽略")

        for idx, media in enumerate(media_list):
            process_this = False
            if media.get('_cached', False):
                process_this = True
            elif media in media_to_process:
                process_this = True

            if not process_this:
                placeholder = f"[MEDIA_PLACEHOLDER_{idx}]"
                decoded_content = decoded_content.replace(placeholder, f"[媒体附件: {media['filename']}] (超过处理限制，已忽略)")
                continue

            url = media.get('url')
            filename = media.get('filename', '未知文件')
            media_type = media.get('type', 'unknown')
            height = media.get('height', 0)
            width = media.get('width', 0)
            placeholder = f"[MEDIA_PLACEHOLDER_{idx}]"

            if media_type == 'image' or media_type == 'video':
                summary = await recognize_media(media_type, url, filename, height, width)
                decoded_content = decoded_content.replace(placeholder, f"[转发媒体识别结果: {filename}]\n{summary}")
            elif media_type == 'text_file':
                try:
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        _executor,
                        lambda: requests.get(url, timeout=10)
                    )
                    if response.status_code == 200:
                        encoding = response.apparent_encoding or 'utf-8'
                        content = response.content.decode(encoding, errors='replace')
                        max_len = 100 * 1024
                        if len(content) > max_len:
                            content = content[:max_len] + "\n... (文件内容过长，已截断)"
                        block = f"[转发文件: {filename}]\n=== 文件内容 ===\n{content}\n=== 文件内容结束 ==="
                        decoded_content = decoded_content.replace(placeholder, block)
                    else:
                        decoded_content = decoded_content.replace(placeholder, f"[转发文件: {filename}] 下载失败 (HTTP {response.status_code})")
                except Exception as e:
                    decoded_content = decoded_content.replace(placeholder, f"[转发文件: {filename}] 下载异常: {e}")
            elif media_type == 'binary_file':
                decoded_content = decoded_content.replace(placeholder, f"[转发文件: {filename}] 不支持的文件类型")
            else:
                decoded_content = decoded_content.replace(placeholder, f"[转发附件: {filename}] 无法识别类型")

    # ==================== 处理附件（包括 file 类型的图片/视频） ====================
    attachments = parsed.get("attachments", [])
    extra_content_parts = []
    loop = asyncio.get_event_loop()

    for att in attachments:
        content_type = att.get("content_type", "")
        filename = att.get("filename", "未知文件")
        url = att.get("url")
        if not url:
            continue

        # 根据 content_type 或扩展名判断媒体类型
        media_type = None
        if content_type.startswith("image/"):
            media_type = "image"
        elif content_type.startswith("video/"):
            media_type = "video"
        elif content_type == "file":
            # 对于 file 类型，根据扩展名判断
            media_type = get_media_type_from_filename(filename)

        if media_type:
            # 图片或视频：走媒体识别流程
            height = att.get("height", 0)
            width = att.get("width", 0)
            summary = await recognize_media(media_type, url, filename, height, width)
            if len(summary) < 50:
                summary = summary + "（摘要过短，可能识别不完整）"
            elif len(summary) > 600:
                summary = summary[:600] + "...（摘要过长，已截断）"
            if media_type == "image":
                block = f"[收到图片：{filename}]===图片{filename}摘要开始===\n{summary}\n===图片{filename}摘要结束==="
            else:
                block = f"[收到视频：{filename}]===视频{filename}摘要开始===\n{summary}\n===视频{filename}摘要结束==="
            extra_content_parts.append(block)
        elif content_type == "file" and is_text_file(filename):
            # 文本文件：下载内容
            try:
                response = await loop.run_in_executor(
                    _executor,
                    lambda: requests.get(url, timeout=10)
                )
                if response.status_code == 200:
                    encoding = response.apparent_encoding or 'utf-8'
                    content = response.content.decode(encoding, errors='replace')
                    max_len = 100 * 1024
                    if len(content) > max_len:
                        content = content[:max_len] + "\n... (文件内容过长，已截断)"
                    block = f"[文件：{filename}] =====文件内容：{filename}开始=====\n{content}\n=====文件内容：{filename}结束====="
                    extra_content_parts.append(block)
                else:
                    extra_content_parts.append(f"[文件：{filename}] 下载失败")
            except Exception as e:
                extra_content_parts.append(f"[文件：{filename}] 下载异常: {e}")
        # 其他类型附件忽略

    # 引用媒体
    ref_media = parsed.get("ref_media", [])
    for ref in ref_media:
        content_type = ref.get("content_type", "")
        filename = ref.get("filename", "未知文件")
        url = ref.get("url")
        if not url:
            continue

        media_type = None
        if content_type.startswith("image/"):
            media_type = "image"
        elif content_type.startswith("video/"):
            media_type = "video"
        elif content_type == "file":
            media_type = get_media_type_from_filename(filename)

        if media_type:
            height = ref.get("height", 0)
            width = ref.get("width", 0)
            summary = await recognize_media(media_type, url, filename, height, width)
            if len(summary) < 50:
                summary = summary + "（摘要过短，可能识别不完整）"
            elif len(summary) > 600:
                summary = summary[:600] + "...（摘要过长，已截断）"
            if media_type == "image":
                block = f"[引用图片：{filename}]===图片{filename}摘要开始===\n{summary}\n===图片{filename}摘要结束==="
            else:
                block = f"[引用视频：{filename}]===视频{filename}摘要开始===\n{summary}\n===视频{filename}摘要结束==="
            extra_content_parts.append(block)
        elif content_type == "file" and is_text_file(filename):
            try:
                response = await loop.run_in_executor(
                    _executor,
                    lambda: requests.get(url, timeout=10)
                )
                if response.status_code == 200:
                    encoding = response.apparent_encoding or 'utf-8'
                    content = response.content.decode(encoding, errors='replace')
                    max_len = 100 * 1024
                    if len(content) > max_len:
                        content = content[:max_len] + "\n... (文件内容过长，已截断)"
                    block = f"[引用文件：{filename}] =====文件内容：{filename}开始=====\n{content}\n=====文件内容：{filename}结束====="
                    extra_content_parts.append(block)
                else:
                    extra_content_parts.append(f"[引用文件：{filename}] 下载失败")
            except Exception as e:
                extra_content_parts.append(f"[引用文件：{filename}] 下载异常: {e}")

    if extra_content_parts:
        if decoded_content:
            decoded_content = decoded_content + "\n" + "\n".join(extra_content_parts)
        else:
            decoded_content = "\n".join(extra_content_parts)

    # ========== 处理消息中的 URL（网页内容获取） ==========
    url_parts = []
    url_pattern = r'https?://[^\s<>"\'，。；！？）]+'
    urls = re.findall(url_pattern, decoded_content)

    for url in urls:
        if not is_valid_url(url):
            continue

        print(f"[网页] 开始获取 {url} ...")
        content = await fetch_webpage_content(url)
        if content is None:
            url_parts.append(f"{url}[网页内容获取失败]")
            continue

        if content.startswith("__MEDIA_URL__:"):
            parts = content.split(":", 2)
            if len(parts) >= 3:
                media_type = parts[1]
                media_url = parts[2]
            else:
                media_url = content.replace("__MEDIA_URL__:", "")
                media_type = None
            print(f"[网页] 检测到媒体 URL，类型: {media_type}，开始识别...")
            filename = url.split('/')[-1].split('?')[0] or "媒体文件"
            summary = await recognize_media_by_url(media_url, filename, media_type=media_type)
            display_block = f"{url}[网页内容为媒体文件]\n=== {url} 的媒体摘要 ===\n{summary}\n=== {url} 的媒体摘要结尾 ==="
            url_parts.append(display_block)
            print(f"[网页] 媒体识别完成，摘要长度 {len(summary)} 字符")
        else:
            summary = await summarize_content_if_needed(content, max_len=5000, summary_len=400)
            if len(summary) < len(content) * 0.7:
                display_block = f"{url}[网页内容摘要]\n=== {url} 的内容摘要 ===\n{summary}\n=== {url} 的内容摘要结尾 ==="
            else:
                display_block = f"{url}[网页内容已自动获取]\n=== {url} 的内容 ===\n{summary}\n=== {url} 的内容结尾 ==="
            url_parts.append(display_block)
            print(f"[网页] 获取 {url} 成功，原始长度 {len(content)} 字符，摘要后 {len(summary)} 字符")

    if url_parts:
        if decoded_content:
            decoded_content = decoded_content + "\n" + "\n".join(url_parts)
        else:
            decoded_content = "\n".join(url_parts)

    # 语音
    is_voice = parsed.get("is_voice", False)
    voice_text = parsed.get("voice_text", "")
    if is_voice and voice_text:
        if not decoded_content:
            decoded_content = f"[语音：{voice_text}]"
        else:
            decoded_content = f"[语音：{voice_text}] {decoded_content}"

    display_content = replace_mentions_with_names(decoded_content, parsed.get("mentions", []))
    display_content = display_content.strip()

    if ref_contents:
        ref_display = ref_text[:200] + "..." if len(ref_text) > 200 else ref_text
        display_content = f"[引用内容: {ref_display}] {display_content}"

    author_id = parsed["author_id"]
    msg_username = parsed.get("username", "")
    display_username = get_display_name(author_id, msg_username)
    user_identifier = f"{display_username}({author_id})"

    if msg_username:
        update_user_mapping(author_id, msg_username)

    parsed["decoded_content"] = decoded_content
    parsed["clean_content"] = re.sub(r'<@[^>]+>\s*', '', decoded_content).strip()
    parsed["user_identifier"] = user_identifier

    msg_type = parsed["msg_type"]
    recipient_id = parsed["recipient_id"]
    msg_id = parsed["msg_id"]
    is_at_me = parsed["is_at_me"]

    print(f"[收到] {msg_type} | {user_identifier}: {display_content}")
    if parsed.get("mentions"):
        print(f"[提及详情] {json.dumps(parsed['mentions'], ensure_ascii=False)}")
    if is_voice:
        print(f"[语音] URL: {parsed.get('voice_url', '')}")

    if msg_type == "c2c":
        thread_key = get_thread_key("c2c", parsed["author_id"])
    else:
        thread_key = get_thread_key("group", None, parsed["recipient_id"])

    store_content = f"{user_identifier}: {decoded_content}" if decoded_content else f"{user_identifier} 发送了附件或引用"
    if ref_contents:
        store_content += f" [引用内容: {ref_text}]"
    elif parsed.get("reply_info"):
        store_content += f" [引用: {parsed['reply_info'].get('summary', '')}]"

    append_message(thread_key, "user", store_content)

    # ==================== 命令处理（包括转移记忆） ====================
    clean_cmd = decoded_content.strip()
    if clean_cmd.startswith("添加记忆："):
        mem_text = clean_cmd[5:].strip()
        if mem_text:
            if msg_type == "group":
                group_id = thread_key.replace("group_", "")
                add_qun_memory(group_id, mem_text)
                await bot_client.send_message(msg_type, recipient_id, f"已添加群记忆：{mem_text}", msg_id)
            else:
                add_global_memory(mem_text)
                await bot_client.send_message(msg_type, recipient_id, f"已添加全局记忆：{mem_text}", msg_id)
            return
        else:
            await bot_client.send_message(msg_type, recipient_id, "请提供要添加的记忆内容，例如：添加记忆：张三(1234567)是管理员", msg_id)
            return
    elif clean_cmd.startswith("删除记忆："):
        try:
            idx_str = clean_cmd[5:].strip()
            idx = int(idx_str) - 1
            if msg_type == "group":
                group_id = thread_key.replace("group_", "")
                if remove_qun_memory(group_id, idx):
                    await bot_client.send_message(msg_type, recipient_id, f"已删除第 {idx+1} 条群记忆", msg_id)
                else:
                    await bot_client.send_message(msg_type, recipient_id, "无效的序号，请检查记忆列表", msg_id)
            else:
                if remove_global_memory(idx):
                    await bot_client.send_message(msg_type, recipient_id, f"已删除第 {idx+1} 条全局记忆", msg_id)
                else:
                    await bot_client.send_message(msg_type, recipient_id, "无效的序号，请检查记忆列表", msg_id)
            return
        except:
            await bot_client.send_message(msg_type, recipient_id, "请提供要删除的记忆序号，例如：删除记忆：1", msg_id)
            return
    elif clean_cmd == "查看记忆":
        if msg_type == "group":
            group_id = thread_key.replace("group_", "")
            mem_list = get_qun_memory_list(group_id)
            if mem_list:
                reply = "当前群记忆列表：\n" + "\n".join([f"{i+1}. {item}" for i, item in enumerate(mem_list)])
            else:
                reply = "暂无群记忆"
            await bot_client.send_message(msg_type, recipient_id, reply, msg_id)
        else:
            mem_list = get_global_memory()
            if mem_list:
                reply = "当前全局记忆列表：\n" + "\n".join([f"{i+1}. {item}" for i, item in enumerate(mem_list)])
            else:
                reply = "暂无全局记忆"
            await bot_client.send_message(msg_type, recipient_id, reply, msg_id)
        return
    elif clean_cmd.startswith("启用群记忆") and msg_type == "group":
        group_id = thread_key.replace("group_", "")
        enable_qun_memory(group_id)
        await bot_client.send_message(msg_type, recipient_id, "已启用本群长期记忆", msg_id)
        return
    elif clean_cmd.startswith("禁用群记忆") and msg_type == "group":
        group_id = thread_key.replace("group_", "")
        disable_qun_memory(group_id)
        await bot_client.send_message(msg_type, recipient_id, "已禁用本群长期记忆", msg_id)
        return
    elif clean_cmd.startswith("转移群记忆到全局：") and msg_type == "group":
        try:
            idx_str = clean_cmd[7:].strip()
            idx = int(idx_str) - 1
            group_id = thread_key.replace("group_", "")
            if transfer_memory_to_global(group_id, idx):
                await bot_client.send_message(msg_type, recipient_id, f"已将第 {idx+1} 条群记忆转移到全局记忆", msg_id)
            else:
                await bot_client.send_message(msg_type, recipient_id, "无效的序号，请检查群记忆列表", msg_id)
            return
        except:
            await bot_client.send_message(msg_type, recipient_id, "请提供要转移的群记忆序号，例如：转移群记忆到全局：1", msg_id)
            return
    elif clean_cmd.startswith("转移全局记忆到群：") and msg_type == "group":
        try:
            idx_str = clean_cmd[7:].strip()
            idx = int(idx_str) - 1
            group_id = thread_key.replace("group_", "")
            if transfer_memory_to_qun(group_id, idx):
                await bot_client.send_message(msg_type, recipient_id, f"已将第 {idx+1} 条全局记忆转移到本群记忆", msg_id)
            else:
                await bot_client.send_message(msg_type, recipient_id, "无效的序号，请检查全局记忆列表", msg_id)
            return
        except:
            await bot_client.send_message(msg_type, recipient_id, "请提供要转移的全局记忆序号，例如：转移全局记忆到群：1", msg_id)
            return

    # ---------- 正常消息队列处理 ----------
    if thread_key in pending_process_tasks:
        old_task = pending_process_tasks[thread_key]
        if not old_task.done():
            old_task.cancel()
            print(f"[中断] 取消线程 {thread_key} 的旧处理任务")
        del pending_process_tasks[thread_key]

    if is_at_me:
        if thread_key not in pending_queues:
            pending_queues[thread_key] = []
        pending_queues[thread_key].append({
            "parsed": parsed,
            "user_identifier": user_identifier,
            "decoded_content": decoded_content,
            "msg_id": msg_id,
        })
        task = asyncio.create_task(process_queue(thread_key, bot_client))
        pending_process_tasks[thread_key] = task
        return

    if thread_key not in pending_queues:
        pending_queues[thread_key] = []
    pending_queues[thread_key].append({
        "parsed": parsed,
        "user_identifier": user_identifier,
        "decoded_content": decoded_content,
        "msg_id": msg_id,
    })

    if thread_key in pending_timers:
        pending_timers[thread_key].cancel()
        del pending_timers[thread_key]

    async def timer_task():
        await asyncio.sleep(COOLDOWN_SECONDS)
        if thread_key in pending_queues and pending_queues[thread_key]:
            if thread_key in pending_process_tasks:
                old_task = pending_process_tasks[thread_key]
                if not old_task.done():
                    old_task.cancel()
                del pending_process_tasks[thread_key]
            task = asyncio.create_task(process_queue(thread_key, bot_client))
            pending_process_tasks[thread_key] = task
        if thread_key in pending_timers:
            del pending_timers[thread_key]

    task = asyncio.create_task(timer_task())
    pending_timers[thread_key] = task

# ---------- WebSocket 连接管理（单个机器人） ----------
async def main_connection(bot_client: BotClient):
    global BOT_NAME
    print(f"[启动] 机器人 {bot_client.app_id} 开始连接...")
    ws_url = bot_client.get_websocket_url()
    print(f"[启动] 地址: {ws_url}")

    async with websockets.connect(ws_url) as ws:
        hello = await ws.recv()
        hello_data = json.loads(hello)
        print(f"[收到 Hello] {hello}")
        heartbeat_interval = hello_data.get("d", {}).get("heartbeat_interval", 30000) / 1000.0

        token = bot_client.get_access_token()
        identify = {
            "op": 2,
            "d": {
                "token": f"QQBot {token}",
                "intents": (1 << 25) | (1 << 30),
                "shard": [0, 1],
                "properties": {"os": "Linux", "browser": "MyBot", "device": "MyBot"}
            }
        }
        await ws.send(json.dumps(identify))
        print("[鉴权] 已发送 Identify")

        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if data.get("op") == 0 and data.get("t") == "READY":
                ready_data = data.get("d", {})
                user_info = ready_data.get("user", {})
                bot_username = user_info.get("username", "灵泽集AI")
                bot_client.bot_name = bot_username
                if BOT_NAME == "灵泽集AI":
                    BOT_NAME = bot_username
                print(f"[收到 Ready] 机器人名字: {bot_username}")
                break
            else:
                print(f"[收到] {msg}")

        print(f"[准备就绪] 机器人 {bot_client.bot_name} 已上线")

        async def heartbeat():
            while True:
                await asyncio.sleep(heartbeat_interval)
                try:
                    await ws.send(json.dumps({"op": 1, "d": int(time.time() * 1000)}))
                    print("[心跳] 发送")
                except websockets.ConnectionClosed:
                    print("[心跳] 连接已关闭")
                    break
                except Exception as e:
                    print(f"[心跳错误] {e}")
                    break

        asyncio.create_task(heartbeat())

        print("[监听] 开始接收消息...")
        async for raw in ws:
            try:
                data = json.loads(raw)
                op = data.get("op")
                if op not in (1, 11):
                    print(f"[收到] {raw[:200]}...")
                if op == 0:
                    await handle_message(data, bot_client)
            except websockets.ConnectionClosed:
                print("[连接] 服务器关闭连接")
                raise
            except Exception as e:
                print(f"[处理消息错误] {e}")

# ---------- 多机器人运行入口 ----------
async def run_bot_for_client(bot_client: BotClient):
    while True:
        try:
            await main_connection(bot_client)
        except (websockets.ConnectionClosedError, websockets.ConnectionClosed,
                asyncio.TimeoutError, ConnectionError) as e:
            print(f"[连接断开] {e}，5秒后重连...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[致命错误] {e}，10秒后重连...")
            await asyncio.sleep(10)

async def run_bots():
    tasks = []
    for bot_info in BOTS:
        app_id = bot_info.get("APP_ID")
        app_secret = bot_info.get("APP_SECRET")
        if not app_id or not app_secret:
            print("警告：配置中缺少 APP_ID 或 APP_SECRET，跳过该机器人")
            continue
        bot_client = BotClient(app_id, app_secret)
        task = asyncio.create_task(run_bot_for_client(bot_client))
        tasks.append(task)
    if not tasks:
        print("没有可用的机器人配置，程序退出。")
        sys.exit(1)
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(run_bots())
