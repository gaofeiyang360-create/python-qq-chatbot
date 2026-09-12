# -*- coding: utf-8 -*-
# msg.py — 消息解析处理（消息解析、媒体/引用/聊天记录、冷却队列、事件处理）
import re
import json
import asyncio
import base64
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone

from config import (
    get_cooldown_seconds, get_judge_context_limit, is_group_manage_enabled,
    get_bot_enabled, get_bot_auto_welcome, get_executor,
)
from memory import (
    update_user_mapping, get_user_name,
    append_message, get_recent_history,
    get_media_cache_key, get_media_cache_path,
    auto_manage_memory,
    record_user_id, record_group_id,
)
from ai import (
    generate_reply, should_reply_in_group, recognize_media, recognize_media_by_url,
    INTERRUPT_CONTEXT,
)

# 仅用于类型提示，避免运行时循环导入
from typing import TYPE_CHECKING
from log import info, warn, error, debug
if TYPE_CHECKING:
    from client import BotClient


# ==================== 文本文件扩展名 ====================
TEXT_FILE_EXTS = {
    '.txt', '.py', '.html', '.htm', '.css', '.js', '.json', '.xml', '.md', '.csv',
    '.log', '.sh', '.bat', '.ps1', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
    '.c', '.cpp', '.java', '.go', '.rs', '.swift', '.kt', '.rb', '.php', '.lua', '.pl',
    '.r', '.m', '.vbs', '.jsx', '.tsx', '.ts', '.svelte', '.vue', '.sql'
}


def is_text_file(filename: str) -> bool:
    ext = Path(filename).suffix.lower()
    return ext in TEXT_FILE_EXTS


def is_image_or_video_file(filename: str) -> bool:
    ext = Path(filename).suffix.lower()
    return ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg',
                   '.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']


def get_media_type_from_filename(filename: str) -> Optional[str]:
    ext = Path(filename).suffix.lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
        return "image"
    elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
        return "video"
    return None


# ==================== 通用工具函数 ====================
def get_thread_key(msg_type: str, user_id: str = None, group_id: str = None) -> str:
    if msg_type == "c2c":
        return f"c2c_{user_id}"
    else:
        return f"group_{group_id}"


def get_display_name(author_id: str, username: str = "", bot_appid: str = "") -> str:
    if username and username.strip():
        return username.strip()
    if bot_appid:
        mapped = get_user_name(author_id, bot_appid)
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
        except Exception:
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
                ext = Path(filename).suffix.lower()
                if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg']:
                    media_type = "image"
                elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']:
                    media_type = "video"
                else:
                    if "图片" in raw_type:
                        media_type = "image"
                    elif "视频" in raw_type:
                        media_type = "video"
                    elif "文件" in raw_type:
                        if ext in TEXT_FILE_EXTS:
                            media_type = "text_file"
                        else:
                            media_type = "binary_file"

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


# ==================== RFC3339 时间规范化 ====================
def ensure_rfc3339_time(expire_str: str, default_seconds: int = 3600) -> str:
    """
    确保时间字符串符合 RFC3339 格式，并带有时区（+08:00）。
    如果 expire_str 无效或缺失，则生成当前时间 + default_seconds 秒的时间。
    """
    beijing_tz = timezone(timedelta(hours=8))
    if expire_str:
        try:
            clean_str = expire_str.replace('Z', '+00:00').replace(' ', 'T')
            if '+' in clean_str or '-' in clean_str:
                dt = datetime.fromisoformat(clean_str)
            else:
                dt = datetime.fromisoformat(clean_str)
                dt = dt.replace(tzinfo=beijing_tz)
            dt = dt.replace(microsecond=0)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=beijing_tz)
            else:
                dt = dt.astimezone(beijing_tz)
            return dt.isoformat(timespec='seconds')
        except Exception as e:
            error(f"[时间修正] 解析输入时间失败 '{expire_str}': {e}，将使用默认时间")
    dt = datetime.now(beijing_tz) + timedelta(seconds=default_seconds)
    dt = dt.replace(microsecond=0)
    return dt.isoformat(timespec='seconds')


# ==================== 群管理（旧自动决策已废弃，仅保留空函数） ====================
async def handle_group_manage(thread_key: str, user_message: str, reply: str,
                              context_hist: List[Dict], raw_json: str, bot_client):
    # 此函数已不再使用，群管理完全由 AI 工具指令触发
    pass


# ==================== 消息解析 ====================
def parse_message(data: Dict, bot_appid: str = "") -> Dict:
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

    # 处理 ark_data，将 prompt 追加到 content
    ark_data = payload.get("ark_data", {})
    if ark_data and ark_data.get("prompt"):
        prompt = ark_data["prompt"].strip()
        if result["content"]:
            result["content"] += " [ARK:" + prompt + "]"
        else:
            result["content"] = "[ARK:" + prompt + "]"

    # 语音消息
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

    # 引用消息
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

    # 按事件类型解析
    if event_type == "C2C_MESSAGE_CREATE":
        author = payload.get("author", {})
        result["msg_type"] = "c2c"
        author_id = author.get("user_openid") or author.get("id")
        username = author.get("username", "")
        result["author_id"] = author_id
        result["username"] = username
        result["recipient_id"] = author_id
        if username and bot_appid:
            update_user_mapping(author_id, username, bot_appid)
        # 记录私聊用户ID
        if bot_appid and author_id:
            record_user_id(bot_appid, author_id)
    elif event_type in ("GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
        result["msg_type"] = "group"
        author = payload.get("author", {})
        author_id = author.get("member_openid") or author.get("id")
        username = author.get("username", "")
        result["author_id"] = author_id
        result["username"] = username
        result["recipient_id"] = payload.get("group_openid")
        # 记录群ID
        if bot_appid and result["recipient_id"]:
            record_group_id(bot_appid, result["recipient_id"])
        if username and bot_appid:
            update_user_mapping(author_id, username, bot_appid)
        for mention in result["mentions"]:
            if mention.get("bot") and mention.get("is_you"):
                result["is_at_me"] = True
                break

    if result["content"]:
        result["clean_content"] = re.sub(r'<@[^>]+>\s*', '', result["content"]).strip()
    else:
        result["clean_content"] = ""

    return result


# ==================== 冷却队列 ====================
pending_queues: Dict[str, List[Dict]] = {}
pending_timers: Dict[str, asyncio.Task] = {}
pending_process_tasks: Dict[str, asyncio.Task] = {}
# 存储被中断任务的上下文（含 messages 和 force_reply 标志）
pending_contexts: Dict[str, Dict[str, Any]] = {}
# 存储当前正在处理任务的 force_reply 状态（用于取消时继承）
task_force_reply: Dict[str, bool] = {}


async def process_queue(thread_key: str, bot_client: 'BotClient'):
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

        # 计算本队列是否强制回复（含 @ 或 c2c）
        force_reply = False
        for q in queue:
            q_mentions = q["parsed"].get("mentions", [])
            for m in q_mentions:
                if m.get("bot") and m.get("is_you"):
                    force_reply = True
                    break
            if force_reply:
                break
        if not force_reply:
            for q in queue:
                if q["parsed"]["msg_type"] == "c2c":
                    force_reply = True
                    break

        # 检查是否有被中断任务的上下文（含 force_reply 覆盖）
        saved_ctx = pending_contexts.pop(thread_key, None)
        initial_messages = saved_ctx.get("messages") if saved_ctx else None
        force_reply_override = saved_ctx.get("force_reply", False) if saved_ctx else False
        if force_reply_override:
            force_reply = True

        # 存储当前任务的 force_reply，以便取消时使用
        task_force_reply[thread_key] = force_reply

        await handle_processed_message(parsed, thread_key, merged_content, queue,
                                       bot_client, initial_messages, force_reply)

        # 正常结束，清理
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]

    except asyncio.CancelledError:
        info(f"[处理取消] 线程 {thread_key} 的处理被中断")
        # 获取该线程的 force_reply（从任务存储中取）
        force_reply = task_force_reply.get(thread_key, False)
        if thread_key in INTERRUPT_CONTEXT:
            pending_contexts[thread_key] = {
                "messages": INTERRUPT_CONTEXT[thread_key],
                "force_reply": force_reply
            }
            del INTERRUPT_CONTEXT[thread_key]
            info(f"[处理取消] 已保存线程 {thread_key} 的上下文（长度 {len(pending_contexts[thread_key]['messages'])}）和 force_reply={force_reply}")
        else:
            info(f"[处理取消] 线程 {thread_key} 无上下文可保存")
        # 清理任务存储
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]
        # 不重新抛出，任务结束
    except Exception as e:
        error(f"[处理异常] 线程 {thread_key} 处理消息时发生错误: {e}")
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]


async def handle_processed_message(parsed: Dict, thread_key: str, merged_content: str,
                                   queue: List[Dict], bot_client: 'BotClient',
                                   initial_messages: Optional[List[Dict]] = None,
                                   force_reply: bool = False):
    msg_type = parsed["msg_type"]
    recipient_id = parsed["recipient_id"]
    msg_id = parsed.get("msg_id")
    username = parsed["username"] or "用户"
    mentions = parsed.get("mentions", [])
    is_voice = parsed.get("is_voice", False)
    voice_text = parsed.get("voice_text", "")

    # 判断是否回复（使用传入的 force_reply）
    should_reply = False
    if force_reply:
        should_reply = True
        info("[强制回复] 根据 force_reply 标志强制回复")
    elif msg_type == "group" and parsed.get("is_at_me", False):
        should_reply = True
    elif msg_type == "group" and not parsed.get("is_at_me", False):
        recent_history = get_recent_history(thread_key, get_judge_context_limit())
        should_reply = await should_reply_in_group(recent_history, merged_content, mentions, bot_client.app_id)
        info(f"[AI Judge] 判定结果: {should_reply}")

    if not should_reply:
        info("[忽略] 不回复")
        return

    if asyncio.current_task().cancelled():
        info("[处理取消] 判断完成但任务已取消，放弃回复")
        return

    # 组装额外信息（引用、附件、语音）
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

    # --- 调用 AI 函数，内部完成发送和保存历史 ---
    reply, sent_success = await generate_reply(
        thread_key,
        full_user_input,
        username,
        msg_type,
        raw_json_str,
        bot_client,
        msg_id=msg_id,
        initial_messages=initial_messages,
        recipient_id=recipient_id   # 传递接收者ID
    )

    if asyncio.current_task().cancelled():
        info("[处理取消] 生成回复完成但任务已取消，放弃后续")
        return

    # --- 后续操作：记忆管理（压缩、长期记忆）等 ---
    # 注意：此时历史记录已在 generate_reply 内部保存，我们只需要处理后续的记忆优化任务
    full_hist = get_recent_history(thread_key, 6)
    if full_hist and full_hist[-1].get("role") == "assistant":
        above_hist = full_hist[:-1]
    else:
        above_hist = full_hist
    user_msg_for_memory = merged_content
    asyncio.create_task(
        auto_manage_memory(
            thread_key,
            user_msg_for_memory,
            reply,       # 回复文本（即使发送失败也有内容）
            above_hist,
            raw_json_str,
            bot_client
        )
    )
    # 可选：打印发送状态
    if sent_success:
        info(f"[处理完成] 消息已发送并保存历史")
    else:
        error(f"[处理完成] 历史已保存，但消息未发送（可能因 skip_reply 或发送失败）")


# ==================== 消息处理入口 ====================
async def handle_message(data: Dict, bot_client: 'BotClient'):
    if not get_bot_enabled(bot_client.app_id):
        info(f"[禁用] 机器人 {bot_client.app_id} 已禁用，忽略消息")
        return

    parsed = parse_message(data, bot_client.app_id)
    if not parsed["msg_type"]:
        return

    raw_content = parsed.get("content", "")
    decoded_content = decode_face_tags(raw_content)

    # 处理 msg_elements 中的引用内容
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

    # 聊天记录转发解析
    if '[群聊的聊天记录]' in decoded_content or '=== 消息' in decoded_content:
        info("[转发解析] 检测到聊天记录转发格式，开始解析...")
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
            info("[转发解析] 仅处理前5个无缓存媒体，其余忽略")

        # 并发处理转发聊天记录中的媒体
        async def process_forwarded_media(idx, media):
            url = media.get('url')
            filename = media.get('filename', '未知文件')
            media_type = media.get('type', 'unknown')
            height = media.get('height', 0)
            width = media.get('width', 0)

            if media_type in ('image', 'video'):
                summary = await recognize_media(media_type, url, filename, height, width)
                return (idx, f"[转发媒体识别结果: {filename}]\nURL: {url}\n{summary}")
            elif media_type == 'text_file':
                try:
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        get_executor(),
                        lambda: requests.get(url, timeout=10)
                    )
                    if response.status_code == 200:
                        encoding = response.apparent_encoding or 'utf-8'
                        content = response.content.decode(encoding, errors='replace')
                        max_len = 100 * 1024
                        if len(content) > max_len:
                            content = content[:max_len] + "\n... (文件内容过长，已截断)"
                        block = f"[转发文件: {filename}]\nURL: {url}\n=== 文件内容 ===\n{content}\n=== 文件内容结束 ==="
                        return (idx, block)
                    else:
                        return (idx, f"[转发文件: {filename}] 下载失败 (HTTP {response.status_code})\nURL: {url}")
                except Exception as e:
                    return (idx, f"[转发文件: {filename}] 下载异常: {e}\nURL: {url}")
            elif media_type == 'binary_file':
                return (idx, f"[转发文件: {filename}] 不支持的文件类型\nURL: {url}")
            else:
                return (idx, f"[转发附件: {filename}] 无法识别类型\nURL: {url}")

        media_tasks = []
        for idx, media in enumerate(media_list):
            process_this = media.get('_cached', False) or media in media_to_process
            if not process_this:
                placeholder = f"[MEDIA_PLACEHOLDER_{idx}]"
                decoded_content = decoded_content.replace(placeholder, f"[媒体附件: {media['filename']}] (超过处理限制，已忽略) URL: {media.get('url', '无')}")
                continue
            media_tasks.append(process_forwarded_media(idx, media))

        if media_tasks:
            results = await asyncio.gather(*media_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    error(f"[转发解析] 媒体处理异常: {res}")
                    continue
                idx, result_text = res
                placeholder = f"[MEDIA_PLACEHOLDER_{idx}]"
                decoded_content = decoded_content.replace(placeholder, result_text)

    # 处理当前消息的附件（并发处理）
    attachments = parsed.get("attachments", [])
    extra_content_parts = []
    loop = asyncio.get_event_loop()

    async def process_attachment(att):
        content_type = att.get("content_type", "")
        filename = att.get("filename", "未知文件")
        url = att.get("url")
        if not url:
            return None
        media_type = None
        if content_type.startswith("image/"):
            media_type = "image"
        elif content_type.startswith("video/"):
            media_type = "video"
        elif content_type == "file":
            media_type = get_media_type_from_filename(filename)

        if media_type:
            height = att.get("height", 0)
            width = att.get("width", 0)
            summary = await recognize_media(media_type, url, filename, height, width)
            if len(summary) < 50:
                summary = summary + "（摘要过短，可能识别不完整）"
            elif len(summary) > 600:
                summary = summary[:600] + "...（摘要过长，已截断）"
            if media_type == "image":
                return f"[收到图片：{filename}]\nURL: {url}\n===图片{filename}摘要开始===\n{summary}\n===图片{filename}摘要结束==="
            else:
                return f"[收到视频：{filename}]\nURL: {url}\n===视频{filename}摘要开始===\n{summary}\n===视频{filename}摘要结束==="
        elif content_type == "file" and is_text_file(filename):
            try:
                response = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.get(url, timeout=10)
                )
                if response.status_code == 200:
                    encoding = response.apparent_encoding or 'utf-8'
                    content = response.content.decode(encoding, errors='replace')
                    max_len = 100 * 1024
                    if len(content) > max_len:
                        content = content[:max_len] + "\n... (文件内容过长，已截断)"
                    return f"[文件：{filename}]\nURL: {url}\n=====文件内容：{filename}开始=====\n{content}\n=====文件内容：{filename}结束====="
                else:
                    return f"[文件：{filename}] 下载失败\nURL: {url}"
            except Exception as e:
                return f"[文件：{filename}] 下载异常: {e}\nURL: {url}"
        return f"[附件: {filename}]\nURL: {url}\n（无法自动解析此附件类型）"

    att_tasks = [process_attachment(att) for att in attachments if att.get("url")]
    if att_tasks:
        att_results = await asyncio.gather(*att_tasks, return_exceptions=True)
        for res in att_results:
            if isinstance(res, Exception):
                error(f"[附件处理] 异常: {res}")
            elif res:
                extra_content_parts.append(res)

    # 处理引用消息中的媒体（并发处理）
    ref_media = parsed.get("ref_media", [])

    async def process_ref_media(ref):
        content_type = ref.get("content_type", "")
        filename = ref.get("filename", "未知文件")
        url = ref.get("url")
        if not url:
            return None
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
                return f"[引用图片：{filename}]\nURL: {url}\n===图片{filename}摘要开始===\n{summary}\n===图片{filename}摘要结束==="
            else:
                return f"[引用视频：{filename}]\nURL: {url}\n===视频{filename}摘要开始===\n{summary}\n===视频{filename}摘要结束==="
        elif content_type == "file" and is_text_file(filename):
            try:
                response = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.get(url, timeout=10)
                )
                if response.status_code == 200:
                    encoding = response.apparent_encoding or 'utf-8'
                    content = response.content.decode(encoding, errors='replace')
                    max_len = 100 * 1024
                    if len(content) > max_len:
                        content = content[:max_len] + "\n... (文件内容过长，已截断)"
                    return f"[引用文件：{filename}]\nURL: {url}\n=====文件内容：{filename}开始=====\n{content}\n=====文件内容：{filename}结束====="
                else:
                    return f"[引用文件：{filename}] 下载失败\nURL: {url}"
            except Exception as e:
                return f"[引用文件：{filename}] 下载异常: {e}\nURL: {url}"
        return f"[引用附件: {filename}]\nURL: {url}\n（无法自动解析此附件类型）"

    ref_tasks = [process_ref_media(ref) for ref in ref_media if ref.get("url")]
    if ref_tasks:
        ref_results = await asyncio.gather(*ref_tasks, return_exceptions=True)
        for res in ref_results:
            if isinstance(res, Exception):
                error(f"[引用媒体处理] 异常: {res}")
            elif res:
                extra_content_parts.append(res)

    if extra_content_parts:
        if decoded_content:
            decoded_content = decoded_content + "\n" + "\n".join(extra_content_parts)
        else:
            decoded_content = "\n".join(extra_content_parts)

    # 语音消息标注
    is_voice = parsed.get("is_voice", False)
    voice_text = parsed.get("voice_text", "")
    if is_voice and voice_text:
        if not decoded_content:
            decoded_content = f"[语音：{voice_text}]"
        else:
            decoded_content = f"[语音：{voice_text}] {decoded_content}"

    # @ 替换为用户名
    display_content = replace_mentions_with_names(decoded_content, parsed.get("mentions", []))
    display_content = display_content.strip()

    if ref_contents:
        ref_display = ref_text[:200] + "..." if len(ref_text) > 200 else ref_text
        display_content = f"[引用内容: {ref_display}] {display_content}"

    author_id = parsed["author_id"]
    msg_username = parsed.get("username", "")
    display_username = get_display_name(author_id, msg_username, bot_client.app_id)
    user_identifier = f"{display_username}({author_id})"
    if msg_username:
        update_user_mapping(author_id, msg_username, bot_client.app_id)

    parsed["decoded_content"] = decoded_content
    parsed["clean_content"] = re.sub(r'<@[^>]+>\s*', '', decoded_content).strip()
    parsed["user_identifier"] = user_identifier

    msg_type = parsed["msg_type"]
    recipient_id = parsed["recipient_id"]
    msg_id = parsed["msg_id"]
    is_at_me = parsed["is_at_me"]

    info(f"[收到] {msg_type} | {user_identifier}: {display_content}")
    if parsed.get("mentions"):
        info(f"[提及详情] {json.dumps(parsed['mentions'], ensure_ascii=False)}")
    if is_voice:
        info(f"[语音] URL: {parsed.get('voice_url', '')}")

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

    # 取消旧的处理任务
    if thread_key in pending_process_tasks:
        old_task = pending_process_tasks[thread_key]
        if not old_task.done():
            old_task.cancel()
            info(f"[中断] 取消线程 {thread_key} 的旧处理任务")
        del pending_process_tasks[thread_key]

    # @机器人：立即处理
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

    # 普通消息：进入冷却队列
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
        cooldown = get_cooldown_seconds()
        await asyncio.sleep(cooldown)
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


# ==================== 事件处理（申请加群、成员加入/退出） ====================
async def handle_event(data: Dict, bot_client: 'BotClient'):
    event_type = data.get("t")
    payload = data.get("d", {})

    if event_type == "GROUP_JOIN_REQUEST":
        member_openid = payload.get("member_openid")
        username = payload.get("username")
        if member_openid and username:
            update_user_mapping(member_openid, username, bot_client.app_id)
            info(f"[事件] 保存申请人信息: {member_openid} -> {username}")
        return

    if event_type == "GROUP_MEMBER_ADD":
        group_openid = payload.get("group_openid")
        member_openid = payload.get("member_openid")
        if not group_openid or not member_openid:
            return
        username = get_user_name(member_openid, bot_client.app_id) or member_openid
        thread_key = get_thread_key("group", None, group_openid)
        join_msg = f"{username} 加入了群聊"
        append_message(thread_key, "user", join_msg)

        if get_bot_enabled(bot_client.app_id) and get_bot_auto_welcome(bot_client.app_id):
            parsed = {
                "author_id": member_openid,
                "username": username,
                "msg_type": "group",
                "recipient_id": group_openid,
                "content": join_msg
            }
            raw_json = json.dumps(parsed, ensure_ascii=False)
            reply = await generate_reply(thread_key, join_msg, username, "group", raw_json, bot_client, msg_id=None)
            if reply:
                success = await bot_client.send_message("group", group_openid, reply, msg_id=None)
                if success:
                    append_message(thread_key, "assistant", reply)
                    info(f"[事件] 自动欢迎回复发送成功: {reply}")
                else:
                    error("[事件] 自动欢迎回复发送失败")
            else:
                info("[事件] 生成回复为空，不发送")
        else:
            info(f"[事件] 自动欢迎关闭或机器人禁用，仅记录加入消息")
        return

    if event_type == "GROUP_MEMBER_REMOVE":
        group_openid = payload.get("group_openid")
        member_openid = payload.get("member_openid")
        if not group_openid or not member_openid:
            return
        username = get_user_name(member_openid, bot_client.app_id) or member_openid
        thread_key = get_thread_key("group", None, group_openid)
        leave_msg = f"{username} 退出了群聊"
        append_message(thread_key, "user", leave_msg)

        if get_bot_enabled(bot_client.app_id):
            recent_history = get_recent_history(thread_key, get_judge_context_limit())
            should = await should_reply_in_group(recent_history, leave_msg, [], bot_client.app_id)
            if should:
                parsed = {
                    "author_id": member_openid,
                    "username": "系统",
                    "msg_type": "group",
                    "recipient_id": group_openid,
                    "content": leave_msg
                }
                raw_json = json.dumps(parsed, ensure_ascii=False)
                reply = await generate_reply(thread_key, leave_msg, "系统", "group", raw_json, bot_client, msg_id=None)
                if reply:
                    success = await bot_client.send_message("group", group_openid, reply, msg_id=None)
                    if success:
                        append_message(thread_key, "assistant", reply)
                        info(f"[事件] 退出回复发送成功: {reply}")
                    else:
                        error("[事件] 退出回复发送失败")
                else:
                    info("[事件] 生成回复为空，不发送")
            else:
                info("[事件] Judge 判定无需回复退出消息")
        else:
            info(f"[事件] 机器人禁用，仅记录退出消息")
        return

    info(f"[未处理事件] {event_type}: {payload}")