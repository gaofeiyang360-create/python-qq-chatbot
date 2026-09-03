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
)
from ai import (
    generate_reply, should_reply_in_group, recognize_media, recognize_media_by_url,
    fetch_webpage_content, is_valid_url, summarize_content_if_needed,
    INTERRUPT_CONTEXT,
)

# 仅用于类型提示，避免运行时循环导入
from typing import TYPE_CHECKING
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
            print(f"[时间修正] 解析输入时间失败 '{expire_str}': {e}，将使用默认时间")
    dt = datetime.now(beijing_tz) + timedelta(seconds=default_seconds)
    dt = dt.replace(microsecond=0)
    return dt.isoformat(timespec='seconds')


# ==================== 群管理（旧自动决策已废弃，仅保留空函数） ====================
async def handle_group_manage(thread_key: str, user_message: str, reply: str,
                              context_hist: List[Dict], raw_json: str, bot_client):
    # 此函数已不再使用，群管理完全由 AI 工具指令触发
    pass


# ==================== 消息解析 ====================
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
        print(f"[处理取消] 线程 {thread_key} 的处理被中断")
        # 获取该线程的 force_reply（从任务存储中取）
        force_reply = task_force_reply.get(thread_key, False)
        if thread_key in INTERRUPT_CONTEXT:
            pending_contexts[thread_key] = {
                "messages": INTERRUPT_CONTEXT[thread_key],
                "force_reply": force_reply
            }
            del INTERRUPT_CONTEXT[thread_key]
            print(f"[处理取消] 已保存线程 {thread_key} 的上下文（长度 {len(pending_contexts[thread_key]['messages'])}）和 force_reply={force_reply}")
        else:
            print(f"[处理取消] 线程 {thread_key} 无上下文可保存")
        # 清理任务存储
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]
        # 不重新抛出，任务结束
    except Exception as e:
        print(f"[处理异常] 线程 {thread_key} 处理消息时发生错误: {e}")
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
        print("[强制回复] 根据 force_reply 标志强制回复")
    elif msg_type == "group" and parsed.get("is_at_me", False):
        should_reply = True
    elif msg_type == "group" and not parsed.get("is_at_me", False):
        recent_history = get_recent_history(thread_key, get_judge_context_limit())
        should_reply = await should_reply_in_group(recent_history, merged_content, mentions, bot_client.app_id)
        print(f"[AI Judge] 判定结果: {should_reply}")

    if not should_reply:
        print("[忽略] 不回复")
        return

    if asyncio.current_task().cancelled():
        print("[处理取消] 判断完成但任务已取消，放弃回复")
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
        print("[处理取消] 生成回复完成但任务已取消，放弃后续")
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
        print(f"[处理完成] 消息已发送并保存历史")
    else:
        print(f"[处理完成] 历史已保存，但消息未发送（可能因 skip_reply 或发送失败）")


# ==================== 消息处理入口 ====================
async def handle_message(data: Dict, bot_client: 'BotClient'):
    if not get_bot_enabled(bot_client.app_id):
        print(f"[禁用] 机器人 {bot_client.app_id} 已禁用，忽略消息")
        return

    parsed = parse_message(data)
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
            process_this = media.get('_cached', False) or media in media_to_process
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

            if media_type in ('image', 'video'):
                summary = await recognize_media(media_type, url, filename, height, width)
                decoded_content = decoded_content.replace(placeholder, f"[转发媒体识别结果: {filename}]\n{summary}")
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

    # 处理当前消息的附件
    attachments = parsed.get("attachments", [])
    extra_content_parts = []
    loop = asyncio.get_event_loop()

    for att in attachments:
        content_type = att.get("content_type", "")
        filename = att.get("filename", "未知文件")
        url = att.get("url")
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
                    block = f"[文件：{filename}] =====文件内容：{filename}开始=====\n{content}\n=====文件内容：{filename}结束====="
                    extra_content_parts.append(block)
                else:
                    extra_content_parts.append(f"[文件：{filename}] 下载失败")
            except Exception as e:
                extra_content_parts.append(f"[文件：{filename}] 下载异常: {e}")

    # 处理引用消息中的媒体
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
                    get_executor(),
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

    # 处理网页链接
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

    # 取消旧的处理任务
    if thread_key in pending_process_tasks:
        old_task = pending_process_tasks[thread_key]
        if not old_task.done():
            old_task.cancel()
            print(f"[中断] 取消线程 {thread_key} 的旧处理任务")
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
            update_user_mapping(member_openid, username)
            print(f"[事件] 保存申请人信息: {member_openid} -> {username}")
        return

    if event_type == "GROUP_MEMBER_ADD":
        group_openid = payload.get("group_openid")
        member_openid = payload.get("member_openid")
        if not group_openid or not member_openid:
            return
        username = get_user_name(member_openid) or member_openid
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
                    print(f"[事件] 自动欢迎回复发送成功: {reply}")
                else:
                    print("[事件] 自动欢迎回复发送失败")
            else:
                print("[事件] 生成回复为空，不发送")
        else:
            print(f"[事件] 自动欢迎关闭或机器人禁用，仅记录加入消息")
        return

    if event_type == "GROUP_MEMBER_REMOVE":
        group_openid = payload.get("group_openid")
        member_openid = payload.get("member_openid")
        if not group_openid or not member_openid:
            return
        username = get_user_name(member_openid) or member_openid
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
                        print(f"[事件] 退出回复发送成功: {reply}")
                    else:
                        print("[事件] 退出回复发送失败")
                else:
                    print("[事件] 生成回复为空，不发送")
            else:
                print("[事件] Judge 判定无需回复退出消息")
        else:
            print(f"[事件] 机器人禁用，仅记录退出消息")
        return

    print(f"[未处理事件] {event_type}: {payload}")