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
    get_bot_disable_ai_reply, get_media_block,
)
from memory import (
    update_user_mapping, get_user_name,
    append_message, get_recent_history, load_history,
    get_media_cache_key, get_media_cache_path,
    auto_manage_memory,
    record_user_id, record_group_id,
    save_group_to_mirror, get_group_name_from_mirror,
)
from ai import (
    generate_reply, should_reply_in_group, recognize_media, recognize_media_by_url,
    INTERRUPT_CONTEXT,
)
from utils import recognize_kind, RECOGNIZE_IMAGE_EXTS, VIDEO_EXTS

# 仅用于类型提示，避免运行时循环导入
from typing import TYPE_CHECKING
from log import info, warn, error, debug, LogCtx
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


# 媒体未识别时的占位文案（提示机器人可以自己调工具识别）
UNRECOGNIZED_HINT = "（未识别，可调用工具自行识别）"

# 中文类型名 → recognize_media 的 media_type 取值
TYPE_NAME_TO_RECOGNIZE_ARG = {
    "图片": "image",
    "视频": "video",
    "媒体": "image",
}


def unrecognized_media_block(kind: str, filename: str, url: str, prefix: str = "") -> str:
    """媒体未识别时写入上下文的占位块。

    kind: "图片" / "视频" / "媒体" 等中文类型名
    prefix: 可加前缀，如 "转发" / "引用"

    为什么要把 filename / media_type 显式写成"调用示例"：
      QQ 的媒体 URL 形如 .../download?appid=..&fileid=..&rkey=..，路径里**没有扩展名**，
      仅靠 URL 无法推断类型。工具 recognize_media_by_url 在拿不到 filename 时会直接返回
      "（内容无法获取/为未知文件格式）"。
      实测中模型只提取了 URL、丢掉文件名，导致识别必然失败 —— 文件名明明就在同一段
      上下文里。因此这里把参数名写清楚，让模型照抄即可，不依赖它自己推断。
    """
    label = f"{prefix}{kind}" if prefix else kind
    type_hint = TYPE_NAME_TO_RECOGNIZE_ARG.get(kind, "image")
    # 注意：QQ 媒体 URL 无扩展名，工具仅凭 URL 无法判断类型，因此把 filename 与
    # media_type 作为"照抄即可"的参数一并给出（见下方 docstring）。
    return (
        f"[收到{label}：{filename}]\n"
        f"URL: {url}\n"
        f"{UNRECOGNIZED_HINT}如需识别，调用 recognize_media 时请带上 "
        f'filename="{filename}" 与 media_type="{type_hint}"'
        f"（只传 media_url 会因 URL 无扩展名而失败）"
    )


# 后台继续跑的媒体识别任务（持有引用，防止被 GC 回收）
_background_media_tasks: set = set()


def _detach_to_background(task: "asyncio.Task", filename: str) -> None:
    """把识别任务转为后台执行：继续跑完、写缓存，异常只记日志。

    注意不在这里 await —— 调用方要立刻返回占位内容，不能等它。
    """
    _background_media_tasks.add(task)

    def _done(t):
        _background_media_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            error(f"[媒体识别] 后台识别失败 {filename}: {exc}", ctx=None)
        else:
            debug(f"[媒体识别] 后台识别完成并已写缓存: {filename}", ctx=None)

    task.add_done_callback(_done)


async def recognize_media_cancellable(media_type: str, url: str, filename: str,
                                      height: int, width: int, app_id: str,
                                      thread_key: str, gen_at_start: int):
    """
    媒体识别：是否阻塞由全局实时配置 MEDIA_BLOCK 决定。

    返回 (ok, value)：
      - ok=True  → value 是识别摘要（本轮直接就拿到了）
      - ok=False → value 固定为 "superseded"：本轮改用占位（URL + 照抄参数）

    MEDIA_BLOCK=1（阻塞，旧逻辑）：
      一直等到识别出结果才返回，期间不理会世代号变化。接收循环因此被占用，
      后续消息排队等待 —— 也就是改动之前的原始行为。

    MEDIA_BLOCK=0（不阻塞，默认）：
      1. 新消息一来就**停止等待**，本轮立刻用占位往下走，不再空等；
      2. **但识别任务不取消** —— 转交后台继续跑完并写入缓存（recognize_media 内部
         完成时会 set_cached_media）。这样 AI 之后按占位里的提示手动调
         recognize_media 时能直接命中缓存，不会重复请求视觉模型；
      3. 不设超时：识别一直跑到出结果为止；
      4. 完成即写：后台任务一完成就写缓存（由 recognize_media 负责，无需额外处理）；
      5. 允许多个后台识别并存：用户连发多张图时每张各自在后台跑，互不影响。
    """
    ctx = LogCtx(app_id=app_id, thread_key=thread_key)

    task = asyncio.ensure_future(
        recognize_media(media_type, url, filename, height, width, app_id=app_id))

    # 实时读取：每次识别都判断，改配置无需重启
    blocking = get_media_block() == 1

    # 登记"本会话有识别在跑"，供 handle_message 判断新消息是否真的打断了识别。
    media_inflight_start(thread_key)

    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=_MEDIA_POLL_INTERVAL)
            if task in done:
                return True, task.result()
            # 阻塞模式下忽略"来了新消息"，继续等识别结果（旧逻辑）
            if not blocking and media_should_abort(thread_key, gen_at_start):
                # 判定让位时再确认一次：可能在轮询间隔内刚好跑完，那就直接用结果。
                if task.done():
                    debug(f"[媒体识别] 判定让位时识别已完成，采用其结果: {filename}", ctx=ctx)
                    return True, task.result()
                # ★ 不 cancel：把任务转交后台，让它跑完并写缓存。
                _detach_to_background(task, filename)
                debug(f"[媒体识别] 新消息到达，本轮改用占位；识别转后台继续（完成后写缓存）: {filename}", ctx=ctx)
                return False, "superseded"
    except asyncio.CancelledError:
        # 上层（如整个机器人关闭）真的要求取消时，才取消底层请求。
        task.cancel()
        raise
    finally:
        # 无论正常返回、让位还是异常，都要撤销"有识别在跑"的登记。
        media_inflight_end(thread_key)

        # 仅在任务尚未完成、且**不是**我们主动转后台的情况下收尾。
        # 转后台的任务已登记在 _background_media_tasks，此处不能再 cancel。
        if not task.done() and task not in _background_media_tasks:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


_MEDIA_POLL_INTERVAL = 0.25   # 轮询间隔：平衡响应速度与开销


def is_image_or_video_file(filename: str) -> bool:
    return recognize_kind(filename) is not None


def get_media_type_from_filename(filename: str) -> Optional[str]:
    # 实现见 utils.recognize_kind（单一事实来源）
    return recognize_kind(filename)


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
                # 优先按扩展名判定（实现见 utils.recognize_kind）
                detected = recognize_kind(filename)
                if detected:
                    media_type = detected
                else:
                    # 扩展名无法判定时，回退到消息里 QQ 标注的类型
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
            warn(f"[时间修正] 解析输入时间失败 '{expire_str}': {e}，将使用默认时间", ctx=None)
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
        summary = ref_content_decoded if ref_content_decoded else "[非文本消息]"
        reply_info = {
            "msg_id": reply.get("msg_id"),
            "content": ref_content_decoded,
            "summary": summary,
        }
        if ref_media:
            reply_info["has_media"] = True
    else:
        msg_elements = payload.get("msg_elements", [])
        ref_contents = []
        ref_attachments = []
        for elem in msg_elements:
            # 处理 message_type==103 的标准引用，或没有 type 但有 content 的嵌套引用
            if elem.get("message_type") == 103 or (not elem.get("message_type") and elem.get("content")):
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
            summary = combined_content[:200] + "..." if len(combined_content) > 200 else combined_content
            reply_info = {
                "summary": summary,
                "content": combined_content,
                "attachments": ref_attachments if ref_attachments else [],
                "has_media": bool(ref_media),
            }

    result["reply_info"] = reply_info
    result["ref_media"] = ref_media

    # 提取 msg_idx / ref_msg_idx（来自 message_scene.ext）
    msg_idx = None
    ref_msg_idx = None
    message_scene = payload.get("message_scene", {})
    if isinstance(message_scene, dict):
        ext_list = message_scene.get("ext", [])
        if isinstance(ext_list, list):
            for ext_item in ext_list:
                if isinstance(ext_item, str):
                    if ext_item.startswith("msg_idx="):
                        msg_idx = ext_item[len("msg_idx="):]
                    elif ext_item.startswith("ref_msg_idx="):
                        ref_msg_idx = ext_item[len("ref_msg_idx="):]
    result["msg_idx"] = msg_idx
    result["ref_msg_idx"] = ref_msg_idx

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
        # 记录群ID及当前成员
        if bot_appid and result["recipient_id"]:
            record_group_id(bot_appid, result["recipient_id"], author_id)
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
# 隔离模式：正在唤醒中的会话集合。
# 唤醒任务处于隔离模式时登记在此，用户消息到达时不会取消它（唤醒不被打断），
# 用户消息正常入队，等唤醒结束后再按冷却队列处理。
wakeup_locked_threads: Dict[str, int] = {}

# ==================== 媒体识别中断 ====================
# 识别世代号：每当同一会话有新消息到达就 +1。
# 识别开始前记录当时的世代号，识别过程中世代号变了说明"后面又有消息"，
# 此时立即放弃识别，只把 URL 留给机器人自己用工具识别（见 recognize_media_nonblocking）。
_media_gen: Dict[str, int] = {}


def bump_media_gen(thread_key: str) -> int:
    """该会话有新消息到达：递增识别世代号，通知正在进行的识别尽快放弃。"""
    _media_gen[thread_key] = _media_gen.get(thread_key, 0) + 1
    return _media_gen[thread_key]


def get_media_gen(thread_key: str) -> int:
    return _media_gen.get(thread_key, 0)


def media_should_abort(thread_key: str, gen_at_start: int) -> bool:
    """识别是否应中止：世代号已变（期间来了新消息）。"""
    return bool(thread_key) and _media_gen.get(thread_key, 0) != gen_at_start


# 当前正在等待结果的识别数（按会话统计）。
# 只有 >0 才说明"上一条识别真的还在跑"，此时新消息到来才谈得上"让其让位"。
# 纯文本消息不会经过 recognize_media_cancellable，因此不会误报。
_media_inflight: Dict[str, int] = {}


def media_inflight_start(thread_key: str) -> None:
    """一次识别开始等待：计数 +1。"""
    if thread_key:
        _media_inflight[thread_key] = _media_inflight.get(thread_key, 0) + 1


def media_inflight_end(thread_key: str) -> None:
    """一次识别结束（出结果 / 让位 / 异常）：计数 -1，归零即清理。"""
    if not thread_key:
        return
    left = _media_inflight.get(thread_key, 0) - 1
    if left > 0:
        _media_inflight[thread_key] = left
    else:
        _media_inflight.pop(thread_key, None)


def media_inflight_count(thread_key: str) -> int:
    """该会话当前有多少个识别正在等待结果。"""
    return _media_inflight.get(thread_key, 0)


def is_wakeup_locked(thread_key: str) -> bool:
    """该会话是否正处于隔离唤醒中（此时用户消息不应打断唤醒处理）"""
    return thread_key in wakeup_locked_threads


def lock_wakeup(thread_key: str):
    """登记隔离唤醒开始"""
    if not thread_key:
        return
    wakeup_locked_threads[thread_key] = wakeup_locked_threads.get(thread_key, 0) + 1


def unlock_wakeup(thread_key: str):
    """登记隔离唤醒结束"""
    if not thread_key:
        return
    left = wakeup_locked_threads.get(thread_key, 0) - 1
    if left > 0:
        wakeup_locked_threads[thread_key] = left
    else:
        wakeup_locked_threads.pop(thread_key, None)


def flush_pending_after_wakeup(thread_key: str, bot_client: 'BotClient'):
    """隔离唤醒结束后，处理唤醒期间堆积的用户消息。

    这些消息在唤醒期间只入队、未处理，此处按原有冷却队列逻辑重新启动处理。
    """
    ctx = LogCtx(app_id=bot_client.app_id, thread_key=thread_key)

    if not thread_key or is_wakeup_locked(thread_key):
        return
    if not pending_queues.get(thread_key):
        return
    # 唤醒任务本身不在 pending_process_tasks 中，正常启动即可
    if thread_key in pending_timers:
        pending_timers[thread_key].cancel()
        del pending_timers[thread_key]
    if thread_key in pending_process_tasks:
        old_task = pending_process_tasks[thread_key]
        if not old_task.done():
            old_task.cancel()
        del pending_process_tasks[thread_key]
    task = asyncio.create_task(process_queue(thread_key, bot_client))
    pending_process_tasks[thread_key] = task
    info(f"[隔离唤醒] 唤醒结束，已启动线程 {thread_key} 积压用户消息的处理", ctx=ctx)


async def process_queue(thread_key: str, bot_client: 'BotClient'):
    ctx = LogCtx(app_id=bot_client.app_id, thread_key=thread_key)

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
        info(f"[处理取消] 线程 {thread_key} 的处理被中断", ctx=ctx)
        # 获取该线程的 force_reply（从任务存储中取）
        force_reply = task_force_reply.get(thread_key, False)
        if thread_key in INTERRUPT_CONTEXT:
            pending_contexts[thread_key] = {
                "messages": INTERRUPT_CONTEXT[thread_key],
                "force_reply": force_reply
            }
            del INTERRUPT_CONTEXT[thread_key]
            info(f"[处理取消] 已保存线程 {thread_key} 的上下文（长度 {len(pending_contexts[thread_key]['messages'])}）和 force_reply={force_reply}", ctx=ctx)
        else:
            info(f"[处理取消] 线程 {thread_key} 无上下文可保存", ctx=ctx)
        # 清理任务存储
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]
        # 不重新抛出，任务结束
    except Exception as e:
        error(f"[处理异常] 线程 {thread_key} 处理消息时发生错误: {e}", ctx=ctx)
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]


async def handle_processed_message(parsed: Dict, thread_key: str, merged_content: str,
                                   queue: List[Dict], bot_client: 'BotClient',
                                   initial_messages: Optional[List[Dict]] = None,
                                   force_reply: bool = False):
    ctx = LogCtx(app_id=bot_client.app_id, thread_key=thread_key)

    msg_type = parsed["msg_type"]
    recipient_id = parsed["recipient_id"]
    msg_id = parsed.get("msg_id")
    username = parsed["username"] or "用户"
    mentions = parsed.get("mentions", [])
    is_voice = parsed.get("is_voice", False)
    voice_text = parsed.get("voice_text", "")

    # ==================== 实时开关：关闭 AI 回复 ====================
    # 开启后跳过 Judge 判定与全部 AI/工具调用，不做任何回复；
    # 消息记录已在 handle_message 中完成，此处仅继续执行摘要/记忆整理。
    if get_bot_disable_ai_reply(bot_client.app_id):
        info(f"[AI回复已关闭] 线程 {thread_key} 仅记录，不回复", ctx=ctx)
        asyncio.create_task(auto_manage_memory(thread_key, bot_client))
        return

    # 判断是否回复（使用传入的 force_reply）
    should_reply = False
    if force_reply:
        should_reply = True
        debug("[强制回复] 根据 force_reply 标志强制回复", ctx=ctx)
    elif msg_type == "group" and parsed.get("is_at_me", False):
        should_reply = True
    elif msg_type == "group" and not parsed.get("is_at_me", False):
        recent_history = get_recent_history(thread_key, get_judge_context_limit())
        should_reply = await should_reply_in_group(recent_history, merged_content, mentions, bot_client.app_id, bot_client.bot_name)
        info(f"[AI Judge] 判定结果: {should_reply}", ctx=ctx)

    if not should_reply:
        debug("[忽略] 不回复", ctx=ctx)
        return

    if asyncio.current_task().cancelled():
        info("[处理取消] 判断完成但任务已取消，放弃回复", ctx=ctx)
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
    reply, sent_success, skip_reply_called = await generate_reply(
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
        info("[处理取消] 生成回复完成但任务已取消，放弃后续", ctx=ctx)
        return

    # --- 后续操作：记忆管理（压缩、长期记忆）等 ---
    # 注意：此时历史记录已在 generate_reply 内部保存，我们只需要处理后续的记忆优化任务
    asyncio.create_task(
        auto_manage_memory(
            thread_key,
            bot_client
        )
    )
    # 可选：打印发送状态
    if skip_reply_called:
        info(f"[处理完成] skip_reply：未发送且未写入助手回复记录", ctx=ctx)
    elif sent_success:
        info(f"[处理完成] 消息已发送并保存历史", ctx=ctx)
    else:
        info(f"[处理完成] 历史已保存，但消息发送失败（非 skip_reply 导致的失败）", ctx=ctx)


# ==================== 群信息记录（user_map.json + mirror.json） ====================
async def record_group_info(bot_client, group_openid: str, member_id: str = None):
    """
    记录群ID到 user_map.json（含成员），同时获取群名称并记录到 mirror.json。
    若 mirror.json 中已有该群信息则跳过 API 调用（缓存）。
    """
    ctx = LogCtx(app_id=bot_client.app_id)

    app_id = bot_client.app_id
    # 先记录到 user_map.json（无论是否获取到名称）
    record_group_id(app_id, group_openid, member_id)

    # 检查 mirror.json 是否已有缓存
    cached_name = get_group_name_from_mirror(app_id, group_openid)
    if cached_name:
        return  # 已有缓存，无需再次请求 API

    # 未缓存：调用 API 获取群名称
    debug(f"[群记录] 尝试获取群信息: {group_openid}", ctx=ctx)
    group_data = await bot_client.get_group_info(group_openid)
    if group_data and group_data.get("group_name"):
        group_name = group_data["group_name"]
        save_group_to_mirror(app_id, group_openid, group_name)
    else:
        # API 失败时不记录，下次收到该群消息会再次尝试
        warn(f"[群记录] 获取群名称失败，暂不记录: {group_openid}", ctx=ctx)


# ==================== 消息处理入口 ====================
def format_referenced_message(thread_key: str, ref_msg_idx: str) -> Optional[Dict]:
    """
    根据 ref_msg_idx 在聊天历史中查找被引用的原消息。
    返回 {"content": "完整内容", "role": "user"|"assistant"}，找不到则返回 None。
    """
    if not ref_msg_idx:
        return None
    try:
        hist = load_history(thread_key)
        for msg in hist:
            if msg.get("msg_idx") == ref_msg_idx:
                raw = msg.get("content", "")
                # 去掉时间戳前缀 [YYYY-MM-DD HH:MM]
                cleaned = re.sub(r'^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}\]', '', raw).strip()
                role = msg.get("role", "user")
                return {"content": cleaned, "role": role}
    except Exception:
        pass
    return None


async def handle_message(data: Dict, bot_client: 'BotClient'):
    ctx = LogCtx(app_id=bot_client.app_id)

    if not get_bot_enabled(bot_client.app_id):
        info(f"[禁用] 机器人 {bot_client.app_id} 已禁用，忽略消息", ctx=ctx)
        return

    parsed = parse_message(data, bot_client.app_id)
    if not parsed["msg_type"]:
        return

    # ==================== 实时开关：关闭 AI 回复 ====================
    # 开启后完全关闭媒体识别（不调视觉模型、不写 media_cache 缓存文件），
    # 后续仅做消息/群/用户/群员记录与历史保存。开关实时读取，改完即生效。
    ai_reply_disabled = get_bot_disable_ai_reply(bot_client.app_id)
    if ai_reply_disabled:
        debug(f"[AI回复已关闭] 机器人 {bot_client.app_id} 处于仅记录模式，媒体识别已完全关闭", ctx=ctx)

    raw_content = parsed.get("content", "")
    decoded_content = decode_face_tags(raw_content)

    # 提前算出 thread_key —— 媒体识别需要用它与"是否来了新消息"做判断。
    # （原先在识别之后才计算，这里前移，后面 927 行附近不再重复计算）
    if parsed.get("msg_type") == "c2c":
        thread_key = get_thread_key("c2c", parsed.get("author_id"))
    else:
        thread_key = get_thread_key("group", None, parsed.get("recipient_id"))

    # 记录本次处理的识别世代号：识别期间该值若变化，说明后面又来了消息。
    #
    # ★ 必须在这里（识别之前）就递增世代号，而不是等函数尾部。
    # client.py 的接收循环是 `await handle_message(...)`，同一会话的消息严格串行：
    # 若等识别结束再 bump，第二条消息根本走不到那一行 —— 它被第一条的识别堵在
    # 接收循环里，形成死锁（第一条等"新消息"打断，新消息又等第一条让路）。
    # 所以每条消息一进来就先宣告"有新消息到达"。
    media_gen_before = get_media_gen(thread_key)
    media_gen_at_start = bump_media_gen(thread_key)
    # 只有该会话确实有识别正在等待结果时，新消息才谈得上"打断上一条"。
    # 纯文本消息、上一条识别已跑完等情况都不会命中，避免日志噪音。
    inflight = media_inflight_count(thread_key)
    if media_gen_before != media_gen_at_start and media_gen_before > 0 and inflight > 0:
        info(f"[媒体识别] 新消息到达，打断 {inflight} 个正在进行的识别，改用占位"
             f"（识别转后台继续并写缓存）", ctx=ctx)

    # 引用内容已由 parse_message() 处理并存入 reply_info，
    # 后续在 process_queue() 中通过 extra_info 统一附加，避免重复
    ref_contents = []
    ref_text = ""

    # 聊天记录转发解析
    if '[群聊的聊天记录]' in decoded_content or '=== 消息' in decoded_content:
        info("[转发解析] 检测到聊天记录转发格式，开始解析...", ctx=ctx)
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
            warn("[转发解析] 仅处理前5个无缓存媒体，其余忽略", ctx=ctx)

        # 并发处理转发聊天记录中的媒体
        async def process_forwarded_media(idx, media):
            url = media.get('url')
            filename = media.get('filename', '未知文件')
            media_type = media.get('type', 'unknown')
            height = media.get('height', 0)
            width = media.get('width', 0)

            if media_type in ('image', 'video'):
                ok, summary = await recognize_media_cancellable(
                    media_type, url, filename, height, width,
                    bot_client.app_id, thread_key, media_gen_at_start)
                if not ok:
                    # 识别被打断/超时：只保留 URL，让机器人自己用工具识别
                    return (idx, unrecognized_media_block(
                        "图片" if media_type == "image" else "视频",
                        filename, url, prefix="转发"))
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
            # 关闭 AI 回复时：完全跳过转发媒体识别，仅记录文件名与 URL
            if ai_reply_disabled:
                placeholder = f"[MEDIA_PLACEHOLDER_{idx}]"
                decoded_content = decoded_content.replace(
                    placeholder,
                    f"[转发媒体: {media['filename']}] (已关闭AI回复，未识别) URL: {media.get('url', '无')}"
                )
                continue
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
                    error(f"[转发解析] 媒体处理异常: {res}", ctx=None)
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
            # 关闭 AI 回复时：完全跳过媒体识别（不调视觉模型、不写缓存），仅记录文件信息
            if ai_reply_disabled:
                return f"[附件: {filename}]\nURL: {url}\n（已关闭AI回复，未识别媒体内容）"
            height = att.get("height", 0)
            width = att.get("width", 0)
            ok, summary = await recognize_media_cancellable(
                media_type, url, filename, height, width,
                bot_client.app_id, thread_key, media_gen_at_start)
            if not ok:
                # 识别被打断/超时：只保留 URL，让机器人自己用工具识别
                return unrecognized_media_block(
                    "图片" if media_type == "image" else "视频", filename, url)
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
                error(f"[附件处理] 异常: {res}", ctx=None)
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
            # 关闭 AI 回复时：完全跳过媒体识别（不调视觉模型、不写缓存），仅记录文件信息
            if ai_reply_disabled:
                return f"[引用附件: {filename}]\nURL: {url}\n（已关闭AI回复，未识别媒体内容）"
            height = ref.get("height", 0)
            width = ref.get("width", 0)
            ok, summary = await recognize_media_cancellable(
                media_type, url, filename, height, width,
                bot_client.app_id, thread_key, media_gen_at_start)
            if not ok:
                # 识别被打断/超时：只保留 URL，让机器人自己用工具识别
                return unrecognized_media_block(
                    "图片" if media_type == "image" else "视频",
                    filename, url, prefix="引用")
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
                error(f"[引用媒体处理] 异常: {res}", ctx=None)
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

    # 此时已知 app_id / thread_key / msg_id，先构造 ctx 供本函数后续日志使用
    ctx = LogCtx(app_id=bot_client.app_id, thread_key=thread_key, msg_id=msg_id)

    info(f"[收到] {msg_type} | {user_identifier}: {display_content}", ctx=ctx)
    if parsed.get("mentions"):
        debug(f"[提及详情] {json.dumps(parsed['mentions'], ensure_ascii=False)}", ctx=ctx)
    if is_voice:
        debug(f"[语音] URL: {parsed.get('voice_url', '')}", ctx=ctx)

    if msg_type != "c2c":
        # 异步记录群信息（不阻塞主流程）
        asyncio.create_task(record_group_info(bot_client, parsed["recipient_id"], author_id))

    # 补上会话显示名（群名/用户名），派生新 ctx 供后续日志使用
    thread_display = ""
    if msg_type == "group":
        thread_display = get_group_name_from_mirror(bot_client.app_id, parsed["recipient_id"]) or ""
    elif msg_type == "c2c":
        thread_display = get_display_name(author_id, msg_username, bot_client.app_id)
    ctx = ctx.with_(thread_display=thread_display)

    store_content = f"{user_identifier}: {decoded_content}" if decoded_content else f"{user_identifier} 发送了附件或引用"

    # 尝试从历史中查找被引用的原消息完整内容
    ref_original = None
    if parsed.get("ref_msg_idx"):
        ref_original = format_referenced_message(thread_key, parsed["ref_msg_idx"])

    if ref_original:
        label = "引用原机器人消息" if ref_original["role"] == "assistant" else "引用原消息"
        store_content += f"\n[{label}: {ref_original['content']}]"
    elif parsed.get("reply_info"):
        ref_text_full = parsed["reply_info"].get("content", "") or parsed["reply_info"].get("summary", "")
        store_content += f"\n[引用: {ref_text_full}]"
    elif parsed.get("ref_msg_idx"):
        # 有 ref_msg_idx（临时引用）但查不到原消息内容
        store_content += "\n[引用：引用内容不可用]"
    append_message(thread_key, "user", store_content,
                   msg_id=msg_id,
                   msg_idx=parsed.get("msg_idx"),
                   ref_msg_idx=parsed.get("ref_msg_idx"))

    # 取消旧的处理任务
    # 隔离唤醒期间：不取消、也不启动新任务，用户消息只入队，等唤醒结束后再处理
    if is_wakeup_locked(thread_key):
        if thread_key not in pending_queues:
            pending_queues[thread_key] = []
        pending_queues[thread_key].append({
            "parsed": parsed,
            "user_identifier": user_identifier,
            "decoded_content": decoded_content,
            "msg_id": msg_id,
        })
        info(f"[隔离唤醒] 线程 {thread_key} 正在唤醒中，用户消息已入队等待，不打断唤醒处理", ctx=None)
        return

    if thread_key in pending_process_tasks:
        old_task = pending_process_tasks[thread_key]
        if not old_task.done():
            old_task.cancel()
            info(f"[中断] 取消线程 {thread_key} 的旧处理任务", ctx=None)
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
    ctx = LogCtx(app_id=bot_client.app_id)

    event_type = data.get("t")
    payload = data.get("d", {})

    # 注意：本函数按事件类型分别构造带 thread_key 的 ctx，
    # 事件级的日志显式传 ctx，避免沿用上一条消息的会话标识。

    if event_type == "GROUP_JOIN_REQUEST":
        member_openid = payload.get("member_openid")
        username = payload.get("username")
        if member_openid and username:
            update_user_mapping(member_openid, username, bot_client.app_id)
            info(f"[事件] 保存申请人信息: {member_openid} -> {username}", ctx=ctx)
        return

    if event_type == "GROUP_MEMBER_ADD":
        group_openid = payload.get("group_openid")
        member_openid = payload.get("member_openid")
        if not group_openid or not member_openid:
            return
        username = get_user_name(member_openid, bot_client.app_id) or member_openid
        thread_key = get_thread_key("group", None, group_openid)
        # 本事件的日志标识（带群名），供后续日志显式使用
        group_name = get_group_name_from_mirror(bot_client.app_id, group_openid) or ""
        ctx = ctx.with_(thread_key=thread_key, thread_display=group_name)
        join_msg = f"{username} 加入了群聊"
        append_message(thread_key, "user", join_msg)

        if get_bot_enabled(bot_client.app_id) and get_bot_auto_welcome(bot_client.app_id) \
                and not get_bot_disable_ai_reply(bot_client.app_id):
            parsed = {
                "author_id": member_openid,
                "username": username,
                "msg_type": "group",
                "recipient_id": group_openid,
                "content": join_msg
            }
            raw_json = json.dumps(parsed, ensure_ascii=False)
            reply_text, _, skip_reply_called = await generate_reply(
                thread_key, join_msg, username, "group", raw_json, bot_client, msg_id=None)
            # skip_reply 表示 AI 主动决定不回复：此时 generate_reply 返回的是
            # 兜底道歉文案，它并没有发给用户，也不能由这里补发（否则就违背了 skip 的意图）
            if skip_reply_called:
                info("[事件] AI 调用 skip_reply，跳过自动欢迎回复", ctx=ctx)
            elif reply_text:
                success = await bot_client.send_message("group", group_openid, reply_text, msg_id=None)
                if success:
                    send_id = bot_client.get_last_send_id()
                    send_msg_idx = bot_client.get_last_send_msg_idx()
                    append_message(thread_key, "assistant", reply_text,
                                   msg_id=send_id, msg_idx=send_msg_idx)
                    info(f"[事件] 自动欢迎回复发送成功: {reply_text}, msg_id={send_id}", ctx=ctx)
                else:
                    error("[事件] 自动欢迎回复发送失败", ctx=ctx)
            else:
                debug("[事件] 生成回复为空，不发送", ctx=ctx)
        else:
            info(f"[事件] 自动欢迎关闭、AI回复已关闭或机器人禁用，仅记录加入消息", ctx=ctx)
        return

    if event_type == "GROUP_MEMBER_REMOVE":
        group_openid = payload.get("group_openid")
        member_openid = payload.get("member_openid")
        if not group_openid or not member_openid:
            return
        username = get_user_name(member_openid, bot_client.app_id) or member_openid
        thread_key = get_thread_key("group", None, group_openid)
        # 本事件的日志标识（带群名），供后续日志显式使用
        group_name = get_group_name_from_mirror(bot_client.app_id, group_openid) or ""
        ctx = ctx.with_(thread_key=thread_key, thread_display=group_name)
        leave_msg = f"{username} 退出了群聊"
        append_message(thread_key, "user", leave_msg)

        if get_bot_enabled(bot_client.app_id) and not get_bot_disable_ai_reply(bot_client.app_id):
            recent_history = get_recent_history(thread_key, get_judge_context_limit())
            should = await should_reply_in_group(recent_history, leave_msg, [], bot_client.app_id, bot_client.bot_name)
            if should:
                parsed = {
                    "author_id": member_openid,
                    "username": "系统",
                    "msg_type": "group",
                    "recipient_id": group_openid,
                    "content": leave_msg
                }
                raw_json = json.dumps(parsed, ensure_ascii=False)
                reply_text, _, skip_reply_called = await generate_reply(
                    thread_key, leave_msg, "系统", "group", raw_json, bot_client, msg_id=None)
                # 同自动欢迎：skip_reply 时不得补发兜底文案
                if skip_reply_called:
                    info("[事件] AI 调用 skip_reply，跳过退出回复", ctx=ctx)
                elif reply_text:
                    success = await bot_client.send_message("group", group_openid, reply_text, msg_id=None)
                    if success:
                        send_id = bot_client.get_last_send_id()
                        send_msg_idx = bot_client.get_last_send_msg_idx()
                        append_message(thread_key, "assistant", reply_text,
                                       msg_id=send_id, msg_idx=send_msg_idx)
                        info(f"[事件] 退出回复发送成功: {reply_text}, msg_id={send_id}", ctx=ctx)
                    else:
                        error("[事件] 退出回复发送失败", ctx=ctx)
                else:
                    debug("[事件] 生成回复为空，不发送", ctx=ctx)
            else:
                debug("[事件] Judge 判定无需回复退出消息", ctx=ctx)
        else:
            info(f"[事件] 机器人禁用或AI回复已关闭，仅记录退出消息", ctx=ctx)
        return

    info(f"[未处理事件] {event_type}: {payload}", ctx=ctx)