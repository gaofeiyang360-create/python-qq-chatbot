# -*- coding: utf-8 -*-
# ai.py — AI 相关（纯请求：对话调用、媒体识别、群聊判定、回复生成、摘要、网页抓取）
import re
import time
import asyncio
import requests
import json as _json
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

from config import (
    get_model_config, get_global_system_prompt, get_bot_system_prompt,
    get_bot_name, set_bot_name, get_executor, is_group_manage_enabled,
    get_bot_enable_tools, get_bot_max_tool_rounds,
)
from memory import (
    get_global_memory, get_global_memory_enabled,
    get_qun_memory_list, get_qun_memory_enabled,
    get_c2c_memory_list, get_c2c_memory_enabled,
    get_bot_memory_list, get_bot_memory_enabled,
    get_history, load_history, save_history, get_compress_threshold,
    get_cached_media_summary, set_cached_media, get_media_cache_key, get_url_cache_key,
    compute_similarity,
    append_message,
)

# ========== 从 tool 导入工具相关函数和常量 ==========
from tool import (
    get_tools_definition,
    get_tools_description,
    execute_tool_call,
    DEFAULT_HEADERS,
)

# ==================== 新增：存储每个线程当前任务的消息上下文（用于打断保存） ====================
INTERRUPT_CONTEXT: Dict[str, List[Dict]] = {}

# ==================== 核心 AI 调用 ====================
async def call_ai(messages: List[Dict], model_key: str, stream: bool = False,
                  temperature: float = 0.7) -> str:
    model_cfg = get_model_config(model_key)
    base_url = model_cfg["base_url"]
    api_key = model_cfg["api_key"]
    model_name = model_cfg["model_name"]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model_name,
        "messages": messages,
        "stream": stream,
        "temperature": temperature,
        "top_p": 0.9,
        "max_tokens": 3000,
    }
    loop = asyncio.get_event_loop()
    try:
        start_time = time.time()
        print(f"[AI调用] 开始请求，模型: {model_key} -> {model_name}")
        response = await loop.run_in_executor(
            get_executor(),
            lambda: requests.post(base_url, json=payload, headers=headers, timeout=360)
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

# ==================== 带工具的 AI 调用 ====================
async def call_ai_with_tools(messages: List[Dict], model_key: str,
                             tools: List[Dict], temperature: float = 0.7) -> Dict:
    """
    调用 AI 并支持 function calling，返回完整的 message dict（含 content 和 tool_calls）。
    """
    model_cfg = get_model_config(model_key)
    base_url = model_cfg["base_url"]
    api_key = model_cfg["api_key"]
    model_name = model_cfg["model_name"]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model_name,
        "messages": messages,
        "tools": tools,
        "tool_choice": "auto",
        "temperature": temperature,
        "top_p": 0.9,
        "max_tokens": 3000,
    }
    loop = asyncio.get_event_loop()
    try:
        start_time = time.time()
        print(f"[AI调用-工具] 开始请求，模型: {model_key} -> {model_name}，工具数: {len(tools)}")
        response = await loop.run_in_executor(
            get_executor(),
            lambda: requests.post(base_url, json=payload, headers=headers, timeout=360)
        )
        elapsed = time.time() - start_time
        print(f"[AI调用-工具] 请求完成，耗时 {elapsed:.2f} 秒")
        response.raise_for_status()
        data = response.json()
        if data.get("choices") and len(data["choices"]) > 0:
            msg = data["choices"][0].get("message", {})
            return {
                "role": "assistant",
                "content": msg.get("content", "") or "",
                "tool_calls": msg.get("tool_calls", [])
            }
        return {"role": "assistant", "content": "（AI 未返回有效内容）", "tool_calls": []}
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            print(f"[AI调用-工具错误] 状态码: {e.response.status_code}, 响应: {e.response.text}")
        else:
            print(f"[AI调用-工具错误] {e}")
        raise

# ==================== 媒体识别（基础 AI 能力，保留在此） ====================
async def recognize_media_by_url(media_url: str, filename: str = "媒体",
                                 media_type: str = None, disable_cache: bool = False) -> str:
    cache_key = get_url_cache_key(media_url)
    if not disable_cache:
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
        result = await recognize_media(media_type, media_url, filename, 0, 0, disable_cache=disable_cache)
        # recognize_media 内部会写入缓存，这里不用再写
        return result
    except Exception as e:
        print(f"[媒体识别] URL 识别失败: {e}")
        error_msg = f"（媒体识别失败: {e}）"
        set_cached_media(cache_key, error_msg, media_type, filename, media_url)
        return error_msg

async def recognize_media(media_type: str, media_url: str, filename: str = "媒体",
                          height: int = 0, width: int = 0, disable_cache: bool = False) -> str:
    cache_key = get_media_cache_key(media_type, filename, height, width)
    if not disable_cache:
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
        result = await call_ai(messages, "vision", temperature=0.3)
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

# ==================== 群聊是否需要回复判定 ====================
async def should_reply_in_group(history: List[Dict], current_message: str,
                                mentions: List[Dict], app_id: str) -> bool:
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
    global_sys = get_global_system_prompt()
    bot_sys = get_bot_system_prompt(app_id)
    combined_sys = f"{global_sys}\n{bot_sys}" if bot_sys else global_sys

    judge_prompt = (
        f"你是一个群聊助手，需要判断机器人是否应该介入回复当前这条消息。\n"
        f"机器人的人设和系统提示如下：\n{combined_sys}\n\n"
        "请先阅读以下【上文】（之前的对话历史），然后重点关注【当前消息】。\n"
        f"机器人的名字是 {get_bot_name()}。如果当前消息或上文中明确提到这个机器人名字，或者请求机器人帮助，则应当回复。\n"
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
        result = await call_ai(judge_messages, "judge", stream=False, temperature=0.2)
        result_clean = result.strip().lower()
        print(f"[AI Judge 结果] {result_clean}")
        return "是" in result_clean or "yes" in result_clean
    except Exception as e:
        print(f"[AI Judge Error] {e}")
        return True

# ==================== 对话摘要生成与插入 ====================
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
            threshold = get_compress_threshold()
            if len(hist) - start_idx <= threshold:
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
                    "judge",
                    temperature=0.3
                )
                if summary and "（AI 未返回有效内容）" not in summary:
                    summary_text = summary
            except Exception as e:
                print(f"[摘要生成] 尝试 {attempt+1}/{retries} 失败: {e}")

            if summary_text is None:
                summary_text = "（摘要生成失败，请稍后重试）"

            # 二次检查并插入
            hist = load_history(thread_key)
            last_summary_idx = -1
            for i, msg in enumerate(hist):
                if msg.get("is_summary"):
                    last_summary_idx = i
            if len(hist) - (last_summary_idx + 1) <= threshold:
                return
            if hist and hist[-1].get("is_summary"):
                return
            insert_pos = len(hist) - 10
            if insert_pos < 0:
                insert_pos = 0
            if last_summary_idx != -1 and insert_pos <= last_summary_idx:
                insert_pos = last_summary_idx + 1
            summary_msg = {"role": "system", "content": summary_text, "is_summary": True}
            hist.insert(insert_pos, summary_msg)
            save_history(thread_key, hist)
            print(f"[摘要] 线程 {thread_key} 已插入摘要，位置 {insert_pos}，长度 {len(summary_text)} 字")
            return
        except Exception as e:
            print(f"[摘要生成] 尝试 {attempt+1}/{retries} 异常: {e}")
            await asyncio.sleep(2)
    print(f"[摘要生成] 线程 {thread_key} 最终失败，已放弃")

# ==================== 构建系统提示（复用逻辑） ====================
def build_system_prompt(thread_key: str, user_message: str, username: str,
                        msg_type: str, raw_message_json: str, bot_client,
                        msg_id: Optional[str] = None,
                        enable_group_manage: bool = False) -> str:
    """构造完整的系统提示，包含记忆、群禁言状态等（不包含群禁言状态，由外部追加）。"""
    app_id = bot_client.app_id
    group_id = thread_key.replace("group_", "") if msg_type == "group" else None
    recipient_id = group_id if msg_type == "group" else thread_key.replace("c2c_", "")

    # —— 全局记忆 ——
    global_memory_text = "（无）"
    if get_global_memory_enabled(app_id):
        global_mem = get_global_memory(app_id)
        if global_mem:
            scored = []
            for mem in global_mem:
                score = compute_similarity(user_message, mem)
                scored.append((score, mem))
            scored.sort(key=lambda x: x[0], reverse=True)
            lines = []
            for i, (score, mem) in enumerate(scored):
                if i in (0, 1):
                    lines.append(f"- {mem} （最相关！！！）")
                else:
                    lines.append(f"- {mem}")
            global_memory_text = "\n".join(lines)

    # —— 群/私聊记忆 ——
    qun_memory_text = "（无）"
    c2c_memory_text = "（无）"
    if msg_type == "group":
        if get_qun_memory_enabled(group_id):
            qun_mem = get_qun_memory_list(group_id)
            if qun_mem:
                scored = []
                for mem in qun_mem:
                    score = compute_similarity(user_message, mem)
                    scored.append((score, mem))
                scored.sort(key=lambda x: x[0], reverse=True)
                lines = []
                for i, (score, mem) in enumerate(scored):
                    if i in (0, 1):
                        lines.append(f"- {mem} （最相关！！！）")
                    else:
                        lines.append(f"- {mem}")
                qun_memory_text = "\n".join(lines)
    elif msg_type == "c2c":
        user_id = thread_key.replace("c2c_", "")
        if get_c2c_memory_enabled(user_id):
            c2c_mem = get_c2c_memory_list(user_id)
            if c2c_mem:
                scored = []
                for mem in c2c_mem:
                    score = compute_similarity(user_message, mem)
                    scored.append((score, mem))
                scored.sort(key=lambda x: x[0], reverse=True)
                lines = []
                for i, (score, mem) in enumerate(scored):
                    if i in (0, 1):
                        lines.append(f"- {mem} （最相关！！！）")
                    else:
                        lines.append(f"- {mem}")
                c2c_memory_text = "\n".join(lines)

    # —— 机器人专属记忆 ——
    bot_memory_text = "（无）"
    if get_bot_memory_enabled(app_id):
        bot_mem = get_bot_memory_list(app_id)
        if bot_mem:
            scored = []
            for mem in bot_mem:
                score = compute_similarity(user_message, mem)
                scored.append((score, mem))
            scored.sort(key=lambda x: x[0], reverse=True)
            lines = []
            for i, (score, mem) in enumerate(scored):
                if i in (0, 1):
                    lines.append(f"- {mem} （最相关！！！）")
                else:
                    lines.append(f"- {mem}")
            bot_memory_text = "\n".join(lines)

    global_sys = get_global_system_prompt()
    bot_sys = get_bot_system_prompt(app_id)
    combined_sys = f"{global_sys}\n{bot_sys}" if bot_sys else global_sys

    memory_text = f"【全局长期记忆】\n{global_memory_text}\n"
    if msg_type == "group":
        memory_text += f"【本群长期记忆】\n{qun_memory_text}\n"
    elif msg_type == "c2c":
        memory_text += f"【私聊长期记忆】\n{c2c_memory_text}\n"
    memory_text += f"【机器人专属记忆】\n{bot_memory_text}"

    # 当前时间
    beijing_tz = timezone(timedelta(hours=8))
    now_beijing = datetime.now(beijing_tz).replace(microsecond=0)
    now_rfc3339 = now_beijing.isoformat(timespec='seconds')

    # 从 tool 获取工具描述
    tools_desc = get_tools_description(enable_group_manage)

    system_prompt = (
        f"{combined_sys}\n"
        f"你的名字是 {get_bot_name()}，用户可能会用这个名字称呼你。\n"
        f"{memory_text}\n"
        f"当前时间（北京时间）：{now_rfc3339}\n"
        "在群聊中，如果需要提及某位用户，请直接使用“@用户名”的形式，例如“@张三”。\n"
        "重要：在回复内容中提及用户时，请仅使用“@用户名”的格式，严禁显示用户的ID（即不要在用户名后面添加括号和ID序列）。\n"
        "用户可能会发送语音消息、文本文件、图片、视频或包含网页链接的消息。语音消息已被自动转写成文字，并显示为 [语音：转文字内容]。文本文件内容会被自动读取并嵌入消息中，格式为 [文件：文件名] 后跟文件内容块。网页内容会被自动获取并嵌入消息中，格式为 [网页内容已自动获取] 或 [网页内容摘要]。图片和视频会被自动识别并生成摘要，格式为 [收到图片：文件名] 或 [收到视频：文件名] 后跟摘要。你可以根据这些内容进行回复。\n"
        f"{tools_desc}\n"
        "注意：系统内部会使用『【使用工具】』格式记录工具调用，但你不应该在回复中模仿或使用这种格式。\n"
        "请尽量在回复中适当提及相关用户。"
    )
    return system_prompt

# ==================== 生成回复（含记忆注入 + 群禁言状态 + 工具提示，支持 msg_id，支持复用上下文） ====================
async def generate_reply(
    thread_key: str,
    user_message: str,
    username: str,
    msg_type: str,
    raw_message_json: str,
    bot_client,
    msg_id: Optional[str] = None,
    initial_messages: Optional[List[Dict]] = None,
    recipient_id: Optional[str] = None,   # 新增：接收者ID
) -> tuple[str, bool]:
    """
    生成回复。
    内部负责：AI生成 → （可选）发送消息 → 保存历史
    返回 (reply_text, sent_success)
    """
    print(f"[DEBUG] generate_reply 收到 msg_id: {msg_id}")
    app_id = bot_client.app_id
    group_id = thread_key.replace("group_", "") if msg_type == "group" else None

    # 如果没有显式传入 recipient_id，则从 thread_key 反推
    if recipient_id is None:
        if msg_type == "group":
            recipient_id = group_id
        else:  # c2c
            recipient_id = thread_key.replace("c2c_", "")

    # 获取群禁言状态（用于系统提示）
    group_mute_status_text = ""
    enable_group_manage = False
    if msg_type == "group" and group_id and is_group_manage_enabled(app_id, group_id):
        enable_group_manage = True
        try:
            mute_status = await bot_client.get_group_mute_status(group_id)
            if mute_status:
                global_rule = mute_status.get("global_rule", {})
                mode = global_rule.get("mode", "none")
                schedule_rules = global_rule.get("schedule_rules", [])
                recurring_rules = global_rule.get("recurring_rules", [])
                members = mute_status.get("members", [])
                lines = []
                lines.append(f"【当前群禁言状态】")
                lines.append(f"全员禁言模式: {mode}")
                if schedule_rules:
                    lines.append("定时禁言规则:")
                    for r in schedule_rules:
                        enabled = "启用" if r.get("enabled") else "禁用"
                        lines.append(f"  - 任务 {r.get('task_id')}: {r.get('start_at')} ~ {r.get('end_at')} ({enabled})")
                if recurring_rules:
                    lines.append("周期禁言规则:")
                    for r in recurring_rules:
                        enabled = "启用" if r.get("enabled") else "禁用"
                        weekdays = r.get("weekdays", [])
                        days_str = ",".join(str(d) for d in weekdays)
                        lines.append(f"  - 任务 {r.get('task_id')}: 每周{days_str} {r.get('start_time')} ~ {r.get('end_time')} ({enabled})")
                if members:
                    lines.append("当前被禁言成员:")
                    for m in members:
                        username_m = m.get("username", "未知")
                        expire = m.get("mute_expire_at", "永久")
                        lines.append(f"  - {username_m} (openid: {m.get('member_openid')}) 禁言至 {expire}")
                else:
                    lines.append("当前无被禁言成员。")
                group_mute_status_text = "\n".join(lines)
            else:
                group_mute_status_text = "【当前群禁言状态】获取失败"
        except Exception as e:
            print(f"[群禁言] 获取状态失败: {e}")
            group_mute_status_text = "【当前群禁言状态】获取异常"

    # 构建系统提示（使用原 build_system_prompt 并追加群禁音状态）
    base_sys = build_system_prompt(
        thread_key, user_message, username,
        msg_type, raw_message_json, bot_client, msg_id,
        enable_group_manage=enable_group_manage
    )
    if group_mute_status_text:
        base_sys = base_sys + "\n" + group_mute_status_text
    system_prompt = base_sys

    # 构建初始 messages
    if initial_messages is not None:
        messages = initial_messages.copy()
        if messages and messages[0].get("role") == "system":
            messages[0] = {"role": "system", "content": system_prompt}
        else:
            messages.insert(0, {"role": "system", "content": system_prompt})
        user_content = f"以下是当前消息的原始 JSON 数据，你可以从中获取发送者ID等信息以便使用 @ 功能和禁言工具：\n```json\n{raw_message_json}\n```\n{user_message}"
        messages.append({"role": "user", "content": user_content})
    else:
        messages = [{"role": "system", "content": system_prompt}]
        json_context = f"以下是当前消息的原始 JSON 数据，你可以从中获取发送者ID等信息以便使用 @ 功能和禁言工具：\n```json\n{raw_message_json}\n```"
        messages.append({"role": "user", "content": json_context})
        hist = get_history(thread_key)
        messages.extend(hist)
        if not messages or messages[-1]["role"] != "user":
            messages.append({"role": "user", "content": user_message})

    # 存储初始上下文
    INTERRUPT_CONTEXT[thread_key] = messages
    print(f"[上下文] 已存储初始上下文，长度 {len(messages)}")

    # 读取工具配置
    enable_tools = get_bot_enable_tools(app_id)
    max_tool_rounds = get_bot_max_tool_rounds(app_id)
    print(f"[工具配置] 开启工具调用: {enable_tools}, 最大循环次数: {max_tool_rounds}")

    final_reply = ""
    sent_success = False
    skip_reply_called = False
    full_response_parts = []

    if not enable_tools:
        try:
            final_reply = await call_ai(messages, "main", stream=False)
            final_reply = final_reply.strip() if final_reply else "抱歉，我暂时无法回复。"
        except Exception as e:
            print(f"[AI Reply Error] {e}")
            final_reply = "抱歉，我暂时无法回复，请稍后再试。"
    else:
        tools = get_tools_definition(enable_group_manage=enable_group_manage)
        MAX_TOOL_ROUNDS = max_tool_rounds
        canceled = False

        try:
            for round_num in range(1, MAX_TOOL_ROUNDS + 1):
                print(f"[工具循环] 第 {round_num}/{MAX_TOOL_ROUNDS} 轮调用")
                if asyncio.current_task().cancelled():
                    raise asyncio.CancelledError()

                ai_msg = await call_ai_with_tools(messages, "main", tools, temperature=0.7)
                tool_calls = ai_msg.get("tool_calls", [])
                content = ai_msg.get("content", "")
                messages.append(ai_msg)
                INTERRUPT_CONTEXT[thread_key] = messages

                if content and content.strip():
                    full_response_parts.append(content.strip())
                    print(f"[工具循环] 第 {round_num} 轮 AI 回复已加入最终返回，长度: {len(content)}")

                # 检测是否调用了 skip_reply
                for tc in tool_calls:
                    if tc.get("function", {}).get("name") == "skip_reply":
                        skip_reply_called = True
                        break

                if not tool_calls:
                    print(f"[工具循环] 第 {round_num} 轮无工具调用，结束循环")
                    break

                print(f"[工具循环] 第 {round_num} 轮 AI 调用 {len(tool_calls)} 个工具")
                for tc in tool_calls:
                    func_name = tc.get("function", {}).get("name", "")
                    args_str = tc.get("function", {}).get("arguments", "{}")
                    try:
                        args_dict = _json.loads(args_str) if args_str else {}
                        args_display = _json.dumps(args_dict, ensure_ascii=False)
                    except Exception:
                        args_display = args_str
                    full_response_parts.append(f"【使用工具】【{func_name}】参数：{args_display}")

                tool_tasks = []
                for tc in tool_calls:
                    tool_tasks.append(execute_tool_call(tc, bot_client, group_id, msg_type, recipient_id, msg_id, thread_key))
                tool_results = await asyncio.gather(*tool_tasks, return_exceptions=True)

                for result in tool_results:
                    if isinstance(result, Exception):
                        print(f"[工具循环] 工具执行异常: {result}")
                        err_msg = f"工具执行异常: {result}"
                        messages.append({"role": "tool", "tool_call_id": "", "content": err_msg})
                    else:
                        messages.append(result)
                    INTERRUPT_CONTEXT[thread_key] = messages

                # 如果调用了 skip_reply，不再继续下一轮（避免多余的AI调用）
                if skip_reply_called:
                    print("[工具循环] 检测到 skip_reply，终止后续AI调用")
                    break

                if round_num == MAX_TOOL_ROUNDS:
                    print(f"[工具循环] 达到最大轮次 {MAX_TOOL_ROUNDS}，追加最终总结")
                    try:
                        final_msg = await call_ai(messages, "main", stream=False)
                        if final_msg and final_msg.strip():
                            full_response_parts.append(final_msg.strip())
                            messages.append({"role": "assistant", "content": final_msg.strip()})
                            INTERRUPT_CONTEXT[thread_key] = messages
                    except Exception:
                        pass
                    break

            final_reply = "\n\n".join(full_response_parts) if full_response_parts else ""
            if not final_reply:
                final_reply = "抱歉，我暂时无法回复，请稍后再试。"

        except asyncio.CancelledError:
            canceled = True
            print(f"[生成回复] 线程 {thread_key} 被取消，上下文已保留")
            raise
        finally:
            if not canceled:
                if thread_key in INTERRUPT_CONTEXT:
                    del INTERRUPT_CONTEXT[thread_key]
                    print(f"[上下文] 清理线程 {thread_key} 的上下文")
            else:
                # 取消时保留上下文，供外部保存
                print(f"[上下文] 保留线程 {thread_key} 的上下文供外部保存")

    # ---------- 发送与保存历史（新增） ----------
    # 1. 发送文本（如果没有调用 skip_reply 且有内容）
    if not skip_reply_called and final_reply and final_reply.strip():
        sent_success = await bot_client.send_message(
            msg_type, recipient_id, final_reply, msg_id
        )
        if sent_success:
            print(f"[AI发送] 成功发送文本")
        else:
            print(f"[AI发送] 文本发送失败")
    else:
        if skip_reply_called:
            print("[AI发送] 因 skip_reply 跳过文本发送")
        # 如果 final_reply 为空但没 skip，可能没有内容，也不发送

    # 2. 保存历史（总是保存，无论发送与否）
    save_content = final_reply if final_reply else "（已通过工具完成回复，未发送文本）"
    append_message(thread_key, "assistant", save_content)
    print(f"[AI历史] 已保存助手回复（长度 {len(save_content)}）")

    return final_reply, sent_success

# ==================== 网页内容获取 ====================
def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme in ('http', 'https'), result.netloc])
    except Exception:
        return False

async def fetch_webpage_content(url: str) -> Optional[str]:
    try:
        loop = asyncio.get_event_loop()
        print(f"[网页] 开始获取 {url[:80]}...")
        resp = await loop.run_in_executor(
            get_executor(),
            lambda: requests.get(
                url,
                timeout=15,
                stream=True,
                headers=DEFAULT_HEADERS   # 从 tool 导入
            )
        )
        if resp.status_code != 200:
            print(f"[网页] GET 失败，状态码: {resp.status_code}")
            return None
        content_type = resp.headers.get('Content-Type', '').lower()
        print(f"[网页] Content-Type: {content_type}")
        try:
            chunk = resp.raw.read(512)
        except Exception:
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

        response = await loop.run_in_executor(
            get_executor(),
            lambda: requests.get(url, timeout=10, headers=DEFAULT_HEADERS)
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

async def summarize_content_if_needed(content: str, max_len: int = 5000,
                                      summary_len: int = 400) -> str:
    if len(content) <= max_len:
        return content
    prompt = f"请将以下网页内容压缩为一篇摘要，字数控制在{summary_len}字以内：\n{content[:3000]}"
    try:
        summary = await call_ai([{"role": "user", "content": prompt}], "judge", temperature=0.3)
        return summary if summary else content[:200] + "...（摘要生成失败）"
    except Exception:
        return content[:200] + "...（摘要生成失败）"