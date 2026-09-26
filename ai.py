# -*- coding: utf-8 -*-
# ai.py — AI 相关（纯请求：对话调用、媒体识别、群聊判定、回复生成、摘要、网页抓取）
import time
import asyncio
import threading
import requests
import json as _json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone

from config import (
    get_model_config, get_global_system_prompt, get_bot_system_prompt,
    get_executor, is_group_manage_enabled,
    get_bot_enable_tools, get_bot_max_tool_rounds, get_tool_choice,
    get_bot_disable_ai_reply,
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
    strip_message_meta,
    get_group_name_from_mirror, get_user_name,
)

from log import info, warn, error, debug, LogCtx
from utils import recognize_kind

# ========== 从 tool 导入工具相关函数和常量 ==========
from tool import (
    get_tools_definition,
    get_tools_description,
    execute_tool_call,
)

# ==================== 存储每个线程当前任务的消息上下文（用于打断保存） ====================
INTERRUPT_CONTEXT: Dict[str, List[Dict]] = {}

# ==================== 多 Key 故障切换（全局状态） ====================
# {model_key: 当前使用的 Key 索引}，多 Key 模式下错误即切换、循环回绕
_ai_key_index: Dict[str, int] = {}
_ai_key_lock = threading.Lock()


def _ai_mask_key(api_key: str) -> str:
    if not api_key:
        return "(空)"
    if len(api_key) <= 10:
        return api_key[:3] + "***"
    return f"{api_key[:7]}...{api_key[-4:]}"


def _ai_model_slots(model_key: str):
    """获取模型全部配置项（list[dict]），始终由 config.get_model_config 统一返回 list。"""
    return get_model_config(model_key)


def _ai_get_index(model_key: str, total: int) -> int:
    """线程安全获取当前 Key 索引，越界自动归位。"""
    if total <= 0:
        return 0
    with _ai_key_lock:
        idx = _ai_key_index.get(model_key, 0)
        if idx >= total or idx < 0:
            idx = idx % total if total else 0
            _ai_key_index[model_key] = idx
        return idx


def _ai_rotate(model_key: str, total: int, reason: str = "", ctx=None) -> int:
    """切换到下一个 Key 并返回新索引。

    ctx: 由调用方（call_ai / call_ai_with_tools）显式传入，用于日志标识。
    """
    if total <= 0:
        return 0
    with _ai_key_lock:
        old_idx = _ai_key_index.get(model_key, 0)
        if old_idx >= total or old_idx < 0:
            old_idx = old_idx % total
        new_idx = (old_idx + 1) % total
        _ai_key_index[model_key] = new_idx
    warn(f"[故障切换] 模型 '{model_key}' 从 Key#{old_idx + 1} 切换到 Key#{new_idx + 1}（共 {total} 个）" +
         (f"｜原因: {reason}" if reason else ""), ctx=ctx)
    return new_idx

# ==================== 核心 AI 调用（带自动重试 + 多Key故障切换） ====================
_RETRY_WAITS = [1, 5, 15, 20]  # 最多重试4次，分别等待对应秒数


async def call_ai(messages: List[Dict], model_key: str, stream: bool = False,
                  temperature: float = 0.7, ctx=None) -> str:
    """
    调用 AI（纯文本）。

    多Key故障切换策略：
      - 配置中只有一个 Key → 使用原有的重试逻辑（_RETRY_WAITS）。
      - 配置中有多个 Key   → 任意错误返回时立即切换到下一个 Key，并持续使用该 Key；
                            下一次错误再切换下一个，索引循环回绕。

    ctx: LogCtx（或 app_id 字符串），用于日志标识。调用方必须传入；
         确实无从获取时传 None，日志会显示 appid=? 占位。
    """
    slots = _ai_model_slots(model_key)
    total = len(slots)

    # ---------- 单 Key：保持原有重试逻辑 ----------
    if total <= 1:
        model_cfg = slots[0]
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
        last_exc = None

        for attempt in range(len(_RETRY_WAITS) + 1):  # 初始 1 次 + 重试次数
            try:
                if attempt > 0:
                    wait = _RETRY_WAITS[attempt - 1]
                    warn(f"[AI调用] 第 {attempt} 次重试，等待 {wait}s...", ctx=ctx)
                    await asyncio.sleep(wait)

                start_time = time.time()
                info(f"[AI调用] 开始请求，模型: {model_key} -> {model_name}" +
                     (f"（第 {attempt + 1} 次尝试）" if attempt > 0 else ""), ctx=ctx)
                response = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.post(base_url, json=payload, headers=headers, timeout=360)
                )
                elapsed = time.time() - start_time
                info(f"[AI调用] 请求完成，耗时 {elapsed:.2f} 秒", ctx=ctx)
                response.raise_for_status()
                data = response.json()
                if data.get("choices") and len(data["choices"]) > 0:
                    content = data["choices"][0].get("message", {}).get("content", "")
                    return content.strip()
                return "（AI 未返回有效内容）"
            except requests.exceptions.RequestException as e:
                last_exc = e
                if hasattr(e, 'response') and e.response is not None:
                    warn(f"[AI调用错误] 状态码: {e.response.status_code}, 响应: {e.response.text}", ctx=ctx)
                else:
                    warn(f"[AI调用错误] {e}", ctx=ctx)
                if attempt < len(_RETRY_WAITS):
                    warn(f"[AI调用] 将在 {_RETRY_WAITS[attempt]}s 后重试...", ctx=ctx)
                else:
                    error(f"[AI调用] 已达最大重试次数，放弃", ctx=ctx)
            except Exception as e:
                last_exc = e
                warn(f"[AI调用错误] {e}", ctx=ctx)
                if attempt < len(_RETRY_WAITS):
                    warn(f"[AI调用] 将在 {_RETRY_WAITS[attempt]}s 后重试...", ctx=ctx)
                else:
                    error(f"[AI调用] 已达最大重试次数，放弃", ctx=ctx)

        raise last_exc  # type: ignore

    # ---------- 多 Key：任一错误立即切换并持续使用，最多尝试 total 次 ----------
    loop = asyncio.get_event_loop()
    last_exc = None

    for switch_round in range(total):
        idx = _ai_get_index(model_key, total)
        model_cfg = slots[idx]
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

        try:
            start_time = time.time()
            info(f"[AI调用] 开始请求，模型: {model_key} -> {model_name}，"
                 f"Key #{idx + 1}/{total}（{_ai_mask_key(api_key)}）", ctx=ctx)
            response = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(base_url, json=payload, headers=headers, timeout=360)
            )
            elapsed = time.time() - start_time
            info(f"[AI调用] 请求完成，耗时 {elapsed:.2f} 秒", ctx=ctx)
            response.raise_for_status()
            data = response.json()

            # 返回体异常也视为错误返回 → 触发切换
            if not data.get("choices") or len(data["choices"]) <= 0:
                warn(f"[AI调用错误] 模型未返回有效 choices，视为错误返回", ctx=ctx)
                if data.get("error"):
                    warn(f"[AI调用错误] 接口错误信息: {data['error']}", ctx=ctx)
                _ai_rotate(model_key, total, reason="返回体无有效 choices", ctx=ctx)
                continue

            content = data["choices"][0].get("message", {}).get("content", "")
            return content.strip()

        except requests.exceptions.RequestException as e:
            last_exc = e
            if hasattr(e, 'response') and e.response is not None:
                warn(f"[AI调用错误] 状态码: {e.response.status_code}, 响应: {e.response.text[:500]}", ctx=ctx)
            else:
                warn(f"[AI调用错误] {e}", ctx=ctx)
            _ai_rotate(model_key, total, reason=f"{type(e).__name__}: {e}", ctx=ctx)
        except Exception as e:
            last_exc = e
            warn(f"[AI调用错误] {e}", ctx=ctx)
            _ai_rotate(model_key, total, reason=f"{type(e).__name__}: {e}", ctx=ctx)

    error(f"[AI调用] 已尝试全部 {total} 个 Key，均失败，放弃", ctx=ctx)
    raise last_exc if last_exc else RuntimeError("所有 API Key 均调用失败")


# ==================== 带工具的 AI 调用（带自动重试 + 多Key故障切换） ====================
async def call_ai_with_tools(messages: List[Dict], model_key: str,
                             tools: List[Dict], temperature: float = 0.7,
                             ctx=None) -> Dict:
    """
    调用 AI 并支持 function calling，返回完整的 message dict（含 content 和 tool_calls）。

    多Key故障切换策略同 call_ai：
      - 单 Key → 原有重试逻辑
      - 多 Key → 任一错误立即切换并持续使用，循环回绕

    ctx: LogCtx（或 app_id 字符串），用于日志标识，调用方必须传入。
    """
    # 读取全局 tool_choice 配置
    tc = get_tool_choice()
    tool_choice_value = "required" if tc == 1 else "auto"

    slots = _ai_model_slots(model_key)
    total = len(slots)

    # ---------- 单 Key：保持原有重试逻辑 ----------
    if total <= 1:
        model_cfg = slots[0]
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
            "tool_choice": tool_choice_value,
            "temperature": temperature,
            "top_p": 0.9,
            "max_tokens": 3000,
        }
        loop = asyncio.get_event_loop()
        last_exc = None

        for attempt in range(len(_RETRY_WAITS) + 1):
            try:
                if attempt > 0:
                    wait = _RETRY_WAITS[attempt - 1]
                    warn(f"[AI调用-工具] 第 {attempt} 次重试，等待 {wait}s...", ctx=ctx)
                    await asyncio.sleep(wait)

                start_time = time.time()
                info(f"[AI调用-工具] 开始请求，模型: {model_key} -> {model_name}，工具数: {len(tools)}，tool_choice={tool_choice_value}" +
                     (f"（第 {attempt + 1} 次尝试）" if attempt > 0 else ""), ctx=ctx)
                response = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.post(base_url, json=payload, headers=headers, timeout=360)
                )
                elapsed = time.time() - start_time
                info(f"[AI调用-工具] 请求完成，耗时 {elapsed:.2f} 秒", ctx=ctx)
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
                last_exc = e
                if hasattr(e, 'response') and e.response is not None:
                    warn(f"[AI调用-工具错误] 状态码: {e.response.status_code}, 响应: {e.response.text}", ctx=ctx)
                else:
                    warn(f"[AI调用-工具错误] {e}", ctx=ctx)
                if attempt < len(_RETRY_WAITS):
                    warn(f"[AI调用-工具] 将在 {_RETRY_WAITS[attempt]}s 后重试...", ctx=ctx)
                else:
                    error(f"[AI调用-工具] 已达最大重试次数，放弃", ctx=ctx)
            except Exception as e:
                last_exc = e
                warn(f"[AI调用-工具错误] {e}", ctx=ctx)
                if attempt < len(_RETRY_WAITS):
                    warn(f"[AI调用-工具] 将在 {_RETRY_WAITS[attempt]}s 后重试...", ctx=ctx)
                else:
                    error(f"[AI调用-工具] 已达最大重试次数，放弃", ctx=ctx)

        raise last_exc  # type: ignore

    # ---------- 多 Key：任一错误立即切换并持续使用，最多尝试 total 次 ----------
    loop = asyncio.get_event_loop()
    last_exc = None

    for switch_round in range(total):
        idx = _ai_get_index(model_key, total)
        model_cfg = slots[idx]
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
            "tool_choice": tool_choice_value,
            "temperature": temperature,
            "top_p": 0.9,
            "max_tokens": 3000,
        }

        try:
            start_time = time.time()
            info(f"[AI调用-工具] 开始请求，模型: {model_key} -> {model_name}，工具数: {len(tools)}，"
                 f"tool_choice={tool_choice_value}，Key #{idx + 1}/{total}（{_ai_mask_key(api_key)}）", ctx=ctx)
            response = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(base_url, json=payload, headers=headers, timeout=360)
            )
            elapsed = time.time() - start_time
            info(f"[AI调用-工具] 请求完成，耗时 {elapsed:.2f} 秒", ctx=ctx)
            response.raise_for_status()
            data = response.json()

            # 返回体异常也视为错误返回 → 触发切换
            if not data.get("choices") or len(data["choices"]) <= 0:
                warn(f"[AI调用-工具错误] 模型未返回有效 choices，视为错误返回", ctx=ctx)
                if data.get("error"):
                    warn(f"[AI调用-工具错误] 接口错误信息: {data['error']}", ctx=ctx)
                _ai_rotate(model_key, total, reason="返回体无有效 choices", ctx=ctx)
                continue

            msg = data["choices"][0].get("message", {})
            return {
                "role": "assistant",
                "content": msg.get("content", "") or "",
                "tool_calls": msg.get("tool_calls", [])
            }

        except requests.exceptions.RequestException as e:
            last_exc = e
            if hasattr(e, 'response') and e.response is not None:
                warn(f"[AI调用-工具错误] 状态码: {e.response.status_code}, 响应: {e.response.text[:500]}", ctx=ctx)
            else:
                warn(f"[AI调用-工具错误] {e}", ctx=ctx)
            _ai_rotate(model_key, total, reason=f"{type(e).__name__}: {e}", ctx=ctx)
        except Exception as e:
            last_exc = e
            warn(f"[AI调用-工具错误] {e}", ctx=ctx)
            _ai_rotate(model_key, total, reason=f"{type(e).__name__}: {e}", ctx=ctx)

    error(f"[AI调用-工具] 已尝试全部 {total} 个 Key，均失败，放弃", ctx=ctx)
    raise last_exc if last_exc else RuntimeError("所有 API Key 均调用失败")

# ==================== 媒体识别（基础 AI 能力，保留在此） ====================
# 媒体摘要缓存有两套 key：
#   A. get_media_cache_key(media_type, filename, height, width) —— 自动识别路径使用
#   B. get_media_cache_key(media_type, filename)               —— 手动识别路径使用
#      （手动调用拿不到 height/width，只能退化为不含尺寸的 key）
# 为了"自动识别在后台跑完写入缓存 → AI 手动识别能命中"，写入时两套都写、
# 读取时两套都查。否则两边 key 不同，缓存永远命中不了，会重复请求视觉模型。
def _media_cache_keys(media_type: str, filename: str,
                      height: int = 0, width: int = 0) -> list:
    keys = [get_media_cache_key(media_type, filename)]          # 不带尺寸（手动路径）
    if height or width:
        keys.append(get_media_cache_key(media_type, filename, height, width))  # 带尺寸
    # 去重且保持顺序
    out = []
    for k in keys:
        if k not in out:
            out.append(k)
    return out


def _lookup_media_cache(media_type: str, filename: str,
                        height: int = 0, width: int = 0) -> Optional[str]:
    """按两种 key 依次查缓存，命中即返回。"""
    for key in _media_cache_keys(media_type, filename, height, width):
        cached = get_cached_media_summary(key)
        if cached:
            return cached
    return None


def _store_media_cache(media_type: str, filename: str, height: int, width: int,
                       result: str, media_url: str) -> None:
    """把摘要按两种 key 都写入缓存。"""
    for key in _media_cache_keys(media_type, filename, height, width):
        set_cached_media(key, result, media_type, filename, media_url, height, width)


async def recognize_media_by_url(media_url: str, filename: str = "媒体",
                                 media_type: str = None, disable_cache: bool = False,
                                 app_id: Optional[str] = None) -> str:
    ctx = LogCtx(app_id=app_id)

    # 关闭 AI 回复时，不做媒体识别，也不写入 media_cache 缓存文件
    if app_id and get_bot_disable_ai_reply(app_id):
        debug(f"[媒体识别] 已关闭 AI 回复，跳过 URL 识别与缓存: {filename}", ctx=ctx)
        return "（已关闭 AI 回复，未识别媒体内容）"

    cache_key = get_url_cache_key(media_url)
    if not disable_cache:
        cached = get_cached_media_summary(cache_key)
        if cached:
            debug(f"[媒体缓存] URL 命中: {media_url[:50]}...", ctx=ctx)
            return cached

    if not media_type:
        media_type = recognize_kind(filename, media_url)

    # 再查一次"按媒体特征"的缓存。
    # 自动识别路径（msg.py）用的是 get_media_cache_key(media_type, filename, h, w)，
    # 与上面的 URL key 不同；若只查 URL key，自动识别刚写进缓存的摘要就命中不了，
    # 手动调用会白白再请求一次视觉模型。
    media_cache_key = None
    if not disable_cache and media_type:
        cached = _lookup_media_cache(media_type, filename)
        if cached:
            debug(f"[媒体缓存] 媒体特征命中: {filename}", ctx=ctx)
            return cached

    if not media_type:
        # 类型未知同样不写缓存：多数是 URL 无扩展名导致的临时判断失败，
        # 缓存后即便之后补上了 filename 也会一直命中这条失败记录。
        error(f"[媒体识别] 无法判断媒体类型（不写缓存）: {filename} / {media_url[:60]}", ctx=ctx)
        return "（内容无法获取/为未知文件格式）"

    debug(f"[媒体识别] 通过 URL 识别: {media_type}, {filename}", ctx=ctx)
    try:
        result = await recognize_media(media_type, media_url, filename, 0, 0,
                                       disable_cache=disable_cache, app_id=app_id)
        # recognize_media 内部已按"媒体特征"写好缓存；这里补写 URL key，
        # 使后续用同一 URL 但换了个 filename 的调用也能命中。
        # 仅成功结果才补写 —— 失败不写缓存（见 recognize_media）。
        if (not disable_cache and result
                and not result.startswith("（媒体识别失败")
                and not result.startswith("（AI 未返回有效内容")):
            set_cached_media(cache_key, result, media_type, filename, media_url)
        return result
    except Exception as e:
        error(f"[媒体识别] URL 识别失败（不写缓存）: {e}", ctx=ctx)
        return f"（媒体识别失败: {e}）"

async def recognize_media(media_type: str, media_url: str, filename: str = "媒体",
                          height: int = 0, width: int = 0, disable_cache: bool = False,
                          app_id: Optional[str] = None) -> str:
    ctx = LogCtx(app_id=app_id)

    # 关闭 AI 回复时，完全不做媒体识别：
    # 不读缓存、不调视觉模型、不下载、不写 media_cache 文件，直接返回占位文本
    if app_id and get_bot_disable_ai_reply(app_id):
        debug(f"[媒体识别] 已关闭 AI 回复，完全跳过识别与缓存: {filename}", ctx=ctx)
        return "（已关闭 AI 回复，未识别媒体内容）"

    # 媒体类型不受支持时直接返回，避免走到写缓存分支
    if media_type not in ("image", "video"):
        debug(f"[媒体识别] 不支持的媒体类型，跳过: {media_type}", ctx=ctx)
        return "（不支持的媒体类型）"

    cache_key = get_media_cache_key(media_type, filename, height, width)
    if not disable_cache:
        cached = _lookup_media_cache(media_type, filename, height, width)
        if cached:
            debug(f"[媒体缓存] 命中: {filename}", ctx=ctx)
            return cached

    if media_type == "image":
        content_parts = [
            {"type": "text", "text": "请描述这张图片的内容，请原样返回所有文本内容！如果没有文本内容，就重点描述图片中的主要对象、场景、颜色、构图或可能表达的情感。"},
            {"type": "image_url", "image_url": {"url": media_url}}
        ]
    else:  # video
        content_parts = [
            {"type": "text", "text": "请描述这个视频的内容，请原样返回所有文本内容！如果没有文本内容，就重点描述视频中的主要场景、动作、颜色或可能表达的情感。"},
            {"type": "video_url", "video_url": {"url": media_url}}
        ]

    try:
        messages = [{"role": "user", "content": content_parts}]
        result = await call_ai(messages, "vision", temperature=0.3, ctx=ctx)
        if result and "（AI 未返回有效内容）" not in result:
            _store_media_cache(media_type, filename, height, width,
                               result, media_url)
            return result
        else:
            # 识别失败不写缓存：失败多为临时性（模型超时/限流/返回空），
            # 若写入缓存，后续同一媒体会一直命中这个失败结果而不再重试。
            error(f"[媒体识别] 模型未返回有效内容，不写缓存: {filename}", ctx=ctx)
            return "（媒体识别失败，模型未返回有效内容）"
    except Exception as e:
        # 同上：异常也不写缓存，留给下次重试的机会
        error(f"[媒体识别] 识别失败（不写缓存）{media_url}: {e}", ctx=ctx)
        return f"（媒体识别失败: {e}）"

# ==================== 群聊是否需要回复判定 ====================
async def should_reply_in_group(history: List[Dict], current_message: str,
                                mentions: List[Dict], app_id: str,
                                bot_name: str = "蓝狼") -> bool:
    ctx = LogCtx(app_id=app_id)

    # 关闭 AI 回复时，不再调用 Judge 判定，直接判定为不回复（省下一次 AI 调用）
    if get_bot_disable_ai_reply(app_id):
        debug("[AI Judge] 已关闭 AI 回复，跳过判定", ctx=ctx)
        return False

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
        f"机器人的名字是 {bot_name}。如果当前消息或上文中明确提到这个机器人名字，或者请求机器人帮助，则应当回复。\n"
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
        result = await call_ai(judge_messages, "judge", stream=False, temperature=0.2, ctx=ctx)
        result_clean = result.strip().lower()
        info(f"[AI Judge 结果] {result_clean}", ctx=ctx)
        return "是" in result_clean or "yes" in result_clean
    except Exception as e:
        warn(f"[AI Judge Error] {e}", ctx=ctx)
        return True

# ==================== 对话摘要生成与插入 ====================
async def generate_and_insert_summary(thread_key: str, retries: int = 3):
    ctx = LogCtx(thread_key=thread_key)

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
                , ctx=ctx)
                if summary and "（AI 未返回有效内容）" not in summary:
                    summary_text = summary
            except Exception as e:
                error(f"[摘要生成] 尝试 {attempt+1}/{retries} 失败: {e}", ctx=ctx)

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
            info(f"[摘要] 线程 {thread_key} 已插入摘要，位置 {insert_pos}，长度 {len(summary_text)} 字", ctx=ctx)
            return
        except Exception as e:
            error(f"[摘要生成] 尝试 {attempt+1}/{retries} 异常: {e}", ctx=ctx)
            await asyncio.sleep(2)
    error(f"[摘要生成] 线程 {thread_key} 最终失败，已放弃", ctx=ctx)

# ==================== 构建系统提示（复用逻辑） ====================
def build_system_prompt(thread_key: str, user_message: str, username: str,
                        msg_type: str, raw_message_json: str, bot_client,
                        msg_id: Optional[str] = None,
                        enable_group_manage: bool = False,
                        round_num: Optional[int] = None,
                        max_rounds: Optional[int] = None) -> str:
    """构造完整的系统提示，包含记忆、聊天上下文、工具轮次信息等。"""
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

    memory_text = ""
    if get_global_memory_enabled(app_id):
        memory_text += f"【全局长期记忆】\n{global_memory_text}\n"
    else:
        memory_text += f"【全局长期记忆】\n（禁用，旧记忆可通过工具查看找回）\n"
    if msg_type == "group":
        if get_qun_memory_enabled(group_id):
            memory_text += f"【本群长期记忆】\n{qun_memory_text}\n"
        else:
            memory_text += f"【本群长期记忆】\n（禁用，旧记忆可通过工具查看找回）\n"
    elif msg_type == "c2c":
        if get_c2c_memory_enabled(recipient_id):
            memory_text += f"【私聊长期记忆】\n{c2c_memory_text}\n"
        else:
            memory_text += f"【私聊长期记忆】\n（禁用，旧记忆可通过工具查看找回）\n"
    if get_bot_memory_enabled(app_id):
        memory_text += f"【机器人专属记忆】\n{bot_memory_text}"
    else:
        memory_text += f"【机器人专属记忆】\n（禁用，旧记忆可通过工具查看找回）"
    memory_text = memory_text.strip()

    # —— 聊天上下文信息 ——
    chat_type_name = "群聊" if msg_type == "group" else "私聊"
    if msg_type == "group":
        chat_target = get_group_name_from_mirror(app_id, group_id) or group_id or "未知群"
    else:
        chat_target = get_user_name(recipient_id, app_id) or recipient_id or "未知用户"
    bot_name = bot_client.bot_name

    # —— 工具轮次信息 ——
    if round_num is not None and max_rounds is not None:
        remaining = max_rounds - round_num
        tool_round_info = f"当前工具调用轮次：第 {round_num}/{max_rounds} 轮（剩余 {remaining} 轮）"
    else:
        tool_round_info = ""

    # 当前时间
    beijing_tz = timezone(timedelta(hours=8))
    now_beijing = datetime.now(beijing_tz).replace(microsecond=0)
    now_rfc3339 = now_beijing.isoformat(timespec='seconds')

    # 从 tool 获取工具描述
    tools_desc = get_tools_description(enable_group_manage)

    # 构建上下文摘要行
    context_line = f"【当前会话】类型：{chat_type_name} | {'群名称' if msg_type=='group' else '私聊对象'}：{chat_target} | 当前机器人：{bot_name} | APP_ID：{app_id}"
    if tool_round_info:
        context_line += f"\n{tool_round_info}"

    system_prompt = (
        f"{combined_sys}\n"
        f"你的名字是 {bot_name}，用户可能会用这个名字称呼你。\n"
        f"{memory_text}\n"
        f"{context_line}\n"
        f"当前时间（北京时间）：{now_rfc3339}\n"
        "在群聊中，如果需要提及某位用户，请直接使用“@用户名”的形式，例如“@张三”。\n"
        "重要：在回复内容中提及用户时，请仅使用“@用户名”的格式，严禁显示用户的ID和时间（即不用在回复前添加时间，不要在用户名后面添加括号和ID序列）。\n"
        "用户可能会发送语音消息、文本文件、图片、视频或包含网页链接的消息。语音消息已被自动转写成文字，并显示为 [语音：转文字内容]。文本文件内容会被自动读取并嵌入消息中，格式为 [文件：文件名] 后跟文件内容块。图片和视频会被自动识别并生成摘要，格式为 [收到图片：文件名] 或 [收到视频：文件名] 后跟摘要。你可以根据这些内容进行回复。\n"
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
) -> tuple[str, bool, bool]:
    """
    生成回复。
    内部负责：AI生成 → （可选）发送消息 → 保存历史
    返回 (reply_text, sent_success, skip_reply_called)
    """
    ctx = LogCtx(app_id=bot_client.app_id, thread_key=thread_key, msg_id=msg_id or "")

    debug(f"[DEBUG] generate_reply 收到 msg_id: {msg_id}", ctx=ctx)
    app_id = bot_client.app_id
    group_id = thread_key.replace("group_", "") if msg_type == "group" else None

    # ==================== 实时开关：关闭 AI 回复 ====================
    # 开启后不调用任何 AI（主模型 / 工具调用 / 视觉模型），不发送任何消息，
    # 仅保留调用方已完成的消息记录与摘要整理。开关实时读取 config.json，改完即生效。
    if get_bot_disable_ai_reply(app_id):
        info(f"[AI回复已关闭] 线程 {thread_key} 跳过 AI 生成与工具调用（仅记录）", ctx=ctx)
        return "", False, False

    # 构建完整日志标识（显式传给后续每一条日志，不再使用隐式上下文）
    from memory import get_group_name_from_mirror, get_user_name
    thread_display = ""
    if msg_type == "group":
        thread_display = get_group_name_from_mirror(app_id, group_id) or ""
    elif msg_type == "c2c":
        c2c_id = thread_key.replace("c2c_", "")
        thread_display = get_user_name(c2c_id, app_id) or ""
    ctx = ctx.with_(thread_display=thread_display)

    # 从原始消息 JSON 中提取当前用户的 author_id（用于推送工具标注）
    author_id = ""
    try:
        parsed_raw = _json.loads(raw_message_json)
        author_id = parsed_raw.get("author_id", "") or ""
    except Exception:
        pass

    # 如果没有显式传入 recipient_id，则从 thread_key 反推
    if recipient_id is None:
        if msg_type == "group":
            recipient_id = group_id
        else:  # c2c
            recipient_id = thread_key.replace("c2c_", "")

    # 群消息始终启用群管理（不再自动注入禁言状态，由工具 get_group_mute_status 按需查询）
    enable_group_manage = True if msg_type == "group" and group_id else False

    # 构建系统提示
    base_sys = build_system_prompt(
        thread_key, user_message, username,
        msg_type, raw_message_json, bot_client, msg_id,
        enable_group_manage=enable_group_manage
    )
    system_prompt = base_sys

    # 构建初始 messages
    if initial_messages is not None:
        messages = initial_messages.copy()
        # 剥离内部元数据后发送给 AI
        messages = [strip_message_meta(m) if isinstance(m, dict) else m for m in messages]
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
        # 剥离内部元数据（msg_id, msg_idx, ref_msg_idx）后发送给 AI
        hist_clean = [strip_message_meta(m) for m in hist]
        messages.extend(hist_clean)
        if not messages or messages[-1]["role"] != "user":
            messages.append({"role": "user", "content": user_message})

    # 存储初始上下文
    INTERRUPT_CONTEXT[thread_key] = messages
    debug(f"[上下文] 已存储初始上下文，长度 {len(messages)}", ctx=ctx)

    # 读取工具配置
    enable_tools = get_bot_enable_tools(app_id)
    max_tool_rounds = get_bot_max_tool_rounds(app_id)
    info(f"[工具配置] 开启工具调用: {enable_tools}, 最大循环次数: {max_tool_rounds}", ctx=ctx)

    final_reply = ""
    sent_success = False
    skip_reply_called = False
    full_response_parts = []

    if not enable_tools:
        try:
            final_reply = await call_ai(messages, "main", stream=False, ctx=ctx)
            final_reply = final_reply.strip() if final_reply else "抱歉，我暂时无法回复。"
        except Exception as e:
            warn(f"[AI Reply Error] {e}", ctx=ctx)
            final_reply = "抱歉，我暂时无法回复，请稍后再试。"
    else:
        tools = get_tools_definition(enable_group_manage=enable_group_manage)
        MAX_TOOL_ROUNDS = max_tool_rounds
        canceled = False

        try:
            for round_num in range(1, MAX_TOOL_ROUNDS + 1):
                info(f"[工具循环] 第 {round_num}/{MAX_TOOL_ROUNDS} 轮调用", ctx=ctx)
                if asyncio.current_task().cancelled():
                    raise asyncio.CancelledError()

                # 动态更新系统提示中的轮次信息
                updated_sys = build_system_prompt(
                    thread_key, user_message, username,
                    msg_type, raw_message_json, bot_client, msg_id,
                    enable_group_manage=enable_group_manage,
                    round_num=round_num,
                    max_rounds=MAX_TOOL_ROUNDS
                )
                if messages and messages[0].get("role") == "system":
                    messages[0] = {"role": "system", "content": updated_sys}
                else:
                    messages.insert(0, {"role": "system", "content": updated_sys})

                ai_msg = await call_ai_with_tools(messages, "main", tools, temperature=0.7, ctx=ctx)
                tool_calls = ai_msg.get("tool_calls", [])
                content = ai_msg.get("content", "")
                messages.append(ai_msg)
                INTERRUPT_CONTEXT[thread_key] = messages

                if content and content.strip():
                    full_response_parts.append(content.strip())
                    info(f"[工具循环] 第 {round_num} 轮 AI 回复已加入最终返回，长度: {len(content)}", ctx=ctx)

                # 检测是否调用了 skip_reply
                for tc in tool_calls:
                    if tc.get("function", {}).get("name") == "skip_reply":
                        skip_reply_called = True
                        break

                if not tool_calls:
                    info(f"[工具循环] 第 {round_num} 轮无工具调用，结束循环", ctx=ctx)
                    break

                info(f"[工具循环] 第 {round_num} 轮 AI 调用 {len(tool_calls)} 个工具", ctx=ctx)

                # 保存原始格式的工具调用消息到历史记录（不转为【使用工具】格式，直接保存原始字典）
                hist = load_history(thread_key)
                hist.append(ai_msg)
                save_history(thread_key, hist)
                info(f"[工具历史] 已保存原始工具调用消息（{len(tool_calls)} 个工具）", ctx=ctx)

                tool_tasks = []
                for tc in tool_calls:
                    tool_tasks.append(execute_tool_call(tc, bot_client, group_id, msg_type, recipient_id, msg_id, thread_key, author_id))
                tool_results = await asyncio.gather(*tool_tasks, return_exceptions=True)

                for result in tool_results:
                    if isinstance(result, Exception):
                        error(f"[工具循环] 工具执行异常: {result}", ctx=ctx)
                        err_msg = f"工具执行异常: {result}"
                        messages.append({"role": "tool", "tool_call_id": "", "content": err_msg})
                        # 保存工具执行异常到历史记录（原始字典格式）
                        err_dict = {"role": "tool", "tool_call_id": "", "content": err_msg}
                        hist = load_history(thread_key)
                        hist.append(err_dict)
                        save_history(thread_key, hist)
                        warn(f"[工具历史] 已保存工具执行异常", ctx=ctx)
                    else:
                        messages.append(result)
                        # 保存原始工具返回结果到历史记录（直接保存原始字典）
                        hist = load_history(thread_key)
                        hist.append(result)
                        save_history(thread_key, hist)
                        info(f"[工具历史] 已保存原始工具返回结果", ctx=ctx)
                    INTERRUPT_CONTEXT[thread_key] = messages

                # 如果调用了 skip_reply，不再继续下一轮（避免多余的AI调用）
                if skip_reply_called:
                    info("[工具循环] 检测到 skip_reply，终止后续AI调用", ctx=ctx)
                    break

                if round_num == MAX_TOOL_ROUNDS:
                    info(f"[工具循环] 达到最大轮次 {MAX_TOOL_ROUNDS}，追加最终总结", ctx=ctx)
                    try:
                        final_msg = await call_ai(messages, "main", stream=False, ctx=ctx)
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
            info(f"[生成回复] 线程 {thread_key} 被取消，上下文已保留", ctx=ctx)
            raise
        finally:
            if not canceled:
                if thread_key in INTERRUPT_CONTEXT:
                    del INTERRUPT_CONTEXT[thread_key]
                    debug(f"[上下文] 清理线程 {thread_key} 的上下文", ctx=ctx)
            else:
                # 取消时保留上下文，供外部保存
                debug(f"[上下文] 保留线程 {thread_key} 的上下文供外部保存", ctx=ctx)

    # ---------- 发送与保存历史（新增） ----------
    # 1. 发送文本（如果没有调用 skip_reply 且有内容）
    send_msg_id = None
    send_msg_idx = None
    if not skip_reply_called and final_reply and final_reply.strip():
        sent_result = await bot_client.send_message(
            msg_type, recipient_id, final_reply, msg_id
        )
        sent_success = sent_result is not None
        if sent_success:
            send_msg_id = bot_client.get_last_send_id()
            send_msg_idx = bot_client.get_last_send_msg_idx()
            info(f"[AI发送] 成功发送文本, msg_id={send_msg_id}, msg_idx={send_msg_idx}", ctx=ctx)
        else:
            error(f"[AI发送] 文本发送失败", ctx=ctx)
    else:
        if skip_reply_called:
            warn("[AI发送] 因 skip_reply 跳过文本发送", ctx=ctx)
        # 如果 final_reply 为空但没 skip，可能没有内容，也不发送

    # 2. 保存历史
    # 注意：skip_reply 表示 AI 主动决定不回复，此时 final_reply 可能是兜底道歉文案，
    # 它既没发给用户，也不该被当成 AI 的回复写进历史（否则会污染上下文，下次喂回给模型）。
    if skip_reply_called:
        info("[AI历史] 因 skip_reply 不写入助手回复记录", ctx=ctx)
    else:
        save_content = final_reply if final_reply else "（已通过工具完成回复，未发送文本）"
        append_message(thread_key, "assistant", save_content,
                       msg_id=send_msg_id,
                       msg_idx=send_msg_idx)  # API返回的ref_idx = 本消息的msg_idx
        info(f"[AI历史] 已保存助手回复（长度 {len(save_content)}）", ctx=ctx)

    return final_reply, sent_success, skip_reply_called