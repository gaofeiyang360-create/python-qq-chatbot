# -*- coding: utf-8 -*-
# tool.py — 工具定义与执行（HTTP请求、搜索音乐、播放音乐、群管理等）
import re
import time
import asyncio
import requests
import json as _json
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

from config import get_executor, is_group_manage_enabled
from memory import (
    get_cached_media_summary, set_cached_media, get_media_cache_key, get_url_cache_key,
    append_message,
)

# ==================== 默认请求头（通用） ====================
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ==================== 网易云音乐搜索专用请求头（禁用压缩） ====================
NETEASE_SEARCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://music.163.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "identity",  # 禁用压缩，避免 Brotli 乱码
    "Connection": "keep-alive",
}

# ==================== 工具描述生成 ====================
def get_tools_description(enable_group_manage: bool = False) -> str:
    """返回工具描述文本，用于系统提示词，与工具定义保持一致。"""
    base_desc = (
        "你可以使用工具来完成任务：通过 http_request 发起任意网络请求获取信息；"
        "使用 send_media 发送图片、视频或文件（支持 HTTP/HTTPS URL ，系统会自动处理上传）；"
        "使用 send_text 向当前会话发送纯文本文字（当你需要通过工具发送自定义文本时使用，发送后请调用 skip_reply 以终止工具循环，避免重复发送）；"
        "使用 recognize_media 主动识别媒体内容并获取摘要；"
        "使用 search_music 搜索歌曲；"
        "使用 play_music 根据歌曲ID播放音乐（仅限非VIP歌曲）。"
    )
    if enable_group_manage:
        base_desc += " 在群管理白名单群内可使用 mute_member/unmute_member 管理群成员禁言。"
    base_desc += " 需要工具时请直接调用，不要在文本中描述工具调用。"
    return base_desc

# ==================== 工具定义 ====================
def get_tools_definition(enable_group_manage: bool = False) -> List[Dict]:
    """返回可用工具定义，群管理工具仅在白名单群内启用"""
    tools = [
        {
            "type": "function",
            "function": {
                "name": "http_request",
                "description": "发起任意HTTP网络请求，可用于获取网页内容、调用API、查询数据等。返回状态码、响应头和响应体（自动截断）。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "method": {
                            "type": "string",
                            "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
                            "description": "HTTP请求方法"
                        },
                        "url": {
                            "type": "string",
                            "description": "完整的请求URL，必须包含http://或https://"
                        },
                        "headers": {
                            "type": "object",
                            "description": "请求头，键值对形式，例如 {\"Content-Type\": \"application/json\"}"
                        },
                        "body": {
                            "type": "string",
                            "description": "请求体内容，JSON请用字符串形式"
                        },
                        "params": {
                            "type": "object",
                            "description": "URL查询参数，键值对形式"
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "超时时间（秒），0表示不限制，默认15"
                        },
                        "max_body_length": {
                            "type": "integer",
                            "description": "响应体最大截断长度（字符），0表示不截断，默认8000"
                        },
                        "max_header_length": {
                            "type": "integer",
                            "description": "响应头最大截断长度（字符），0表示不截断，默认500"
                        },
                        "verify_ssl": {
                            "type": "boolean",
                            "description": "是否验证SSL证书，默认true。设为false可访问自签名证书的HTTPS站点"
                        },
                        "cookies": {
                            "type": "object",
                            "description": "Cookie，键值对形式，例如 {\"session_id\": \"abc123\"}"
                        },
                        "proxy": {
                            "type": "string",
                            "description": "代理地址，例如 http://127.0.0.1:8080 或 socks5://127.0.0.1:1080"
                        },
                        "allow_redirects": {
                            "type": "boolean",
                            "description": "是否自动跟随重定向，默认true。设为false可获取原始重定向响应"
                        }
                    },
                    "required": ["method", "url"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "send_media",
                "description": "向当前会话发送一张图片、视频或文件。支持 HTTP/HTTPS URL 。系统会自动上传并处理大小限制。可以附加文字说明。可指定文件类型和文件名，若不指定则自动识别。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "source": {
                            "type": "string",
                            "description": "媒体文件的 URL（http/https）"
                        },
                        "caption": {
                            "type": "string",
                            "description": "可选的文字说明，不超过100字"
                        },
                        "file_type": {
                            "type": "integer",
                            "enum": [1, 2, 3, 4],
                            "description": "媒体类型：1=图片，2=视频，3=语音，4=文件。不传则根据文件名或URL自动识别。"
                        },
                        "file_name": {
                            "type": "string",
                            "description": "文件名（可选），用于辅助判断类型和显示。"
                        }
                    },
                    "required": ["source"]
                }
            }
        },
        # ==================== 发送文本工具 ====================
        {
            "type": "function",
            "function": {
                "name": "send_text",
                "description": "向当前会话发送纯文本内容。当你需要通过工具发送一段自定义文字，而不是让AI直接生成文本回复时使用此工具。注意：发送纯文本后必须紧接着调用 skip_reply（skip_reply 会终止工具循环，防止 AI 再生成多余文本回复）。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "text_content": {
                            "type": "string",
                            "description": "要发送的文本内容，建议不超过2000字"
                        }
                    },
                    "required": ["text_content"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "recognize_media",
                "description": "识别指定URL的媒体（图片或视频）内容，生成一段摘要描述。支持缓存，重复请求会返回缓存结果。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "media_url": {
                            "type": "string",
                            "description": "媒体文件的URL（http/https）"
                        },
                        "media_type": {
                            "type": "string",
                            "enum": ["image", "video"],
                            "description": "媒体类型，如果不指定则根据文件扩展名自动判断"
                        },
                        "filename": {
                            "type": "string",
                            "description": "文件名，用于辅助判断类型，可选"
                        },
                        "disable_cache": {
                            "type": "boolean",
                            "description": "是否禁用缓存，如果为 true，则强制重新识别并覆盖缓存结果"
                        }
                    },
                    "required": ["media_url"]
                }
            }
        },
        # ==================== 搜索音乐 ====================
        {
            "type": "function",
            "function": {
                "name": "search_music",
                "description": "根据关键词搜索音乐（歌曲），返回匹配的歌曲列表，包括歌曲名、歌手、歌曲ID。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "keywords": {
                            "type": "string",
                            "description": "搜索关键词，如歌曲名或歌手名"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "返回结果数量，默认10，最大30"
                        },
                        "offset": {
                            "type": "integer",
                            "description": "分页偏移，默认0"
                        }
                    },
                    "required": ["keywords"]
                }
            }
        },
        # ==================== 播放音乐（增加 use_file 参数） ====================
        {
            "type": "function",
            "function": {
                "name": "play_music",
                "description": "根据歌曲ID播放音乐，获取可播放的URL并发送为音频文件（仅限非VIP歌曲）。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "song_id": {
                            "type": "string",
                            "description": "歌曲的ID（从search_music结果中获取）"
                        },
                        "caption": {
                            "type": "string",
                            "description": "可选的发送时附带的文字说明，如'播放歌曲：xxx'"
                        },
                        "use_file": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "是否强制使用文件模式（跳过语音尝试），0=优先语音，1=仅文件，默认0"
                        }
                    },
                    "required": ["song_id"]
                }
            }
        },
        # ==================== 新增：跳过回复工具 ====================
        {
            "type": "function",
            "function": {
                "name": "skip_reply",
                "description": "调用此工具表示终止本轮工具循环，AI 不再继续调用工具，也不再生成文本回复。适用于已经通过 send_media、send_text、play_music 等工具完成回复的场景。注意：调用 skip_reply 后本轮工具循环立即终止，不再执行后续工具调用。",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        }
    ]

    if enable_group_manage:
        tools.extend([
            {
                "type": "function",
                "function": {
                    "name": "mute_member",
                    "description": "禁言群内指定成员。只能禁言普通成员，不能禁言群主、管理员或机器人。禁言到期后自动解除。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "member_openid": {
                                "type": "string",
                                "description": "要禁言的成员openid，从消息JSON的author.member_openid获取"
                            },
                            "mute_expire_at": {
                                "type": "string",
                                "description": "禁言到期时间，RFC3339格式，例如 2026-08-23T18:00:00+08:00"
                            }
                        },
                        "required": ["member_openid", "mute_expire_at"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "unmute_member",
                    "description": "解除群内指定成员的禁言状态。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "member_openid": {
                                "type": "string",
                                "description": "要解除禁言的成员openid"
                            }
                        },
                        "required": ["member_openid"]
                    }
                }
            }
        ])

    return tools

# ==================== HTTP 请求工具 ====================
async def execute_http_request(method: str, url: str, headers: Dict = None,
                               body: str = None, params: Dict = None,
                               timeout: int = 15, max_body_length: int = 8000,
                               max_header_length: int = 500, verify_ssl: bool = True,
                               cookies: Dict = None, proxy: str = None,
                               allow_redirects: bool = True) -> str:
    """执行任意HTTP请求，返回格式化的结果字符串"""
    import json as _json
    loop = asyncio.get_event_loop()

    def _do_request():
        # 合并 headers：模型传的优先，缺 UA 则自动补默认 UA
        merged_headers = dict(headers or {})
        if not any(k.lower() == "user-agent" for k in merged_headers):
            merged_headers.setdefault("User-Agent", DEFAULT_HEADERS["User-Agent"])
        kwargs = {
            "headers": merged_headers,
            "verify": verify_ssl,
            "allow_redirects": allow_redirects,
        }
        # 超时：0 表示不限制，传 None 给 requests
        if timeout == 0:
            kwargs["timeout"] = None
        else:
            kwargs["timeout"] = timeout

        if params:
            kwargs["params"] = params
        if body:
            kwargs["data"] = body.encode("utf-8") if isinstance(body, str) else body
        if cookies:
            kwargs["cookies"] = cookies
        if proxy:
            kwargs["proxies"] = {
                "http": proxy,
                "https": proxy,
            }
        resp = requests.request(method.upper(), url, **kwargs)
        return resp

    try:
        timeout_display = timeout if timeout != 0 else "不限"
        print(f"[HTTP工具] {method} {url[:80]}（超时={timeout_display}s, SSL验证={verify_ssl}, 跟随重定向={allow_redirects}）")
        resp = await loop.run_in_executor(get_executor(), _do_request)
        status = resp.status_code
        resp_headers = dict(resp.headers)

        content = resp.text
        truncated = False
        if max_body_length != 0 and len(content) > max_body_length:
            content = content[:max_body_length]
            truncated = True

        result = f"HTTP {status} {resp.reason}\n"
        result += f"URL: {url}\n"
        if resp_headers:
            header_str = _json.dumps(resp_headers, ensure_ascii=False)
            if max_header_length != 0 and len(header_str) > max_header_length:
                header_str = header_str[:max_header_length]
            result += f"响应头: {header_str}\n"
        result += f"响应体:\n{content}"
        if truncated:
            result += f"\n...（响应过长，已截断至{max_body_length}字符）"

        print(f"[HTTP工具] 完成，状态码 {status}，响应长度 {len(content)}")
        return result
    except requests.exceptions.Timeout:
        timeout_msg = f"HTTP请求超时（{timeout}秒）" if timeout != 0 else "HTTP请求超时（无限制超时仍有系统级超时）"
        return f"{timeout_msg}: {method} {url}"
    except requests.exceptions.ConnectionError as e:
        return f"HTTP连接失败: {method} {url}\n错误: {e}"
    except Exception as e:
        return f"HTTP请求异常: {method} {url}\n错误: {type(e).__name__}: {e}"

# ==================== 工具调用执行（不含媒体识别，动态导入 ai） ====================
async def execute_tool_call(tool_call: Dict, bot_client, group_id: str = None,
                            msg_type: str = None, recipient_id: str = None,
                            msg_id: Optional[str] = None,
                            thread_key: Optional[str] = None) -> Dict:
    """
    执行单个工具调用，返回 {"role": "tool", "tool_call_id": ..., "content": ...} 格式的结果消息。
    当识别到 recognize_media 时，动态导入 ai 模块调用相关函数。
    """
    import json as _json
    from msg import ensure_rfc3339_time  # 用于群管理

    tool_call_id = tool_call.get("id", "")
    function_name = tool_call.get("function", {}).get("name", "")
    arguments_str = tool_call.get("function", {}).get("arguments", "{}")

    try:
        arguments = _json.loads(arguments_str) if arguments_str else {}
    except _json.JSONDecodeError:
        arguments = {}
        print(f"[工具执行] 参数解析失败: {arguments_str[:200]}")

    print(f"[工具执行] 调用 {function_name}，参数: {_json.dumps(arguments, ensure_ascii=False)[:200]}")

    result_content = ""

    if function_name == "http_request":
        method = arguments.get("method", "GET")
        url = arguments.get("url", "")
        req_headers = arguments.get("headers")
        req_body = arguments.get("body")
        req_params = arguments.get("params")
        req_timeout = arguments.get("timeout", 15)
        req_max_body = arguments.get("max_body_length", 8000)
        req_max_header = arguments.get("max_header_length", 500)
        req_verify_ssl = arguments.get("verify_ssl", True)
        req_cookies = arguments.get("cookies")
        req_proxy = arguments.get("proxy")
        req_allow_redirects = arguments.get("allow_redirects", True)
        if not url:
            result_content = "错误：http_request 缺少 url 参数"
        else:
            result_content = await execute_http_request(
                method, url, req_headers, req_body, req_params,
                timeout=req_timeout, max_body_length=req_max_body,
                max_header_length=req_max_header, verify_ssl=req_verify_ssl,
                cookies=req_cookies, proxy=req_proxy,
                allow_redirects=req_allow_redirects,
            )

    elif function_name == "send_media":
        source = arguments.get("source")
        caption = arguments.get("caption", "")
        file_type = arguments.get("file_type")
        file_name = arguments.get("file_name", "")
        if not source:
            result_content = "错误：send_media 缺少 source 参数"
        elif not recipient_id:
            result_content = "错误：无法获取当前会话的接收者ID，请检查上下文"
        else:
            # 自动推断文件类型（如果未指定）
            if file_type is None:
                ext = ""
                if file_name:
                    ext = Path(file_name).suffix.lower()
                if not ext:
                    # 从 URL 中提取扩展名
                    url_path = urlparse(source).path
                    ext = Path(url_path).suffix.lower()
                if ext in ['.jpg', '.jpeg', '.png']:
                    file_type = 1
                elif ext in ['.mp4']:
                    file_type = 2
                elif ext in ['.silk']:
                    file_type = 3
                else:
                    file_type = 4  # 默认文件

            # 调用客户端发送媒体，传递文件类型、文件名和 msg_id（关键修复）
            success = await bot_client.send_message(
                msg_type=msg_type,
                recipient_id=recipient_id,
                content=caption,
                media_source=source,
                file_type=file_type,
                file_name=file_name,
                msg_id=msg_id   # 传入原始消息ID，实现被动回复
            )
            if success:
                result_content = f"媒体发送成功：{source}"
                if caption:
                    result_content += f"（附带文字：{caption}）"
                # 将实际发送内容写入历史记录
                if thread_key:
                    media_desc = f"媒体发送成功（{source}）"
                    if caption:
                        media_desc = f"媒体发送成功（{source}，附带文字：{caption}）"
                    append_message(thread_key, "assistant", media_desc)
            else:
                result_content = f"媒体发送失败：{source}，请检查文件是否有效或上传权限"

    # ==================== 发送纯文本 ====================
    elif function_name == "send_text":
        text_content = arguments.get("text_content", "")
        if not text_content or not text_content.strip():
            result_content = "错误：send_text 缺少 text_content 参数"
        elif not recipient_id:
            result_content = "错误：无法获取当前会话的接收者ID，请检查上下文"
        else:
            success = await bot_client.send_message(
                msg_type=msg_type,
                recipient_id=recipient_id,
                content=text_content,
                msg_id=msg_id
            )
            if success:
                result_content = f"文本发送成功（长度 {len(text_content)} 字）"
                # 将实际发送的文本内容写入历史记录
                if thread_key:
                    append_message(thread_key, "assistant", text_content)
            else:
                result_content = "文本发送失败，请检查网络或权限"

    # ==================== 媒体识别（动态导入 ai 调用基础函数） ====================
    elif function_name == "recognize_media":
        media_url = arguments.get("media_url")
        media_type = arguments.get("media_type")
        filename = arguments.get("filename", "媒体")
        disable_cache = arguments.get("disable_cache", False)  # 新增
        if not media_url:
            result_content = "错误：recognize_media 缺少 media_url 参数"
        else:
            try:
                # 动态导入 ai 模块以避免循环依赖
                import ai
                result_content = await ai.recognize_media_by_url(
                    media_url, filename, media_type, disable_cache=disable_cache
                )
            except Exception as e:
                result_content = f"媒体识别失败：{e}"

    # ==================== 搜索音乐（禁用压缩，避免乱码） ====================
    elif function_name == "search_music":
        keywords = arguments.get("keywords")
        limit = arguments.get("limit", 10)
        offset = arguments.get("offset", 0)
        if not keywords:
            result_content = "错误：search_music 缺少 keywords 参数"
        else:
            try:
                import urllib.parse
                encoded_keywords = urllib.parse.quote(keywords)
                url = f"https://music.163.com/api/search/get/web?s={encoded_keywords}&type=1&offset={offset}&total=true&limit={limit}"
                loop = asyncio.get_event_loop()
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.get(url, timeout=10, headers=NETEASE_SEARCH_HEADERS)
                )
                if resp.status_code == 200:
                    # 获取原始文本并去除前导空白（换行、BOM等）
                    raw_text = resp.text.strip()
                    if not raw_text:
                        result_content = "搜索返回空内容"
                    else:
                        try:
                            data = _json.loads(raw_text)
                        except _json.JSONDecodeError as e:
                            # 如果仍然解析失败，打印部分内容以供调试
                            print(f"[搜索音乐] JSON解析失败，原始内容前200字符: {raw_text[:200]}")
                            raise e
                        songs = data.get("result", {}).get("songs", [])
                        if songs:
                            lines = [f"找到 {len(songs)} 首歌曲："]
                            for idx, song in enumerate(songs, 1):
                                name = song.get("name", "未知歌曲")
                                artists = ", ".join([a.get("name", "") for a in song.get("artists", [])])
                                song_id = song.get("id")
                                lines.append(f"{idx}. {name} - {artists} (ID: {song_id})")
                            result_content = "\n".join(lines)
                        else:
                            result_content = "未找到相关歌曲。"
                else:
                    result_content = f"搜索失败，状态码：{resp.status_code}"
            except _json.JSONDecodeError as e:
                result_content = f"搜索音乐响应解析失败：{e}，请检查接口返回格式。"
            except Exception as e:
                result_content = f"搜索音乐异常：{e}"

    # ==================== 播放音乐（支持 use_file 参数） ====================
    elif function_name == "play_music":
        song_id = arguments.get("song_id")
        caption = arguments.get("caption", "")
        use_file = arguments.get("use_file", 0)  # 默认 0
        # 转换为布尔值
        force_file = bool(use_file) or use_file == "1" or use_file == 1

        if not song_id:
            result_content = "错误：play_music 缺少 song_id 参数"
        elif not recipient_id:
            result_content = "错误：无法获取当前会话的接收者ID"
        else:
            try:
                original_url = f"https://music.163.com/song/media/outer/url?id={song_id}.mp3"
                loop = asyncio.get_event_loop()
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.get(original_url, timeout=10, allow_redirects=False, headers=DEFAULT_HEADERS)
                )
                if resp.status_code not in (301, 302):
                    result_content = f"获取音乐链接失败，状态码：{resp.status_code}，可能为VIP歌曲。"
                else:
                    real_url = resp.headers.get("Location")
                    if not real_url:
                        result_content = "获取音乐链接失败，未找到重定向地址。"
                    elif "404" in real_url or "music.163.com/404" in real_url:
                        result_content = f"该歌曲（ID: {song_id}）为VIP歌曲或无法播放，请尝试其他版本。"
                    else:
                        file_name = "1.mp3"
                        success = False
                        # 根据 use_file 构建尝试列表
                        if force_file:
                            attempts = [
                                ("原始外链-文件", original_url, 4),
                                ("真实URL-文件", real_url, 4),
                            ]
                        else:
                            attempts = [
                                ("原始外链-语音", original_url, 3),
                                ("原始外链-文件", original_url, 4),
                                ("真实URL-语音", real_url, 3),
                                ("真实URL-文件", real_url, 4),
                            ]
                        for label, url, ftype in attempts:
                            print(f"[播放音乐] 尝试 {label}")
                            ok = await bot_client.send_message(
                                msg_type=msg_type,
                                recipient_id=recipient_id,
                                content=caption or "正在播放音乐",
                                media_source=url,
                                file_type=ftype,
                                file_name=file_name,
                                msg_id=msg_id
                            )
                            if ok:
                                success = True
                                result_content = f"音乐播放成功（{label}）：{url}"
                                if caption:
                                    result_content += f"（{caption}）"
                                break
                            else:
                                print(f"[播放音乐] {label} 失败")
                        if not success:
                            result_content = "音乐发送失败（所有尝试均失败）。"
            except Exception as e:
                result_content = f"播放音乐异常：{e}"

    elif function_name == "mute_member":
        member_openid = arguments.get("member_openid", "")
        mute_expire_at = arguments.get("mute_expire_at", "")
        if not member_openid or not mute_expire_at:
            result_content = "错误：mute_member 缺少 member_openid 或 mute_expire_at 参数"
        elif not group_id:
            result_content = "错误：mute_member 只能在群聊中使用"
        else:
            valid_time = ensure_rfc3339_time(mute_expire_at, default_seconds=3600)
            success, error_code = await bot_client.set_group_mute(group_id, "add", member_openid, valid_time)
            if success:
                result_content = f"禁言成功：成员 {member_openid} 已禁言至 {valid_time}"
            elif error_code == 10007:
                beijing_tz = timezone(timedelta(hours=8))
                new_time = (datetime.now(beijing_tz) + timedelta(hours=1)).replace(microsecond=0).isoformat(timespec='seconds')
                success2, error_code2 = await bot_client.set_group_mute(group_id, "add", member_openid, new_time)
                if success2:
                    result_content = f"禁言成功（自动修正为1小时）：成员 {member_openid} 已禁言至 {new_time}"
                else:
                    result_content = f"禁言失败：成员 {member_openid}，错误码 {error_code2}"
            else:
                result_content = f"禁言失败：成员 {member_openid}，错误码 {error_code}"

    elif function_name == "unmute_member":
        member_openid = arguments.get("member_openid", "")
        if not member_openid:
            result_content = "错误：unmute_member 缺少 member_openid 参数"
        elif not group_id:
            result_content = "错误：unmute_member 只能在群聊中使用"
        else:
            success, error_code = await bot_client.set_group_mute(group_id, "del", member_openid, "")
            if success:
                result_content = f"解除禁言成功：成员 {member_openid}"
            else:
                result_content = f"解除禁言失败：成员 {member_openid}，错误码 {error_code}"

    # ==================== 新增 skip_reply 工具 ====================
    elif function_name == "skip_reply":
        # 不执行任何操作，仅返回成功标记
        result_content = "已标记跳过文本回复"

    else:
        result_content = f"错误：未知工具 {function_name}"

    print(f"[工具执行] {function_name} 结果: {result_content[:200]}")

    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": result_content
    }