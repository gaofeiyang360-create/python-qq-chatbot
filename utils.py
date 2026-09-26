# -*- coding: utf-8 -*-
# utils.py — 通用工具（媒体类型判定等纯函数）
#
# 设计约定：本模块只依赖标准库，绝不 import 本项目的其他模块。
# 原因是 ai.py / tool.py / msg.py 之间存在相互依赖，任何业务模块放进公共依赖链
# 都可能触发循环导入。作为"最底层"的 utils 保持零项目依赖，才可以被任何模块安全导入。
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional


# ==================== 媒体类型常量 ====================
# QQ 官方 /files 接口的 file_type 取值。提示词、工具定义与运行时逻辑全部以此为准。
FILE_TYPE_IMAGE = 1   # 图片
FILE_TYPE_VIDEO = 2   # 视频
FILE_TYPE_AUDIO = 3   # 语音
FILE_TYPE_FILE = 4    # 文件

# file_type → 中文名（用于日志、工具返回、提示模型）
FILE_TYPE_NAMES = {
    FILE_TYPE_IMAGE: "图片",
    FILE_TYPE_VIDEO: "视频",
    FILE_TYPE_AUDIO: "语音",
    FILE_TYPE_FILE: "文件",
}

# ==================== 扩展名分组 ====================
# 单一事实来源：发送侧推断、双发判定、接收侧识别全部引用这里，不要再各写一份。
IMAGE_EXTS = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')
VIDEO_EXTS = ('.mp4', '.mov', '.mkv', '.webm', '.avi', '.wmv', '.flv')
AUDIO_EXTS = ('.silk', '.mp3', '.wav', '.ogg', '.m4a', '.amr', '.flac', '.aac')

# 接收侧识别（视觉/视频模型）额外支持 svg（矢量图，仅用于识别，不作为发送侧图片）
RECOGNIZE_IMAGE_EXTS = IMAGE_EXTS + ('.svg',)


# ==================== 扩展名解析 ====================
def ext_from(file_name: str = None, source: str = None) -> str:
    """
    取扩展名（小写，含点）。优先文件名，其次从 URL 的 path 部分取。

    URL 的查询串会被自动剥离（urlparse().path 不含 ?query），
    因此 'https://x.com/a.PNG?w=100' 能正确得到 '.png'。
    取不到时返回空串。
    """
    if file_name:
        ext = Path(file_name).suffix.lower()
        if ext:
            return ext
    if source:
        return Path(urlparse(source).path).suffix.lower()
    return ""


# ==================== file_type 判定 ====================
def infer_file_type(file_name: str = None, source: str = None) -> int:
    """
    根据扩展名推断发送用的 file_type（1/2/3/4）。

    识别不出来时返回 4（文件），这是最安全的选择：
    非图片必须双发才能保证文字可见，当文件处理必然触发双发。
    """
    ext = ext_from(file_name, source)
    if ext in IMAGE_EXTS:
        return FILE_TYPE_IMAGE
    if ext in VIDEO_EXTS:
        return FILE_TYPE_VIDEO
    if ext in AUDIO_EXTS:
        return FILE_TYPE_AUDIO
    return FILE_TYPE_FILE


def needs_dual_send(file_type: int = None, file_name: str = None, source: str = None,
                    text: str = None) -> bool:
    """
    判断媒体消息是否需要"双发"（先发媒体，再单独发一条文本）。

    原因：图片(1)的 content 文字会随媒体一起显示，一条即可；
    视频/语音/文件(2/3/4)的 content 会被 QQ 客户端忽略，
    必须再单独发一条文本才能让说明/发起者信息可见。

    显式给了 file_type 就以它为准；没给则退化为按扩展名推断
    （推断不出图片就按需要双发处理，宁可多发一条也不丢文字）。

    text：随媒体一起附带的文字（caption / 推送正文）。
          - None（默认）= 不检查文字，仅按类型判定（保持旧行为）
          - 传字符串时：文字为空则一律单发 —— 没有文字就没有"要让文字可见"
            的诉求，双发只会多出一条空消息。
    """
    if text is not None and not str(text).strip():
        return False
    if file_type is not None:
        return file_type != FILE_TYPE_IMAGE
    return infer_file_type(file_name, source) != FILE_TYPE_IMAGE


# ==================== 接收侧识别用 ====================
def recognize_kind(file_name: str = None, source: str = None) -> Optional[str]:
    """
    判断接收到的媒体属于哪一类，供 AI 识别使用。

    返回 "image" / "video"，都不是则返回 None（调用方据此跳过识别）。
    注意与 infer_file_type 的区别：这里额外接受 svg 作为图片，
    且不区分语音/文件（这两类不做识别）。
    """
    ext = ext_from(file_name, source)
    if ext in RECOGNIZE_IMAGE_EXTS:
        return "image"
    if ext in VIDEO_EXTS:
        return "video"
    return None


def file_type_name(file_type: int) -> str:
    """file_type 数值 → 中文名，未知返回 '媒体'。"""
    return FILE_TYPE_NAMES.get(file_type, "媒体")


# ==================== 消息格式解析 ====================
# 统一的正文参数名。send_text / push_message / schedule_push / api 全部以 content 为准。
# markdown_content / text_content 为历史写法，仅作兼容回退（见 parse_message_type）。
CONTENT_KEYS = ("content", "markdown_content", "text_content")

def parse_message_type(args: dict, allow_markdown_only: bool = False) -> tuple:
    """
    解析消息格式与正文，统一各发送入口的行为。

    参数名以 content 为准；markdown_content / text_content 作为历史写法继续兼容。
    正文取值优先级：
      - Markdown 模式：content > markdown_content > text_content
      - 纯文本模式：  content > text_content > markdown_content
    即 content 始终最优先（与"输入是 content"的约定一致），未给 content 时
    才回退到对应的旧参数名。若两者都给了且内容不同，以 content 为准。

    格式判定：message_type（兼容 msg_type）为 markdown/md/2 时视为富文本。
    仅传 markdown_content 而未声明 message_type 时，也按 Markdown 处理
    —— 参数名本身已表明意图，避免旧调用静默降级为纯文本。

    allow_markdown_only：传入 markdown 字段（非 None）但未指定 message_type 时，
    是否判定为 Markdown。api_server.py 需要该行为（支持模板对象），工具侧不需要。

    返回 (is_markdown, body)，两者都按需取用。
    """
    if not isinstance(args, dict):
        return False, ""

    raw_type = args.get("message_type")
    if raw_type is None or str(raw_type).strip() == "":
        raw_type = args.get("msg_type")
    # 兼容数字 0/2：2=markdown，0/其他=text
    # 注意：None 必须归一到 ""，否则 str(None) 会得到 "none" 导致后续判断失效
    declared = "" if raw_type is None else str(raw_type).strip().lower()
    is_markdown = declared in ("markdown", "md", "2")

    # 旧写法：只给了 markdown_content 没声明格式 → 按 Markdown 处理
    if not is_markdown and declared == "" and args.get("markdown_content") is not None:
        is_markdown = True

    if not is_markdown and allow_markdown_only and args.get("markdown") is not None:
        is_markdown = True

    body = ""
    # content 始终最优先；其余按键按模式排列作为兼容回退
    fallback = ("markdown_content", "text_content") if is_markdown \
        else ("text_content", "markdown_content")
    for key in ("content",) + fallback:
        val = args.get(key)
        if val is not None and str(val).strip():
            # 统一转字符串：模型可能把数字/布尔当正文传（如 content:123），
            # 保留原类型会让下游的 len()/切片/发送接口报错
            body = val if isinstance(val, str) else str(val)
            break
    # 兜底：显式传入的 markdown 对象/字符串（api 侧用法）
    if not body and args.get("markdown") is not None:
        md = args.get("markdown")
        body = md.get("content", "") if isinstance(md, dict) else md
        if not isinstance(body, str):
            body = str(body)

    return is_markdown, body