# -*- coding: utf-8 -*-
# msg.py — 消息解析处理（消息解析、媒体/引用/聊天记录、冷却队列、事件处理）
import re
import json
import asyncio
import time
import base64
import requests
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone

from config import (
    get_cooldown_seconds, get_judge_context_limit,
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
    history_len,   # 记录本轮对话的历史起点（见 task_history_start）
)
from ai import (
    generate_reply, should_reply_in_group, recognize_media, recognize_media_by_url,
    media_result_ok,
    INTERRUPT_CONTEXT,
)
from utils import recognize_kind, RECOGNIZE_IMAGE_EXTS, VIDEO_EXTS

# 仅用于类型提示，避免运行时循环导入
from typing import TYPE_CHECKING
from log import info, warn, error, debug, LogCtx, spawn_background
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

# message_scene.ext 的条目解析：文档规定为 "键=值" 字符串，键名如 msg_idx / ref_msg_idx。
# 兼容 "=" / ":" 分隔、等号两侧空格、大小写；值部分由调用方再按 "&" 取第一段。
_EXT_KV_RE = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*[=:]\s*(.*)$')


def annotate_media_summary(summary: str) -> str:
    """对识别摘要做轻量标注（不截断）。

    原先这里会在摘要超过 600 字时砍掉后半段（"摘要过长，已截断"）。
    实测中长摘要往往正是因为画面文字多、信息密 —— 被砍掉的恰恰是最该让
    机器人看到的部分，导致它答复时漏掉关键内容。因此改为完整保留，
    只对"过短"仍加一句提示（过短通常意味着识别不完整，值得标注）。

    截断移除后，摘要长度完全由视觉模型的输出决定，不再有本地上限。
    """
    text = str(summary or "")
    if len(text) < 50:
        return text + "（摘要过短，可能识别不完整）"
    return text


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


def _detach_to_background(task: "asyncio.Task", filename: str, ctx=None) -> None:
    """把识别任务转为后台执行：继续跑完、写缓存，异常只记日志。

    注意不在这里 await —— 调用方要立刻返回占位内容，不能等它。

    ctx 必须由调用方传入：媒体识别本身是有归属的（属于某个机器人的某个会话），
    后台任务只是"延迟收尾"，不该因此丢掉 app_id / thread_key。
    """
    _background_media_tasks.add(task)

    def _done(t):
        _background_media_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            error(f"[媒体识别] 后台识别失败 {filename}: {exc}", ctx=ctx)
        else:
            debug(f"[媒体识别] 后台识别完成并已写缓存: {filename}", ctx=ctx)

    task.add_done_callback(_done)


def _media_task_outcome(task: "asyncio.Task", ctx=None) -> tuple:
    """把已完成的识别任务结果转成 (ok, value)。

    成功 → (True, 摘要文本)
    失败 → (False, 占位说明)：调用方据此改用 unrecognized_media_block，
           把 URL 交给机器人自己用工具识别。成功与否取 ai.MediaResult.ok，
           不再靠匹配返回文本里的中文文案。

    ctx 由调用方传入：识别失败是有归属的（某个机器人 / 某个会话），
    日志里应能直接看出是谁的媒体出了问题。
    """
    try:
        result = task.result()
    except Exception as e:
        error(f"[媒体识别] 识别任务异常: {e}", ctx=ctx)
        return False, f"（媒体识别失败: {e}）"
    if media_result_ok(result):
        return True, result
    return False, str(result)


async def recognize_media_cancellable(media_type: str, url: str, filename: str,
                                      height: int, width: int, app_id: str,
                                      thread_key: str, gen_at_start: int):
    """
    媒体识别：是否阻塞由全局实时配置 MEDIA_BLOCK 决定。

    返回 (ok, value)：
      - ok=True  → value 是识别摘要（本轮直接就拿到了，且识别确实成功）
      - ok=False → value 为 "superseded"：本轮改用占位（URL + 照抄参数）；
                   或为识别失败的占位说明（识别跑完了但没拿到有效摘要）

    说明：识别失败（模型返回空/超时/不支持的类型）与"被新消息打断"是两种不同的
    让位原因，但调用方处理方式一致 —— 都改用 unrecognized_media_block 占位，
    把 URL 留给机器人自己用工具识别。成功与否由 ai.MediaResult.ok 结构化判定。

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
                return _media_task_outcome(task, ctx=ctx)
            # 阻塞模式下忽略"来了新消息"，继续等识别结果（旧逻辑）
            if not blocking and media_should_abort(thread_key, gen_at_start):
                # 判定让位时再确认一次：可能在轮询间隔内刚好跑完，那就直接用结果。
                if task.done():
                    debug(f"[媒体识别] 判定让位时识别已完成，采用其结果: {filename}", ctx=ctx)
                    return _media_task_outcome(task, ctx=ctx)
                # ★ 不 cancel：把任务转交后台，让它跑完并写缓存。
                _detach_to_background(task, filename, ctx=ctx)
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


def replace_mentions_with_names(text: str, mentions: List[Dict],
                                bot_appid: str = "") -> str:
    """把正文里的 <@openid> / <@!openid> 标记替换成「@用户名(id)」。

    ★ 替换而非删除：QQ 下发的纯 @ 消息 content 为空串，@ 信息只存在于
      payload.mentions 中。若把标记删掉，正文就彻底空了，模型无从得知
      「有人@了我」。保留成 @用户名(id) 才能让 AI 看见这次提及。

    输出格式固定为 @用户名(openid)：
      - 用户名让模型能自然称呼对方，也便于在回复里 @ 对应的人；
      - 括号里的 openid 保留原始身份标识，模型需要精确 @（<@!user_id>）
        或调用禁言等按 id 操作的工具时可以直接取用，无需另查 JSON。

    用户名兜底优先级（逐级降级，保证绝不丢提及）：
      1. 本条消息 mentions 里带的 username（QQ 随包下发，最准）；
      2. mirror.json 的 user_map 映射表（历史消息里积累的昵称）；
      3. 原始 openid 本身（都没有时按 @id(id) 输出，格式保持统一）。
    """
    def replacer(match):
        id_str = match.group(1).lstrip('!')
        name = ""
        for m in mentions:
            if m.get("id") == id_str:
                name = (m.get("username") or "").strip()
                break
        if not name and bot_appid:
            name = (get_user_name(id_str, bot_appid) or "").strip()
        # 兜底：查不到用户名就用 openid 充当名字，格式仍是 @X(id)
        return f"@{name or id_str}({id_str})"
    return re.sub(r'<@([^>]+)>', replacer, text)


# ==================== 聊天记录转发媒体解析 ====================
def parse_forwarded_chatlog(text: str) -> Tuple[str, List[Dict]]:
    """把转发聊天记录里的附件行替换成占位符，返回 (替换后文本, 媒体列表)。

    media_list[i] 对应文本里的 [MEDIA_PLACEHOLDER_i]。两者由同一个
    placeholder_index 递增产生，因此**始终一一对应**：无 URL 的附件行走
    else 分支、保留原文，不生成占位符也不占下标。

    调用方请用 media_list 的下标去替换占位符（见 replace_media_placeholders），
    不要自己拼序号 —— 序号文案属于本函数的内部约定。
    """
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
                placeholder = make_media_placeholder(placeholder_index)
                result_lines.append(placeholder)
                placeholder_index += 1
            else:
                result_lines.append(line)
        else:
            result_lines.append(line)

    return '\n'.join(result_lines), media_list


def make_media_placeholder(idx: int) -> str:
    """生成媒体占位符。序号生成与替换统一走这里，避免两处各拼一遍文案。"""
    return f"[MEDIA_PLACEHOLDER_{idx}]"


def replace_media_placeholders(text: str, replacements: Dict[int, str]) -> str:
    """按 media_list 下标替换占位符，每处只替换一次。

    原先调用方直接用 str.replace("[MEDIA_PLACEHOLDER_i]", ...)。str.replace
    会替换**所有**出现位置，因此转发内容里若原本就含有 "[MEDIA_PLACEHOLDER_0]"
    这样的字面文本（用户手打、或转发的内容本身就是一段带占位符的日志），
    那处原文会被一并改写成媒体描述，污染聊天记录。

    这里改为从上到下扫描、命中的占位符就地消费一次，保证：
      - 每个下标只替换它自己那一处
      - 原文里碰巧同名的字样，若已被真实占位符消费则不再受影响
      - 未被替换的占位符原样保留（便于排查）
    """
    if not replacements:
        return text

    def _sub(match: "re.Match") -> str:
        idx = int(match.group(1))
        # 只在首次出现时替换，之后同下标的文本保留原样
        repl = replacements.get(idx)
        if repl is None:
            return match.group(0)
        del replacements[idx]
        return repl

    return re.sub(r'\[MEDIA_PLACEHOLDER_(\d+)\]', _sub, text)


# ==================== RFC3339 时间规范化 ====================
def ensure_rfc3339_time(expire_str: str, default_seconds: int = 3600, ctx=None) -> str:
    """
    确保时间字符串符合 RFC3339 格式，并带有时区（+08:00）。
    如果 expire_str 无效或缺失，则生成当前时间 + default_seconds 秒的时间。

    ctx 可选：解析失败是有归属的（谁在设置禁言时间），传入后日志能直接看出
    app_id / thread，而不是一行 appid=? 的无主记录。
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
            warn(f"[时间修正] 解析输入时间失败 '{expire_str}': {e}，将使用默认时间", ctx=ctx)
    dt = datetime.now(beijing_tz) + timedelta(seconds=default_seconds)
    dt = dt.replace(microsecond=0)
    return dt.isoformat(timespec='seconds')


# ==================== 群管理（旧自动决策已废弃，仅保留空函数） ====================
# ==================== ARK 卡片消息格式化 ====================
# 说明：本模块对 ark_data 不做任何字段硬编码 —— 字段名一律沿用 ark_data 的原始键名，
# 因此 QQ 新增/改名字段时无需改动此文件。下方仅保留渲染所需的最小常量。

# markdown 表格单元格内必须处理的字符。
#
# ★ 竖线一律替换为全角「｜」而不是用反斜杠转义：
#   `\|` 只在普通行内文本里被可靠识别，一旦内容处于代码段/含反斜杠的 JSON 中，
#   许多解析器仍把它当作分列符，表格会被撑破（实测切列数从 3 变 5）。
#   全角竖线语义等价、视觉可辨，且在任何解析器下都不可能分列。
# 换行/回车压成空格，避免一行被拆断。
_ARK_CELL_ESCAPE = str.maketrans({"|": "｜", "\n": " ", "\r": " "})

# ark_data 中这些键的值是「子字典的容器」，拆开展示而不是塞成一坨 JSON。
# 注意：这不是字段白名单 —— 未列出的键照样会被完整渲染，
# 只是它们本身是 dict 时也会被展平，行为与 fields 一致。
_ARK_NESTED_KEYS = ("fields", "kv", "values", "data")


def _ark_cell(value: Any) -> str:
    """
    把任意值安全地渲染为 markdown 表格单元格文本。

    数组 / 字典 → JSON 文本（如 ["a", "b"]、{"k": 1}），保持标准 JSON 间距便于阅读；
    None → 空串（由调用方跳过该行）；
    其余标量 → 原样 str()。
    """
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            text = json.dumps(value, ensure_ascii=False)
        except Exception:
            text = str(value)
    else:
        text = str(value)
    text = text.translate(_ARK_CELL_ESCAPE).strip()
    return text


def _ark_flatten(ark_data: Dict) -> List[Tuple[str, Any]]:
    """
    把 ark_data 展平成「字段名 → 值」的有序列表。

    完全通用，不做任何字段硬编码：
      - 顶层每个键都成为一行，字段名就是它自己的键名（如 ark_type、ark_name、prompt）；
      - 容器键（fields / kv / values / data）里的键值对被摊平到同一层，
        字段名保留其原始键名（如 title、source、tag、preview）；
      - 容器与顶层键名冲突时用「父键.子键」消歧，保证不丢字段、不覆盖；
      - 嵌套 dict 且非容器键时，递归展平为「父.子」多行，避免大段 JSON 难以阅读；
      - list 保持为一行，按 JSON 文本呈现（如 ["a", "b"]），不逐项拆行 —— 逐项拆行没有
        确定的语义（可能是多图、多链接、多标签），JSON 能完整保留结构与顺序。
    """
    rows: List[Tuple[str, Any]] = []
    seen_names = set()

    def emit(name: str, value: Any) -> None:
        # 同名冲突时加序号后缀，宁可名字难看也不能覆盖丢数据
        final = name
        n = 2
        while final in seen_names:
            final = f"{name}#{n}"
            n += 1
        seen_names.add(final)
        rows.append((final, value))

    def walk(container: Dict, prefix: str = "", depth: int = 0) -> None:
        for key, value in container.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            # 非空 dict 才递归展平；空 dict 落到 emit，由 _ark_cell 渲染为 {}
            if isinstance(value, dict) and value and depth < 8:
                # 容器键不加前缀，让子字段保持其原始键名（fields.title → title）
                walk(value, "" if str(key) in _ARK_NESTED_KEYS else name, depth + 1)
            else:
                # list / 标量 / 空 dict / 超深嵌套：整体作为一个单元格（数组按 JSON 显示）
                emit(name, value)

    walk(ark_data)
    return rows


def format_ark_message(ark_data: Dict) -> str:
    """
    把 QQ 机器人下发的 ark_data 卡片消息格式化成 markdown 表格文本。

    输入形如：
        {
          "ark_type": "miniapp",
          "ark_name": "小程序",
          "prompt": "[每日打卡]快来完成今日学习打卡",
          "fields": {
            "title": "快来完成今日学习打卡",
            "source": "学习助手",
            "tag": "微信小程序",
            "preview": "https://...",
            "source_logo": "https://...",
            "tag_icon": "https://..."
          }
        }

    输出形如：
        | 字段 | 内容 |
        | --- | --- |
        | ark_type | miniapp |
        | ark_name | 小程序 |
        | prompt | [每日打卡]快来完成今日学习打卡 |
        | title | 快来完成今日学习打卡 |
        | source | 学习助手 |
        ...

    设计要点：
      - 本函数是「派生视图生成器」，只依据 ark_data 自身渲染，不接触也不改写原始 content；
      - ★ 不硬编码任何字段名：字段名一律使用 ark_data 里的原始键名，
        因此 QQ 新增字段、改字段名、或用本文档没描述过的卡片类型时都不会丢数据；
      - 容器键（fields/kv/values/data）内的键值对摊平到同一层，字段名保持原样；
      - 值为空的字段跳过（空字符串/None），非空值一律保留，不做「看起来像链接」之类的猜测；
      - 无任何可渲染字段时返回空串，由调用方决定回退策略（原始 content 始终由调用方保留）。
    """
    if not isinstance(ark_data, dict) or not ark_data:
        return ""

    rows = _ark_flatten(ark_data)

    lines = ["| 字段 | 内容 |", "| --- | --- |"]
    emitted = 0
    for name, value in rows:
        cell = _ark_cell(value)
        if not cell:
            continue
        lines.append(f"| {_ark_cell(name)} | {cell} |")
        emitted += 1

    if not emitted:
        return ""
    return "\n".join(lines)


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

    # 处理 ark_data：卡片消息格式化为 markdown 表格（含 prompt/fields 全部字段）
    #
    # ★ 原始 content 必须原样保留 —— 它是 QQ 下发的权威文本视图，而表格是我们
    #   依据 ark_data.fields 重建的派生视图。二者来源不同：只要 fields 缺字段、
    #   键名未收录、或结构变更，派生表格就可能比原始 content 少信息。
    #   因此表格只做「追加增强」，绝不替换或删除任何原始内容。
    ark_data = payload.get("ark_data", {})
    if isinstance(ark_data, dict) and ark_data:
        raw_content = result["content"] or ""
        ark_text = format_ark_message(ark_data)
        if ark_text:
            # 保留 [ARK:...] 标记，兼容既有依赖该标记的历史逻辑/记忆
            prompt_marker = str(ark_data.get("prompt") or "").strip()
            marker = f"[ARK:{prompt_marker}]" if prompt_marker else "[ARK]"
            if raw_content.strip():
                result["content"] = f"{raw_content}\n{ark_text}\n{marker}"
            else:
                result["content"] = f"{ark_text}\n{marker}"
    # 便于下游（记忆/日志/历史）直接取用结构化卡片信息
    result["ark_data"] = ark_data if isinstance(ark_data, dict) else {}

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
    # ext 是文档规定的 "键=值" 字符串数组，故按 k=v 解析（不是 JSON 对象）。
    # 这里只做健壮性加固，不改变格式约定：
    #   - 容忍 "=" / ":" 两种分隔符、等号两侧空格、键名大小写
    #   - 值里若混入 "&" 分隔的其它字段，只取第一段（避免脏值落进历史记录）
    #   - 空值归一为 None（原先会存成空串，下游 if 判假但字段已存在）
    #   - 解析不到时留一条 debug，不再完全静默
    msg_idx = None
    ref_msg_idx = None
    message_scene = payload.get("message_scene", {})
    if isinstance(message_scene, dict):
        ext_list = message_scene.get("ext", [])
        if isinstance(ext_list, list):
            skipped = []
            for ext_item in ext_list:
                if not isinstance(ext_item, str):
                    continue
                m = _EXT_KV_RE.match(ext_item)
                if not m:
                    skipped.append(ext_item[:60])
                    continue
                key = m.group(1).lower()
                # 只取值的第一段：文档里每个条目对应一个字段，
                # 万一上游把多个字段塞进同一条目也能拿到正确的值
                value = m.group(2).split("&", 1)[0].strip() or None
                if key == "msg_idx":
                    msg_idx = value
                elif key == "ref_msg_idx":
                    ref_msg_idx = value
            if skipped:
                warn(f"[索引解析] message_scene.ext 有 {len(skipped)} 条无法解析为 k=v: {skipped}",
                     ctx=LogCtx(app_id=bot_appid))
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
        # 同样只做标记→@用户名 的转换，不整段删除：删掉会丢掉这次提及。
        result["clean_content"] = re.sub(
            r'<@([^>]+)>',
            lambda m: f"@{m.group(1).lstrip('!')}({m.group(1).lstrip('!')})",
            result["content"]).strip()
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
# 存储当前任务的「历史起点」（见 ai.generate_reply 的 task_start）。
# 反复打断时每次恢复都要沿用同一个起点，否则已发送消息清单会被越过而失效，
# 所以它必须活在任务之外（INTERRUPT_CONTEXT 被取走时它仍在）。
task_history_start: Dict[str, int] = {}
# 隔离模式：正在唤醒中的会话集合。
# 唤醒任务处于隔离模式时登记在此，用户消息到达时不会取消它（唤醒不被打断），
# 用户消息正常入队，等唤醒结束后再按冷却队列处理。
wakeup_locked_threads: Dict[str, int] = {}
# 隔离唤醒锁的进入时间戳，用于 is_wakeup_locked 的自愈检查（见该函数说明）
_wakeup_lock_since: Dict[str, float] = {}
# 唤醒锁最长持有时间：正常唤醒几分钟内必然结束，超过即视为任务卡死
_WAKEUP_LOCK_MAX_SECONDS = 1800.0

# ==================== 会话处理串行锁（M1） ====================
# 同一个 thread_key 上的「生成回复 → 发送 → 写历史」必须串行执行。
#
# 为什么需要（M1）：成员进出事件（GROUP_MEMBER_ADD / GROUP_MEMBER_REMOVE）
# 走的是 handle_group_event，里面**直接 await generate_reply**，完全绕过了
# process_queue 的那套「取消旧任务 + 入队 + 冷却」机制。于是当这个群同时
# 有正常消息在处理时，两条链路会并发跑同一个 thread_key：
#   - 双方各自 generate_reply，都基于同一份历史做判断；
#   - 发送后各自 append_message，历史里出现交错/重复的 assistant 记录；
#   - 更糟的是 A 的 append 可能落在 B 读取历史**之后**，B 完全看不到 A
#     刚说的话，于是把同一件事回复两次。
# 这里用「每会话一把 asyncio.Lock」把两条链路合并到同一把锁上：
# process_queue 与事件处理都先取锁再跑，从根上消除并发写同一 thread_key。
#
# 为什么不用 threading.Lock：本函数全部在单一事件循环内 await，
# asyncio.Lock 才是正确工具（且不会阻塞事件循环）。
_thread_locks: Dict[str, "asyncio.Lock"] = {}


def get_thread_lock(thread_key: str) -> "asyncio.Lock":
    """取（或创建）该会话的处理锁。"""
    lock = _thread_locks.get(thread_key)
    if lock is None:
        lock = asyncio.Lock()
        _thread_locks[thread_key] = lock
    return lock


# 会话锁的数量上限保护：会话数理论上有限（群+私聊），但若书签/历史被
# 反复用不同 id 探测，字典会无界增长。超过上限时清掉「当前未加锁」的条目
# （正在被持有的锁绝不能移除，否则会凭空多出一把新锁而失去互斥）。
_THREAD_LOCKS_MAX = 5000


def _prune_thread_locks():
    if len(_thread_locks) <= _THREAD_LOCKS_MAX:
        return
    for k in [k for k, lk in _thread_locks.items() if not lk.locked()]:
        _thread_locks.pop(k, None)
        if len(_thread_locks) <= _THREAD_LOCKS_MAX // 2:
            break

# ==================== 媒体识别中断 ====================
# 识别世代号：每当同一会话有新消息到达就 +1。
# 识别开始前记录当时的世代号，识别过程中世代号变了说明"后面又有消息"，
# 此时立即放弃识别，只把 URL 留给机器人自己用工具识别（见 recognize_media_nonblocking）。
_media_gen: Dict[str, int] = {}


# ==================== 会话态字典的过期清理 ====================
# pending_contexts / task_history_start 都只在「任务被新消息打断」时才写入，
# 并**故意**保留到下一次恢复调用（见 process_queue 的 CancelledError 分支）——
# 这个设计本身是对的，反复打断时必须沿用同一个历史起点。
#
# 问题在于「没有下一次」的情形：会话最后一条消息触发打断后，用户再也不说话
# （或机器人重启后该会话被弃用），这两个字典里的条目就再也没有机会被 pop。
# 一个只被 @ 过一次的群就会永久留下一条，长期运行下随会话数无上限增长。
#
# 这里加一个带时间戳的登记表，按 TTL 惰性清理：每次有会话活动时顺手扫一遍，
# 不引入后台线程，也不改变「打断期间必须保留」的语义。
_SESSION_STATE_TTL = 1800.0          # 30 分钟没有后续活动即视为废弃
_session_state_touched: Dict[str, float] = {}


def _touch_session_state(thread_key: str):
    """登记会话活动时间，并顺带清理过期条目。"""
    now = time.monotonic()
    _session_state_touched[thread_key] = now

    # 惰性清理：只在有活动时扫描，且用「距上次清理」节流，避免每条消息都全表遍历
    last_sweep = getattr(_touch_session_state, "_last_sweep", 0.0)
    if now - last_sweep < 60.0:
        return
    _touch_session_state._last_sweep = now

    expired = [k for k, t in _session_state_touched.items() if now - t > _SESSION_STATE_TTL]
    for k in expired:
        _session_state_touched.pop(k, None)
        # 只清理「等待恢复」的残留；正在跑的任务不受影响
        # （正在跑的任务其 thread_key 会持续被 _touch 刷新）
        if not pending_process_tasks.get(k) or pending_process_tasks[k].done():
            pending_contexts.pop(k, None)
            task_history_start.pop(k, None)
    if expired:
        info(f"[会话态清理] 清理 {len(expired)} 个超过 {int(_SESSION_STATE_TTL)}s 未活动的会话残留", ctx=None)


def bump_media_gen(thread_key: str) -> int:
    """该会话有新消息到达：递增识别世代号，通知正在进行的识别尽快放弃。

    ★ LOW 修复：本字典每个会话留一项且**永不清理**，属于无界增长
      （进程长期运行 + 大量不同会话时会持续吃内存）。
      这里顺手做上限保护：超过上限时只清掉「当前没有识别在进行」的会话
      —— 正在识别的会话若被清掉，它的 media_should_abort 会因为取到 0
      而误判「世代没变」，导致识别该中断却没中断。
      清理是安全的：一个会话的世代号只有在「有识别在跑」时才有意义，
      空闲会话的编号被重置为 0 不影响任何判断。
    """
    if len(_media_gen) > _MEDIA_GEN_MAX:
        _prune_media_gen()
    _media_gen[thread_key] = _media_gen.get(thread_key, 0) + 1
    return _media_gen[thread_key]


_MEDIA_GEN_MAX = 5000


def _prune_media_gen():
    """清掉空闲会话的世代号（见 bump_media_gen 说明）。"""
    for k in [k for k in _media_gen if not _media_inflight.get(k)]:
        _media_gen.pop(k, None)
        if len(_media_gen) <= _MEDIA_GEN_MAX // 2:
            break


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


def lock_wakeup(thread_key: str):
    """登记隔离唤醒开始。

    ★ 与 unlock_wakeup 必须严格配对。调用方（wakeup_scheduler）已把
      unlock 放在 finally 里，因此正常返回、异常、被取消三条路径都会释放，
      这一点已实测确认。这里的兜底是为了另一种情况：唤醒任务卡死
      （generate_reply 内部 await 永不返回），finally 永远走不到，
      计数会一直 >= 1，该会话后续的用户消息就永远只入队、不被处理。
      记录进入时间戳，配合下面的自愈检查把它解开。
    """
    if not thread_key:
        return
    wakeup_locked_threads[thread_key] = wakeup_locked_threads.get(thread_key, 0) + 1
    _wakeup_lock_since[thread_key] = time.monotonic()


def unlock_wakeup(thread_key: str):
    """登记隔离唤醒结束"""
    if not thread_key:
        return
    left = wakeup_locked_threads.get(thread_key, 0) - 1
    if left > 0:
        wakeup_locked_threads[thread_key] = left
    else:
        wakeup_locked_threads.pop(thread_key, None)
        _wakeup_lock_since.pop(thread_key, None)


def is_wakeup_locked(thread_key: str) -> bool:
    """该会话是否正处于隔离唤醒中（此时用户消息不应打断唤醒处理）。

    自愈：唤醒锁持有超过 _WAKEUP_LOCK_MAX_SECONDS 视为「唤醒任务已卡死」，
    强制解锁并告警。正常唤醒最多几分钟，30 分钟仍未释放一定是异常路径
    （见 lock_wakeup 的说明），继续锁着会让该会话彻底失去响应 ——
    宁可放开、让堆积的用户消息正常处理。
    """
    if thread_key not in wakeup_locked_threads:
        return False
    since = _wakeup_lock_since.get(thread_key)
    if since is not None and time.monotonic() - since > _WAKEUP_LOCK_MAX_SECONDS:
        warn(f"[唤醒锁] 线程 {thread_key} 的隔离锁已持有超过 "
             f"{int(_WAKEUP_LOCK_MAX_SECONDS)}s，判定为异常残留，强制释放", ctx=None)
        wakeup_locked_threads.pop(thread_key, None)
        _wakeup_lock_since.pop(thread_key, None)
        return False
    return True


def flush_pending_after_wakeup(thread_key: str, bot_client: 'BotClient'):
    """隔离唤醒结束后，处理唤醒期间堆积的用户消息。

    这些消息在唤醒期间只入队、未处理，此处按原有冷却队列逻辑重新启动处理。

    ★ M2：本函数是「唤醒结束后把积压消息放出来」的唯一出口，因此它必须
      保证「锁已释放」且「积压队列非空」时一定启动处理。此前的写法是
        if not thread_key or is_wakeup_locked(thread_key): return
      问题在于 is_wakeup_locked 会在**锁残留**（唤醒任务卡死、自愈尚未
      触发）时返回 True —— 调用方（wakeup_scheduler 的 finally）此时已经
      执行过 unlock_wakeup，之后再没有任何代码会来重试 flush，
      于是队列里的用户消息**永久滞留**：表现为「机器人在这个群突然不理人了」，
      且再也恢复不了，除非进程重启或有新消息进来触发一次 flush。
      这里改为：只在「确实仍被锁着」时才推迟，并把推迟原因写进日志；
      同时把自愈判定提前（见下方 force 参数），确保调用方能强制放行。
    """
    ctx = LogCtx(app_id=bot_client.app_id, thread_key=thread_key)

    if not thread_key:
        return
    if is_wakeup_locked(thread_key):
        # 仍有其它唤醒在跑（计数 > 1）：让那次唤醒结束时再放行。
        # 但要留下来过的痕迹，否则滞留时无从排查。
        info(f"[隔离唤醒] 线程 {thread_key} 仍有唤醒进行中，积压消息继续等待", ctx=ctx)
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

    # 先过会话锁（M1）：与会话内其它处理链路（成员进出事件等）串行
    _prune_thread_locks()
    async with get_thread_lock(thread_key):
        await _process_queue_locked(thread_key, bot_client, ctx)


async def _process_queue_locked(thread_key: str, bot_client: 'BotClient', ctx):
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
                # 正文为空：区分「纯@」与「真·非文本」——两者对模型的含义完全不同。
                # 纯@ 时 content 为空但 mentions 里有 is_you=True，此时说「非文本内容」
                # 会让模型以为有图片/文件可处理（而 attachments 其实是空的），
                # 于是去猜或反问「你发了什么」。这里改为如实描述这次提及。
                _q = msg["parsed"]
                _mentioned = any(m.get("bot") and m.get("is_you")
                                 for m in _q.get("mentions", []))
                if _mentioned:
                    # 格式与正文替换保持一致：@用户名(id)
                    _names = [
                        f"@{m.get('username') or m.get('id', '')}({m.get('id', '')})"
                        for m in _q.get("mentions", [])
                        if m.get("bot") and m.get("is_you")
                    ]
                    merged_content += (
                        f"{user_identifier}: @了你（{'/'.join(_names)}，没有附带文字）\n")
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
        # 有会话活动 → 刷新时间戳并顺带清理过期残留（见 _touch_session_state）
        _touch_session_state(thread_key)
        initial_messages = saved_ctx.get("messages") if saved_ctx else None
        force_reply_override = saved_ctx.get("force_reply", False) if saved_ctx else False
        if force_reply_override:
            force_reply = True

        # 存储当前任务的 force_reply，以便取消时使用
        task_force_reply[thread_key] = force_reply

        # 本轮对话的历史起点：首次由 generate_reply 自行取；
        # 若上次是被打断的（起点已记在 task_history_start），这里取回来沿用，
        # 保证反复打断时「已发送消息」清单不会因起点后移而清空。
        inherited_start = task_history_start.get(thread_key)

        await handle_processed_message(parsed, thread_key, merged_content, queue,
                                       bot_client, initial_messages, force_reply,
                                       task_start=inherited_start)

        # 正常结束，清理
        task_history_start.pop(thread_key, None)
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
        # 注意：这里**故意不清** task_history_start —— 它要活到下一次恢复调用，
        # 否则反复打断时起点会被重置（本函数的调用方在正常结束时才清理）。
        # 不重新抛出，任务结束
    except Exception as e:
        error(f"[处理异常] 线程 {thread_key} 处理消息时发生错误: {e}", ctx=ctx)
        if thread_key in task_force_reply:
            del task_force_reply[thread_key]
        # 异常不是打断，没有"下一次恢复"，起点必须清掉，避免污染后续新对话
        task_history_start.pop(thread_key, None)


async def handle_processed_message(parsed: Dict, thread_key: str, merged_content: str,
                                   queue: List[Dict], bot_client: 'BotClient',
                                   initial_messages: Optional[List[Dict]] = None,
                                   force_reply: bool = False,
                                   task_start: Optional[int] = None):
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
        spawn_background(auto_manage_memory(thread_key, bot_client),
                         "auto_manage_memory", ctx)
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
    # task_start：沿用调用方传下来的起点（反复打断时不变）；首次为 None，由
    # generate_reply 自行取当前值，并在这里登记，供打断后的下一次调用沿用。
    if task_start is None:
        # history_len 失败返回 None 表示「未知」，此处原样保留：
        # generate_reply 会据此跳过「已发送消息」清单。不能退化成 0，
        # 否则 get_sent_messages_since(0) 会把整段历史算成本轮已发送。
        task_start = history_len(thread_key)
    if task_start is not None:
        task_history_start[thread_key] = task_start

    reply, sent_success, skip_reply_called = await generate_reply(
        thread_key,
        full_user_input,
        username,
        msg_type,
        raw_json_str,
        bot_client,
        msg_id=msg_id,
        initial_messages=initial_messages,
        recipient_id=recipient_id,   # 传递接收者ID
        task_start=task_start
    )

    if asyncio.current_task().cancelled():
        info("[处理取消] 生成回复完成但任务已取消，放弃后续", ctx=ctx)
        return

    # --- 后续操作：记忆管理（压缩、长期记忆）等 ---
    # 注意：此时历史记录已在 generate_reply 内部保存，我们只需要处理后续的记忆优化任务
    spawn_background(
        auto_manage_memory(
            thread_key,
            bot_client
        ),
        "auto_manage_memory", ctx
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

    ★ 群名每次消息都实时刷新（不再「有缓存就跳过」）：
      原先只要 mirror.json 里已有该群名字就直接 return，导致群改名后
      mirror 永远停留在旧名字上 —— save_group_to_mirror 的覆盖逻辑虽然
      正确，却再也没有机会被调用。改为每次都向 API 要一次最新群名，
      由 save_group_to_mirror 内部比对「值是否变化」来决定是否写盘，
      因此没有改名时不会产生多余的磁盘写入。

    这样用户昵称（依赖 payload 实时下发）与群名称（依赖本次 API 调用）
    两条更新路径的行为终于一致：都是「下次交互即刷新」。
    """
    ctx = LogCtx(app_id=bot_client.app_id)

    app_id = bot_client.app_id
    # 先记录到 user_map.json（无论是否获取到名称）
    record_group_id(app_id, group_openid, member_id)

    # 每次都用 API 的最新结果覆盖比对（save_group_to_mirror 内部仅在变化时写盘）
    debug(f"[群记录] 获取群信息: {group_openid}", ctx=ctx)
    group_data = await bot_client.get_group_info(group_openid)
    if group_data and group_data.get("group_name"):
        # 首次记录 / 群名称已更新 的日志由 save_group_to_mirror 统一输出，
        # 此处不再重复打 —— 否则改名时会出现两条含义相同的日志。
        save_group_to_mirror(app_id, group_openid, group_data["group_name"])
    else:
        # API 失败时保留原值（若有），下次收到该群消息会再次尝试
        warn(f"[群记录] 获取群名称失败，暂不记录: {group_openid}", ctx=ctx)


# ==================== 消息处理入口 ====================
def format_referenced_message(thread_key: str, ref_msg_idx: str) -> Optional[Dict]:
    """
    根据 ref_msg_idx 在聊天历史中查找被引用的原消息。
    返回 {"content": "完整内容", "role": "user"|"assistant", "username": ..., "user_id": ...}，
    找不到则返回 None。

    记录已是结构化格式（content 不含时间戳、不含昵称前缀），
    这里直接取字段，不再用正则去剥前缀。
    """
    if not ref_msg_idx:
        return None
    try:
        hist = load_history(thread_key)
        for msg in hist:
            if msg.get("msg_idx") == ref_msg_idx:
                return {
                    "content": str(msg.get("content", "")),
                    "role": msg.get("role", "user"),
                    "username": msg.get("username", ""),
                    "user_id": msg.get("user_id", ""),
                }
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
            # 说明：超出部分仍会出现在 history 里（带 URL），只是本轮不做
            # 视觉/文件预识别。日志里给出确切数量，便于判断是否该调整上限。
            info(f"[转发解析] 共 {len(media_list)} 个媒体，"
                 f"{len(media_to_process)} 个做预识别，"
                 f"{len(media_list) - len(media_to_process) - sum(1 for m in media_list if m.get('_cached'))} "
                 f"个未预识别（保留 URL 供工具获取）", ctx=ctx)

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
        # 先把"不需要跑识别"的下标定下来，与识别结果统一走一次替换
        # （逐个 replace 既慢，也会让原文里同名字样被反复命中）
        immediate = {}
        # ★ 按**下标**判断是否需要预识别，而不是 `media in media_to_process`：
        #   dict 的相等比较在媒体元数据完全相同时会误判为"同一个"（同一张图
        #   转发两次就会命中），导致本该识别的那个被跳过、而超限的那个反而
        #   被处理。下标是唯一可靠的标识（media_list 与占位符一一对应）。
        _process_idx = {id(m) for m in media_to_process}
        for idx, media in enumerate(media_list):
            # 关闭 AI 回复时：完全跳过转发媒体识别，仅记录文件名与 URL
            if ai_reply_disabled:
                immediate[idx] = (
                    f"[转发媒体: {media['filename']}] (已关闭AI回复，未识别) "
                    f"URL: {media.get('url', '无')}"
                )
                continue
            process_this = media.get('_cached', False) or id(media) in _process_idx
            if not process_this:
                # ★ M12：这里**不是**「媒体被丢弃」—— url 一直都在，机器人
                #   完全可以用工具自己去取。原文案写「已忽略」会让模型以为
                #   这条媒体不可用，于是对第 6 条起的图片/文件视而不见
                #   （表现为「转发的聊天记录里后半段内容凭空消失」）。
                #   改成如实说明：本轮没做预识别，但 URL 可用、需要时请自行获取。
                immediate[idx] = (
                    f"[媒体附件: {media['filename']}] (本轮未预识别，可自行用 URL 获取) "
                    f"URL: {media.get('url', '无')}"
                )
                continue
            media_tasks.append(process_forwarded_media(idx, media))

        if media_tasks:
            results = await asyncio.gather(*media_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    error(f"[转发解析] 媒体处理异常: {res}", ctx=ctx)
                    continue
                idx, result_text = res
                immediate[idx] = result_text

        if immediate:
            decoded_content = replace_media_placeholders(decoded_content, immediate)

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
            summary = annotate_media_summary(summary)
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
                error(f"[附件处理] 异常: {res}", ctx=ctx)
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
            summary = annotate_media_summary(summary)
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
                error(f"[引用媒体处理] 异常: {res}", ctx=ctx)
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

    # @ 替换为用户名：<@openid> → @用户名（查不到则退映射表、再退 openid）。
    # ★ 结果必须回写进 decoded_content / clean_content —— 这三个值分别供
    #   历史存储、日志显示、AI 上下文使用，只有全部同步，AI 看到的正文里
    #   才会保留「谁@了我」。之前这里只赋给 display_content（日志专用），
    #   decoded_content 仍是原始标记，纯 @ 时甚至会退化成空串。
    decoded_content = replace_mentions_with_names(
        decoded_content, parsed.get("mentions", []), bot_client.app_id)
    display_content = decoded_content.strip()

    author_id = parsed["author_id"]
    msg_username = parsed.get("username", "")
    display_username = get_display_name(author_id, msg_username, bot_client.app_id)
    user_identifier = f"{display_username}({author_id})"
    if msg_username:
        update_user_mapping(author_id, msg_username, bot_client.app_id)

    parsed["decoded_content"] = decoded_content
    # decoded_content 里的 <@openid> 已在上方替换为 @用户名(id)，这里只需再兜一层
    # 残留标记（例如 mentions 未覆盖到的 <@...>），不再整段删除 —— 删了会丢提及。
    parsed["clean_content"] = re.sub(
        r'<@([^>]+)>',
        lambda m: f"@{m.group(1).lstrip('!')}({m.group(1).lstrip('!')})",
        decoded_content).strip()
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
        spawn_background(record_group_info(bot_client, parsed["recipient_id"], author_id),
                         "record_group_info", ctx)

    # 补上会话显示名（群名/用户名），派生新 ctx 供后续日志使用
    thread_display = ""
    if msg_type == "group":
        thread_display = get_group_name_from_mirror(bot_client.app_id, parsed["recipient_id"]) or ""
    elif msg_type == "c2c":
        thread_display = get_display_name(author_id, msg_username, bot_client.app_id)
    ctx = ctx.with_(thread_display=thread_display)

    # 正文不再拼 "昵称(id): " 前缀 —— 昵称与 openid 作为独立字段随记录一起保存，
    # 只在发给 AI 时由 render_history_for_ai 拼回可读行。
    # 兜底文案必须与真实场景一致：纯@ 写成「发送了附件或引用」是假信息，
    # 会随历史长期回放给模型（见 render_history_for_ai），此处按 is_at_me 区分。
    if decoded_content:
        store_content = decoded_content
    elif parsed.get("is_at_me"):
        store_content = "@了你（没有附带文字）"
    else:
        store_content = "发送了附件或引用"

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
                   ref_msg_idx=parsed.get("ref_msg_idx"),
                   username=display_username,
                   user_id=author_id)

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
        info(f"[隔离唤醒] 线程 {thread_key} 正在唤醒中，用户消息已入队等待，不打断唤醒处理", ctx=ctx)
        return

    # ★ M2 兜底：走到这里说明「锁已释放」。若此时队列里还留着东西，
    #   说明上一次唤醒结束时 flush 没能把队列放出来（唤醒任务卡死、
    #   或被 unlock 与 flush 之间的异常打断），这些消息会永久滞留。
    #   新消息到达是唯一能重新触发处理链路的时机，借此把积压一并放出来：
    #   不丢弃新消息，只是让本次处理同时带上旧的积压内容。
    if pending_queues.get(thread_key):
        _stranded = len(pending_queues[thread_key])
        warn(f"[隔离唤醒] 线程 {thread_key} 检测到 {_stranded} 条滞留消息"
             f"（上次唤醒结束未放行），本次一并处理", ctx=ctx)

    if thread_key in pending_process_tasks:
        old_task = pending_process_tasks[thread_key]
        if not old_task.done():
            old_task.cancel()
            info(f"[中断] 取消线程 {thread_key} 的旧处理任务", ctx=ctx)
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
async def _reply_join_event(thread_key: str, group_openid: str, member_openid: str,
                            username: str, join_msg: str, bot_client: 'BotClient',
                            ctx):
    """成员加入的自动欢迎回复。调用方必须已持有该会话的处理锁（M1）。"""
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


async def _reply_leave_event(thread_key: str, group_openid: str, member_openid: str,
                             leave_msg: str, bot_client: 'BotClient', ctx):
    """成员退出的回复。调用方必须已持有该会话的处理锁（M1）。"""
    recent_history = get_recent_history(thread_key, get_judge_context_limit())
    should = await should_reply_in_group(recent_history, leave_msg, [],
                                         bot_client.app_id, bot_client.bot_name)
    if not should:
        debug("[事件] Judge 判定不回复退出事件", ctx=ctx)
        return
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
        append_message(thread_key, "user", join_msg,
                       username=username, user_id=member_openid)

        if get_bot_enabled(bot_client.app_id) and get_bot_auto_welcome(bot_client.app_id) \
                and not get_bot_disable_ai_reply(bot_client.app_id):
            # ★ M1：取会话锁再生成回复。
            #   本事件此前直接 await generate_reply，绕过了 process_queue 的
            #   取消/入队机制，会与「该群正在处理的普通消息」并发跑同一
            #   thread_key（两边各自读历史、各自写历史），造成重复回复与
            #   历史交错。加锁后两条链路串行，与会话内其它处理互斥。
            _prune_thread_locks()
            async with get_thread_lock(thread_key):
                await _reply_join_event(thread_key, group_openid, member_openid,
                                        username, join_msg, bot_client, ctx)
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
        append_message(thread_key, "user", leave_msg,
                       username=username, user_id=member_openid)

        if get_bot_enabled(bot_client.app_id) and not get_bot_disable_ai_reply(bot_client.app_id):
            # ★ M1：同加入事件 —— 取会话锁后再判定与生成，避免与
            #   process_queue 并发操作同一 thread_key（历史交错/重复回复）。
            _prune_thread_locks()
            async with get_thread_lock(thread_key):
                await _reply_leave_event(thread_key, group_openid, member_openid,
                                         leave_msg, bot_client, ctx)
        else:
            info(f"[事件] 机器人禁用或AI回复已关闭，仅记录退出消息", ctx=ctx)
        return

    info(f"[未处理事件] {event_type}: {payload}", ctx=ctx)