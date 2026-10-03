# -*- coding: utf-8 -*-
# tool.py — 工具定义与执行（HTTP请求、搜索音乐、播放音乐、群管理等）
import re
import time
import asyncio
import requests
import json as _json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone

from log import info, warn, error, debug, LogCtx
from config import get_executor, is_group_manage_enabled, \
    get_bot_allow_cross_push, get_bot_allow_cross_push_incoming, \
    get_bot_allow_manage_all_push, get_bots, \
    get_bot_allow_cross_wakeup, get_bot_allow_cross_wakeup_incoming, \
    get_bot_allow_cross_get_list, get_bot_allow_cross_get_list_incoming, \
    get_bot_allow_cross_history, get_bot_allow_cross_history_incoming
from client import BotClient, send_with_policy
from memory import (
    get_cached_media_summary, set_cached_media, get_media_cache_key, get_url_cache_key,
    load_user_map, load_mirror, get_user_name, get_group_name_from_mirror,
    append_message, load_history, mark_message_revoked,
    parse_history_ts,   # 从结构化 ts 字段解析消息时间
    get_global_memory, add_global_memory, remove_global_memory, replace_global_memory,
    get_global_memory_enabled, set_global_memory_enabled,
    get_bot_memory_list, add_bot_memory, remove_bot_memory, replace_bot_memory,
    get_bot_memory_enabled, set_bot_memory_enabled,
    get_qun_memory_list, add_qun_memory, remove_qun_memory, replace_qun_memory,
    get_qun_memory_enabled, set_qun_memory_enabled,
    get_c2c_memory_list, add_c2c_memory, remove_c2c_memory, replace_c2c_memory,
    get_c2c_memory_enabled, set_c2c_memory_enabled,
    search_memory,
    # 记忆管理统一入口（与 api_server 的 /api/memory/* 共用同一份实现）
    MEMORY_LEVELS,
    MemoryError_ as MemoryError_,
    MemoryDisabledError,
    MemoryLevelError,
    _memory_get,
    _memory_add,
    _memory_replace,
    _memory_remove,
    _memory_set_enabled,
    _memory_batch,
)
from scheduler import add_task, delete_task, update_task, list_tasks, load_tasks
from wakeup_scheduler import add_wakeup, delete_wakeup, update_wakeup, list_wakeups
from utils import infer_file_type, file_type_name, parse_message_type
# 两个「查看任务列表」工具支持的筛选参数（与下方 _filter_tasks 对应）
_LIST_FILTER_KEYS = {
    "status", "keywords", "keyword", "q", "search", "match_all",
    "task_id", "target_type", "target_id", "isolation_mode",
    "start", "start_time", "end", "end_time", "time_field",
    "order", "limit", "task_app_id",
}

# ==================== 定时任务列表筛选（推送 / 唤醒共用） ====================
# 供两处使用：
#   1. AI 工具 list_scheduled_push / list_scheduled_wakeup
#   2. api_server.py 的 HTTP 接口 /api/push/list、/api/wakeup/list（由该模块导入本段）
# 参数来源用 _TaskParams 适配：既能吃普通 dict（AI 工具参数），
# 也能吃 aiohttp 的 request.query / request 对象。

# ==================== 状态定义 ====================
# 唯一事实来源在 task_core.py（scheduler / wakeup_scheduler 也要用它，
# 放这里会与「tool 延迟导入 scheduler」形成循环导入）。
from task_core import (  # noqa: E402
    TASK_STATUSES, _TASK_STATUS_LABELS, _STATUS_CN, _LEGACY_STATUS,
    norm_task_status as _norm_task_status, task_status_of,
)


def _split_multi(raw: Any) -> List[str]:
    """把参数值拆成多值列表。

    支持：逗号 / 中文逗号 / 分号 / 竖线 分隔，也接受已经是列表的输入。
    会自动去空项。
    """
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        out: List[str] = []
        for item in raw:
            out.extend(_split_multi(item))
        return out
    s = str(raw)
    for ch in ("，", ";", "；", "|"):
        s = s.replace(ch, ",")
    return [p.strip() for p in s.split(",") if p.strip()]


def _parse_task_ts(value: Any) -> Optional[datetime]:
    """把任务时间字段解析成 datetime，失败返回 None。

    兼容 ISO 字符串（带时区）、空格分隔、纯日期、Z 结尾与时间戳。
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone(timedelta(hours=8)))
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    # Python 3.11+ 能直接解析 "Z" 结尾，旧版本手动替换
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
        return dt
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone(timedelta(hours=8)))
        except Exception:
            continue
    return None


def _find_task(task_id: str) -> Optional[Dict[str, Any]]:
    """按 task_id 查找定时推送任务，未找到返回 None。"""
    if not task_id:
        return None
    for t in load_tasks():
        if t.get("task_id") == task_id:
            return t
    return None


def _task_search_text(task: Dict[str, Any]) -> str:
    """汇总任务的可搜索文本（小写），供关键词匹配使用。"""
    parts: List[str] = []
    for k in ("task_id", "app_id", "content", "description", "message",
              "markdown", "markdown_content", "schedule_time", "status"):
        v = task.get(k)
        if v:
            parts.append(str(v))
    # 发起人
    for k in ("initiator", "initiator_info"):
        v = task.get(k)
        if isinstance(v, dict):
            for kk in ("id", "name", "app_id", "type"):
                if v.get(kk):
                    parts.append(str(v[kk]))
        elif v:
            parts.append(str(v))
    # 目标（群/用户 id、名称、类型、所属机器人）
    targets = task.get("targets")
    if isinstance(targets, list):
        for t in targets:
            if isinstance(t, dict):
                for kk in ("id", "type", "name", "app_id"):
                    if t.get(kk):
                        parts.append(str(t[kk]))
            elif t:
                parts.append(str(t))
    # 执行结果细节与历史里的错误信息
    result = task.get("result")
    if isinstance(result, dict):
        for d in (result.get("details") or []):
            if isinstance(d, dict):
                for kk in ("name", "error", "id"):
                    if d.get(kk):
                        parts.append(str(d[kk]))
    for h in (task.get("execution_history") or []):
        if isinstance(h, dict):
            if h.get("error"):
                parts.append(str(h["error"]))
            for tr in (h.get("target_results") or []):
                if isinstance(tr, dict):
                    for kk in ("name", "error", "id"):
                        if tr.get(kk):
                            parts.append(str(tr[kk]))
    return "\n".join(parts).lower()


# ==================== 参数适配层 ====================

class _TaskParams:
    """统一的查询参数访问器。

    包装 aiohttp 的 request.query（MultiDict）或普通 dict / 工具参数对象，
    对外只暴露 get / getall / has。

    也接受完整的 aiohttp request（会自动取它的 .query），
    这样调用方可以直接把 request 传进来。
    """

    def __init__(self, source: Any):
        if source is None:
            source = {}
        # 传进来的是 request 对象时，自动取它的 query
        if not isinstance(source, dict) and not hasattr(source, "getall"):
            q = getattr(source, "query", None)
            if q is not None:
                source = q
        self._src = source

    def getall(self, key: str) -> List[str]:
        src = self._src
        getter = getattr(src, "getall", None)
        if callable(getter):
            try:
                got = getter(key, [])
                if got:
                    return [str(x) for x in got]
            except Exception:
                pass
            # aiohttp 里 get() 对重复参数只返回第一个，getall 才是全部
            one = src.get(key)
            return [str(one)] if one else []
        # 普通 dict：值本身可能是列表
        val = src.get(key) if hasattr(src, "get") else None
        if val is None:
            return []
        if isinstance(val, (list, tuple, set)):
            out: List[str] = []
            for x in val:
                if x is not None and str(x) != "":
                    out.append(str(x))
            return out
        return [str(val)] if str(val) != "" else []

    def get(self, key: str, default: str = "") -> str:
        vals = self.getall(key)
        return vals[0] if vals else default

    def has(self, key: str) -> bool:
        return bool(self.getall(key))

    def multi(self, *keys: str) -> List[str]:
        """按顺序取第一个非空的键，返回其全部值。"""
        for k in keys:
            vals = self.getall(k)
            if vals:
                return vals
        return []


# ==================== 过滤主函数 ====================

def _filter_tasks(tasks: List[Dict[str, Any]], params: Any,
                 time_fields: Tuple[str, ...] = ("schedule_time", "created_at")
                 ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """按查询参数过滤任务列表。params 可为 _TaskParams / request.query / dict。

    支持的参数（全部可选，可任意组合）：
      status      状态：pending/completed/running/failed/cancelled
                  或中文（未开始/执行完成/运行中/执行失败/已取消）
                  支持多值（逗号分隔或重复参数），多个之间取「或」
      keywords    关键词数组：任务内多个字段的「或」匹配，多个关键词之间取「或」
      keyword/q/search  单关键词
      match_all   1 时关键词需全部命中（默认任一命中即可）
      task_app_id 仅保留该机器人的任务
      task_id     任务 id（包含匹配）
      target_type 目标类型：group / user（c2c 亦可）
      target_id   目标 id（包含匹配）
      start/end   时间范围（含），start_time/end_time 为别名
      time_field  用哪个字段做时间比较，默认依次尝试 schedule_time / created_at
      order       asc / desc 按时间排序
      limit       最多返回多少条（取最近的，配合 order 使用）

    返回 (过滤后的任务列表, 过滤元信息)。
    """
    p = params if isinstance(params, _TaskParams) else _TaskParams(params)
    meta: Dict[str, Any] = {"filters": {}}
    out = list(tasks)

    # ---- 状态过滤 ----
    # 只接受规范状态（pending/completed/failed）与其中文标签。
    # 无法识别的值不再静默兜底，而是让筛选结果为空，并在 meta 里回显
    # 以便调用方知道条件没生效（例如 status=运行中 这种旧写法）。
    raw_status = _split_multi(p.multi("status", "state"))
    want_status = [_norm_task_status(s) for s in raw_status]
    if want_status:
        # 去重保序
        seen_s: set = set()
        want_status = [s for s in want_status if not (s in seen_s or seen_s.add(s))]
        unknown = [s for s in want_status if s not in _TASK_STATUS_LABELS]
        valid = [s for s in want_status if s in _TASK_STATUS_LABELS]
        out = [t for t in out if task_status_of(t) in valid] if valid else []
        meta["filters"]["status"] = valid
        if unknown:
            meta["filters"]["status_ignored"] = unknown

    # ---- 关键词过滤（多关键词，默认任一命中）----
    keywords: List[str] = []
    for chunk in p.multi("keywords", "keyword", "q", "search"):
        keywords.extend(_split_multi(chunk))
    seen: set = set()
    keywords = [k for k in keywords if not (k.lower() in seen or seen.add(k.lower()))]
    if keywords:
        want_all = p.get("match_all").strip().lower() in ("1", "true", "yes")
        lows = [k.lower() for k in keywords]
        if want_all:
            out = [t for t in out if all(k in _task_search_text(t) for k in lows)]
        else:
            out = [t for t in out if any(k in _task_search_text(t) for k in lows)]
        meta["filters"]["keywords"] = keywords
        meta["filters"]["match_all"] = want_all

    # ---- app_id 过滤 ----
    want_app = p.get("task_app_id").strip()
    if want_app:
        out = [t for t in out if str(t.get("app_id", "")) == want_app]
        meta["filters"]["task_app_id"] = want_app

    # ---- task_id 过滤（包含匹配）----
    want_tid = p.get("task_id").strip()
    if want_tid:
        out = [t for t in out if want_tid in str(t.get("task_id", ""))]
        meta["filters"]["task_id"] = want_tid

    # ---- 隔离模式过滤（仅唤醒任务有此字段）----
    raw_iso = p.get("isolation_mode").strip()
    if raw_iso != "":
        want_iso = []
        for v in _split_multi(raw_iso):
            try:
                want_iso.append(int(v))
            except (TypeError, ValueError):
                continue
        if want_iso:
            def _iso_ok(t: Dict[str, Any]) -> bool:
                if "isolation_mode" not in t:
                    return False
                try:
                    return int(t.get("isolation_mode") or 0) in want_iso
                except (TypeError, ValueError):
                    return False
            out = [t for t in out if _iso_ok(t)]
            meta["filters"]["isolation_mode"] = want_iso

    # ---- 目标过滤 ----
    want_ttype = p.get("target_type").strip().lower()
    if want_ttype == "c2c":
        want_ttype = "user"     # 兼容两种叫法
    want_oid = p.get("target_id").strip()
    if want_ttype or want_oid:
        def _match_target(t: Dict[str, Any]) -> bool:
            for tg in (t.get("targets") or []):
                if not isinstance(tg, dict):
                    continue
                tt = str(tg.get("type", "")).lower()
                if tt == "c2c":
                    tt = "user"
                if want_ttype and tt != want_ttype:
                    continue
                if want_oid and want_oid not in str(tg.get("id", "")):
                    continue
                return True
            return False
        out = [t for t in out if _match_target(t)]
        if want_ttype:
            meta["filters"]["target_type"] = want_ttype
        if want_oid:
            meta["filters"]["target_id"] = want_oid

    # ---- 时间范围过滤 ----
    raw_start = p.multi("start", "start_time")
    raw_end = p.multi("end", "end_time")
    rs = raw_start[0].strip() if raw_start else ""
    re_ = raw_end[0].strip() if raw_end else ""
    if rs or re_:
        t_start = _parse_task_ts(rs)
        t_end = _parse_task_ts(re_)
        if rs and t_start is None:
            meta["filters"]["time_error"] = f"start 时间格式无法解析: {rs}"
        if re_ and t_end is None:
            meta["filters"]["time_error"] = f"end 时间格式无法解析: {re_}"
        # 只传日期时，结束时间补到当天 23:59:59
        if t_end is not None and len(re_) <= 10:
            t_end = t_end.replace(hour=23, minute=59, second=59)

        want_field = p.get("time_field").strip()
        fields = (want_field,) if want_field else time_fields

        def _in_range(t: Dict[str, Any]) -> bool:
            for f in fields:
                dt = _parse_task_ts(t.get(f))
                if dt is None:
                    continue
                if t_start is not None and dt < t_start:
                    continue
                if t_end is not None and dt > t_end:
                    continue
                return True
            return False

        out = [t for t in out if _in_range(t)]
        if t_start is not None:
            meta["filters"]["start"] = t_start.isoformat()
        if t_end is not None:
            meta["filters"]["end"] = t_end.isoformat()
        meta["filters"]["time_field"] = list(fields)

    # ---- 排序 ----
    order = p.get("order").strip().lower()
    if order in ("asc", "desc"):
        def _sort_key(t: Dict[str, Any]):
            for f in time_fields:
                dt = _parse_task_ts(t.get(f))
                if dt is not None:
                    return dt
            return datetime.min.replace(tzinfo=timezone(timedelta(hours=8)))
        out.sort(key=_sort_key, reverse=(order == "desc"))
        meta["filters"]["order"] = order

    # ---- 数量限制（取最近 limit 条）----
    raw_limit = p.get("limit").strip()
    if raw_limit:
        try:
            lim = int(raw_limit)
        except (TypeError, ValueError):
            lim = 0
        if lim > 0 and len(out) > lim:
            out = out[-lim:]
            meta["filters"]["limit"] = lim

    return out, meta


def _task_counts(tasks: List[Dict[str, Any]]) -> Dict[str, int]:
    """按状态统计数量。键严格等于 TASK_STATUSES + total，
    保证每个 count 都能对应到一个真实存在的筛选 tab（不会多出无名键）。"""
    counts = {k: 0 for k in TASK_STATUSES}
    for t in tasks:
        s = task_status_of(t)
        counts[s] = counts.get(s, 0) + 1
    counts["total"] = len(tasks)
    return counts


def _status_summary_text(counts: Dict[str, int]) -> str:
    """把状态统计拼成一行中文摘要，供 AI 工具输出使用。"""
    parts = []
    for k in TASK_STATUSES:
        n = counts.get(k, 0)
        if n:
            parts.append(f"{_TASK_STATUS_LABELS[k]} {n}")
    return "，".join(parts) if parts else "无"


def _describe_filters(meta: Dict[str, Any]) -> str:
    """把生效的过滤条件拼成中文说明，供 AI 工具回显。"""
    f = (meta or {}).get("filters") or {}
    if not f:
        return ""
    parts = []
    if f.get("status"):
        parts.append("状态=" + "/".join(_TASK_STATUS_LABELS.get(s, s) for s in f["status"]))
    if f.get("status_ignored"):
        parts.append("（忽略无法识别的状态：" + "/".join(f["status_ignored"]) + "）")
    if f.get("keywords"):
        kw = "、".join(f["keywords"])
        parts.append(f"关键词={'全部含' if f.get('match_all') else '任一含'}「{kw}」")
    if f.get("task_app_id"):
        parts.append(f"机器人={f['task_app_id']}")
    if f.get("task_id"):
        parts.append(f"任务ID含 {f['task_id']}")
    if f.get("target_type"):
        parts.append("目标类型=" + ("群" if f["target_type"] == "group" else "用户"))
    if f.get("target_id"):
        parts.append(f"目标ID含 {f['target_id']}")
    if f.get("isolation_mode"):
        parts.append("隔离模式=" + "/".join("是" if v == 1 else "否" for v in f["isolation_mode"]))
    if f.get("start"):
        parts.append(f"起始 {f['start'][:16].replace('T', ' ')}")
    if f.get("end"):
        parts.append(f"截止 {f['end'][:16].replace('T', ' ')}")
    if f.get("order"):
        parts.append("排序=" + ("倒序" if f["order"] == "desc" else "正序"))
    if f.get("limit"):
        parts.append(f"限制 {f['limit']} 条")
    if f.get("time_error"):
        parts.append("⚠ " + f["time_error"])
    return "，".join(parts)


# ==================== 记忆管理（API / AI 工具共用） ====================
# 业务层实现在 memory.py 的「记忆管理统一入口」一节，本文件在顶部 import 导入。
#   1. AI 工具 execute_tool_call 的 view_memory / add_memory / modify_memory /
#      delete_memory / enable_memory / disable_memory
#   2. api_server.py 的 /api/memory/* 接口
# 两边导入的是 memory.py 里同一份函数对象，不是各写一份。


# ==================== 媒体推送策略判断 ====================
# 双发判定见 utils.needs_dual_send，发送编排见 client.send_with_policy。


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
        "使用 send_media 发送图片、视频或文件（支持 HTTP/HTTPS URL ，系统会自动处理上传）。调用 send_media 时必须明确传入 file_type 与 file_name，不得省略；file_type 取值：1=图片（jpg/jpeg/png/gif/webp/bmp 等），2=视频（mp4/mov/mkv 等），3=语音（通用音频如 mp3/wav/ogg 填 3 或 4 都可以），4=文件（文档、压缩包等其余类型）；"
        "使用 send_text 向当前会话发送纯文本文字（当你需要通过工具发送自定义文本时使用，发送后请调用 skip_reply 以结束工具调用循环，避免重复发送）；"
        "使用 recognize_media 主动识别媒体内容并获取摘要（可选传入 prompt 从特定角度重新识别，结果会追加到原摘要之后）；"
        "使用 search_music 搜索歌曲；"
        "使用 play_music 根据歌曲ID播放音乐（仅限非VIP歌曲）。"
        "使用 get_targets 获取所有机器人的私聊用户、群及群成员的列表（含名称、ID、所属机器人 APP_ID、推送/唤醒权限标注），"
        "支持通过 appid 参数筛选指定机器人、search 参数按名称/ID 搜索、mode 参数切换推送/唤醒/全部视图；"
        "使用 push_message 向指定的一组用户和/或群推送消息，每个目标需指定 type（user/group）、id（openid）和 app_id（所属机器人），"
        "支持跨机器人推送（受各机器人 ALLOW_CROSS_BOT_PUSH 配置约束），消息末尾会自动标注发起者信息；"
        "使用 schedule_push 创建定时推送任务（支持多媒体和三种调度类型：one_time=一次性、daily=每天、interval=间隔），"
        "需指定 schedule_type、targets、content 和可选的 schedule_time/interval_seconds/media_source/file_name；"
        "凡涉及媒体（media_source），必须同时明确传入 file_type 与 file_name，不得省略；file_type 取值：1=图片，2=视频，3=语音（通用音频填 3 或 4 均可），4=文件；"
        "使用 list_scheduled_push 查看所有定时推送任务列表（含全部状态：pending/sent/已完成及消息ID）；"
        "使用 delete_scheduled_push 删除一个定时推送任务（task_id）；"
        "使用 update_scheduled_push 修改一个定时推送任务的内容/目标/时间/媒体/调度类型。"
        "使用 revoke_push 撤回已发送的推送消息，可撤回整个推送任务（按 task_id 撤回所有目标）或部分目标的消息（指定 target_ids），"
        "系统会自动根据存储的消息ID调用撤回API。注意：发送超过2分钟的消息不可撤回。"
        "使用 create_scheduled_wakeup 创建定时唤醒任务（在指定时间向指定会话模拟用户消息唤醒机器人），"
        "支持一次性、每天、间隔（如每1h）唤醒，支持 isolation_mode 隔离模式（1=开启，唤醒期间不打断用户消息处理），"
        "需指定 targets（目标列表）、initiator、description 等，"
        "使用 list_scheduled_wakeup 查看定时唤醒列表（含最近3次执行时间）；"
        "使用 delete_scheduled_wakeup 删除一个定时唤醒任务；"
        "使用 update_scheduled_wakeup 修改一个定时唤醒任务的时间/类型/说明/目标列表/隔离模式。"
        "使用 view_memory 查看各级记忆（全局/机器人/群聊/私聊）的内容和启用状态，需要指定 level 和 identifier；"
        "使用 add_memory 向指定记忆级别添加记忆，内容需包含用户名(QQ号)格式；"
        "使用 modify_memory 修改指定记忆级别的某条记忆（按索引替换）；"
        "使用 delete_memory 删除指定记忆级别的某条记忆（按索引）；"
        "使用 enable_memory / disable_memory 启用或禁用指定记忆级别。"
        "以上 add_memory / modify_memory / delete_memory 均支持传入 items 参数一次操作多条（详见各工具定义）。"
    )
    base_desc += (" 可使用 query_history 查询聊天历史记录。支持多关键词搜索（keywords 数组，OR 逻辑）、按时间范围过滤（start_time/end_time RFC3339）。"
                   "可跨机器人跨群查询：传入 app_id 指定目标机器人（如 \"1905417157\"）、target_type=\"group\"/\"c2c\"、target_id 为目标openid，即可读取任意机器人的任意群/私聊聊天记录（受 ALLOW_CROSS_BOT_HISTORY/INCOMING 配置约束）。"
                   "如果不传跨群参数则默认查询当前会话的记录。")
    base_desc += (" 可使用 search_memory 搜索记忆库，传入多个关键词和可选的记忆层级（global=全局, bot=机器人, group=群聊, c2c=私聊, all=全部），按相关性返回匹配的记忆内容。")
    base_desc += (" 可使用 revoke_message 撤回消息，支持传入单条或多条消息ID进行批量撤回。"
                   "支持跨机器人撤回：传入 app_id=目标机器人APP_ID（如 \"1905417157\"）、target_type + target_id 指定目标会话，即可撤回其他机器人发在群里的消息（受 ALLOW_CROSS_BOT_HISTORY/INCOMING 配置约束）。"
                   "如果不传跨机器人参数则默认撤回当前机器人的消息。"
                   "私聊可撤回机器人2分钟内发送的消息；群聊中管理员可撤回普通成员消息，普通成员只能撤回自己消息。先 get_bot_state 确认有权限后使用。")
    base_desc += (" 可使用 batch_revoke 按关键词和时间范围批量撤回消息。"
                   "支持传入多个关键词（如 [\"广告\", \"加V\"]），任一匹配即触发；支持 start_time/end_time 时间范围（RFC3339格式）。"
                   "支持跨机器人跨群：传入 app_id=目标机器人APP_ID、target_type + target_id 指定目标会话，即可批量撤回其他机器人管理的群里的消息（受 ALLOW_CROSS_BOT_HISTORY/INCOMING 配置约束）。"
                   "建议先 dry_run=1 预览匹配结果，确认后再将 dry_run=0 执行撤回。"
                   "私聊只能撤回机器人2分钟内发送的消息；群聊中管理员可撤回任意成员消息，普通成员只能撤回自己消息。先 get_bot_state 确认权限。")
    base_desc += (" 群管理工具使用流程：\n"
                  "  第一步：需要群管理操作时，先调用 get_bot_state 查看机器人自身在群内的角色（member_role）。\n"
                  "  第二步：\n"
                  "    - 如果 member_role 为 admin（管理员）或 owner（群主）：可以进一步使用 get_group_mute_status 查看当前群禁言状态，或使用 mute_member/unmute_member 管理群成员禁言。\n"
                  "    - 如果 member_role 为 member（普通成员）：机器人没有管理权限，不能执行禁言/解禁操作。\n"
                  "  第三步：有权限时调用 get_group_mute_status 查看禁言详情，或调用 mute_member 禁言（需指定禁言到期时间 RFC3339 格式）/ unmute_member 解禁。")
    base_desc += (" 重要：你回复内容中的『思考/分析文本』用户是看不到的。要发送任何文字给用户，必须使用 send_text 工具。无论是纯文字回复、查询结果还是任何需要用户看到的内容，都要通过 send_text 发送。\n")
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
                "description": "向当前会话发送一张图片、视频或文件。支持 HTTP/HTTPS URL 。系统会自动上传并处理大小限制。可以附加文字说明。必须指定 file_type 和 file_name。",
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
                            "description": "媒体类型（必填）：1=图片，2=视频，3=语音，4=文件。每次调用都必须明确写出该参数，请根据 URL 或文件名自行判断后填入，不要省略。判断规则：常见图片（jpg/jpeg/png/gif/webp/bmp）填 1；视频（mp4/mov/mkv/webm 等）填 2；语音填 3，通用的音频文件（mp3/wav/ogg/m4a 等）填 3 或 4 都可以；其余文档、压缩包等一律填 4。"
                        },
                        "file_name": {
                            "type": "string",
                            "description": "文件名（必填），用于辅助判断类型和显示，请带上正确的扩展名。"
                        }
                    },
                    "required": ["source", "file_type"]
                }
            }
        },
        # ==================== 发送文本工具 ====================
        {
            "type": "function",
            "function": {
                "name": "send_text",
                "description": (
                    "向当前会话发送文本内容。当你需要通过工具发送一段自定义文字，而不是让AI直接生成文本回复时使用此工具。"
                    "支持 Markdown 格式：将 message_type 设为 \"markdown\"，正文仍填入 content，即可发送富文本样式消息"
                    "（标题、加粗、斜体、删除线、链接、图片、有序/无序列表、块引用、分割线等）。"
                    "注意：单聊场景 Markdown 可发送但对方无法渲染为富文本，群聊场景收发均支持 Markdown。"
                    "发送后必须紧接着调用 skip_reply（skip_reply 会结束工具调用循环，防止 AI 再生成多余文本回复）。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": (
                                "要发送的正文，建议不超过2000字。纯文本与 Markdown 都统一用此参数传入，"
                                "格式由 message_type 决定。Markdown 支持：# 标题、**加粗**、_斜体_、~~删除线~~、"
                                "- 无序列表、1. 有序列表、> 块引用、*** 分割线、[文字](链接)、![](图片URL)。"
                            )
                        },
                        "message_type": {
                            "type": "string",
                            "enum": ["text", "markdown"],
                            "description": "消息格式。text=纯文本（默认）；markdown=富文本消息。"
                        }
                    },
                    "required": ["content"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "recognize_media",
                "description": (
                    "识别指定URL的媒体（图片或视频）内容，生成一段摘要描述。支持缓存，重复请求会返回缓存结果。"
                    "重要：很多媒体URL（尤其 QQ 的 multimedia.nt.qq.com.cn/download?...）路径里不带扩展名，"
                    "此时若只传 media_url，将无法判断媒体类型而失败。media_url、media_type、filename 三个参数都必填，请务必一并传入。"
                    "上下文中形如 [收到图片：xxx.png] 的占位已注明该传的参数，照抄即可。"
                    "可选参数 prompt：当你需要从某个特定角度重新识别时传入（例如\"读出图中所有文字\"、"
                    "\"这张图里有没有表格\"）。传了 prompt 会忽略缓存重新识别一次，"
                    "并把新的识别结果追加到已有摘要之后（不会覆盖），因此可以多次用不同角度提问，逐步补充细节。"
                ),
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
                            "description": (
                                "媒体类型（必填）。image=图片，video=视频。"
                                "每次调用都必须明确写出该参数，不要省略——"
                                "QQ 下载链接等 URL 不带扩展名，无法自动推断。"
                                "上下文占位里写了 media_type=... 时请照抄。"
                            )
                        },
                        "filename": {
                            "type": "string",
                            "description": (
                                "文件名（必填），需带扩展名，如 xxx.png / v.mp4。"
                                "每次调用都必须明确写出该参数。"
                                "上下文占位里写了 filename=\"...\" 时请照抄。"
                            )
                        },
                        "prompt": {
                            "type": "string",
                            "description": (
                                "可选。本次识别的额外关注点，会附加到识别提示词中让模型聚焦，"
                                "例如\"重点描述图中人物的表情\"、\"完整读出所有文字\"。"
                                "传入后会忽略缓存重新识别，并把结果追加到原摘要之后。"
                                "只想拿已有摘要时不要传该参数。"
                            )
                        },
                        "disable_cache": {
                            "type": "boolean",
                            "description": "是否禁用缓存，如果为 true，则强制重新识别并覆盖缓存结果"
                        }
                    },
                    "required": ["media_url", "media_type", "filename"]
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
        # ==================== 获取目标列表（合并推送/唤醒） ====================
        {
            "type": "function",
            "function": {
                "name": "get_targets",
                "description": "获取所有机器人已知的私聊用户、群及群成员的列表，每项标注根据配置文件判断的推送/唤醒权限状态。支持按 appid 筛选指定机器人、按 search 关键词搜索名称或 ID、按 mode 切换推送/唤醒/全部视图。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "appid": {
                            "type": "string",
                            "description": "可选，指定机器人 APP_ID，只返回该机器人的列表；为空则返回所有机器人"
                        },
                        "search": {
                            "type": "string",
                            "description": "可选，按名称或 ID 关键词搜索过滤目标"
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["all", "push", "wakeup"],
                            "description": "可选，视图模式：all=同时显示推送和唤醒权限（默认），push=只关注推送相关权限，wakeup=只关注唤醒相关权限"
                        }
                    },
                    "required": []
                }
            }
        },
        # ==================== 获取联系人列表 ====================
        {
            "type": "function",
            "function": {
                "name": "get_contact_list",
                "description": "获取所有机器人的用户和群列表（含群内已知成员），按机器人分组返回格式化结果，不含推送/唤醒权限标注。受各机器人 ALLOW_CROSS_BOT_GET_LIST 配置约束。支持按 appid 筛选和 search 关键词搜索。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "appid": {
                            "type": "string",
                            "description": "可选，指定机器人 APP_ID，只返回该机器人的列表；为空则返回所有机器人"
                        },
                        "search": {
                            "type": "string",
                            "description": "可选，按名称或 ID 关键词搜索过滤联系人"
                        }
                    },
                    "required": []
                }
            }
        },
        # ==================== 主动推送消息 ====================
        {
            "type": "function",
            "function": {
                "name": "push_message",
                "description": (
                    "向指定的一组用户和/或群推送消息（支持多媒体、Markdown）。"
                    "每个目标需指定 type（user/group）、id（openid）、app_id（所属机器人 ID）。支持跨机器人推送。"
                    "消息末尾自动标注发起者信息。有媒体时先发媒体再单独发文本确保可见。"
                    "支持 Markdown：将 message_type 设为 \"markdown\"，正文仍填入 content。""发媒体时需同时提供 media_source 与 file_type；只发文本则无需二者。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": (
                                "要推送的正文。纯文本与 Markdown 都统一用此参数传入，格式由 message_type 决定。"
                                "Markdown 支持：# 标题、**加粗**、_斜体_、~~删除线~~、- 列表、1. 有序列表、"
                                "> 块引用、*** 分割线、[文字](链接)、![](图片URL)。"
                            )
                        },
                        "message_type": {
                            "type": "string",
                            "enum": ["text", "markdown"],
                            "description": "消息格式。text=纯文本（默认）；markdown=富文本消息。"
                        },
                        "targets": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["user", "group"],
                                        "description": "目标类型：user=私聊用户，group=群"
                                    },
                                    "id": {
                                        "type": "string",
                                        "description": "目标 openid（从 get_targets 结果中获取，也可传入任意 ID 尝试推送）"
                                    },
                                    "app_id": {
                                        "type": "string",
                                        "description": "该目标所属的机器人 APP_ID（从 get_targets 结果中获取）"
                                    }
                                },
                                "required": ["type", "id", "app_id"]
                            },
                            "description": "推送目标列表，每个目标需指定 type、id 和 app_id"
                        },
                        "media_source": {
                            "type": "string",
                            "description": "媒体文件 URL（可选），支持图片/视频/语音/文件。当存在媒体时，系统会先发送媒体消息，再单独发送文本消息以确保推送发起信息可见。"
                        },
                        "file_type": {
                            "type": "integer",
                            "enum": [1, 2, 3, 4],
                            "description": "媒体类型：1=图片，2=视频，3=语音，4=文件。仅在同时传了 media_source（要发媒体）时必填，请自行判断后填入；只发纯文本/Markdown 时不要传该参数。判断规则：常见图片（jpg/jpeg/png/gif/webp/bmp）填 1；视频（mp4/mov/mkv/webm 等）填 2；语音填 3，通用的音频文件（mp3/wav/ogg/m4a 等）填 3 或 4 都可以；其余文档、压缩包等一律填 4。"
                        },
                        "file_name": {
                            "type": "string",
                            "description": "文件名，辅助判断媒体类型，请带上正确的扩展名（可选，仅发媒体时建议填写）。"
                        }
                    },
                    "required": ["targets"]
                }
            }
        },
        # ==================== 定时推送（支持间隔/每天/一次性） ====================
        {
            "type": "function",
            "function": {
                "name": "schedule_push",
                "description": (
                    "创建一条定时推送任务，到指定时间自动向 targets 中的用户/群推送消息。"
                    "支持三种调度类型：one_time=一次性（按 schedule_time 执行一次）、daily=每天（每天同一时间执行）、"
                    "interval=间隔（按 interval_seconds 间隔执行）。支持多媒体推送（图片/视频/语音/文件）。"
                    "支持 Markdown：将 message_type 设为 \"markdown\"，正文仍填入 content。"
                    "返回 task_id 供后续管理。"
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": (
                                "要推送的正文。纯文本与 Markdown 都统一用此参数传入，格式由 message_type 决定。"
                                "Markdown 支持：# 标题、**加粗**、_斜体_、~~删除线~~、- 列表、1. 有序列表、"
                                "> 块引用、*** 分割线、[文字](链接)、![](图片URL)。"
                            )
                        },
                        "message_type": {
                            "type": "string",
                            "enum": ["text", "markdown"],
                            "description": "消息格式。text=纯文本（默认）；markdown=富文本消息。"
                        },
                        "targets": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["user", "group"],
                                        "description": "目标类型：user=私聊用户，group=群"
                                    },
                                    "id": {"type": "string", "description": "目标 openid"},
                                    "app_id": {"type": "string", "description": "该目标所属的机器人 APP_ID"}
                                },
                                "required": ["type", "id", "app_id"]
                            },
                            "description": "推送目标列表"
                        },
                        "schedule_type": {
                            "type": "string",
                            "enum": ["one_time", "daily", "interval"],
                            "description": "调度类型：one_time=一次性（默认），daily=每天执行，interval=间隔执行"
                        },
                        "schedule_time": {
                            "type": "string",
                            "description": "计划推送时间（RFC3339 格式），例如 2026-08-23T18:00:00+08:00。对于 one_time 和 daily 类型必填；interval 类型为首次执行时间（可选，不填则立即开始计时）"
                        },
                        "interval_seconds": {
                            "type": "integer",
                            "description": "间隔秒数（仅 schedule_type=interval 时使用），例如 3600 表示每1小时推送一次"
                        },
                        "media_source": {
                            "type": "string",
                            "description": "媒体文件 URL（可选），支持图片/视频/语音/文件"
                        },
                        "file_type": {
                            "type": "integer",
                            "enum": [1, 2, 3, 4],
                            "description": "媒体类型（必填）：1=图片，2=视频，3=语音，4=文件。每次调用都必须明确写出该参数，请自行判断后填入，不要省略。判断规则：常见图片（jpg/jpeg/png/gif/webp/bmp）填 1；视频（mp4/mov/mkv/webm 等）填 2；语音填 3，通用的音频文件（mp3/wav/ogg/m4a 等）填 3 或 4 都可以；其余文档、压缩包等一律填 4。"
                        },
                        "file_name": {
                            "type": "string",
                            "description": "文件名（必填），辅助判断媒体类型，请带上正确的扩展名。"
                        }
                    },
                    "required": ["content", "targets", "file_type"]
                }
            }
        },
        # ==================== 查看定时推送列表（含全部状态和消息ID） ====================
        {
            "type": "function",
            "function": {
                "name": "list_scheduled_push",
                "description": "查看定时推送任务列表（含全部状态），支持按状态、关键词、时间等条件筛选。返回任务 ID、类型、内容摘要、目标数、计划时间、状态、执行结果（含发送的消息ID）。若配置文件 allow_manage_all_push=1 则可看到所有机器人的推送。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "description": "按状态筛选，可多选（逗号分隔取「或」）。取值：pending(未开始)/completed(执行完成)/failed(执行失败)，也接受中文写法。例如 \"completed\" 或 \"pending,completed\""
                        },
                        "keywords": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "关键词数组，多个关键词任一命中即可（默认）。匹配范围含任务ID、内容、发起人、目标群/用户、错误信息等。例如 [\"天气\",\"新闻\"]。也可直接传字符串 \"天气,新闻\""
                        },
                        "keyword": {
                            "type": "string",
                            "description": "单个关键词（等价于 keywords 只传一个）"
                        },
                        "match_all": {
                            "type": "boolean",
                            "description": "为 true 时要求传入的所有关键词都命中（默认 false，即任一命中即可）"
                        },
                        "task_id": {
                            "type": "string",
                            "description": "按任务 ID 筛选（包含匹配）"
                        },
                        "task_app_id": {
                            "type": "string",
                            "description": "按创建该任务的机器人 APP_ID 筛选，只看某个机器人的任务（多机器人场景用）。APP_ID 从 get_targets 结果中获取"
                        },
                        "target_type": {
                            "type": "string",
                            "enum": ["group", "user", "c2c"],
                            "description": "按目标类型筛选：group=群，user/c2c=用户"
                        },
                        "target_id": {
                            "type": "string",
                            "description": "按目标 ID 筛选（包含匹配）"
                        },
                        "start": {
                            "type": "string",
                            "description": "起始时间（含），格式 2026-09-21 或 2026-09-21T20:00:00+08:00"
                        },
                        "end": {
                            "type": "string",
                            "description": "结束时间（含），只写日期时自动补到当天 23:59:59"
                        },
                        "time_field": {
                            "type": "string",
                            "enum": ["schedule_time", "created_at"],
                            "description": "时间筛选用哪个字段比较，默认 schedule_time（找不到时回退 created_at）"
                        },
                        "order": {
                            "type": "string",
                            "enum": ["asc", "desc"],
                            "description": "按时间排序：desc=新的在前，asc=旧的在前"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "最多返回多少条（取最近的前 N 条）"
                        }
                    },
                    "required": []
                }
            }
        },
        # ==================== 删除定时推送 ====================
        {
            "type": "function",
            "function": {
                "name": "delete_scheduled_push",
                "description": "删除一个定时推送任务。只能删除当前机器人发起的任务，除非 allow_manage_all_push=1。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "要删除的任务 ID（从 list_scheduled_push 结果中获取）"
                        }
                    },
                    "required": ["task_id"]
                }
            }
        },
        # ==================== 修改定时推送（含调度类型） ====================
        {
            "type": "function",
            "function": {
                "name": "update_scheduled_push",
                "description": "修改一个定时推送任务的内容、目标、计划时间、媒体或调度类型。只传需要修改的字段。只能修改当前机器人发起的任务，除非 allow_manage_all_push=1。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "要修改的任务 ID"
                        },
                        "content": {
                            "type": "string",
                            "description": (
                                "新的消息正文（可选，不传则不修改）。纯文本与 Markdown 都统一用此参数传入，"
                                "格式由 message_type 决定。Markdown 支持：# 标题、**加粗**、_斜体_、"
                                "~~删除线~~、- 列表、> 块引用、*** 分割线、[文字](链接)。"
                            )
                        },
                        "message_type": {
                            "type": "string",
                            "enum": ["text", "markdown"],
                            "description": (
                                "消息格式（可选）。text=纯文本；markdown=富文本。"
                                "传入时会同步切换该任务的格式（从 markdown 改回 text 会清除原 Markdown 正文）。"
                                "不传则保持任务原有格式不变。"
                            )
                        },
                        "targets": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["user", "group"]},
                                    "id": {"type": "string"},
                                    "app_id": {"type": "string"}
                                },
                                "required": ["type", "id", "app_id"]
                            },
                            "description": "新的推送目标列表（可选）"
                        },
                        "schedule_type": {
                            "type": "string",
                            "enum": ["one_time", "daily", "interval"],
                            "description": "新的调度类型（可选）"
                        },
                        "schedule_time": {
                            "type": "string",
                            "description": "新的计划推送时间（RFC3339 格式，可选）"
                        },
                        "interval_seconds": {
                            "type": "integer",
                            "description": "新的间隔秒数（仅 interval 类型，可选）"
                        },
                        "media_source": {
                            "type": "string",
                            "description": "新的媒体 URL（可选）"
                        },
                        "file_type": {
                            "type": "integer",
                            "enum": [1, 2, 3, 4],
                            "description": "媒体类型（必填）：1=图片，2=视频，3=语音，4=文件。修改媒体时必须明确写出该参数。判断规则：常见图片（jpg/jpeg/png/gif/webp/bmp）填 1；视频（mp4/mov/mkv/webm 等）填 2；语音填 3，通用的音频文件（mp3/wav/ogg/m4a 等）填 3 或 4 都可以；其余文档、压缩包等一律填 4。"
                        },
                        "file_name": {
                            "type": "string",
                            "description": "文件名（必填），请带上正确的扩展名。"
                        }
                    },
                    "required": ["task_id"]
                }
            }
        },
        # ==================== 撤回推送消息 ====================
        {
            "type": "function",
            "function": {
                "name": "revoke_push",
                "description": "撤回已发送的推送消息。可撤回整个推送任务（按 task_id 撤回所有目标的消息），或撤回部分目标的消息（指定 target_ids）。需先通过 list_scheduled_push 查看任务详情以获取目标的消息ID。注意：发送超过2分钟的消息不可撤回。机器人如果是群管理员可撤回普通成员消息，普通成员只能撤回自己的消息。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "推送任务 ID（从 list_scheduled_push 结果中获取）"
                        },
                        "target_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "可选，指定要撤回的目标 ID 列表（仅撤回这些目标的消息）。不传则撤回整个推送任务的所有消息。"
                        }
                    },
                    "required": ["task_id"]
                }
            }
        },
        # ==================== 定时唤醒 ====================
        {
            "type": "function",
            "function": {
                "name": "create_scheduled_wakeup",
                "description": "创建一条定时唤醒任务。在指定时间向指定会话模拟用户消息唤醒机器人，消息会以【定时唤醒】开头记录到聊天记录。支持一次性（one_time）、每天（daily）、间隔（interval，如每1h）三种唤醒类型。需指定 schedule_type、targets（目标用户/群列表）、initiator（发起者信息）、description（唤醒说明）等。返回 task_id 供后续管理。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "schedule_type": {
                            "type": "string",
                            "enum": ["one_time", "daily", "interval"],
                            "description": "唤醒类型：one_time=一次性，daily=每天，interval=间隔执行"
                        },
                        "schedule_time": {
                            "type": "string",
                            "description": "计划唤醒时间（RFC3339 格式），例如 2026-08-23T18:00:00+08:00。对于 one_time 和 daily 类型必填；interval 类型为首次执行时间（可选，不填则立即开始计时）"
                        },
                        "interval_seconds": {
                            "type": "integer",
                            "description": "间隔秒数（仅 schedule_type=interval 时使用），例如 3600 表示每1小时唤醒一次"
                        },
                        "targets": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {
                                        "type": "string",
                                        "enum": ["user", "group"],
                                        "description": "目标类型：user=私聊，group=群"
                                    },
                                    "id": {
                                        "type": "string",
                                        "description": "目标 openid"
                                    },
                                    "app_id": {
                                        "type": "string",
                                        "description": "目标所属机器人 APP_ID"
                                    },
                                    "name": {
                                        "type": "string",
                                        "description": "目标名称（可选）"
                                    }
                                },
                                "required": ["type", "id", "app_id"]
                            },
                            "description": "唤醒目标列表（至少一个），每个目标需指定 type、id 和 app_id"
                        },
                        "initiator": {
                            "type": "object",
                            "properties": {
                                "type": {
                                    "type": "string",
                                    "enum": ["user", "group"],
                                    "description": "发起者类型：user=用户，group=群"
                                },
                                "id": {
                                    "type": "string",
                                    "description": "发起者 ID（用户 openid 或群 member_openid）"
                                },
                                "name": {
                                    "type": "string",
                                    "description": "发起者名称"
                                },
                                "group_id": {
                                    "type": "string",
                                    "description": "如果是在群中设置的，填群 openid（可选）"
                                },
                                "group_name": {
                                    "type": "string",
                                    "description": "群名称（如果是在群中设置的，可选）"
                                }
                            },
                            "required": ["type", "id", "name"],
                            "description": "发起者信息（谁创建的唤醒任务，会在唤醒消息中显示）"
                        },
                        "description": {
                            "type": "string",
                            "description": "唤醒说明内容，描述唤醒的目标/工作内容，会在唤醒消息中显示"
                        },
                        "isolation_mode": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "隔离模式（可选，默认0）：1=开启隔离，唤醒过程中用户消息不会打断唤醒处理，两者互不干扰；0=关闭隔离，唤醒和用户消息会互相打断共享上下文"
                        }
                    },
                    "required": ["schedule_type", "targets", "initiator", "description"]
                }
            }
        },
        # ==================== 查看定时唤醒列表 ====================
        {
            "type": "function",
            "function": {
                "name": "list_scheduled_wakeup",
                "description": "查看定时唤醒任务列表，支持按状态、关键词、时间等条件筛选。若配置中 allow_manage_all_push=1 则可看到所有机器人的唤醒。返回任务 ID、类型、时间、目标、说明、状态、最近3次执行时间。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "description": "按状态筛选，可多选（逗号分隔取「或」）。取值：pending(未开始)/completed(执行完成)/failed(执行失败)，也接受中文写法。例如 \"completed\" 或 \"pending,completed\""
                        },
                        "keywords": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "关键词数组，多个关键词任一命中即可（默认）。匹配范围含任务ID、唤醒说明、发起人、目标群/用户、错误信息等。例如 [\"总结\",\"日报\"]。也可直接传字符串 \"总结,日报\""
                        },
                        "keyword": {
                            "type": "string",
                            "description": "单个关键词（等价于 keywords 只传一个）"
                        },
                        "match_all": {
                            "type": "boolean",
                            "description": "为 true 时要求传入的所有关键词都命中（默认 false，即任一命中即可）"
                        },
                        "task_id": {
                            "type": "string",
                            "description": "按任务 ID 筛选（包含匹配）"
                        },
                        "task_app_id": {
                            "type": "string",
                            "description": "按创建该任务的机器人 APP_ID 筛选，只看某个机器人的任务（多机器人场景用）。APP_ID 从 get_targets 结果中获取"
                        },
                        "target_type": {
                            "type": "string",
                            "enum": ["group", "user", "c2c"],
                            "description": "按目标类型筛选：group=群，user/c2c=用户"
                        },
                        "target_id": {
                            "type": "string",
                            "description": "按目标 ID 筛选（包含匹配）"
                        },
                        "start": {
                            "type": "string",
                            "description": "起始时间（含），格式 2026-09-21 或 2026-09-21T20:00:00+08:00"
                        },
                        "end": {
                            "type": "string",
                            "description": "结束时间（含），只写日期时自动补到当天 23:59:59"
                        },
                        "isolation_mode": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "按隔离模式筛选：1=只看隔离唤醒，0=只看非隔离唤醒"
                        },
                        "time_field": {
                            "type": "string",
                            "enum": ["schedule_time", "created_at"],
                            "description": "时间筛选用哪个字段比较，默认 schedule_time（找不到时回退 created_at）"
                        },
                        "order": {
                            "type": "string",
                            "enum": ["asc", "desc"],
                            "description": "按时间排序：desc=新的在前，asc=旧的在前"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "最多返回多少条（取最近的前 N 条）"
                        }
                    },
                    "required": []
                }
            }
        },
        # ==================== 删除定时唤醒 ====================
        {
            "type": "function",
            "function": {
                "name": "delete_scheduled_wakeup",
                "description": "删除一个定时唤醒任务。只能删除当前机器人发起的任务，除非 allow_manage_all_push=1。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "要删除的任务 ID（从 list_scheduled_wakeup 结果中获取）"
                        }
                    },
                    "required": ["task_id"]
                }
            }
        },
        # ==================== 修改定时唤醒 ====================
        {
            "type": "function",
            "function": {
                "name": "update_scheduled_wakeup",
                "description": "修改一个定时唤醒任务的时间、类型、说明、目标列表、发起者或隔离模式。只传需要修改的字段。只能修改当前机器人发起的任务，除非 allow_manage_all_push=1。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "要修改的任务 ID"
                        },
                        "schedule_type": {
                            "type": "string",
                            "enum": ["one_time", "daily", "interval"],
                            "description": "新的唤醒类型（可选）"
                        },
                        "schedule_time": {
                            "type": "string",
                            "description": "新的计划唤醒时间（RFC3339 格式，可选）"
                        },
                        "interval_seconds": {
                            "type": "integer",
                            "description": "新的间隔秒数（仅 interval 类型，可选）"
                        },
                        "description": {
                            "type": "string",
                            "description": "新的唤醒说明内容（可选）"
                        },
                        "targets": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["user", "group"]},
                                    "id": {"type": "string"},
                                    "app_id": {"type": "string"},
                                    "name": {"type": "string"}
                                },
                                "required": ["type", "id", "app_id"]
                            },
                            "description": "新的唤醒目标列表（可选）"
                        },
                        "initiator": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["user", "group"]},
                                "id": {"type": "string", "description": "发起者ID"},
                                "name": {"type": "string", "description": "发起者名称"},
                                "group_id": {"type": "string", "description": "如果是在群中设置的，群openid（可选）"},
                                "group_name": {"type": "string", "description": "群名称（可选）"}
                            },
                            "description": "新的发起者信息（可选）"
                        },
                        "isolation_mode": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "新的隔离模式：1=开启（唤醒与用户消息互不干扰），0=关闭（可选）"
                        }
                    },
                    "required": ["task_id"]
                }
            }
        },
        # ==================== 新增：跳过回复工具 ====================
        {
            "type": "function",
            "function": {
                "name": "skip_reply",
                "description": "调用此工具表示结束本轮工具调用循环，AI 不再继续调用工具，也不再生成文本回复。适用于已经通过 send_media、send_text、play_music 等工具完成回复的场景。注意：调用 skip_reply 后本轮工具循环立即结束，不再执行后续工具调用。",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        },
        # ==================== 记忆管理工具 ====================
        {
            "type": "function",
            "function": {
                "name": "view_memory",
                "description": "查看指定记忆级别的当前记忆列表。level=global 查看全局记忆，level=bot 查看某机器人的专属记忆，level=group 查看某群的记忆，level=c2c 查看某私聊用户的记忆。identifier 为对应的 ID（app_id/group_openid/user_openid）。返回格式化后的记忆内容和启用状态。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level": {
                            "type": "string",
                            "enum": ["global", "bot", "group", "c2c"],
                            "description": "记忆级别：global=全局记忆，bot=机器人专属记忆，group=群聊记忆，c2c=私聊记忆"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符：global 和 bot 级别填机器人 APP_ID，group 级别填群 openid，c2c 级别填用户 openid"
                        }
                    },
                    "required": ["level", "identifier"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "add_memory",
                "description": "添加记忆。支持单条（传入 level + identifier + content）或批量（传入 items 数组）。批量模式可以一次向多个不同目标添加多条记忆。如果涉及用户，内容中必须包含用户名(QQ号)的格式。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level": {
                            "type": "string",
                            "enum": ["global", "bot", "group", "c2c"],
                            "description": "记忆级别（单条模式使用）"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符（单条模式使用）：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                        },
                        "content": {
                            "type": "string",
                            "description": "要添加的记忆内容（单条模式使用）。如果涉及用户，必须包含用户名(QQ号)格式，如'张三(1234567) 是管理员'"
                        },
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "level": {
                                        "type": "string",
                                        "enum": ["global", "bot", "group", "c2c"],
                                        "description": "记忆级别"
                                    },
                                    "identifier": {
                                        "type": "string",
                                        "description": "标识符：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                                    },
                                    "content": {
                                        "type": "string",
                                        "description": "要添加的记忆内容，如果涉及用户必须包含用户名(QQ号)格式"
                                    }
                                },
                                "required": ["level", "identifier", "content"]
                            },
                            "description": "批量添加模式：传入 items 数组可一次操作多条"
                        }
                    },
                    "required": []
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "modify_memory",
                "description": "修改记忆。支持单条（传入 level + identifier + index + content）或批量（传入 items 数组）。批量模式可以一次修改多个不同目标的记忆。如果涉及用户，内容中必须包含用户名(QQ号)的格式。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level": {
                            "type": "string",
                            "enum": ["global", "bot", "group", "c2c"],
                            "description": "记忆级别（单条模式使用）"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符（单条模式使用）：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                        },
                        "index": {
                            "type": "integer",
                            "description": "要修改的记忆索引，从 0 开始（单条模式使用）"
                        },
                        "content": {
                            "type": "string",
                            "description": "新的记忆内容（单条模式使用）。如果涉及用户，必须包含用户名(QQ号)格式"
                        },
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "level": {
                                        "type": "string",
                                        "enum": ["global", "bot", "group", "c2c"],
                                        "description": "记忆级别"
                                    },
                                    "identifier": {
                                        "type": "string",
                                        "description": "标识符：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                                    },
                                    "index": {
                                        "type": "integer",
                                        "description": "要修改的记忆索引（从 0 开始）"
                                    },
                                    "content": {
                                        "type": "string",
                                        "description": "新的记忆内容，如果涉及用户必须包含用户名(QQ号)格式"
                                    }
                                },
                                "required": ["level", "identifier", "index", "content"]
                            },
                            "description": "批量修改模式：传入 items 数组可一次操作多条"
                        }
                    },
                    "required": []
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "delete_memory",
                "description": "删除记忆。支持单条（传入 level + identifier + index）或批量（传入 items 数组）。批量模式可以一次删除多个不同目标的记忆。先用 view_memory 查看列表获取索引。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level": {
                            "type": "string",
                            "enum": ["global", "bot", "group", "c2c"],
                            "description": "记忆级别（单条模式使用）"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符（单条模式使用）：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                        },
                        "index": {
                            "type": "integer",
                            "description": "要删除的记忆索引，从 0 开始（单条模式使用）"
                        },
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "level": {
                                        "type": "string",
                                        "enum": ["global", "bot", "group", "c2c"],
                                        "description": "记忆级别"
                                    },
                                    "identifier": {
                                        "type": "string",
                                        "description": "标识符：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                                    },
                                    "index": {
                                        "type": "integer",
                                        "description": "要删除的记忆索引（从 0 开始）"
                                    }
                                },
                                "required": ["level", "identifier", "index"]
                            },
                            "description": "批量删除模式：传入 items 数组可一次操作多条"
                        }
                    },
                    "required": []
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "enable_memory",
                "description": "启用指定记忆级别。启用后该级别的记忆会在 AI 回复时显示在系统提示中。全局记忆用 global，机器人专属记忆用 bot，群聊记忆用 group，私聊记忆用 c2c。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level": {
                            "type": "string",
                            "enum": ["global", "bot", "group", "c2c"],
                            "description": "记忆级别"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                        }
                    },
                    "required": ["level", "identifier"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "disable_memory",
                "description": "禁用指定记忆级别。禁用后该级别的记忆不会在 AI 回复时显示，但记忆内容仍保留。全局记忆用 global，机器人专属记忆用 bot，群聊记忆用 group，私聊记忆用 c2c。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level": {
                            "type": "string",
                            "enum": ["global", "bot", "group", "c2c"],
                            "description": "记忆级别"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符：global/bot=APP_ID，group=群 openid，c2c=用户 openid"
                        }
                    },
                    "required": ["level", "identifier"]
                }
            }
        },
        # ==================== 记忆搜索工具 ====================
        {
            "type": "function",
            "function": {
                "name": "search_memory",
                "description": "搜索记忆库，支持多个关键词（用逗号分隔）和可选的记忆层级。level=global 搜索全局记忆，level=bot 搜索机器人专属记忆，level=group 搜索当前群聊记忆，level=c2c 搜索当前私聊记忆，level=all（默认）搜索全部层级。identifier 为对应的 ID（app_id/群openid/用户openid）。返回按层级分组的匹配记忆条目列表。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "keywords": {
                            "type": "string",
                            "description": "搜索关键词，多个关键词用逗号分隔，如'张三,管理员,禁言'，只要匹配任意一个即返回"
                        },
                        "level": {
                            "type": "string",
                            "enum": ["all", "global", "bot", "group", "c2c"],
                            "description": "记忆层级：all=全部（默认），global=全局，bot=机器人专属，group=群聊，c2c=私聊"
                        },
                        "identifier": {
                            "type": "string",
                            "description": "标识符：all/global/bot 层级填机器人 APP_ID，group 层级填群 openid，c2c 层级填用户 openid。不填时自动使用当前会话的上下文。"
                        }
                    },
                    "required": ["keywords"]
                }
            }
        }
    ]

    # 群管理工具始终可用（不再依赖白名单配置）
    tools.extend([
        {
            "type": "function",
            "function": {
                "name": "mute_member",
                "description": "禁言群内指定成员，支持批量。只能禁言普通成员，不能禁言群主、管理员或机器人。禁言到期后自动解除。使用流程：第一步：先调用 get_bot_state 确认机器人角色为 admin 或 owner；第二步：有权限时再调用此工具禁言。members 为成员数组：传一个元素即单个禁言，传多个元素即批量禁言；每个元素可用自己的 mute_expire_at，也可以在外层统一指定。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "members": {
                            "type": "array",
                            "description": "要禁言的成员数组。传 1 个元素即单个，传多个即批量。元素可为字符串（openid）或对象：member_id 加 mute_expire_at",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "member_id": {
                                        "type": "string",
                                        "description": "要禁言的成员openid，从消息JSON的author.member_openid获取"
                                    },
                                    "mute_expire_at": {
                                        "type": "string",
                                        "description": "该成员的禁言到期时间（RFC3339），不填则用外层 mute_expire_at"
                                    }
                                },
                                "required": ["member_id"]
                            }
                        },
                        "member_id": {
                            "type": "string",
                            "description": "单个成员快捷写法（等价于 members 只放一个元素）"
                        },
                        "mute_expire_at": {
                            "type": "string",
                            "description": "禁言到期时间，RFC3339格式，例如 2026-08-23T18:00:00+08:00"
                        }
                    },
                    "required": ["mute_expire_at"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "unmute_member",
                "description": "解除群内指定成员的禁言状态，支持批量。使用流程：第一步：先调用 get_bot_state 确认机器人角色为 admin 或 owner；第二步：有权限时再调用此工具解除禁言。members 为成员数组：传一个元素即单个，传多个即批量。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "members": {
                            "type": "array",
                            "description": "要解除禁言的成员数组。传 1 个元素即单个，传多个即批量。元素可为字符串（openid）或对象：member_id",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "member_id": {
                                        "type": "string",
                                        "description": "要解除禁言的成员openid"
                                    }
                                },
                                "required": ["member_id"]
                            }
                        },
                        "member_id": {
                            "type": "string",
                            "description": "单个成员快捷写法（等价于 members 只放一个元素）"
                        }
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_bot_state",
                "description": "获取机器人在指定群中的状态信息，用于判断是否有权限执行群管理操作。返回信息包括：member_role（机器人角色：member=普通成员无管理权限, admin=管理员有禁言权限, owner=群主有全部权限）、allow_proactive_msg（是否接收主动推送）、recv_msg_setting（接收消息设置）。根据 member_role 决定后续操作：admin/owner 可继续使用 mute_member/unmute_member，member 不可执行禁言操作。参数 target_id：要查询的群ID。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "target_id": {
                            "type": "string",
                            "description": "群 openid，需要查询的群ID"
                        }
                    },
                    "required": ["target_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_group_mute_status",
                "description": "获取当前群的禁言状态信息。使用流程：第一步：先调用 get_bot_state 确认机器人角色为 admin 或 owner；第二步：有权限时再调用此工具查看群内的全员禁言模式、定时禁言规则、周期禁言规则以及当前被禁言的成员列表。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "target_id": {
                            "type": "string",
                            "description": "群 openid，需要查询禁言状态的群ID"
                        }
                    },
                    "required": ["target_id"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "query_history",
                "description": "查询聊天历史记录。默认查询当前会话，支持跨群跨机器人查询。跨机器人用法：传入 app_id=目标机器人APP_ID（如\"1905417157\"，从get_targets获取）、target_type=\"group\"或\"c2c\"、target_id=目标群openid或用户openid，即可读取任意机器人的任意群/私聊的聊天记录。支持多关键词搜索（keywords 数组，任一匹配即返回）、按时间范围过滤（start_time/end_time RFC3339格式）。返回按时间倒序编号的消息列表，包含内容、角色和消息ID。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "range": {
                            "type": "integer",
                            "description": "要查询的最近消息条数，例如 5 表示最近5条"
                        },
                        "keywords": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "可选，关键词列表，匹配任意一个即返回该消息（OR 逻辑）。不传则不做关键词过滤。"
                        },
                        "search": {
                            "type": "string",
                            "description": "可选，单关键词搜索（与 keywords 等效，二选一即可）"
                        },
                        "start_time": {
                            "type": "string",
                            "description": "可选，起始时间（RFC3339格式），例如 2026-09-15T00:00:00+08:00。只返回此时间之后的消息。"
                        },
                        "end_time": {
                            "type": "string",
                            "description": "可选，截止时间（RFC3339格式），例如 2026-09-15T23:59:59+08:00。只返回此时间之前的消息。"
                        },
                        "show_full": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "是否显示完整消息内容：0=只显示前40字（默认），1=显示完整内容"
                        },
                        "app_id": {
                            "type": "string",
                            "description": "可选，跨机器人查询时填写目标机器人的 APP_ID（如 \"1905417157\"）。不传则默认当前机器人。受 ALLOW_CROSS_BOT_HISTORY / ALLOW_CROSS_BOT_HISTORY_INCOMING 配置约束。"
                        },
                        "target_type": {
                            "type": "string",
                            "enum": ["c2c", "group"],
                            "description": "可选，跨群/跨机器人查询时指定目标类型：c2c=私聊用户，group=群聊。不传则自动判断当前会话类型。"
                        },
                        "target_id": {
                            "type": "string",
                            "description": "可选，跨群/跨机器人查询时指定目标ID：target_type=c2c时填用户openid，target_type=group时填群openid。可从 get_targets 结果中获取。不传则使用当前会话的目标ID。"
                        }
                    },
                    "required": ["range"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "revoke_message",
                "description": "撤回消息。可传入单条或多条消息ID进行批量撤回。支持跨机器人撤回：传入 app_id=目标机器人APP_ID（如\"1905417157\"，从get_targets获取），target_type+target_id指定目标会话，即可撤回其他机器人管理的群中的消息。如果不传跨机器人参数则默认撤回当前机器人的消息。私聊中可以撤回机器人2分钟内发送的消息（超时不可撤回）。群聊中如果机器人是群管理员(admin)或群主(owner)可以撤回任意成员的消息且没有2分钟限制，机器人只是普通成员(member)只能撤回自己2分钟内发送的消息。使用流程：第一步：先调用 get_bot_state 确认机器人在群内的角色；第二步：有权限时再调用此工具撤回。跨机器人撤回受 ALLOW_CROSS_BOT_HISTORY / ALLOW_CROSS_BOT_HISTORY_INCOMING 配置约束。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "message_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "要撤回的消息ID列表，可以传一个或多个"
                        },
                        "target_type": {
                            "type": "string",
                            "enum": ["c2c", "group"],
                            "description": "消息所属类型：c2c=私聊，group=群聊。跨机器人撤回时必填。不传则自动判断当前会话类型。"
                        },
                        "target_id": {
                            "type": "string",
                            "description": "消息所属的用户openid（私聊）或群openid（群聊）。跨机器人撤回时必填。不传则使用当前会话的目标ID。可从 get_targets 结果中获取。"
                        },
                        "app_id": {
                            "type": "string",
                            "description": "可选，跨机器人撤回时填写目标机器人的 APP_ID（如 \"1905417157\"）。不传则默认当前机器人。受 ALLOW_CROSS_BOT_HISTORY / ALLOW_CROSS_BOT_HISTORY_INCOMING 配置控制。"
                        }
                    },
                    "required": ["message_ids"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "batch_revoke",
                "description": "按关键词和/或时间范围批量撤回消息。支持两种用法：①按关键词撤（keywords 数组，如[\"广告\",\"加V\"]，任一匹配即触发）；②按时间撤（只传 start_time/end_time，不传 keywords，则撤回该时间范围内的全部消息）。两种可组合使用（时间范围内且含关键词）。自动扫描目标会话的历史消息匹配后批量撤回。支持跨机器人跨群操作：传入 app_id=目标机器人APP_ID（如\"1905417157\"，从get_targets获取）、target_type+target_id指定目标会话，即可批量撤回其他机器人管理的群中的消息。如果不传跨机器人参数则默认操作当前机器人的消息。安全机制：默认只做预览不执行，必须先在不传 confirm 的情况下调用一次查看匹配结果，确认无误后再带 confirm=1 真正执行。私聊中只能撤回机器人2分钟内发送的消息；群聊中管理员可撤回任意成员消息，普通成员只能撤回自己2分钟内发送的消息。跨机器人操作受 ALLOW_CROSS_BOT_HISTORY / ALLOW_CROSS_BOT_HISTORY_INCOMING 配置约束。",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "keywords": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "可选，关键词列表，匹配任意一个即撤回该消息。例如 [\"广告\", \"违规\", \"刷屏\"]。不传则仅按时间范围撤回。与 start_time/end_time 至少要提供一个。"
                        },
                        "start_time": {
                            "type": "string",
                            "description": "可选，起始时间（RFC3339格式），例如 2026-09-15T00:00:00+08:00。只匹配此时间之后的消息。只传时间、不传 keywords 时即按时间范围撤回。"
                        },
                        "end_time": {
                            "type": "string",
                            "description": "可选，截止时间（RFC3339格式），例如 2026-09-15T23:59:59+08:00。只匹配此时间之前的消息。只传时间、不传 keywords 时即按时间范围撤回。"
                        },
                        "target_type": {
                            "type": "string",
                            "enum": ["c2c", "group"],
                            "description": "可选，跨群/跨机器人操作时指定目标类型：c2c=私聊用户，group=群聊。不传则自动判断当前会话类型。"
                        },
                        "target_id": {
                            "type": "string",
                            "description": "可选，跨群/跨机器人操作时指定目标ID：target_type=c2c时填用户openid，target_type=group时填群openid。可从 get_targets 结果中获取。不传则使用当前会话的目标ID。"
                        },
                        "app_id": {
                            "type": "string",
                            "description": "可选，跨机器人操作时填写目标机器人的 APP_ID（如 \"1905417157\"）。不传则默认当前机器人。受 ALLOW_CROSS_BOT_HISTORY / ALLOW_CROSS_BOT_HISTORY_INCOMING 配置控制。"
                        },
                        "dry_run": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "可选（旧参数），预览模式：1=只列出匹配的消息，不实际撤回。等价于不传 confirm。"
                        },
                        "confirm": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "可选，安全确认开关。默认（不传或传 0）为预览模式，只返回匹配到的消息列表、不做任何撤回；必须显式传 confirm=1 才会真正执行撤回。建议先不加此参数预览匹配结果，确认无误后再带 confirm=1 调用。"
                        }
                    },
                    "required": []
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
                               allow_redirects: bool = True, ctx=None) -> str:
    """执行任意HTTP请求，返回格式化的结果字符串

    ctx: LogCtx（由 execute_tool_call 传入），用于日志标识。
    """
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
        info(f"[HTTP工具] {method} {url[:80]}（超时={timeout_display}s, SSL验证={verify_ssl}, 跟随重定向={allow_redirects}）", ctx=ctx)
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

        info(f"[HTTP工具] 完成，状态码 {status}，响应长度 {len(content)}", ctx=ctx)
        return result
    except requests.exceptions.Timeout:
        timeout_msg = f"HTTP请求超时（{timeout}秒）" if timeout != 0 else "HTTP请求超时（无限制超时仍有系统级超时）"
        return f"{timeout_msg}: {method} {url}"
    except requests.exceptions.ConnectionError as e:
        return f"HTTP连接失败: {method} {url}\n错误: {e}"
    except Exception as e:
        return f"HTTP请求异常: {method} {url}\n错误: {type(e).__name__}: {e}"

# ==================== 跨机器人工具辅助函数 ====================
def _ensure_bot_client(target_app_id: str, caller_client) -> tuple:
    """
    确保目标机器人的 BotClient 可用。返回 (bot_client, is_cross)。
    如果 target_app_id 与调用者相同或未指定，返回调用者的 client。
    如果跨机器人，检查权限后创建新的 BotClient。
    """
    caller_app_id = caller_client.app_id
    if not target_app_id or target_app_id == caller_app_id:
        return caller_client, False
    # 跨机器人权限检查
    if not get_bot_allow_cross_history(caller_app_id):
        raise PermissionError(f"本机器人（{caller_app_id}）禁止跨机器人聊天记录操作（ALLOW_CROSS_BOT_HISTORY=0）")
    if not get_bot_allow_cross_history_incoming(target_app_id):
        raise PermissionError(f"目标机器人（{target_app_id}）禁止被跨机器人聊天记录操作（ALLOW_CROSS_BOT_HISTORY_INCOMING=0）")
    all_bots = {b["APP_ID"]: b["APP_SECRET"] for b in get_bots() if b.get("APP_ID")}
    secret = all_bots.get(target_app_id)
    if not secret:
        raise ValueError(f"未找到目标机器人 {target_app_id} 的凭证")
    from client import BotClient
    return BotClient(target_app_id, secret), True


def _resolve_thread_and_target(target_type: str, target_id: str,
                               default_msg_type: str, default_recipient_id: str,
                               default_group_id: str) -> tuple:
    """
    解析目标会话参数，返回 (thread_key, resolved_type, resolved_id)。
    """
    t_type = target_type or default_msg_type or "group"
    t_id = target_id or ""
    if not t_id:
        if t_type == "c2c":
            t_id = default_recipient_id
        else:
            t_id = default_recipient_id or default_group_id
    if not t_id:
        raise ValueError("无法确定目标会话ID，请指定 target_id 或在当前会话中使用")
    thread_key = f"{'c2c' if t_type == 'c2c' else 'group'}_{t_id}"
    return thread_key, t_type, t_id


# ==================== 工具调用执行（不含媒体识别，动态导入 ai） ====================

async def execute_tool_call(tool_call: Dict, bot_client, group_id: str = None,
                            msg_type: str = None, recipient_id: str = None,
                            msg_id: Optional[str] = None,
                            thread_key: Optional[str] = None,
                            author_id: str = None) -> Dict:  # 新增：当前发送者 ID
    """
    执行单个工具调用，返回 {"role": "tool", "tool_call_id": ..., "content": ...} 格式的结果消息。
    当识别到 recognize_media 时，动态导入 ai 模块调用相关函数。
    """
    import json as _json
    from msg import ensure_rfc3339_time  # 用于群管理

    # 本次工具执行的日志标识（app_id 来自 bot_client，thread_key/msg_id 来自调用方）
    ctx = LogCtx(app_id=getattr(bot_client, "app_id", ""),
                 thread_key=thread_key or "", msg_id=msg_id or "")

    tool_call_id = tool_call.get("id", "")
    function_name = tool_call.get("function", {}).get("name", "")
    arguments_str = tool_call.get("function", {}).get("arguments", "{}")

    try:
        arguments = _json.loads(arguments_str) if arguments_str else {}
    except (_json.JSONDecodeError, TypeError) as e:
        arguments = {}
        error(f"[工具执行] 参数解析失败: {arguments_str[:200]}", ctx=ctx)
        # 不猜测、不修补，直接把问题交回模型让它重发。
        # 常见成因：输出了空值（"headers": ,）或漏了引号/逗号。
        _detail = str(e)[:120]
        _raw = (arguments_str or "")[:300]
        return {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": (
                f"错误：{function_name} 的参数不是合法 JSON，无法执行。\n"
                f"解析器报错：{_detail}\n"
                f"你发出的原始参数：{_raw}\n\n"
                "请重新调用该工具，并确保：\n"
                "1) 整体是一个 JSON 对象，键和字符串值都用双引号；\n"
                "2) 每个键后面必须有值——不需要的字段请整个省略，"
                "不要写成空值（例如不要写 \"headers\": , 而应直接不写 headers）；\n"
                "3) 对象/数组字段要给完整值（如 \"cookies\": {\"k\": \"v\"}），"
                "不确定就省略该字段。"
            ),
        }

    # 合法 JSON 但非对象（如 [1,2,3] / "str" / 123）也要归一到 {}，
    # 否则后面 123 处 arguments.get(...) 会直接 AttributeError。
    if not isinstance(arguments, dict):
        error(f"[工具执行] 参数不是 JSON 对象（{type(arguments).__name__}）: {arguments_str[:200]}", ctx=ctx)
        arguments = {}

    info(f"[工具执行] 调用 {function_name}，参数: {_json.dumps(arguments, ensure_ascii=False)[:200]}", ctx=ctx)

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
                ctx=ctx,
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
            # 自动推断文件类型（未指定时）；实现见 utils.infer_file_type
            if file_type is None:
                file_type = infer_file_type(file_name, source)

            # 经 send_with_policy 发送：视频/语音/文件（caption 会被 QQ 忽略）
            # 会自动"先媒体、后独立文本"双发，保证 caption 可见；图片仍为单条。
            sent = await send_with_policy(
                bot_client, msg_type, recipient_id, caption,
                media_source=source, file_type=file_type, file_name=file_name,
                msg_id=msg_id,   # 传入原始消息ID，实现被动回复
            )
            success = sent["ok"]
            if success:
                ids = sent["message_ids"]
                id_desc = "、".join(
                    f"{'媒体' if i['type'] in ('media', 'media_combined') else '文本'}消息ID: {i['msg_id']}"
                    for i in ids
                )
                result_content = f"媒体发送成功：{source}（{id_desc}）"
                result_content += f"\n媒体URL: {source}"
                if caption:
                    result_content += f"\n附带文字：{caption}"
                # 写入历史记录：媒体与文本分别成条，媒体条采用
                # 「媒体：<正文>（<URL>）」格式，便于 AI 与查询接口直接识别媒体及其来源。
                if thread_key:
                    _media_body = caption or "媒体内容"
                    for _m in ids:
                        _is_media = _m.get("type") in ("media", "media_combined")
                        if _is_media:
                            _text = f"媒体：{_media_body}（{source}）"
                        else:
                            # 双发时随后的独立文本消息只带正文，与媒体条分开记录
                            _text = caption or ""
                        append_message(thread_key, "assistant", _text,
                                       msg_id=_m.get("msg_id"), msg_idx=_m.get("msg_idx"),
                                       media_url=source if _is_media else None)
            else:
                result_content = f"媒体发送失败：{source}，请检查文件是否有效或上传权限"

    # ==================== 发送纯文本 ====================
    elif function_name == "send_text":
        # 格式与正文统一解析（content 为准，兼容 markdown_content/text_content）
        use_markdown, payload_content = parse_message_type(arguments)

        if not payload_content or not str(payload_content).strip():
            result_content = ("错误：send_text 缺少 content 参数"
                              + ("（message_type=markdown 时正文同样填入 content）"
                                 if use_markdown else ""))
        elif not recipient_id:
            result_content = "错误：无法获取当前会话的接收者ID，请检查上下文"
        else:
            send_kwargs = {
                "msg_type": msg_type,
                "recipient_id": recipient_id,
                "msg_id": msg_id,
            }
            if use_markdown:
                send_kwargs["markdown"] = payload_content
            else:
                send_kwargs["content"] = payload_content

            success = await bot_client.send_message(**send_kwargs)
            if success:
                kind = "Markdown 消息" if use_markdown else "文本"
                send_id = bot_client.get_last_send_id()
                send_msg_idx = bot_client.get_last_send_msg_idx()
                result_content = f"{kind}发送成功（长度 {len(str(payload_content))} 字）\n消息ID: {send_id}"
                # 将实际发送的内容写入历史记录
                if thread_key:
                    append_message(thread_key, "assistant", payload_content,
                                   msg_id=send_id, msg_idx=send_msg_idx,
                                   is_markdown=use_markdown)
            else:
                kind = "Markdown 消息" if use_markdown else "文本"
                result_content = f"{kind}发送失败，请检查网络或权限"

    # ==================== 媒体识别（动态导入 ai 调用基础函数） ====================
    elif function_name == "recognize_media":
        media_url = arguments.get("media_url")
        media_type = arguments.get("media_type")
        filename = arguments.get("filename", "媒体")
        disable_cache = arguments.get("disable_cache", False)  # 新增
        # 可选：本次识别的额外关注点。传了即进入聚焦识别
        # （忽略缓存重新识别，并把结果追加到原摘要之后，见 ai.recognize_media）
        focus_prompt = arguments.get("prompt")
        if not media_url:
            result_content = "错误：recognize_media 缺少 media_url 参数"
        else:
            try:
                # 动态导入 ai 模块以避免循环依赖
                import ai
                result_content = await ai.recognize_media_by_url(
                    media_url, filename, media_type, disable_cache=disable_cache,
                    app_id=getattr(bot_client, "app_id", None),
                    prompt=focus_prompt,
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
                            error(f"[搜索音乐] JSON解析失败，原始内容前200字符: {raw_text[:200]}", ctx=ctx)
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
                            info(f"[播放音乐] 尝试 {label}", ctx=ctx)
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
                                send_id = bot_client.get_last_send_id()
                                send_msg_idx = bot_client.get_last_send_msg_idx()
                                result_content = (f"音乐播放成功（{label}）：{url}"
                                                  f"\n消息ID: {send_id}"
                                                  f"\n媒体URL: {url}")
                                if caption:
                                    result_content += f"\n附带文字：{caption}"
                                # 写入聊天记录，格式与 send_media 统一：「媒体：<正文>（<URL>）」
                                if thread_key:
                                    append_message(thread_key, "assistant",
                                                   f"媒体：{caption or '正在播放音乐'}（{url}）",
                                                   msg_id=send_id, msg_idx=send_msg_idx,
                                                   media_url=url)
                                break
                            else:
                                error(f"[播放音乐] {label} 失败", ctx=ctx)
                        if not success:
                            result_content = "音乐发送失败（所有尝试均失败）。"
            except Exception as e:
                result_content = f"播放音乐异常：{e}"

    elif function_name == "mute_member":
        # 成员一律用数组：传一个即单个、传多个即批量
        from task_core import normalize_members, mute_result_summary
        fallback_expire = arguments.get("mute_expire_at", "")
        members_src = arguments.get("members")
        if members_src is None:
            members_src = arguments.get("member_id")
        members, merr = normalize_members(members_src, fallback_expire)
        if merr:
            result_content = f"错误：{merr}"
        elif not members:
            result_content = "错误：mute_member 缺少 members（数组）或 member_id"
        elif not any(m.get("mute_expire_at") for m in members) and not fallback_expire:
            result_content = "错误：mute_member 缺少 mute_expire_at 参数"
        elif not group_id:
            result_content = "错误：mute_member 只能在群聊中使用"
        else:
            beijing_tz = timezone(timedelta(hours=8))
            results = []
            for m in members:
                mid = m["member_id"]
                valid_time = ensure_rfc3339_time(m.get("mute_expire_at") or fallback_expire,
                                                 default_seconds=3600)
                success, error_code = await bot_client.set_group_mute(group_id, "add", mid, valid_time)
                if success:
                    results.append({"member_id": mid, "ok": True, "mute_expire_at": valid_time})
                    continue
                if error_code == 10007:
                    new_time = (datetime.now(beijing_tz) + timedelta(hours=1)).replace(
                        microsecond=0).isoformat(timespec='seconds')
                    success2, error_code2 = await bot_client.set_group_mute(group_id, "add", mid, new_time)
                    if success2:
                        results.append({"member_id": mid, "ok": True, "mute_expire_at": new_time,
                                        "note": "自动修正为1小时"})
                        continue
                    error_code = error_code2
                results.append({"member_id": mid, "ok": False, "error_code": error_code})

            if len(results) == 1:
                r0 = results[0]
                if r0["ok"]:
                    note = "（自动修正为1小时）" if r0.get("note") else ""
                    result_content = f"禁言成功：成员 {r0['member_id']} 已禁言至 {r0['mute_expire_at']}{note}"
                else:
                    result_content = f"禁言失败：成员 {r0['member_id']}，错误码 {r0.get('error_code')}"
            else:
                s = mute_result_summary(results)
                lines = [f"批量禁言完成：成功 {s['success']} 人，失败 {s['failed']} 人（共 {s['total']} 人）"]
                for r in results:
                    if r["ok"]:
                        lines.append(f"  ✓ {r['member_id']} → {r['mute_expire_at']}")
                    else:
                        lines.append(f"  ✗ {r['member_id']} → 错误码 {r.get('error_code')}")
                result_content = "\n".join(lines)

    elif function_name == "unmute_member":
        from task_core import normalize_members, mute_result_summary
        members_src = arguments.get("members")
        if members_src is None:
            members_src = arguments.get("member_id")
        members, merr = normalize_members(members_src, "")
        if merr:
            result_content = f"错误：{merr}"
        elif not members:
            result_content = "错误：unmute_member 缺少 members（数组）或 member_id"
        elif not group_id:
            result_content = "错误：unmute_member 只能在群聊中使用"
        else:
            results = []
            for m in members:
                mid = m["member_id"]
                success, error_code = await bot_client.set_group_mute(group_id, "del", mid, "")
                results.append({"member_id": mid, "ok": success, "error_code": error_code})

            if len(results) == 1:
                r0 = results[0]
                if r0["ok"]:
                    result_content = f"解除禁言成功：成员 {r0['member_id']}"
                else:
                    result_content = f"解除禁言失败：成员 {r0['member_id']}，错误码 {r0.get('error_code')}"
            else:
                s = mute_result_summary(results)
                lines = [f"批量解除禁言完成：成功 {s['success']} 人，失败 {s['failed']} 人（共 {s['total']} 人）"]
                for r in results:
                    lines.append(f"  {'✓' if r['ok'] else '✗'} {r['member_id']}"
                                 + ("" if r["ok"] else f" → 错误码 {r.get('error_code')}"))
                result_content = "\n".join(lines)

    elif function_name == "get_bot_state":
        target_id = arguments.get("target_id", "")
        if not target_id:
            # 未指定群ID时使用当前群
            if group_id:
                target_id = group_id
            else:
                result_content = "错误：get_bot_state 需要在群聊中使用或指定 target_id"
        if target_id:
            try:
                state = await bot_client.get_bot_state(target_id)
                if state:
                    lines = []
                    lines.append(f"群ID: {target_id}")
                    lines.append(f"机器人角色: {state.get('member_role', '未知')}")
                    lines.append(f"接受主动推送: {'是' if state.get('allow_proactive_msg') else '否'}")
                    lines.append(f"接收消息设置: {state.get('recv_msg_setting', '未知')}")
                    lines.append(f"入群时间: {state.get('joined_at', '未知')}")
                    lines.append(f"机器人openid: {state.get('member_openid', '未知')}")
                    result_content = "\n".join(lines)
                else:
                    result_content = f"获取机器人在群 {target_id} 的状态失败（可能接口无权限）"
            except Exception as e:
                result_content = f"获取机器人状态异常：{e}"

    elif function_name == "get_group_mute_status":
        target_id = arguments.get("target_id", "")
        if not target_id:
            if group_id:
                target_id = group_id
            else:
                result_content = "错误：get_group_mute_status 需要在群聊中使用或指定 target_id"
        if target_id:
            try:
                mute_status = await bot_client.get_group_mute_status(target_id)
                if mute_status:
                    lines = []
                    global_rule = mute_status.get("global_rule", {})
                    mode = global_rule.get("mode", "none")
                    schedule_rules = global_rule.get("schedule_rules", [])
                    recurring_rules = global_rule.get("recurring_rules", [])
                    members = mute_status.get("members", [])
                    lines.append(f"【群 {target_id} 禁言状态】")
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
                    result_content = "\n".join(lines)
                else:
                    result_content = f"获取群 {target_id} 禁言状态失败（可能机器人不是群管理员）"
            except Exception as e:
                result_content = f"获取群禁言状态异常：{e}"

    elif function_name == "query_history":
        try:
            range_count = int(arguments.get("range", 0))
        except (ValueError, TypeError):
            range_count = 0
        if range_count <= 0:
            result_content = "错误：range 必须为正整数"
        else:
            try:
                # --- 解析跨机器人/跨群目标 ---
                query_app_id = arguments.get("app_id", "").strip()
                query_target_type = arguments.get("target_type", "")
                query_target_id = arguments.get("target_id", "")

                # 如果指定了 app_id 且不同于当前机器人，需要在跨机器人目标上操作
                # 但查询历史只是本地读取 history/*.json，无需 BotClient
                if query_app_id and query_app_id != bot_client.app_id:
                    # 跨机器人读取需要权限检查
                    if not get_bot_allow_cross_history(bot_client.app_id):
                        result_content = f"错误：本机器人（{bot_client.app_id}）禁止跨机器人聊天记录读取（ALLOW_CROSS_BOT_HISTORY=0）"
                        return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                    if not get_bot_allow_cross_history_incoming(query_app_id):
                        result_content = f"错误：目标机器人（{query_app_id}）禁止被跨机器人聊天记录读取（ALLOW_CROSS_BOT_HISTORY_INCOMING=0）"
                        return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}

                # 解析目标会话
                try:
                    q_thread_key, q_type, q_id = _resolve_thread_and_target(
                        query_target_type, query_target_id,
                        msg_type, recipient_id, group_id
                    )
                except ValueError as e:
                    result_content = f"错误：{e}"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}

                # 读取历史
                hist = load_history(q_thread_key)
                if not hist:
                    result_content = f"会话 {q_thread_key} 暂无聊天记录"
                else:
                    show_full_raw = arguments.get("show_full", 0)
                    show_full = 1 if show_full_raw and int(show_full_raw) == 1 else 0

                    # --- 收集关键词（支持 keywords 数组 和 search 单关键词） ---
                    keywords_raw = arguments.get("keywords", [])
                    search_keyword = arguments.get("search", "").strip()
                    all_keywords = []
                    if isinstance(keywords_raw, list):
                        all_keywords.extend([kw.strip() for kw in keywords_raw if kw and kw.strip()])
                    if search_keyword:
                        all_keywords.append(search_keyword)
                    # 去重
                    all_keywords = list(dict.fromkeys(all_keywords))

                    # --- 时间范围解析 ---
                    beijing_tz = timezone(timedelta(hours=8))
                    start_dt = None
                    end_dt = None
                    start_time_str = arguments.get("start_time", "")
                    end_time_str = arguments.get("end_time", "")
                    if start_time_str:
                        try:
                            start_dt = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                            start_dt = start_dt.astimezone(beijing_tz)
                        except Exception as e:
                            result_content = f"错误：start_time 格式无效 '{start_time_str}': {e}"
                            return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                    if end_time_str:
                        try:
                            end_dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                            end_dt = end_dt.astimezone(beijing_tz)
                        except Exception as e:
                            result_content = f"错误：end_time 格式无效 '{end_time_str}': {e}"
                            return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}

                    # 取最近的 range 条（按时间倒序），然后过滤
                    recent = list(reversed(hist[-range_count:] if range_count <= len(hist) else hist))
                    lines = []
                    for idx, msg in enumerate(recent, 1):
                        raw_content = msg.get("content", "")
                        role = msg.get("role", "unknown")
                        msg_id_field = msg.get("msg_id", "")
                        msg_idx = msg.get("msg_idx", "")
                        ref_idx = msg.get("ref_msg_idx", "")
                        # 跳过 tool 消息（工具调用结果）
                        if role == "tool":
                            continue

                        # 消息时间取结构化 ts 字段（旧记录由 load_history 归一后同样可用），
                        # 不再从正文里正则抠 "[YYYY-MM-DD HH:MM]" 前缀。
                        # content 已是纯正文，无需再剥前缀。
                        msg_dt = parse_history_ts(msg)

                        # 时间范围过滤
                        if start_dt and msg_dt and msg_dt < start_dt:
                            continue
                        if end_dt and msg_dt and msg_dt > end_dt:
                            continue

                        # 多关键词过滤（OR 逻辑，任一匹配即返回）
                        if all_keywords:
                            content_lower = raw_content.lower()
                            matched = any(kw.lower() in content_lower for kw in all_keywords)
                            if not matched:
                                continue

                        # 内容截断
                        display_content = raw_content if show_full else raw_content[:40]

                        # 构建元数据字符串
                        meta_parts = []
                        if msg_id_field:
                            meta_parts.append(f"msg_id={msg_id_field}")
                        if msg_idx:
                            meta_parts.append(f"msg_idx={msg_idx}")
                        if ref_idx:
                            meta_parts.append(f"ref_msg_idx={ref_idx}")
                        # 发送者信息取自结构化字段（正文里不再含 "昵称(id): " 前缀）
                        sender = msg.get("username") or ""
                        sender_id = msg.get("user_id") or ""
                        if sender or sender_id:
                            meta_parts.append(f"from={sender}({sender_id})" if sender_id else f"from={sender}")
                        meta_str = f" ({', '.join(meta_parts)})" if meta_parts else ""

                        # 时间标签
                        if msg_dt:
                            meta_str = f" [{msg_dt.strftime('%Y-%m-%d %H:%M')}]" + meta_str

                        lines.append(f"{idx}. [{role}]{meta_str}\n{display_content}")

                    if not lines:
                        reason_parts = []
                        if all_keywords:
                            reason_parts.append(f"关键词「{'、'.join(all_keywords)}」")
                        if start_dt or end_dt:
                            time_range = f"{start_time_str or '不限'} ~ {end_time_str or '不限'}"
                            reason_parts.append(f"时间范围「{time_range}」")
                        if reason_parts:
                            result_content = f"未找到匹配{'且'.join(reason_parts)}的消息（共扫描 {len(recent)} 条）"
                        else:
                            result_content = "当前会话暂无聊天记录"
                    else:
                        header = f"会话 {q_thread_key} 的聊天记录（最近 {range_count} 条中匹配 {len(lines)} 条）："
                        if all_keywords:
                            header = f"会话 {q_thread_key} 含关键词「{'、'.join(all_keywords)}」的消息（共 {len(lines)} 条）："
                        result_content = header + "\n" + "\n".join(lines)

            except Exception as e:
                result_content = f"查询历史记录异常：{e}"

    elif function_name == "revoke_message":
        # 统一为数组：只接受 message_ids 数组形式
        message_ids = arguments.get("message_ids", [])
        if isinstance(message_ids, str):
            message_ids = [message_ids] if message_ids.strip() else []
        elif not isinstance(message_ids, list):
            message_ids = [message_ids] if message_ids else []
        if not message_ids or not isinstance(message_ids, list):
            result_content = "错误：message_ids 必须为非空列表"
        else:
            # 解析跨机器人参数
            revoke_app_id = arguments.get("app_id", "").strip()
            # 确定目标类型和ID
            t_type = arguments.get("target_type", msg_type or "group")  # 默认 group
            t_id = arguments.get("target_id", "")
            if not t_id:
                if t_type == "c2c":
                    t_id = recipient_id
                else:
                    t_id = recipient_id or group_id
            if not t_id:
                result_content = "错误：无法确定消息所属的目标ID，请指定 target_id 或在当前会话中使用"
            else:
                # 解析使用的 BotClient（跨机器人或当前）
                try:
                    sender, is_cross = _ensure_bot_client(revoke_app_id, bot_client)
                except (PermissionError, ValueError) as e:
                    result_content = f"错误：{e}"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}

                # 构建线程key（用于本地标记已撤回）
                revoke_thread_key = f"{'c2c' if t_type == 'c2c' else 'group'}_{t_id}"

                results = []
                for mid in message_ids:
                    if not mid or not isinstance(mid, str):
                        results.append(f"  ✗ 无效消息ID: {mid}")
                        continue
                    try:
                        ok = await sender.revoke_message(t_type, t_id, mid)
                        if ok:
                            # 在聊天记录中标记为已撤回（仅限本地机器人的记录）
                            if revoke_thread_key:
                                mark_message_revoked(revoke_thread_key, mid)
                            results.append(f"  ✓ {mid[:40]}... 撤回成功（已标记为已撤回）")
                        else:
                            results.append(f"  ✗ {mid[:40]}... 撤回失败（可能超时或无权限）")
                    except Exception as e:
                        results.append(f"  ✗ {mid[:40]}... 撤回异常: {e}")
                result_content = f"撤回结果（共 {len(message_ids)} 条）：\n" + "\n".join(results)

    # ==================== 按关键词和时间范围批量撤回 ====================
    elif function_name == "batch_revoke":
        keywords = arguments.get("keywords", [])
        start_time_str = arguments.get("start_time", "")
        end_time_str = arguments.get("end_time", "")
        # 安全默认：只有显式传 confirm=1 才真正执行，否则一律预览。
        # 兼容旧参数 dry_run=1（显式传 1 时同样只预览）。
        confirm = int(arguments.get("confirm", 0) or 0)
        legacy_dry = int(arguments.get("dry_run", 0) or 0)
        dry_run = 1 if (confirm != 1 or legacy_dry == 1) else 0
        # keywords 与时间范围至少要有一个，否则等于"无条件撤整个会话历史"
        raw_keywords = [k.strip() for k in keywords if k and str(k).strip()] if isinstance(keywords, list) else []
        if not raw_keywords and not start_time_str and not end_time_str:
            result_content = ("错误：batch_revoke 至少需要指定 keywords 或时间范围（start_time/end_time）之一，"
                              "以免无条件撤回整个会话的历史消息")
        else:
            keywords = raw_keywords
            # 解析跨机器人参数
            batch_app_id = arguments.get("app_id", "").strip()
            # 确定目标类型和ID
            t_type = arguments.get("target_type", msg_type or "group")
            t_id = arguments.get("target_id", "")
            if not t_id:
                if t_type == "c2c":
                    t_id = recipient_id
                else:
                    t_id = recipient_id or group_id
            if not t_id:
                result_content = "错误：无法确定消息所属的目标ID，请指定 target_id 或在当前会话中使用"
            else:
                # 解析使用的 BotClient（跨机器人或当前）
                try:
                    sender, is_cross = _ensure_bot_client(batch_app_id, bot_client)
                except (PermissionError, ValueError) as e:
                    result_content = f"错误：{e}"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}

                # 构建线程key
                thread_key_target = f"{'c2c' if t_type == 'c2c' else 'group'}_{t_id}"

                # 解析时间范围
                beijing_tz = timezone(timedelta(hours=8))
                start_dt = None
                end_dt = None
                if start_time_str:
                    try:
                        start_dt = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        start_dt = start_dt.astimezone(beijing_tz)
                    except Exception as e:
                        result_content = f"错误：start_time 格式无效 '{start_time_str}': {e}"
                        return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                if end_time_str:
                    try:
                        end_dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                        end_dt = end_dt.astimezone(beijing_tz)
                    except Exception as e:
                        result_content = f"错误：end_time 格式无效 '{end_time_str}': {e}"
                        return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}

                try:
                    hist = load_history(thread_key_target)
                    if not hist:
                        result_content = f"会话 {thread_key_target} 暂无聊天记录"
                    else:
                        matched_messages = []

                        for msg in hist:
                            role = msg.get("role", "")
                            content = msg.get("content", "")
                            msg_id_val = msg.get("msg_id", "")
                            if not msg_id_val:
                                continue
                            if msg.get("revoked"):
                                continue
                            if content.startswith("[已撤回]"):
                                continue

                            # 时间取结构化 ts 字段（旧记录由 load_history 归一后同样可用）；
                            # content 已是纯正文，无需再剥时间戳前缀。
                            msg_dt = parse_history_ts(msg)

                            # 时间范围过滤。
                            # 注意：历史记录里少数条目（如工具调用记录）没有时间信息，
                            # 此时 msg_dt 为 None，无法判断时间。
                            # 这类条目在指定了时间范围时一律排除 —— 否则"撤今天上午的消息"
                            # 会把无时间的旧消息也一起撤掉。
                            if start_dt or end_dt:
                                if msg_dt is None:
                                    continue
                                if start_dt and msg_dt < start_dt:
                                    continue
                                if end_dt and msg_dt > end_dt:
                                    continue

                            content_lower = content.lower()
                            matched_keyword = None
                            for kw in keywords:
                                if kw.lower() in content_lower:
                                    matched_keyword = kw
                                    break

                            # keywords 为空（纯时间范围撤回）时，时间命中即算匹配
                            if matched_keyword or not keywords:
                                matched_messages.append({
                                    "msg_id": msg_id_val,
                                    "content_preview": content[:60],
                                    "keyword": matched_keyword,
                                    "timestamp": msg_dt,
                                    "role": role,
                                })

                        # 匹配条件描述（供输出文案使用）
                        if keywords:
                            cond_desc = f"关键词: {keywords}"
                            if start_time_str or end_time_str:
                                cond_desc += f"，时间: {start_time_str or '(不限)'} ~ {end_time_str or '(不限)'}"
                        else:
                            cond_desc = f"时间范围: {start_time_str or '(不限)'} ~ {end_time_str or '(不限)'}"

                        if not matched_messages:
                            result_content = f"未找到匹配的消息（{cond_desc}）"
                        elif dry_run:
                            lines = [f"【预览模式】找到 {len(matched_messages)} 条匹配的消息（不会实际撤回）："]
                            for i, m in enumerate(matched_messages, 1):
                                ts_str = m["timestamp"].strftime("%Y-%m-%d %H:%M") if m["timestamp"] else "未知时间"
                                hit_desc = f"匹配关键词「{m['keyword']}」" if m["keyword"] else "时间范围内"
                                lines.append(f"  {i}. [{m['role']}] {ts_str} {hit_desc} msg_id={m['msg_id']}")
                                lines.append(f"     内容: {m['content_preview']}")
                            lines.append("\n这是预览结果，尚未撤回任何消息。"
                                         "确认无误后，请带 confirm=1 再次调用以真正执行撤回。")
                            result_content = "\n".join(lines)
                        else:
                            results = []
                            success_count = 0
                            fail_count = 0
                            for m in matched_messages:
                                mid = m["msg_id"]
                                hit_desc = f"关键词: {m['keyword']}" if m["keyword"] else "时间范围"
                                try:
                                    ok = await sender.revoke_message(t_type, t_id, mid)
                                    if ok:
                                        if thread_key_target:
                                            mark_message_revoked(thread_key_target, mid)
                                        results.append(f"  ✓ {mid[:30]}... 撤回成功（{hit_desc}）")
                                        success_count += 1
                                    else:
                                        results.append(f"  ✗ {mid[:30]}... 撤回失败（可能超时或无权限）")
                                        fail_count += 1
                                except Exception as e:
                                    results.append(f"  ✗ {mid[:30]}... 撤回异常: {e}")
                                    fail_count += 1

                            result_content = f"批量撤回完成：成功 {success_count}，失败 {fail_count}（共 {len(matched_messages)} 条匹配）\n" + "\n".join(results)

                except Exception as e:
                    result_content = f"批量撤回异常：{e}"

    # ==================== 获取目标列表（合并推送/唤醒） ====================
    elif function_name == "get_targets":
        try:
            user_map = load_user_map()
            mirror = load_mirror()
            all_bots = get_bots()
            caller_app_id = bot_client.app_id

            filter_appid = arguments.get("appid", "").strip()
            search_keyword = arguments.get("search", "").strip().lower()
            mode = arguments.get("mode", "all")  # all / push / wakeup

            # 实时读取调用者权限
            caller_allow_push = get_bot_allow_cross_push(caller_app_id)
            caller_allow_wakeup = get_bot_allow_cross_wakeup(caller_app_id)

            lines = ["所有机器人已知的用户和群列表（每项标注推送/唤醒权限）："]
            lines.append("注解说明：")
            lines.append("  ✅=可操作  ❌=配置禁止  ⚠️=不一定可操作  🔒=需通过群+@用户")
            if mode == "push":
                lines.append("（当前 mode=push，仅关注推送权限）")
            elif mode == "wakeup":
                lines.append("（当前 mode=wakeup，仅关注唤醒权限）")

            for bot in all_bots:
                bid = bot.get("APP_ID", "")
                if not bid:
                    continue
                if filter_appid and bid != filter_appid:
                    continue

                # 判断各权限
                own_bot = (bid == caller_app_id)
                can_push_flag = own_bot or (caller_allow_push and get_bot_allow_cross_push_incoming(bid))
                can_wakeup_flag = own_bot or (caller_allow_wakeup and get_bot_allow_cross_wakeup_incoming(bid))

                # 跨机器人权限阻断检查
                if not own_bot:
                    skip_bot = False
                    if mode in ("push", "all") and not caller_allow_push:
                        lines.append(f"\n{'='*50}")
                        lines.append(f"机器人 APP_ID: {bid}  ❌ 本机器人 ALLOW_CROSS_BOT_PUSH=0，无法跨机器人查看推送目标")
                        skip_bot = True
                    elif mode in ("push", "all") and not get_bot_allow_cross_push_incoming(bid):
                        lines.append(f"\n{'='*50}")
                        lines.append(f"机器人 APP_ID: {bid}  ❌ 目标机器人 ALLOW_CROSS_BOT_PUSH_INCOMING=0，无法跨机器人查看推送目标")
                        skip_bot = True
                    if mode in ("wakeup", "all") and not caller_allow_wakeup:
                        if skip_bot:
                            continue
                        lines.append(f"\n{'='*50}")
                        lines.append(f"机器人 APP_ID: {bid}  ❌ 本机器人 ALLOW_CROSS_BOT_WAKEUP=0，无法跨机器人查看唤醒目标")
                        skip_bot = True
                    elif mode in ("wakeup", "all") and not get_bot_allow_cross_wakeup_incoming(bid):
                        if skip_bot:
                            continue
                        lines.append(f"\n{'='*50}")
                        lines.append(f"机器人 APP_ID: {bid}  ❌ 目标机器人 ALLOW_CROSS_BOT_WAKEUP_INCOMING=0，无法跨机器人查看唤醒目标")
                        skip_bot = True
                    if skip_bot:
                        continue

                user_names = mirror.get("users", {}).get(bid, {})
                group_names = mirror.get("groups", {}).get(bid, {})
                bot_user_ids = user_map.get(bid, {}).get("user", [])
                bot_group_ids_raw = user_map.get(bid, {}).get("group", {})

                # 兼容处理：旧格式可能是列表，新格式是 dict
                if isinstance(bot_group_ids_raw, list):
                    bot_group_ids = {gid: [] for gid in bot_group_ids_raw}
                elif isinstance(bot_group_ids_raw, dict):
                    bot_group_ids = bot_group_ids_raw
                else:
                    bot_group_ids = {}

                # 收集群成员ID
                all_group_member_ids = set()
                for member_ids in bot_group_ids.values():
                    all_group_member_ids.update(member_ids)

                # 构建权限标签
                def _tag(can_push, can_wakeup, is_extra=False, is_group_member=False):
                    parts = []
                    if is_extra:
                        return "  ⚠️不一定可推送/唤醒（仅有名称缓存）"
                    if is_group_member:
                        return "  🔒不可直接操作，可通过群+@用户实现"
                    if mode == "push":
                        parts.append("✅可推送" if can_push else "❌不可推送")
                    elif mode == "wakeup":
                        parts.append("✅可唤醒" if can_wakeup else "❌不可唤醒")
                    else:
                        pu = "✅推" if can_push else "❌推"
                        wu = "✅唤" if can_wakeup else "❌唤"
                        parts.append(f"{pu}/{wu}")
                    return "  " + " ".join(parts)

                lines.append(f"\n{'='*50}")
                lines.append(f"机器人 APP_ID: {bid}")

                # ---------- 私聊用户 ----------
                if bot_user_ids:
                    displayed = 0
                    lines.append(f"  -- 私聊用户（{len(bot_user_ids)} 个） --")
                    for uid in bot_user_ids:
                        name = user_names.get(uid, "（未知名称）")
                        label = f"{name} | id={uid} | app_id={bid}"
                        if search_keyword and search_keyword not in label.lower():
                            continue
                        displayed += 1
                        lines.append(f"  [用户] {label}{_tag(can_push_flag, can_wakeup_flag)}")
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")
                else:
                    lines.append("  -- 私聊用户：无记录 --")

                # ---------- 群（含群成员） ----------
                if bot_group_ids:
                    displayed = 0
                    lines.append(f"  -- 群（{len(bot_group_ids)} 个） --")
                    for gid, member_ids in bot_group_ids.items():
                        gname = group_names.get(gid, "（未知名称）")
                        glabel = f"{gname} | id={gid} | app_id={bid}"
                        g_match = not search_keyword or search_keyword in glabel.lower()
                        if g_match:
                            displayed += 1
                            lines.append(f"  [群] {glabel}{_tag(can_push_flag, can_wakeup_flag)}")
                        for mid in member_ids:
                            mname = user_names.get(mid, "（未知名称）")
                            mlabel = f"    [成员] {mname} | id={mid}"
                            if search_keyword and search_keyword not in mlabel.lower() and not g_match:
                                continue
                            if g_match:
                                lines.append(f"{mlabel}{_tag(False, False, is_group_member=True)}")
                            else:
                                displayed += 1
                                lines.append(f"  [成员] {mname} | id={mid} | app_id={bid}{_tag(False, False, is_group_member=True)}")
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")
                else:
                    lines.append("  -- 群：无记录 --")

                # ---------- 缓存用户 ----------
                extra_users = [uid for uid in user_names if uid not in bot_user_ids and uid not in all_group_member_ids]
                extra_groups = [gid for gid in group_names if gid not in bot_group_ids]
                if extra_users:
                    displayed = 0
                    lines.append("  （以下用户有名称缓存但未归入任何群或私聊）")
                    for uid in extra_users:
                        name = user_names[uid]
                        label = f"  [用户] {name} | id={uid} | app_id={bid}"
                        if search_keyword and search_keyword not in label.lower():
                            continue
                        displayed += 1
                        lines.append(f"{label}{_tag(False, False, is_extra=True)}")
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")
                if extra_groups:
                    displayed = 0
                    lines.append("  （以下群有名称缓存但未在 user_map 中）")
                    for gid in extra_groups:
                        name = group_names[gid]
                        label = f"  [群] {name} | id={gid} | app_id={bid}"
                        if search_keyword and search_keyword not in label.lower():
                            continue
                        displayed += 1
                        lines.append(f"{label}{_tag(can_push_flag, can_wakeup_flag)}")
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")

            lines.append(f"\n{'='*50}")
            if mode == "push":
                lines.append('将选中的目标按格式填入 push_message 的 targets 参数：')
                lines.append('[{"type": "user", "id": "...", "app_id": "..."}, {"type": "group", "id": "...", "app_id": "..."}]')
            elif mode == "wakeup":
                lines.append("将选中的目标填入 create_scheduled_wakeup 的 target 参数：")
                lines.append('{"type": "user", "id": "...", "app_id": "..."}')
                lines.append('或 {"type": "group", "id": "...", "app_id": "..."}')
            else:
                lines.append("推送用法 → 将目标按格式填入 push_message 的 targets 参数：")
                lines.append('[{"type": "user", "id": "...", "app_id": "..."}, {"type": "group", "id": "...", "app_id": "..."}]')
                lines.append("唤醒用法 → 将目标填入 create_scheduled_wakeup 的 target 参数：")
                lines.append('{"type": "user", "id": "...", "app_id": "..."}')
                lines.append('或 {"type": "group", "id": "...", "app_id": "..."}')
            result_content = "\n".join(lines)
        except Exception as e:
            result_content = f"获取目标列表异常：{e}"

    # ==================== 获取联系人列表（不含权限标注） ====================
    elif function_name == "get_contact_list":
        try:
            user_map = load_user_map()
            mirror = load_mirror()
            all_bots = get_bots()
            caller_app_id = bot_client.app_id

            filter_appid = arguments.get("appid", "").strip()
            search_keyword = arguments.get("search", "").strip().lower()

            # 实时读取跨机器人列表获取权限
            caller_allow_cross = get_bot_allow_cross_get_list(caller_app_id)

            lines = ["所有机器人的用户和群列表（含已知群成员）："]

            for bot in all_bots:
                bid = bot.get("APP_ID", "")
                if not bid:
                    continue
                if filter_appid and bid != filter_appid:
                    continue

                # 跨机器人权限检查
                if bid != caller_app_id:
                    if not caller_allow_cross:
                        lines.append(f"\n{'='*50}")
                        lines.append(f"机器人 APP_ID: {bid}  ❌ 本机器人 ALLOW_CROSS_BOT_GET_LIST=0，无法获取跨机器人列表")
                        continue
                    if not get_bot_allow_cross_get_list_incoming(bid):
                        lines.append(f"\n{'='*50}")
                        lines.append(f"机器人 APP_ID: {bid}  ❌ 目标机器人 ALLOW_CROSS_BOT_GET_LIST_INCOMING=0，无法被跨机器人获取列表")
                        continue

                user_names = mirror.get("users", {}).get(bid, {})
                group_names = mirror.get("groups", {}).get(bid, {})
                bot_user_ids = user_map.get(bid, {}).get("user", [])
                bot_group_ids_raw = user_map.get(bid, {}).get("group", {})

                if isinstance(bot_group_ids_raw, list):
                    bot_group_ids = {gid: [] for gid in bot_group_ids_raw}
                elif isinstance(bot_group_ids_raw, dict):
                    bot_group_ids = bot_group_ids_raw
                else:
                    bot_group_ids = {}

                lines.append(f"\n{'='*50}")
                lines.append(f"机器人 APP_ID: {bid}")

                # 私聊用户
                if bot_user_ids:
                    displayed = 0
                    lines.append(f"  -- 私聊用户（{len(bot_user_ids)} 个） --")
                    for uid in bot_user_ids:
                        name = user_names.get(uid, "（未知名称）")
                        label = f"  [用户] {name} | id={uid}"
                        if search_keyword and search_keyword not in label.lower():
                            continue
                        displayed += 1
                        lines.append(label)
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")
                else:
                    lines.append("  -- 私聊用户：无记录 --")

                # 群（含成员）
                if bot_group_ids:
                    displayed = 0
                    lines.append(f"  -- 群（{len(bot_group_ids)} 个） --")
                    for gid, member_ids in bot_group_ids.items():
                        gname = group_names.get(gid, "（未知名称）")
                        glabel = f"  [群] {gname} | id={gid}"
                        g_match = not search_keyword or search_keyword in glabel.lower()
                        if g_match:
                            displayed += 1
                            lines.append(glabel)
                        for mid in member_ids:
                            mname = user_names.get(mid, "（未知名称）")
                            mlabel = f"    [成员] {mname} | id={mid}"
                            if search_keyword and search_keyword not in mlabel.lower() and not g_match:
                                continue
                            if g_match:
                                lines.append(mlabel)
                            else:
                                displayed += 1
                                lines.append(f"  [成员] {mname} | id={mid}")
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")
                else:
                    lines.append("  -- 群：无记录 --")

                # 缓存用户/群
                all_group_member_ids = set()
                for member_ids in bot_group_ids.values():
                    all_group_member_ids.update(member_ids)
                extra_users = [uid for uid in user_names if uid not in bot_user_ids and uid not in all_group_member_ids]
                extra_groups = [gid for gid in group_names if gid not in bot_group_ids]
                if extra_users:
                    displayed = 0
                    lines.append("  （以下用户有名称缓存但未归入任何群或私聊）")
                    for uid in extra_users:
                        label = f"  [用户] {user_names[uid]} | id={uid}"
                        if search_keyword and search_keyword not in label.lower():
                            continue
                        displayed += 1
                        lines.append(label)
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")
                if extra_groups:
                    displayed = 0
                    lines.append("  （以下群有名称缓存但未在 user_map 中）")
                    for gid in extra_groups:
                        label = f"  [群] {group_names[gid]} | id={gid}"
                        if search_keyword and search_keyword not in label.lower():
                            continue
                        displayed += 1
                        lines.append(label)
                    if search_keyword and displayed == 0:
                        lines.append("    （无匹配）")

            result_content = "\n".join(lines)
        except Exception as e:
            result_content = f"获取联系人列表异常：{e}"

    # ==================== 主动推送消息（含跨机器人支持 + 多媒体） ====================
    elif function_name == "push_message":
        # 格式与正文统一解析（content 为准，兼容 markdown_content/text_content）
        use_md, body_content = parse_message_type(arguments)

        targets = arguments.get("targets", [])
        media_source = arguments.get("media_source")
        file_type = arguments.get("file_type")
        file_name = arguments.get("file_name")
        if not body_content or not str(body_content).strip():
            result_content = "错误：push_message 缺少 content 参数"
        elif not targets:
            result_content = "错误：targets 为空，请至少指定一个目标"
        else:
            try:
                caller_app_id = bot_client.app_id

                # 构建发起者标注
                mirror = load_mirror()
                if msg_type == "c2c":
                    sender_name = get_user_name(recipient_id, caller_app_id) or recipient_id
                    footer = f"\n\n—— 来自 {sender_name}（用户 {recipient_id}）发起的推送"
                elif msg_type == "group":
                    group_name = get_group_name_from_mirror(caller_app_id, recipient_id) or recipient_id
                    sender_id = author_id or "未知"
                    sender_name = get_user_name(author_id, caller_app_id) or sender_id
                    footer = f"\n\n—— 来自 群 {group_name}（{recipient_id}）的 {sender_name}（用户 {sender_id}）发起的推送"
                else:
                    footer = "\n\n—— 来自系统发起的推送"

                full_content = body_content + footer

                # 预加载所有 bot 凭证（用于跨机器人推送）
                all_bots = {b["APP_ID"]: b["APP_SECRET"] for b in get_bots() if b.get("APP_ID")}

                success_count = 0
                fail_count = 0
                detail_msgs = []
                all_target_msg_ids = []  # 记录所有消息ID

                # 跨机器人 BotClient 缓存，避免重复获取 token
                cross_clients = {}

                for t in targets:
                    t_type = t.get("type", "")      # "user" 或 "group"
                    t_id = t.get("id", "")
                    t_app_id = t.get("app_id", "")

                    if not t_id:
                        detail_msgs.append(f"  跳过：目标 id 为空")
                        fail_count += 1
                        continue
                    if t_type not in ("user", "group"):
                        detail_msgs.append(f"  跳过 {t_id}：type 必须为 user 或 group，收到 '{t_type}'")
                        fail_count += 1
                        continue

                    # 确定使用哪个 bot_client 发送
                    if t_app_id == caller_app_id:
                        sender = bot_client
                    else:
                        if not get_bot_allow_cross_push(caller_app_id):
                            detail_msgs.append(f"  跳过 {t_id}：本机器人（{caller_app_id}）禁止跨机器人推送")
                            fail_count += 1
                            continue
                        if not get_bot_allow_cross_push_incoming(t_app_id):
                            detail_msgs.append(f"  跳过 {t_id}：目标机器人（{t_app_id}）禁止接收跨机器人推送")
                            fail_count += 1
                            continue
                        if t_app_id not in cross_clients:
                            secret = all_bots.get(t_app_id)
                            if not secret:
                                detail_msgs.append(f"  跳过 {t_id}：未找到目标机器人 {t_app_id} 的凭证")
                                fail_count += 1
                                continue
                            cross_clients[t_app_id] = BotClient(t_app_id, secret)
                        sender = cross_clients[t_app_id]

                    api_msg_type = "c2c" if t_type == "user" else t_type
                    hist_key = f"{'c2c' if t_type == 'user' else 'group'}_{t_id}"
                    target_msg_ids = []
                    target_error = ""

                    # 双发策略与发送编排统一由 client.send_with_policy 处理。
                    # 传裸正文 body_content，footer 由 helper 统一追加。
                    sent = await send_with_policy(
                        sender, api_msg_type, t_id, body_content,
                        media_source=media_source, file_type=file_type, file_name=file_name,
                        markdown=body_content if use_md else None,
                        footer=footer,
                    )
                    target_msg_ids.extend(sent["message_ids"])
                    for _m in sent["message_ids"]:
                        if _m["type"] == "media":
                            detail_msgs.append(f"  ✓ {t_type} {t_id} 媒体消息ID: {_m['msg_id'][:30]}...")
                        else:
                            detail_msgs.append(f"  ✓ {t_type} {t_id} {_m['type']}消息ID: {_m['msg_id'][:30]}...")
                    if sent["ok"]:
                        success_count += 1
                    else:
                        fail_count += 1
                        target_error = sent["error"] or "发送失败"
                        detail_msgs.append(f"  ✗ {t_type} {t_id}（app_id={t_app_id}）{target_error}")

                    target_ok = bool(target_msg_ids)
                    all_target_msg_ids.append({
                        "type": t_type, "id": t_id, "app_id": t_app_id,
                        "ok": target_ok, "error": target_error,
                        "message_ids": target_msg_ids
                    })
                    if target_ok:
                        # 逐条写入历史并带上各自的 msg_id/msg_idx，供撤回与 API 查询使用
                        # 媒体条用「媒体：<正文>（<URL>）」，文本条只带正文，两者分开记录
                        if sent["message_ids"]:
                            for _m in sent["message_ids"]:
                                _is_media = _m.get("type") in ("media", "media_combined")
                                _txt = (f"媒体：{body_content}（{media_source}）"
                                        if _is_media else full_content)
                                append_message(hist_key, "assistant", _txt,
                                               msg_id=_m.get("msg_id"),
                                               msg_idx=_m.get("msg_idx"),
                                               is_markdown=bool(use_md) and not _is_media,
                                               media_url=media_source if _is_media else None)
                        else:
                            append_message(hist_key, "assistant", full_content,
                                           is_markdown=bool(use_md))
                    await asyncio.sleep(0.3)

                total = len(targets)

                # ★ 先保存推送记录（获取 task_id），再组织返回内容
                push_task_id = None
                try:
                    from scheduler import load_tasks, save_tasks
                    import uuid
                    push_tasks = load_tasks()
                    push_task_id = str(uuid.uuid4())[:8]
                    task_record = {
                        "task_id": push_task_id,
                        "app_id": caller_app_id,
                        "content": body_content,
                        "targets": targets,
                        "schedule_type": "one_time",
                        "schedule_time": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
                        # 这是「立即发送」的记录，结果已出，按真实成败落规范状态。
                        # 之前无条件写 completed，导致发送失败也显示「执行完成」。
                        "status": ("completed" if (total > 0 and success_count > 0 and fail_count == 0)
                                   else "failed"),
                        "created_at": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
                        "initiator_info": {
                            "app_id": caller_app_id,
                            "type": msg_type or "system",
                            "id": recipient_id or "",
                            "name": get_user_name(recipient_id, caller_app_id) if recipient_id else "系统",
                        },
                        "result": {
                            "success_count": success_count,
                            "fail_count": fail_count,
                            "total": total,
                            "details": all_target_msg_ids,
                        },
                        "execution_history": [{
                            "time": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
                            # 执行历史同样只使用规范状态，界面徽章才能统一着色。
                            # 必须同时看 success_count：发不出去时 fail_count 也是 0。
                            "status": "completed" if (total > 0 and success_count > 0 and fail_count == 0) else "failed",
                            "success_count": success_count,
                            "fail_count": fail_count,
                            "total": total,
                            "target_results": [
                                {"type": d.get("type",""), "id": d.get("id",""), "ok": d.get("ok",False), "error": d.get("error","")}
                                for d in all_target_msg_ids
                            ],
                        }],
                    }
                    if media_source:
                        media_info = {"source": media_source}
                        if file_type is not None:
                            media_info["file_type"] = file_type
                        if file_name:
                            media_info["file_name"] = file_name
                        task_record["media"] = media_info
                    push_tasks.append(task_record)
                    save_tasks(push_tasks)
                    info(f"[推送记录] 已保存立即推送记录 task_id={push_task_id}", ctx=ctx)
                except Exception as save_err:
                    error(f"[推送记录] 保存失败: {save_err}", ctx=ctx)

                result_msg = f"推送完成：成功 {success_count}，失败 {fail_count}（共 {total} 个目标）"
                if push_task_id:
                    result_msg += f"\n任务ID: {push_task_id}"
                if media_source:
                    result_msg += f"\n媒体URL: {media_source}"
                if detail_msgs:
                    result_content = result_msg + "\n" + "\n".join(detail_msgs)
                else:
                    result_content = result_msg

            except Exception as e:
                result_content = f"推送消息异常：{e}"

    # ==================== 定时推送 ====================
    elif function_name == "schedule_push":
        # 格式与正文统一解析（content 为准，兼容 markdown_content/text_content）
        use_md, body = parse_message_type(arguments)
        targets = arguments.get("targets", [])
        schedule_type = arguments.get("schedule_type", "one_time")
        schedule_time = arguments.get("schedule_time", "")
        interval_seconds = arguments.get("interval_seconds")
        media_source = arguments.get("media_source")
        file_type = arguments.get("file_type")
        file_name = arguments.get("file_name")
        if not body or not str(body).strip():
            result_content = "错误：schedule_push 缺少 content 参数"
        elif not targets:
            result_content = "错误：targets 为空"
        elif schedule_type in ("one_time", "daily") and not schedule_time:
            result_content = "错误：schedule_push 的 schedule_type 为 one_time 或 daily 时缺少 schedule_time 参数"
        else:
            try:
                # 记录发起者信息，定时执行时用其构建 footer
                initiator_info = {"app_id": bot_client.app_id, "created_at": None}
                if msg_type == "c2c":
                    initiator_info["type"] = "user"
                    initiator_info["id"] = recipient_id
                    initiator_info["name"] = get_user_name(recipient_id, bot_client.app_id) or recipient_id
                elif msg_type == "group":
                    initiator_info["type"] = "group"
                    initiator_info["id"] = author_id or ""
                    initiator_info["name"] = get_user_name(author_id, bot_client.app_id) or author_id or "未知"
                    initiator_info["group_id"] = recipient_id
                    initiator_info["group_name"] = get_group_name_from_mirror(bot_client.app_id, recipient_id) or recipient_id
                else:
                    initiator_info["type"] = "system"
                    initiator_info["id"] = ""
                    initiator_info["name"] = "系统"

                task_data = {
                    "app_id": bot_client.app_id,
                    "content": body,
                    "targets": targets,
                    "schedule_type": schedule_type,
                    "schedule_time": schedule_time,
                    "initiator_info": initiator_info,
                }
                # Markdown 推送：记录格式与正文，执行时按富文本发送
                if use_md:
                    task_data["message_type"] = "markdown"
                    task_data["markdown"] = body
                    task_data["is_markdown"] = 1
                if schedule_type == "interval" and interval_seconds is not None:
                    task_data["interval_seconds"] = interval_seconds
                if media_source:
                    media = {"source": media_source}
                    if file_type is not None:
                        media["file_type"] = file_type
                    if file_name:
                        media["file_name"] = file_name
                    task_data["media"] = media

                task_id = add_task(task_data)
                schedule_desc = f"计划执行时间: {schedule_time}"
                if schedule_type == "daily":
                    schedule_desc = f"每天 {schedule_time} 执行"
                elif schedule_type == "interval":
                    iv = interval_seconds or 3600
                    schedule_desc = f"每 {iv} 秒执行一次"
                result_content = f"定时推送已创建，任务 ID: {task_id}，类型: {schedule_type}，{schedule_desc}，共 {len(targets)} 个目标"
            except Exception as e:
                result_content = f"创建定时推送异常：{e}"

    # ==================== 查看定时推送列表（含全部状态、调度类型、执行记录和消息ID） ====================
    elif function_name == "list_scheduled_push":
        try:
            all_tasks = list_tasks(bot_client.app_id)
            # 先算全量状态分布，供无结果时给出提示
            all_counts = _task_counts(all_tasks)

            # 按传入条件过滤（状态 / 关键词 / 时间 / 目标 …）
            filter_args = {k: v for k, v in arguments.items()
                           if k in _LIST_FILTER_KEYS and v not in (None, "", [], {})}
            tasks, fmeta = _filter_tasks(all_tasks, filter_args)
            fdesc = _describe_filters(fmeta)

            if not all_tasks:
                result_content = "当前没有定时推送任务"
            elif not tasks:
                result_content = (
                    f"没有符合条件的定时推送任务（共 {len(all_tasks)} 个，"
                    f"状态分布：{_status_summary_text(all_counts)}）"
                    + (f"\n筛选条件：{fdesc}" if fdesc else "")
                    + "\n可放宽或去掉筛选条件后重试。"
                )
            else:
                header = f"定时推送任务列表（{len(tasks)}/{len(all_tasks)} 个"
                header += f"，状态分布：{_status_summary_text(all_counts)}）"
                if fdesc:
                    header += f"\n筛选条件：{fdesc}"
                header += "："
                lines = [header]
                for t in tasks:
                    tid = t.get("task_id", "?")
                    status = _TASK_STATUS_LABELS.get(task_status_of(t), "?")
                    schedule_type = t.get("schedule_type", "one_time")
                    app = t.get("app_id", "?")
                    sched_time = t.get("schedule_time", "")
                    interval_sec = t.get("interval_seconds")
                    content_full = t.get("content", "")
                    created_at = t.get("created_at", "?")

                    lines.append(f"\n{'='*55}")
                    lines.append(f"  [{status}] 任务ID: {tid}")
                    lines.append(f"  机器人: {app}")
                    lines.append(f"  创建时间: {created_at}")

                    # 调度信息
                    if schedule_type == "one_time":
                        lines.append(f"  调度类型: 一次性")
                        lines.append(f"  执行时间: {sched_time or '（立即）'}")
                    elif schedule_type == "daily":
                        lines.append(f"  调度类型: 每天")
                        lines.append(f"  每天时间: {sched_time}")
                    elif schedule_type == "interval":
                        iv = interval_sec or 3600
                        lines.append(f"  调度类型: 间隔执行")
                        lines.append(f"  间隔秒数: {iv} 秒（每 {iv//3600 if iv>=3600 else iv} {'小时' if iv>=3600 else '秒'}）")
                        lines.append(f"  首次时间: {sched_time or '（立即开始）'}")
                    else:
                        lines.append(f"  调度类型: {schedule_type}")

                    # 媒体信息
                    media = t.get("media")
                    if media:
                        mt = file_type_name(media.get("file_type"))
                        fn = media.get("file_name", "")
                        lines.append(f"  媒体: {mt} {fn}" if fn else f"  媒体: {mt}")
                    
                    # 目标列表（全部显示）
                    targets = t.get("targets", [])
                    lines.append(f"  目标（{len(targets)} 个）:")
                    for tg in targets:
                        tg_type = tg.get("type", "")
                        tg_id = tg.get("id", "")
                        tg_app = tg.get("app_id", app)
                        if tg_type == "user":
                            tg_name = get_user_name(tg_id, tg_app) or "（未知）"
                            lines.append(f"    · {tg_name}（{tg_id}）")
                        elif tg_type == "group":
                            tg_name = get_group_name_from_mirror(tg_app, tg_id) or "（未知）"
                            lines.append(f"    · 群 {tg_name}（{tg_id}）")
                    
                    # 内容
                    lines.append(f"  内容: {content_full[:100]}{'...' if len(content_full) > 100 else ''}")

                    # 执行结果和消息ID（含撤回状态实时判断）
                    result = t.get("result")
                    if result:
                        sc = result.get("success_count", 0)
                        fc = result.get("fail_count", 0)
                        total = result.get("total", 0)
                        lines.append(f"  结果: 成功 {sc} / 失败 {fc}（共 {total}）")
                        
                        # 计算参考发送时间（用于判断撤回窗口）
                        ref_send_time = None
                        exec_hist = t.get("execution_history", [])
                        if exec_hist:
                            try:
                                ref_send_time = datetime.fromisoformat(exec_hist[0].get("time", ""))
                            except Exception:
                                pass
                        if ref_send_time is None:
                            # push_message 记录的 schedule_time 就是发送时间
                            try:
                                ref_send_time = datetime.fromisoformat(t.get("schedule_time", ""))
                            except Exception:
                                pass
                        # 2分钟撤回窗口
                        now = datetime.now(timezone(timedelta(hours=8)))
                        within_revoke_window = ref_send_time is not None and (now - ref_send_time).total_seconds() <= 120
                        
                        for d in result.get("details", []):
                            tgt_type = d.get("type", "")
                            tgt_id = d.get("id", "")
                            tgt_app = d.get("app_id", "")
                            if tgt_type == "user":
                                name = get_user_name(tgt_id, tgt_app) or "（未知）"
                            elif tgt_type == "group":
                                name = get_group_name_from_mirror(tgt_app, tgt_id) or "（未知）"
                            else:
                                name = "（未知）"
                            msg_ids = d.get("message_ids", [])
                            if d.get("ok") or (msg_ids and any(m.get("msg_id") for m in msg_ids)):
                                msg_parts = []
                                for m in msg_ids:
                                    mid = m.get("msg_id", "?")
                                    rs = m.get("revoke_status")
                                    if rs == "revoked":
                                        msg_parts.append(f"{mid} [已撤回]")
                                    elif rs == "expired":
                                        msg_parts.append(f"{mid} [撤回失败-已超时]")
                                    elif within_revoke_window:
                                        msg_parts.append(f"{mid} [可撤回]")
                                    else:
                                        msg_parts.append(mid)
                                msg_id_str = " | ".join(msg_parts) if msg_parts else "无ID"
                                lines.append(f"      ✓ {name}（{tgt_id}）消息ID: {msg_id_str}")
                            else:
                                err = d.get("error", "")
                                err_txt = f" - {err}" if err else ""
                                lines.append(f"      ✗ {name}（{tgt_id}）{err_txt}")
                    else:
                        lines.append(f"  状态: {status}（尚未执行）")

                    # 执行历史
                    history = t.get("execution_history", [])
                    if history:
                        lines.append(f"  执行记录（共 {len(history)} 次）:")
                        recent = history[-3:]
                        for h in recent:
                            h_time = h.get("time", "?")
                            h_status = _TASK_STATUS_LABELS.get(_norm_task_status(h.get("status")), h.get("status", "?"))
                            h_err = h.get("error", "")
                            # 执行结果详情（兼容新旧格式）
                            h_sc = h.get("success_count")
                            h_fc = h.get("fail_count")
                            h_to = h.get("total")
                            result_info = ""
                            if h_sc is not None and h_to is not None:
                                result_info = f" ✓{h_sc}/✗{h_fc}（共{h_to}）"
                            err_info = f" - {h_err}" if h_err else ""
                            lines.append(f"    · {h_time} [{h_status}]{result_info}{err_info}")
                            # 每个目标的具体情况
                            target_results = h.get("target_results")
                            if target_results:
                                for tr in target_results[:6]:  # 最多展示6个
                                    tr_type = tr.get("type", "")
                                    tr_id = tr.get("id", "")
                                    tr_ok = tr.get("ok")
                                    tr_name = tr.get("name", "")
                                    # 优先用 name，回退到 id
                                    tr_label = f"{tr_type} {tr_name or tr_id}"
                                    if tr_ok is True:
                                        lines.append(f"      ✓ {tr_label}")
                                    elif tr_ok is False:
                                        tr_err = tr.get("error", "")
                                        lines.append(f"      ✗ {tr_label} ({tr_err})" if tr_err else f"      ✗ {tr_label}")
                                    else:
                                        # 唤醒任务没有 ok 字段，直接显示
                                        lines.append(f"      · {tr_label}")
                                if len(target_results) > 6:
                                    lines.append(f"      ... 还有 {len(target_results) - 6} 个目标")

                result_content = "\n".join(lines)
        except Exception as e:
            result_content = f"查询定时推送异常：{e}"

    # ==================== 删除定时推送 ====================
    elif function_name == "delete_scheduled_push":
        task_id = arguments.get("task_id", "")
        if not task_id:
            result_content = "错误：delete_scheduled_push 缺少 task_id 参数"
        else:
            try:
                ok, msg = delete_task(task_id, bot_client.app_id)
                result_content = msg
            except Exception as e:
                result_content = f"删除定时推送异常：{e}"

    # ==================== 修改定时推送 ====================
    elif function_name == "update_scheduled_push":
        task_id = arguments.get("task_id", "")
        if not task_id:
            result_content = "错误：update_scheduled_push 缺少 task_id 参数"
        else:
            try:
                updates = {}
                content = arguments.get("content")
                if content is not None:
                    updates["content"] = content
                    # 未显式指定格式时，正文同步写入 markdown 字段（若任务当前是 markdown），
                    # 避免只改 content 而实际发送仍取旧的 markdown 正文。
                    if arguments.get("message_type") is None:
                        existing = _find_task(task_id) or {}
                        if str(existing.get("message_type") or "").strip().lower() in ("markdown", "md", "2"):
                            updates["markdown"] = content

                # 格式切换：markdown ⇄ text
                mt_raw = arguments.get("message_type")
                if mt_raw is not None and str(mt_raw).strip() != "":
                    is_md = str(mt_raw).strip().lower() in ("markdown", "md", "2")
                    if is_md:
                        updates["message_type"] = "markdown"
                        updates["is_markdown"] = 1
                        # 正文优先取本次传入的 content，否则沿用已有正文
                        if content is not None:
                            updates["markdown"] = content
                        else:
                            existing = _find_task(task_id) or {}
                            body = existing.get("markdown") or existing.get("content")
                            if body is not None:
                                updates["markdown"] = body
                    else:
                        updates["message_type"] = "text"
                        # 清空 markdown 字段：否则 scheduler 执行时
                        # task.get("markdown") or content 仍会取到旧 markdown 正文
                        updates["markdown"] = None
                        updates["is_markdown"] = 0

                targets = arguments.get("targets")
                if targets is not None:
                    updates["targets"] = targets
                schedule_type = arguments.get("schedule_type")
                if schedule_type is not None:
                    updates["schedule_type"] = schedule_type
                schedule_time = arguments.get("schedule_time")
                if schedule_time is not None:
                    updates["schedule_time"] = schedule_time
                interval_seconds = arguments.get("interval_seconds")
                if interval_seconds is not None:
                    updates["interval_seconds"] = interval_seconds
                media_source = arguments.get("media_source")
                if media_source is not None:
                    media = updates.get("media", {})
                    media["source"] = media_source
                    updates["media"] = media
                file_type = arguments.get("file_type")
                if file_type is not None:
                    media = updates.get("media", {})
                    media["file_type"] = file_type
                    updates["media"] = media
                file_name = arguments.get("file_name")
                if file_name is not None:
                    media = updates.get("media", {})
                    media["file_name"] = file_name
                    updates["media"] = media

                if not updates:
                    result_content = "未指定任何要修改的字段"
                else:
                    ok, msg = update_task(task_id, bot_client.app_id, updates)
                    result_content = msg
            except Exception as e:
                result_content = f"修改定时推送异常：{e}"

    # ==================== 撤回推送消息 ====================
    elif function_name == "revoke_push":
        task_id = arguments.get("task_id", "")
        target_ids_filter = arguments.get("target_ids", None)
        if not task_id:
            result_content = "错误：revoke_push 缺少 task_id 参数"
        else:
            try:
                from scheduler import load_tasks, save_tasks
                tasks = load_tasks()
                matched_task = None
                for t in tasks:
                    if t.get("task_id") == task_id:
                        matched_task = t
                        break
                if not matched_task:
                    result_content = f"未找到任务 {task_id}"
                else:
                    # 权限检查
                    if matched_task.get("app_id") != bot_client.app_id and not get_bot_allow_manage_all_push(bot_client.app_id):
                        result_content = f"无权限管理机器人 {matched_task.get('app_id')} 发起的推送"
                    else:
                        result = matched_task.get("result", {})
                        details = result.get("details", [])
                        if not details:
                            result_content = f"任务 {task_id} 尚无执行结果（未执行或执行无数据），无法撤回"
                        else:
                            # 构建 BotClient 缓存
                            all_bots = {b["APP_ID"]: b["APP_SECRET"] for b in get_bots() if b.get("APP_ID")}
                            bot_cache = {}

                            revoke_count = 0
                            fail_count = 0
                            revoke_lines = []

                            for d in details:
                                t_type = d.get("type", "")
                                t_id = d.get("id", "")
                                t_app_id = d.get("app_id", matched_task.get("app_id"))
                                # 判断是否发送成功：有 ok=true 或有 message_ids 内容都算成功
                                msg_ids_list = d.get("message_ids", [])
                                target_ok = d.get("ok", False) or bool(msg_ids_list and any(m.get("msg_id") for m in msg_ids_list))

                                # 如果指定了 target_ids_filter，跳过不在列表中的目标
                                if target_ids_filter is not None and t_id not in target_ids_filter:
                                    continue

                                if not target_ok:
                                    revoke_lines.append(f"  跳过 {t_id}：该目标发送失败，无需撤回")
                                    continue

                                # 获取消息ID列表
                                msg_ids_list = d.get("message_ids", [])
                                if not msg_ids_list:
                                    revoke_lines.append(f"  跳过 {t_id}：无记录的消息ID，可能旧版数据")
                                    continue

                                api_msg_type = "c2c" if t_type == "user" else t_type

                                # 获取 BotClient
                                if t_app_id not in bot_cache:
                                    secret = all_bots.get(t_app_id)
                                    if not secret:
                                        revoke_lines.append(f"  跳过 {t_id}：未找到机器人 {t_app_id} 的凭证")
                                        fail_count += 1
                                        continue
                                    bot_cache[t_app_id] = BotClient(t_app_id, secret)
                                sender = bot_cache[t_app_id]

                                for msg_entry in msg_ids_list:
                                    msg_id_val = msg_entry.get("msg_id", "")
                                    if msg_id_val:
                                        ok_revoke = await sender.revoke_message(api_msg_type, t_id, msg_id_val)
                                        if ok_revoke:
                                            msg_entry["revoke_status"] = "revoked"
                                            revoke_count += 1
                                        else:
                                            msg_entry["revoke_status"] = "expired"
                                            fail_count += 1
                                            revoke_lines.append(f"  ✗ {t_id} 消息 {msg_id_val[:30]}... 撤回失败（已超2分钟或无权撤回）")

                            if target_ids_filter:
                                revoke_lines.insert(0, f"已指定撤回部分目标（{len(target_ids_filter)} 个）：")
                            result_content = f"撤回完成：成功 {revoke_count}，失败 {fail_count}" + ("\n" + "\n".join(revoke_lines) if revoke_lines else "")

                            # ★ 直接保存已修改的 msg_entry（revoke_status 已写入每条消息记录）
                            save_tasks(tasks)
                            info(f"[撤回记录] 任务 {task_id} 已保存撤回状态到消息ID记录", ctx=ctx)
            except Exception as e:
                result_content = f"撤回推送异常：{e}"

    # ==================== 创建定时唤醒 ====================
    elif function_name == "create_scheduled_wakeup":
        schedule_type = arguments.get("schedule_type", "")
        schedule_time = arguments.get("schedule_time", "")
        interval_seconds = arguments.get("interval_seconds")
        targets = arguments.get("targets", [])
        initiator = arguments.get("initiator", {})
        description = arguments.get("description", "")
        if not schedule_type:
            result_content = "错误：create_scheduled_wakeup 缺少 schedule_type 参数"
        elif not targets or not isinstance(targets, list) or len(targets) == 0:
            result_content = "错误：targets 为空或不是数组，请至少指定一个目标"
        elif not initiator or not initiator.get("id") or not initiator.get("name"):
            result_content = "错误：initiator 缺少 id 或 name"
        elif not description:
            result_content = "错误：create_scheduled_wakeup 缺少 description 参数"
        else:
            try:
                # 跨机器人权限检查（所有目标）
                caller_app_id = bot_client.app_id
                for tgt in targets:
                    tgt_app_id = tgt.get("app_id", "")
                    if tgt_app_id != caller_app_id:
                        if not get_bot_allow_cross_wakeup(caller_app_id):
                            result_content = f"错误：本机器人（{caller_app_id}）禁止跨机器人定时唤醒（ALLOW_CROSS_BOT_WAKEUP=0）"
                            return {
                                "role": "tool",
                                "tool_call_id": tool_call_id,
                                "content": result_content
                            }
                        if not get_bot_allow_cross_wakeup_incoming(tgt_app_id):
                            result_content = f"错误：目标机器人（{tgt_app_id}）禁止接收跨机器人定时唤醒（ALLOW_CROSS_BOT_WAKEUP_INCOMING=0）"
                            return {
                                "role": "tool",
                                "tool_call_id": tool_call_id,
                                "content": result_content
                            }

                task_data = {
                    "app_id": caller_app_id,
                    "schedule_type": schedule_type,
                    "schedule_time": schedule_time,
                    "targets": targets,
                    "initiator": initiator,
                    "description": description,
                }
                if schedule_type == "interval" and interval_seconds is not None:
                    task_data["interval_seconds"] = interval_seconds
                # 隔离模式
                isolation_mode = arguments.get("isolation_mode")
                if isolation_mode is not None:
                    task_data["isolation_mode"] = 1 if isolation_mode else 0

                task_id = add_wakeup(task_data)
                schedule_desc = f"计划执行时间: {schedule_time}"
                if schedule_type == "daily":
                    schedule_desc = f"每天 {schedule_time} 执行"
                elif schedule_type == "interval":
                    interval_display = interval_seconds or 3600
                    schedule_desc = f"每 {interval_display} 秒执行一次"
                isolation_tag = "，隔离模式开启" if (isolation_mode is not None and isolation_mode) else ""
                target_names = ", ".join([f"{t.get('type')}({t.get('name', t.get('id'))})" for t in targets])
                result_content = f"定时唤醒已创建，任务 ID: {task_id}，类型: {schedule_type}，{schedule_desc}，目标({len(targets)}个): {target_names}{isolation_tag}"
            except Exception as e:
                result_content = f"创建定时唤醒异常：{e}"

    # ==================== 查看定时唤醒列表 ====================
    elif function_name == "list_scheduled_wakeup":
        try:
            all_tasks = list_wakeups(bot_client.app_id)
            all_counts = _task_counts(all_tasks)

            # 按传入条件过滤（状态 / 关键词 / 时间 / 目标 / 隔离模式 …）
            filter_args = {k: v for k, v in arguments.items()
                           if k in _LIST_FILTER_KEYS and v not in (None, "", [], {})}
            tasks, fmeta = _filter_tasks(all_tasks, filter_args)
            fdesc = _describe_filters(fmeta)

            if not all_tasks:
                result_content = "当前没有定时唤醒任务"
            elif not tasks:
                result_content = (
                    f"没有符合条件的定时唤醒任务（共 {len(all_tasks)} 个，"
                    f"状态分布：{_status_summary_text(all_counts)}）"
                    + (f"\n筛选条件：{fdesc}" if fdesc else "")
                    + "\n可放宽或去掉筛选条件后重试。"
                )
            else:
                header = f"定时唤醒任务列表（{len(tasks)}/{len(all_tasks)} 个"
                header += f"，状态分布：{_status_summary_text(all_counts)}）"
                if fdesc:
                    header += f"\n筛选条件：{fdesc}"
                header += "："
                lines = [header]
                for t in tasks:
                    tid = t.get("task_id", "?")
                    status = _TASK_STATUS_LABELS.get(task_status_of(t), "?")
                    sched_type = t.get("schedule_type", "?")
                    app = t.get("app_id", "?")
                    sched_time = t.get("schedule_time", "") or "（未设置）"
                    interval_sec = t.get("interval_seconds")
                    desc = t.get("description", "")
                    created_at = t.get("created_at", "?")
                    target = t.get("target", {})
                    initiator = t.get("initiator", {})

                    lines.append(f"\n{'='*55}")
                    lines.append(f"  [{status}] 任务ID: {tid}")
                    lines.append(f"  创建机器人 APP_ID: {app}")
                    lines.append(f"  创建时间: {created_at}")
                    # 隔离模式
                    iso_mode = t.get("isolation_mode", 0)
                    lines.append(f"  隔离模式: {'开启 ✓' if iso_mode else '关闭'} (唤醒与用户消息{'互不干扰' if iso_mode else '会互相打断'})")

                    # 调度参数 — 全部显示
                    if sched_type == "one_time":
                        lines.append(f"  调度类型: 一次性")
                        lines.append(f"  执行时间: {sched_time}")
                    elif sched_type == "daily":
                        lines.append(f"  调度类型: 每天")
                        lines.append(f"  每天时间: {sched_time}")
                    elif sched_type == "interval":
                        iv = interval_sec or 3600
                        lines.append(f"  调度类型: 间隔执行")
                        lines.append(f"  间隔秒数: {iv} 秒（每 {iv//3600 if iv>=3600 else iv} {'小时' if iv>=3600 else '秒'}）")
                        lines.append(f"  首次时间: {sched_time}")
                    else:
                        lines.append(f"  调度类型: {sched_type}")
                        lines.append(f"  调度时间: {sched_time}")

                    lines.append(f"  唤醒说明: {desc}")

                    # 目标详情 — 完整显示（支持多目标）
                    targets_list = t.get("targets", [])
                    lines.append(f"  ── 唤醒目标（{len(targets_list)} 个）──")
                    for tg in targets_list:
                        tg_type = tg.get("type", "")
                        tg_id = tg.get("id", "")
                        tg_app_id = tg.get("app_id", "")
                        tg_name = tg.get("name", "")
                        if tg_type == "user":
                            lines.append(f"     · 用户 {tg_name or '（未知）'}（{tg_id}）")
                        elif tg_type == "group":
                            lines.append(f"     · 群 {tg_name or '（未知）'}（{tg_id}）")
                        else:
                            lines.append(f"     · {tg_type} {tg_name or '（未知）'}（{tg_id}）")
                        # 目标跨机器人权限状态
                        if tg_app_id and tg_app_id != app:
                            can_out = get_bot_allow_cross_wakeup(app)
                            can_in = get_bot_allow_cross_wakeup_incoming(tg_app_id)
                            perm_flags = []
                            if can_out:
                                perm_flags.append("本机允许跨出 ✓")
                            else:
                                perm_flags.append("本机禁止跨出 ✗")
                            if can_in:
                                perm_flags.append("目标允许传入 ✓")
                            else:
                                perm_flags.append("目标禁止传入 ✗")
                            lines.append(f"       跨机器人权限: {' | '.join(perm_flags)}")
                        else:
                            lines.append(f"       所属机器人: {tg_app_id}")

                    # 发起者详情 — 完整显示
                    ini_type = initiator.get("type", "")
                    ini_id = initiator.get("id", "")
                    ini_name = initiator.get("name", "")
                    ini_gid = initiator.get("group_id", "")
                    ini_gname = initiator.get("group_name", "")
                    lines.append(f"  ── 发起者 ──")
                    if ini_type == "group" and ini_gid:
                        lines.append(f"     类型: 群用户")
                        lines.append(f"     群 ID: {ini_gid}")
                        lines.append(f"     群名称: {ini_gname or '（未知）'}")
                        lines.append(f"     用户 ID: {ini_id}")
                        lines.append(f"     用户名称: {ini_name}")
                    elif ini_type == "user":
                        lines.append(f"     类型: 用户")
                        lines.append(f"     用户 ID: {ini_id}")
                        lines.append(f"     用户名称: {ini_name}")
                    else:
                        lines.append(f"     类型: {ini_type}")
                        lines.append(f"     ID: {ini_id}")
                        lines.append(f"     名称: {ini_name}")

                    # 最近3次执行时间（含目标详情）
                    history = t.get("execution_history", [])
                    lines.append(f"  ── 执行记录 ──")
                    if history:
                        recent = history[-3:]
                        lines.append(f"     共执行 {len(history)} 次，最近 {len(recent)} 次:")
                        for h in recent:
                            h_time = h.get("time", "?")
                            h_status = _TASK_STATUS_LABELS.get(_norm_task_status(h.get("status")), h.get("status", "?"))
                            h_err = h.get("error", "")
                            err_info = f" - {h_err}" if h_err else ""
                            lines.append(f"       · {h_time} [{h_status}]{err_info}")
                            # 每个目标的具体情况
                            target_results = h.get("target_results")
                            if target_results:
                                for tr in target_results[:6]:
                                    tr_type = tr.get("type", "")
                                    tr_id = tr.get("id", "")
                                    tr_name = tr.get("name", "")
                                    tr_ok = tr.get("ok")
                                    tr_label = f"{tr_type} {tr_name or tr_id}"
                                    if tr_ok is True:
                                        lines.append(f"         ✓ {tr_label}")
                                    elif tr_ok is False:
                                        tr_err = tr.get("error", "")
                                        lines.append(f"         ✗ {tr_label} ({tr_err})" if tr_err else f"         ✗ {tr_label}")
                                    else:
                                        lines.append(f"         · {tr_label}")
                                if len(target_results) > 6:
                                    lines.append(f"         ... 还有 {len(target_results) - 6} 个目标")
                    else:
                        lines.append(f"     尚未执行过")

                result_content = "\n".join(lines)
        except Exception as e:
            result_content = f"查询定时唤醒异常：{e}"

    # ==================== 删除定时唤醒 ====================
    elif function_name == "delete_scheduled_wakeup":
        task_id = arguments.get("task_id", "")
        if not task_id:
            result_content = "错误：delete_scheduled_wakeup 缺少 task_id 参数"
        else:
            try:
                ok, msg = delete_wakeup(task_id, bot_client.app_id)
                result_content = msg
            except Exception as e:
                result_content = f"删除定时唤醒异常：{e}"

    # ==================== 修改定时唤醒 ====================
    elif function_name == "update_scheduled_wakeup":
        task_id = arguments.get("task_id", "")
        if not task_id:
            result_content = "错误：update_scheduled_wakeup 缺少 task_id 参数"
        else:
            try:
                updates = {}
                schedule_type = arguments.get("schedule_type")
                if schedule_type is not None:
                    updates["schedule_type"] = schedule_type
                schedule_time = arguments.get("schedule_time")
                if schedule_time is not None:
                    updates["schedule_time"] = schedule_time
                interval_seconds = arguments.get("interval_seconds")
                if interval_seconds is not None:
                    updates["interval_seconds"] = interval_seconds
                description = arguments.get("description")
                if description is not None:
                    updates["description"] = description
                targets = arguments.get("targets")
                if targets is not None:
                    updates["targets"] = targets
                initiator = arguments.get("initiator")
                if initiator is not None:
                    updates["initiator"] = initiator
                # 状态由调度器根据执行结果写入（pending/completed/failed），
                # 不接受外部直接改，避免出现程序从不生成的状态值
                isolation_mode = arguments.get("isolation_mode")
                if isolation_mode is not None:
                    updates["isolation_mode"] = 1 if isolation_mode else 0

                if not updates:
                    result_content = "未指定任何要修改的字段"
                else:
                    ok, msg = update_wakeup(task_id, bot_client.app_id, updates)
                    result_content = msg
            except Exception as e:
                result_content = f"修改定时唤醒异常：{e}"

    # ==================== 新增 skip_reply 工具 ====================
    elif function_name == "skip_reply":
        # 不执行任何操作，仅返回成功标记
        result_content = "已标记跳过回复，结束工具调用循环"

    # ==================== 记忆管理工具 ====================
    # 业务逻辑一律走上面的共用函数（与 api_server 的 /api/memory/* 同一份实现），
    # 这里只负责把结构化结果拼成给模型看的中文文案。
    elif function_name == "view_memory":
        level = arguments.get("level", "")
        identifier = arguments.get("identifier", "")
        if not level or not identifier:
            result_content = "错误：view_memory 缺少 level 或 identifier 参数"
        else:
            try:
                mem_list, enabled = _memory_get(level, identifier)
                status_text = "启用" if enabled else "禁用"
                if mem_list:
                    items = "\n".join([f"  [{i}] {item}" for i, item in enumerate(mem_list)])
                    result_content = f"记忆级别: {level}（{status_text}）共 {len(mem_list)} 条：\n{items}"
                else:
                    result_content = f"记忆级别: {level}（{status_text}）当前无记忆"
            except MemoryLevelError as e:
                result_content = f"错误：未知记忆级别 {e.level}"
            except Exception as e:
                result_content = f"查看记忆异常：{e}"

    elif function_name == "add_memory":
        items = arguments.get("items")
        if items and isinstance(items, list):
            # 批量模式
            try:
                res = _memory_batch("add", items)
                success_list = res["success"]
                fail_list = [f"第{d['index']}条: {d['error']}" for d in res["failed"]]
                parts = []
                if success_list:
                    parts.append(f"成功 {len(success_list)} 条")
                if fail_list:
                    parts.append(f"失败 {len(fail_list)} 条: {'; '.join(fail_list)}")
                result_content = "添加记忆完成：" + "，".join(parts)
                info(f"[记忆工具] add_memory 批量: 成功 {len(success_list)}, 失败 {len(fail_list)}", ctx=ctx)
            except Exception as e:
                result_content = f"添加记忆异常：{e}"
        else:
            # 单条模式
            level = arguments.get("level", "")
            identifier = arguments.get("identifier", "")
            content = arguments.get("content", "")
            if not level or not identifier or not content:
                result_content = "错误：add_memory 缺少 level、identifier 或 content 参数"
            else:
                try:
                    _memory_add(level, identifier, content)
                    result_content = f"已向 {level} 记忆添加：{content}"
                    info(f"[记忆工具] {level} 添加: {content}", ctx=ctx)
                except MemoryDisabledError:
                    _label = {"global": "全局记忆", "bot": "机器人记忆",
                              "group": "群聊记忆", "c2c": "私聊记忆"}.get(level, f"{level} 记忆")
                    result_content = f"错误：{_label}当前已禁用，请先使用 enable_memory 启用"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                except MemoryLevelError as e:
                    result_content = f"错误：未知记忆级别 {e.level}"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                except Exception as e:
                    result_content = f"添加记忆异常：{e}"

    elif function_name == "modify_memory":
        items = arguments.get("items")
        if items and isinstance(items, list):
            # 批量模式
            try:
                res = _memory_batch("update", items)
                success_list = res["success"]
                fail_list = [f"第{d['index']}条: {d['error']}" for d in res["failed"]]
                parts = []
                if success_list:
                    parts.append(f"成功 {len(success_list)} 条")
                if fail_list:
                    parts.append(f"失败 {len(fail_list)} 条: {'; '.join(fail_list)}")
                result_content = "修改记忆完成：" + "，".join(parts)
                info(f"[记忆工具] modify_memory 批量: 成功 {len(success_list)}, 失败 {len(fail_list)}", ctx=ctx)
            except Exception as e:
                result_content = f"修改记忆异常：{e}"
        else:
            # 单条模式
            level = arguments.get("level", "")
            identifier = arguments.get("identifier", "")
            idx = arguments.get("index")
            content = arguments.get("content", "")
            if not level or not identifier or idx is None or not content:
                result_content = "错误：modify_memory 缺少 level、identifier、index 或 content 参数"
            else:
                try:
                    ok = _memory_replace(level, identifier, idx, content)
                    if ok:
                        result_content = f"已修改 {level} 记忆索引 {idx} 为：{content}"
                        info(f"[记忆工具] {level} 修改索引 {idx}", ctx=ctx)
                    else:
                        result_content = f"修改失败：索引 {idx} 超出范围或无效"
                except MemoryLevelError as e:
                    result_content = f"错误：未知记忆级别 {e.level}"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                except Exception as e:
                    result_content = f"修改记忆异常：{e}"

    elif function_name == "delete_memory":
        items = arguments.get("items")
        if items and isinstance(items, list):
            # 批量模式
            try:
                res = _memory_batch("delete", items)
                success_list = res["success"]
                fail_list = [f"第{d['index']}条: {d['error']}" for d in res["failed"]]
                parts = []
                if success_list:
                    parts.append(f"成功 {len(success_list)} 条")
                if fail_list:
                    parts.append(f"失败 {len(fail_list)} 条: {'; '.join(fail_list)}")
                result_content = "删除记忆完成：" + "，".join(parts)
                info(f"[记忆工具] delete_memory 批量: 成功 {len(success_list)}, 失败 {len(fail_list)}", ctx=ctx)
            except Exception as e:
                result_content = f"删除记忆异常：{e}"
        else:
            # 单条模式
            level = arguments.get("level", "")
            identifier = arguments.get("identifier", "")
            idx = arguments.get("index")
            if not level or not identifier or idx is None:
                result_content = "错误：delete_memory 缺少 level、identifier 或 index 参数"
            else:
                try:
                    ok = _memory_remove(level, identifier, idx)
                    if ok:
                        result_content = f"已删除 {level} 记忆索引 {idx}"
                        info(f"[记忆工具] {level} 删除索引 {idx}", ctx=ctx)
                    else:
                        result_content = f"删除失败：索引 {idx} 超出范围或无效"
                except MemoryLevelError as e:
                    result_content = f"错误：未知记忆级别 {e.level}"
                    return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
                except Exception as e:
                    result_content = f"删除记忆异常：{e}"

    elif function_name == "enable_memory":
        level = arguments.get("level", "")
        identifier = arguments.get("identifier", "")
        if not level or not identifier:
            result_content = "错误：enable_memory 缺少 level 或 identifier 参数"
        else:
            try:
                _memory_set_enabled(level, identifier, True)
                result_content = f"已启用 {level} 记忆"
                info(f"[记忆工具] {level} 记忆已启用", ctx=ctx)
            except MemoryLevelError as e:
                result_content = f"错误：未知记忆级别 {e.level}"
                return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
            except Exception as e:
                result_content = f"启用记忆异常：{e}"

    elif function_name == "disable_memory":
        level = arguments.get("level", "")
        identifier = arguments.get("identifier", "")
        if not level or not identifier:
            result_content = "错误：disable_memory 缺少 level 或 identifier 参数"
        else:
            try:
                _memory_set_enabled(level, identifier, False)
                result_content = f"已禁用 {level} 记忆"
                info(f"[记忆工具] {level} 记忆已禁用", ctx=ctx)
            except MemoryLevelError as e:
                result_content = f"错误：未知记忆级别 {e.level}"
                return {"role": "tool", "tool_call_id": tool_call_id, "content": result_content}
            except Exception as e:
                result_content = f"禁用记忆异常：{e}"

    # ==================== 记忆搜索工具 ====================
    elif function_name == "search_memory":
        keywords_raw = arguments.get("keywords", "")
        level = arguments.get("level", "all")
        identifier = arguments.get("identifier", "")
        # 兼容模型可能误传数组（schema 声明为字符串，逗号分隔）
        if isinstance(keywords_raw, (list, tuple)):
            keywords_str = ",".join(str(k) for k in keywords_raw)
        else:
            keywords_str = str(keywords_raw or "")
        if not keywords_str.strip():
            result_content = "错误：search_memory 缺少 keywords 参数"
        else:
            try:
                keywords = [kw.strip() for kw in keywords_str.split(",") if kw.strip()]
                # 根据记忆层级确定所需标识符
                search_app_id = None
                search_group_id = None
                search_c2c_id = None
                if level in ("all", "global", "bot"):
                    search_app_id = identifier or bot_client.app_id
                if level in ("all", "group"):
                    search_group_id = identifier or group_id
                if level in ("all", "c2c"):
                    search_c2c_id = identifier or (recipient_id if msg_type == "c2c" else None)
                result_content = search_memory(
                    keywords=keywords,
                    layer=level,
                    app_id=search_app_id,
                    group_id=search_group_id,
                    c2c_user_id=search_c2c_id,
                )
                info(f"[记忆搜索] 关键词: {keywords}, 层级: {level}, 结果长度: {len(result_content)}", ctx=ctx)
            except Exception as e:
                result_content = f"搜索记忆异常：{e}"

    # ==================== 批量添加记忆 ====================
    else:
        result_content = f"错误：未知工具 {function_name}"

    debug(f"[工具执行] {function_name} 结果: {result_content[:200]}", ctx=ctx)

    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": result_content
    }