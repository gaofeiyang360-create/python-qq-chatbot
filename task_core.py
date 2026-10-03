# -*- coding: utf-8 -*-
# task_core.py — 定时推送/唤醒 的共享核心
#
# 设计目的：api_server.py（HTTP 接口）与 tool.py（AI 工具）过去各自实现了一遍
# 推送/唤醒的增删改查，导致两边参数不统一、字段白名单互有缺失。这里把「核心」
# 下沉成一份，两边都只调它，从根上保证行为一致。
#
# 约定：函数只负责业务本身，不做鉴权（由调用方各自完成），
#       返回统一的 (ok: bool, data: dict, err: str) 三元组。
import re
from typing import Any, Dict, List, Optional, Tuple

# ==================== 任务状态（唯一事实来源） ====================
# 任务级 status 只有以下三种，且写进文件的就是这三个字面量：
#   pending    未开始 —— 创建时的初始值
#   completed  执行完成 —— 到期执行成功后置位
#   failed     执行失败 —— 执行出错或有目标失败时置位
#
# 放在 task_core 而不是 tool.py：scheduler / wakeup_scheduler 都要用它
# （读旧文件时归类），而 tool.py 会延迟导入 scheduler，放在那边会循环导入。
TASK_STATUSES = ("pending", "completed", "failed")

_TASK_STATUS_LABELS = {
    "pending": "未开始",
    "completed": "执行完成",
    "failed": "执行失败",
}

# 中文写法也接受（仅用于筛选参数，不落库）
_STATUS_CN = {
    "未开始": "pending",
    "执行完成": "completed",
    "执行失败": "failed",
}

# 历史文件的兼容映射。
#
# 这不是「别名表」，区别很关键：
#   - 别名：给程序从不产生的值编显示名（旧表的 running/cancelled 就是，
#     结果界面有 tab、数据永远为空）。这类已彻底删除。
#   - 兼容映射：旧版本程序**确实写进过磁盘**的状态拼写。现在改写成规范值，
#     但老文件不会自动重写，读到时必须按它当初的真实含义归类。
#
# 旧版本实际写入：
#   推送  scheduler.py        : pending(未开始) -> sent(执行完成)
#   唤醒  wakeup_scheduler.py : active(未开始)  -> completed(执行完成)
# 所以 sent 归 completed（已发送就是执行完成），active 归 pending。
_LEGACY_STATUS = {
    "sent": "completed",
    "active": "pending",
}


def norm_task_status(raw: Any) -> str:
    """把状态值规范化成规范状态之一。

    只认规范值、中文标签、以及历史文件的旧拼写；
    其余无法识别的值**原样返回**，让上层按「无匹配」处理，
    而不是静默兜底成 pending
    （静默兜底会让 ?status=乱填 变成「筛出全部未开始」，很误导）。
    """
    s = str(raw or "").strip()
    if not s:
        return ""
    low = s.lower()
    if low in _TASK_STATUS_LABELS:
        return low
    if s in _STATUS_CN:
        return _STATUS_CN[s]
    if low in _LEGACY_STATUS:
        return _LEGACY_STATUS[low]
    return low


def task_status_of(t: Dict[str, Any]) -> str:
    """取任务级状态并归类到三个规范状态之一。

    兼容旧文件里的 sent/active；只有真正无法识别的脏值才兜底为 pending
    （此时它是「未知」，而不是被冒充成某个已知状态）。
    """
    s = norm_task_status((t or {}).get("status"))
    return s if s in _TASK_STATUS_LABELS else "pending"

# ==================== 统一参数命名 ====================
# 两边过去各叫各的，同一个东西好几个名字（group_id / group_openid / id），
# 调用方要猜。现在只保留**一个**规范名，不再接受任何别名：
#
#   target_type  会话类型：group / c2c（user 视作 c2c）
#   target_id    会话 ID（群 openid 或用户 openid）
#   member_id    群成员 openid（仅禁言类接口使用）
#   message_ids  消息 ID 数组（仅撤回类接口使用，一律数组）
#   content      推送正文
#   description  唤醒提示词
#
# 注意 target_id 与 member_id 各自独立：会话上下文只读 target_id，
# 成员上下文只读 member_id，不互相兜底，避免读错对象。
CANONICAL_PARAMS = (
    "target_type", "target_id", "member_id",
    "message_ids", "content", "description",
)


# ==================== 常量 ====================

# 合法的计划类型。与 api_server 的创建校验、前端下拉框必须保持一致。
# 历史坑：前端编辑表单曾写入 "once"，调度器不认 → 任务永远卡在
# 「未知类型」不再执行，且只在日志里每 5 秒刷一条 warning，极难发现。
VALID_SCHEDULE_TYPES = {"one_time", "daily", "interval"}

# 推送更新字段白名单。
# 必须同时容纳「API 风格」与「工具风格」的字段名，否则会出现
#   API 建的带媒体任务、工具改不动；工具建的 Markdown 任务、API 改不动。
PUSH_UPDATE_FIELDS = {
    # 通用
    "content", "targets", "schedule_type", "schedule_time", "interval_seconds",
    # 媒体：工具与 API 各用一套名字，两边都收
    "media_source", "file_type", "file_name", "media",
    # Markdown
    "message_type", "markdown", "is_markdown",
}

# 唤醒更新字段白名单
# 注意：不含 status —— 任务状态由调度器按执行结果写入
# （pending/completed/failed），外部不可直接改。
WAKEUP_UPDATE_FIELDS = {
    "schedule_type", "schedule_time", "interval_seconds",
    "targets", "initiator", "description", "isolation_mode",
}

# 创建推送时允许直接落库的字段（除 targets/schedule_* 等必填）
PUSH_CREATE_EXTRA_FIELDS = {
    "media_source", "file_type", "file_name",   # 工具与 API 共用
    "media",                                     # 兼容 update 里的写法
    "message_type", "markdown", "is_markdown",   # Markdown：API 建任务也能设
}


# ==================== 参数归一化 ====================

def first_str(src: Any, *keys: str, default: str = "") -> str:
    """按顺序取第一个非空值并转成 str。用于同时接受两边的参数名。"""
    if not isinstance(src, dict):
        return default
    for k in keys:
        v = src.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


def norm_schedule_type(raw: Any, default: str = "one_time") -> str:
    """归一化并校验 schedule_type，非法值抛 ValueError。"""
    st = str(raw or "").strip()
    if not st:
        st = default
    if st not in VALID_SCHEDULE_TYPES:
        raise ValueError(
            f"schedule_type 无效: {st!r}，只能是 {' / '.join(sorted(VALID_SCHEDULE_TYPES))}")
    return st


def norm_targets(raw: Any) -> List[Dict[str, str]]:
    """把 targets 归一化成 [{type,id,...}]。

    同时接受两种写法：
      targets: [{type, id, name?}, ...]        ← API / 工具都支持
      type + id（单个目标）                     ← 工具侧常用简写
    非法时抛 ValueError。
    """
    out: List[Dict[str, str]] = []
    if isinstance(raw, list):
        for i, t in enumerate(raw):
            if not isinstance(t, dict):
                raise ValueError(f"targets[{i}] 必须为对象")
            ty = str(t.get("type") or "").strip().lower()
            tid = str(t.get("id") or "").strip()
            if ty not in ("user", "group"):
                raise ValueError(f"targets[{i}].type 必须为 user 或 group")
            if not tid:
                raise ValueError(f"targets[{i}] 缺少 id")
            item = dict(t)
            item["type"] = ty
            item["id"] = tid
            out.append(item)
    return out


def build_targets(raw: Any, single_type: Any = None, single_id: Any = None
                  ) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """统一构造 targets：优先用数组，其次用 type+id 简写。

    返回 (targets, err)。err 非空表示参数有问题。
    """
    try:
        out = norm_targets(raw)
    except ValueError as e:
        return [], str(e)
    if out:
        return out, None

    ty = str(single_type or "").strip().lower()
    tid = str(single_id or "").strip()
    if ty or tid:
        if ty not in ("user", "group"):
            return [], "type 必须为 user 或 group"
        if not tid:
            return [], "缺少 id"
        # 与数组写法保持完全一致：不额外塞 name，避免两种写法落库结果不同
        return [{"type": ty, "id": tid}], None
    return [], "targets 必须为非空数组（或用 type+id 指定单个目标）"


def split_updates(body: Any, flat_keys: set) -> Dict[str, Any]:
    """统一解析更新参数，兼容两种写法：

      {"task_id": X, "updates": {content: ...}}    ← API 风格（嵌套）
      {"task_id": X, "content": ...}               ← 工具风格（平铺）
    两者同时出现时，updates 内的字段优先。
    """
    if not isinstance(body, dict):
        return {}
    nested = body.get("updates")
    out: Dict[str, Any] = {}
    # 先收平铺字段
    for k in flat_keys:
        if k in body:
            out[k] = body[k]
    # 再收嵌套字段（覆盖平铺）
    if isinstance(nested, dict):
        for k, v in nested.items():
            if k in flat_keys:
                out[k] = v
    return out


def apply_push_updates(task: Dict[str, Any], updates: Dict[str, Any]
                       ) -> Tuple[bool, str]:
    """把 updates 写进推送任务 dict。返回 (ok, err)。

    只接受 PUSH_UPDATE_FIELDS；显式 None 表示清除该字段。
    """
    if not isinstance(updates, dict):
        return False, "updates 必须为对象"
    if not updates:
        return False, "updates 不能为空"

    if "schedule_type" in updates and updates["schedule_type"] is not None:
        try:
            updates = dict(updates)
            updates["schedule_type"] = norm_schedule_type(updates["schedule_type"])
        except ValueError as e:
            return False, str(e)

    if "interval_seconds" in updates and updates["interval_seconds"] is not None:
        try:
            updates = dict(updates)
            updates["interval_seconds"] = int(updates["interval_seconds"])
        except (TypeError, ValueError):
            return False, "interval_seconds 必须为整数"

    if "targets" in updates and updates["targets"] is not None:
        try:
            updates = dict(updates)
            updates["targets"] = norm_targets(updates["targets"])
        except ValueError as e:
            return False, str(e)

    for k, v in updates.items():
        if k not in PUSH_UPDATE_FIELDS:
            continue
        if v is None:
            task.pop(k, None)
        else:
            task[k] = v
    return True, ""


def apply_wakeup_updates(task: Dict[str, Any], updates: Dict[str, Any]
                         ) -> Tuple[bool, str]:
    """把 updates 写进唤醒任务 dict。返回 (ok, err)。"""
    if not isinstance(updates, dict):
        return False, "updates 必须为对象"
    if not updates:
        return False, "updates 不能为空"

    if "schedule_type" in updates and updates["schedule_type"] is not None:
        try:
            updates = dict(updates)
            updates["schedule_type"] = norm_schedule_type(updates["schedule_type"])
        except ValueError as e:
            return False, str(e)

    if "interval_seconds" in updates and updates["interval_seconds"] is not None:
        try:
            updates = dict(updates)
            updates["interval_seconds"] = int(updates["interval_seconds"])
        except (TypeError, ValueError):
            return False, "interval_seconds 必须为整数"

    if "targets" in updates and updates["targets"] is not None:
        try:
            updates = dict(updates)
            updates["targets"] = norm_targets(updates["targets"])
        except ValueError as e:
            return False, str(e)

    for k, v in updates.items():
        if k in WAKEUP_UPDATE_FIELDS:
            task[k] = v
    return True, ""


def push_create_fields(body: Dict[str, Any]) -> Dict[str, Any]:
    """从创建请求里挑出可直接落库的扩展字段（媒体 / Markdown）。

    媒体字段要特别注意：scheduler.execute_task 只读 `task["media"]`（嵌套对象），
    而 API 一直写的是扁平的 media_source/file_type/file_name —— 结果 API 建的
    带媒体推送在执行时不会发媒体，且毫无报错。这里统一成嵌套 media。
    """
    out: Dict[str, Any] = {}
    for k in PUSH_CREATE_EXTRA_FIELDS:
        if body.get(k) is not None:
            out[k] = body[k]

    # 扁平三件套 -> 嵌套 media（执行器认这个）
    src = first_str(body, "media_source")
    if src and not out.get("media"):
        media: Dict[str, Any] = {"source": src}
        if body.get("file_type") is not None:
            media["file_type"] = body["file_type"]
        if str(body.get("file_name") or "").strip():
            media["file_name"] = str(body["file_name"]).strip()
        out["media"] = media
    return out


def normalize_push_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """把任务里的媒体字段归一化成执行器认识的 `media` 嵌套结构。

    兼容两种历史写法：
      media: {source, file_type, file_name}   ← 工具 / 执行器
      media_source + file_type + file_name    ← 早期 API
    返回修正后的 task（就地修改并返回）。
    """
    if task.get("media"):
        return task
    src = first_str(task, "media_source")
    if src:
        media: Dict[str, Any] = {"source": src}
        if task.get("file_type") is not None:
            media["file_type"] = task["file_type"]
        if str(task.get("file_name") or "").strip():
            media["file_name"] = str(task["file_name"]).strip()
        task["media"] = media
    return task


def validate_schedule_fields(schedule_type: str, schedule_time: Any,
                             interval_seconds: Any) -> Optional[str]:
    """校验时间字段组合，返回错误信息或 None。"""
    if schedule_type in ("one_time", "daily") and not str(schedule_time or "").strip():
        return f"schedule_type={schedule_type} 时必须提供 schedule_time"
    if schedule_type == "interval" and interval_seconds is not None:
        try:
            if int(interval_seconds) <= 0:
                return "interval_seconds 必须为正整数"
        except (TypeError, ValueError):
            return "interval_seconds 必须为整数"
    return None


# ==================== 禁言成员列表（单个 / 批量统一形态） ====================
# 与 targets 一致：一律用数组。传一个就是单个操作，传多个就是批量。
#
#   members: [
#     {"member_id": "xxx", "mute_expire_at": "2026-09-26T20:00:00+08:00"},
#     ...
#   ]
#
# 兼容简写：members 也可以直接给字符串数组 ["a", "b"]（此时共用外层
# mute_expire_at / seconds）；或给单个 member_id（自动包成数组）。
MAX_MUTE_MEMBERS = 50


def normalize_members(src: Any, fallback_expire: str = "") -> Tuple[List[Dict[str, str]], Optional[str]]:
    """把 members 参数归一成 [{"member_id", "mute_expire_at"}, ...]。

    接受三种写法：
      ["a", "b"]                          字符串数组
      [{"member_id": "a"}, ...]           对象数组
      "a" / {"member_id": "a"}            单个（自动包成数组）
    返回 (members, err)。
    """
    if src is None or (isinstance(src, str) and not src.strip()):
        return [], None

    raw: List[Any]
    if isinstance(src, str):
        raw = [src]
    elif isinstance(src, dict):
        raw = [src]
    elif isinstance(src, list):
        raw = src
    else:
        return [], "members 必须为数组"

    out: List[Dict[str, str]] = []
    seen = set()
    for i, item in enumerate(raw):
        if isinstance(item, str):
            mid, exp = item.strip(), str(fallback_expire or "").strip()
        elif isinstance(item, dict):
            mid = str(item.get("member_id") or "").strip()
            exp = str(item.get("mute_expire_at") or fallback_expire or "").strip()
        else:
            return [], f"members[{i}] 必须为字符串或对象"
        if not mid:
            return [], f"members[{i}] 缺少 member_id"
        if mid in seen:
            continue          # 去重，避免重复调用接口
        seen.add(mid)
        out.append({"member_id": mid, "mute_expire_at": exp})

    if len(out) > MAX_MUTE_MEMBERS:
        return [], f"单次最多操作 {MAX_MUTE_MEMBERS} 个成员（收到 {len(out)} 个）"
    return out, None


def mute_result_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """汇总批量禁言结果。"""
    ok_n = sum(1 for r in results if r.get("ok"))
    return {
        "total": len(results),
        "success": ok_n,
        "failed": len(results) - ok_n,
        "results": results,
    }