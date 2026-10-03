# -*- coding: utf-8 -*-
# wakeup_scheduler.py — 定时唤醒任务调度器
import json
import uuid
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from log import info, warn, error, LogCtx
from config import BASE_DIR, get_bots, get_bot_allow_manage_all_push, \
    get_bot_allow_cross_wakeup, get_bot_allow_cross_wakeup_incoming, \
    get_bot_disable_ai_reply
from client import BotClient
from memory import append_message


# ==================== 存储文件 ====================
WAKEUP_FILE = BASE_DIR / "scheduled_wakeup.json"
CHECK_INTERVAL = 5  # 秒，后台检查周期

# 共享核心：合法类型 / 字段白名单 / 参数归一化。见 task_core.py。
from task_core import (  # noqa: E402
    VALID_SCHEDULE_TYPES, WAKEUP_UPDATE_FIELDS,
    norm_schedule_type, apply_wakeup_updates,
    TASK_STATUSES, task_status_of,
)

# ==================== 存储 ====================
def load_wakeups() -> List[Dict]:
    """加载所有定时唤醒任务，自动将旧版单 target 迁移为 targets 数组"""
    if not WAKEUP_FILE.exists():
        return []
    try:
        with open(WAKEUP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        tasks = data if isinstance(data, list) else []
        # 自动迁移旧数据：将单 target 转为 targets 数组
        needs_save = False
        for t in tasks:
            if "target" in t and "targets" not in t:
                t["targets"] = [t.pop("target")]
                needs_save = True
            # 迁移旧版状态拼写（active -> pending 等）到规范值，一次性写回
            norm = task_status_of(t)
            if t.get("status") != norm and norm in TASK_STATUSES:
                t["status"] = norm
                needs_save = True
        if needs_save:
            save_wakeups(tasks)
            info("[唤醒调度] 已自动迁移旧版唤醒任务数据（target → targets / 状态规范化）", ctx=None)
        return tasks
    except Exception as e:
        warn(f"[唤醒调度] 加载定时唤醒文件失败: {e}", ctx=None)
        return []


def save_wakeups(tasks: List[Dict]):
    """保存所有定时唤醒任务"""
    try:
        WAKEUP_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(WAKEUP_FILE, "w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)
    except Exception as e:
        error(f"[唤醒调度] 保存定时唤醒文件失败: {e}", ctx=None)


# ==================== 工具函数 ====================
def _parse_rfc3339(time_str: str) -> Optional[datetime]:
    """解析 RFC3339 时间字符串为 datetime（北京时间）"""
    if not time_str:
        return None
    try:
        clean = time_str.replace('Z', '+00:00').replace(' ', 'T')
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            bj = timezone(timedelta(hours=8))
            dt = dt.replace(tzinfo=bj)
        return dt.astimezone(timezone(timedelta(hours=8)))
    except Exception:
        return None


def _now_bj() -> datetime:
    """返回当前北京时间"""
    return datetime.now(timezone(timedelta(hours=8)))


def _now_bj_iso() -> str:
    """返回当前北京时间 ISO 字符串"""
    return _now_bj().isoformat(timespec="seconds")


def _executed_on_date(history: List[Dict], day: datetime) -> bool:
    """判断执行历史里是否存在"发生在 day 这一天（北京时间）"的记录。

    原先用 h["time"].startswith("2026-09-30") 做前缀比较，隐含假设 time 一定是
    "YYYY-MM-DD..." 开头且不带时区换算。写入侧目前是北京时间 ISO 字符串，
    但一旦改成 UTC 存储、换成 "2026/09/30" 分隔符、或前补空格，前缀比较就会
    静默判错 —— 表现为"每日任务在同一天被重复执行"或"当天不再执行"。

    这里改为解析成 datetime 再按北京时间比较日期，兼容带时区（含 Z）、
    空格分隔、纯日期等写法；无法解析的记录一律忽略（不参与"今天已执行"判断，
    宁可多执行一次，也不要因为一条脏记录导致当天彻底不执行）。

    与 scheduler._executed_on_date 保持同一实现（两个调度器各自独立，
    不互相导入，避免循环依赖）。
    """
    want = day.astimezone(timezone(timedelta(hours=8))).date()
    for h in history or []:
        if not isinstance(h, dict):
            continue
        dt = _parse_rfc3339(str(h.get("time") or ""))
        if dt and dt.date() == want:
            return True
    return False


# ==================== 任务管理 ====================
def add_wakeup(task_data: Dict) -> str:
    """添加一条定时唤醒任务，返回 task_id。

    schedule_type 统一校验，非法值抛 ValueError（与 scheduler.add_task 一致）。
    """
    tasks = load_wakeups()
    task_id = str(uuid.uuid4())[:8]
    task_data["task_id"] = task_id
    task_data["status"] = "pending"
    task_data["schedule_type"] = norm_schedule_type(task_data.get("schedule_type"))
    task_data["created_at"] = _now_bj_iso()
    task_data.setdefault("execution_history", [])
    task_data.setdefault("isolation_mode", 0)
    tasks.append(task_data)
    save_wakeups(tasks)
    schedule_type = task_data.get("schedule_type", "one_time")
    schedule_desc = task_data.get("schedule_time", "未知")
    if schedule_type == "interval":
        schedule_desc = f"每{task_data.get('interval_seconds', 3600)}秒"
    elif schedule_type == "daily":
        schedule_desc = f"每天 {task_data.get('schedule_time', '未知')}"
    info(f"[唤醒调度] 添加定时唤醒任务 {task_id}，类型={schedule_type}，时间={schedule_desc}",
         ctx=LogCtx(app_id=task_data.get("app_id", "")))
    return task_id


def delete_wakeup(task_id: str, caller_app_id: str) -> Tuple[bool, str]:
    """删除指定唤醒任务。返回 (成功, 消息)"""
    tasks = load_wakeups()
    for i, t in enumerate(tasks):
        if t.get("task_id") == task_id:
            if t.get("app_id") != caller_app_id and not get_bot_allow_manage_all_push(caller_app_id):
                return False, f"无权限删除机器人 {t.get('app_id')} 发起的定时唤醒"
            removed = tasks.pop(i)
            save_wakeups(tasks)
            info(f"[唤醒调度] 删除定时唤醒任务 {task_id} (app_id={removed.get('app_id')})",
                 ctx=LogCtx(app_id=removed.get("app_id", "")))
            return True, f"已删除定时唤醒任务 {task_id}"
    return False, f"未找到任务 {task_id}"


def update_wakeup(task_id: str, caller_app_id: str, updates: Dict) -> Tuple[bool, str]:
    """更新指定唤醒任务。返回 (成功, 消息)"""
    tasks = load_wakeups()
    for t in tasks:
        if t.get("task_id") == task_id:
            if t.get("app_id") != caller_app_id and not get_bot_allow_manage_all_push(caller_app_id):
                return False, f"无权限修改机器人 {t.get('app_id')} 发起的定时唤醒"
            # 与推送一样走共享核心，保证两边行为一致
            ok_u, err_u = apply_wakeup_updates(t, updates)
            if not ok_u:
                return False, err_u
            save_wakeups(tasks)
            info(f"[唤醒调度] 更新定时唤醒任务 {task_id}", ctx=LogCtx(app_id=t.get("app_id", "")))
            return True, f"已更新定时唤醒任务 {task_id}"
    return False, f"未找到任务 {task_id}"


def list_wakeups(caller_app_id: str) -> List[Dict]:
    """列出当前机器人可见的定时唤醒任务。
    可见规则：
    - 自己的任务始终可见
    - 其他机器人的任务：需要同时满足 caller 的 ALLOW_CROSS_BOT_WAKEUP=1 和对方的 ALLOW_CROSS_BOT_WAKEUP_INCOMING=1
    - ALLOW_MANAGE_ALL_PUSH=1 可见所有
    """
    tasks = load_wakeups()
    can_manage_all = get_bot_allow_manage_all_push(caller_app_id)
    if can_manage_all:
        return tasks
    caller_can_wakeup = get_bot_allow_cross_wakeup(caller_app_id)
    result = []
    for t in tasks:
        t_app_id = t.get("app_id", "")
        if t_app_id == caller_app_id:
            result.append(t)
        elif caller_can_wakeup and get_bot_allow_cross_wakeup_incoming(t_app_id):
            result.append(t)
    return result


# ==================== 唤醒执行 ====================
def build_wakeup_message(task: Dict, description: str = None, tag: str = "定时唤醒") -> str:
    """构建唤醒注入的消息内容。

    tag 用于区分来源：【定时唤醒】= 调度器任务，【API唤醒】= 通过 API 立即触发。
    """
    initiator = task.get("initiator", {}) or {}
    if description is None:
        description = task.get("description", "")

    bj_now = _now_bj()
    time_str = bj_now.strftime("%Y-%m-%d %H:%M:%S")

    initiator_name = initiator.get("name", "未知用户")
    initiator_id = initiator.get("id", "未知")
    initiator_group_id = initiator.get("group_id", "")
    initiator_group_name = initiator.get("group_name", "")

    if initiator.get("type") == "group" and initiator_group_id:
        return (
            f"【{tag}】\n"
            f"当前时间: {time_str}\n"
            f"发起人: {initiator_name}（{initiator_id}）\n"
            f"来自群: {initiator_group_name}（{initiator_group_id}）\n"
            f"唤醒说明: {description}"
        )
    return (
        f"【{tag}】\n"
        f"当前时间: {time_str}\n"
        f"发起人: {initiator_name}（{initiator_id}）\n"
        f"唤醒说明: {description}"
    )


async def run_wakeup_on_target(task: Dict, target: Dict, bot_cache: Dict[str, BotClient],
                               tag: str = "定时唤醒") -> Dict:
    """对单个目标执行一次唤醒（模拟用户输入 → 触发 AI 回复）。

    返回 {"type","id","name","ok","error"}。
    供定时调度器与 API 立即唤醒共用。
    """
    app_id = task.get("app_id", "")
    t_type = target.get("type", "")  # "user" or "group"
    t_id = target.get("id", "")
    t_app_id = target.get("app_id", app_id)
    t_name = target.get("name", "")

    # 每个目标的标识不同，在函数开头按目标构造 ctx（原 set_log_context 已废弃）
    ctx = LogCtx(app_id=t_app_id, thread_key=f"{'c2c' if t_type == 'user' else 'group'}_{t_id}",
                 thread_display=t_name)

    if not t_id:
        warn(f"[唤醒] 任务 {task.get('task_id')} 目标缺少 id，跳过", ctx=ctx)
        return {"type": t_type, "id": t_id, "name": t_name,
                "ok": False, "error": "目标缺少 id"}

    api_msg_type = "c2c" if t_type == "user" else t_type
    thread_key = f"{'c2c' if t_type == 'user' else 'group'}_{t_id}"

    # ==================== 实时开关：目标机器人已关闭 AI 回复 ====================
    # 直接判定为错误，不创建 BotClient、不写历史、不调用 AI、不发送任何消息
    if get_bot_disable_ai_reply(t_app_id):
        warn(f"[唤醒] 任务 {task.get('task_id')} 目标 {t_id}（{t_app_id}）"
             f" 跳过：目标机器人ai处于关闭状态", ctx=ctx)
        return {"type": t_type, "id": t_id, "name": t_name,
                "ok": False, "error": "目标机器人ai处于关闭状态"}

    # 获取或创建 BotClient
    if t_app_id not in bot_cache:
        all_bots = {b["APP_ID"]: b["APP_SECRET"] for b in get_bots() if b.get("APP_ID")}
        secret = all_bots.get(t_app_id)
        if not secret:
            error(f"[唤醒] 未找到机器人 {t_app_id} 的凭证，跳过目标 {t_id}", ctx=ctx)
            return {"type": t_type, "id": t_id, "name": t_name,
                    "ok": False, "error": f"未找到目标机器人 {t_app_id} 的凭证"}
        bot_cache[t_app_id] = BotClient(t_app_id, secret)
    sender = bot_cache[t_app_id]

    initiator = task.get("initiator", {}) or {}
    initiator_id = initiator.get("id", "未知")
    initiator_name = initiator.get("name", "未知用户")
    isolation_mode = task.get("isolation_mode", 0)
    wakeup_msg = build_wakeup_message(task, tag=tag)

    # 1. 记录到聊天历史（标记 is_wakeup=1，API 返回，发给 AI 前会剥离）
    append_message(thread_key, "user", wakeup_msg, is_wakeup=True,
                   username=initiator_name, user_id=initiator_id)
    info(f"[唤醒] 任务 {task.get('task_id')} 已写入聊天历史: {thread_key}", ctx=ctx)

    # 2. 尝试调用 AI 生成回复
    locked = False
    try:
        # 动态导入避免循环依赖
        from ai import generate_reply

        # 隔离模式：唤醒期间不让用户消息打断本次唤醒处理。
        # 通过 wakeup_locked_threads 登记，用户消息到达时只入队等待，
        # 不会取消本次唤醒；唤醒结束后自动释放并触发队列处理。
        if isolation_mode == 1:
            from msg import lock_wakeup, unlock_wakeup
            lock_wakeup(thread_key)
            locked = True
            info(f"[唤醒] 隔离模式已开启，线程 {thread_key} 唤醒期间用户消息不会打断本次处理", ctx=ctx)

        # 构建模拟的 parsed JSON（供 AI 使用）
        raw_json = json.dumps({
            "author_id": initiator_id,
            "username": initiator_name,
            "msg_type": api_msg_type,
            "recipient_id": t_id,
            "content": wakeup_msg,
            "is_wakeup": True,
        }, ensure_ascii=False)

        reply, sent_success, skip_reply_called = await generate_reply(
            thread_key,
            wakeup_msg,
            initiator_name,
            api_msg_type,
            raw_json,
            sender,
            msg_id=None,
            recipient_id=t_id,
        )

        # skip_reply：机器人已通过工具（send_text/send_media 等）完成回复，
        # 主动结束工具循环、不再生成文本，属于正常成功，不算失败
        if skip_reply_called:
            info(f"[唤醒] 任务 {task.get('task_id')} 目标 {t_id} 调用了 skip_reply，"
                 f"视为成功（回复已由工具完成）", ctx=ctx)
            return {"type": t_type, "id": t_id, "name": t_name, "ok": True, "error": ""}
        elif sent_success:
            info(f"[唤醒] 任务 {task.get('task_id')} AI 回复已由 generate_reply 自动发送并记录（目标 {t_id}）", ctx=ctx)
            return {"type": t_type, "id": t_id, "name": t_name, "ok": True, "error": ""}
        elif reply:
            append_message(thread_key, "assistant", reply)
            info(f"[唤醒] 任务 {task.get('task_id')} 回复未发送，已记录到历史（目标 {t_id}）", ctx=ctx)
            return {"type": t_type, "id": t_id, "name": t_name, "ok": False, "error": "回复未发送"}
        else:
            info(f"[唤醒] 任务 {task.get('task_id')} AI 回复为空（目标 {t_id}）", ctx=ctx)
            return {"type": t_type, "id": t_id, "name": t_name, "ok": False, "error": "AI 回复为空"}
    except Exception as e:
        error(f"[唤醒] 任务 {task.get('task_id')} 目标 {t_id} AI 调用异常: {e}", ctx=ctx)
        return {"type": t_type, "id": t_id, "name": t_name, "ok": False, "error": str(e)[:100]}
    finally:
        # 隔离模式：唤醒结束，释放锁；期间堆积的用户消息此时再正常处理
        if locked:
            try:
                from msg import unlock_wakeup, flush_pending_after_wakeup
                unlock_wakeup(thread_key)
                info(f"[唤醒] 隔离唤醒结束，已释放线程 {thread_key}", ctx=ctx)
                flush_pending_after_wakeup(thread_key, sender)
            except Exception as e:
                warn(f"[唤醒] 释放隔离锁异常: {e}", ctx=ctx)


async def execute_wakeup(task: Dict, bot_cache: Dict[str, BotClient]) -> List[Dict]:
    """
    执行一条定时唤醒任务。
    模拟用户发消息，记录聊天记录，触发 AI 回复。
    支持多目标：遍历 task["targets"] 列表，每个目标独立执行唤醒。
    bot_cache: 已创建的 BotClient 缓存 {app_id: BotClient}

    返回每个目标的执行结果列表：
        [{"type","id","name","ok","error"}, ...]
    """
    targets = task.get("targets", [])
    if not targets:
        warn(f"[唤醒调度] 任务 {task.get('task_id')} 无目标列表，跳过",
             ctx=LogCtx(app_id=task.get("app_id", "")))
        return []

    results: List[Dict] = []
    for target in targets:
        results.append(await run_wakeup_on_target(task, target, bot_cache, tag="定时唤醒"))

    return results


async def check_and_execute_wakeups():
    """检查并执行到期的定时唤醒任务（由后台循环调用）"""
    tasks = load_wakeups()
    now = _now_bj()
    bot_cache: Dict[str, BotClient] = {}

    due = []
    remaining = []

    for t in tasks:
        status = t.get("status", "pending")
        if status != "pending":
            remaining.append(t)
            continue

        schedule_type = t.get("schedule_type", "one_time")
        task_id = t.get("task_id", "?")
        # 每个任务的标识可能不同，因此在循环内按任务构造 ctx
        ctx = LogCtx(app_id=t.get("app_id", ""))

        if schedule_type == "one_time":
            # 一次性：比较 schedule_time 是否到期
            sched = _parse_rfc3339(t.get("schedule_time", ""))
            if sched and now >= sched:
                info(f"[唤醒调度] 一次性任务 {task_id} 到期，准备执行", ctx=ctx)
                due.append(t)
                # 最终状态由下面的执行循环按实际结果写成 completed / failed
                remaining.append(t)
            else:
                remaining.append(t)

        elif schedule_type == "daily":
            # 每天：首次按完整 schedule_time（含日期）触发，之后每天同一时分
            sched = _parse_rfc3339(t.get("schedule_time", ""))
            if sched:
                history = t.get("execution_history", [])
                if not history:
                    # 首次执行：按完整时间（含日期）触发
                    if now >= sched:
                        info(f"[唤醒调度] 每日任务 {task_id} 首次到期（{sched}），准备执行", ctx=ctx)
                        due.append(t)
                else:
                    # 后续执行：每天同一时分
                    today_sched = now.replace(hour=sched.hour, minute=sched.minute, second=sched.second, microsecond=0)
                    # 按日期比较（不再用字符串前缀），见 _executed_on_date
                    executed_today = _executed_on_date(history, now)
                    if not executed_today and now >= today_sched:
                        info(f"[唤醒调度] 每日任务 {task_id} 到期（每天 {today_sched.time()}），准备执行", ctx=ctx)
                        due.append(t)
                remaining.append(t)
            else:
                # schedule_time 无效，跳过
                warn(f"[唤醒调度] 每日任务 {task_id} schedule_time 无效: {t.get('schedule_time')}", ctx=ctx)
                remaining.append(t)

        elif schedule_type == "interval":
            # 间隔执行：检查距离上次执行是否已达到 interval_seconds
            interval = int(t.get("interval_seconds", 3600))
            history = t.get("execution_history", [])
            last_time = None
            if history:
                try:
                    last_entry = history[-1]
                    last_time = _parse_rfc3339(last_entry.get("time", ""))
                except Exception:
                    pass

            if last_time is None:
                # 从未执行过：检查初始时间
                start_time = _parse_rfc3339(t.get("schedule_time", ""))
                if start_time is None:
                    # 没有起始时间 → 立即执行第一次
                    info(f"[唤醒调度] 间隔任务 {task_id} 无起始时间，立即执行首次", ctx=ctx)
                    due.append(t)
                elif now >= start_time:
                    info(f"[唤醒调度] 间隔任务 {task_id} 首次到期（{start_time}），准备执行", ctx=ctx)
                    due.append(t)
                remaining.append(t)
            else:
                next_time = last_time + timedelta(seconds=interval)
                if now >= next_time:
                    info(f"[唤醒调度] 间隔任务 {task_id} 到期（上次 {last_time.isoformat()}，间隔 {interval}s），准备执行", ctx=ctx)
                    due.append(t)
                remaining.append(t)
        else:
            warn(f"[唤醒调度] 任务 {task_id} 未知类型: {schedule_type}", ctx=ctx)
            remaining.append(t)

    if not due:
        return

    info(f"[唤醒调度] 发现 {len(due)} 个待执行唤醒任务", ctx=None)

    # 执行到期任务
    for t in due:
        now_iso = _now_bj_iso()
        # 每个待执行任务的标识可能不同，循环内重新构造 ctx
        ctx = LogCtx(app_id=t.get("app_id", ""))
        try:
            target_results = await execute_wakeup(t, bot_cache)
            # 记录执行历史（含每个目标的成功/失败信息）
            history = t.setdefault("execution_history", [])
            success_count = sum(1 for r in target_results if r.get("ok"))
            fail_count = len(target_results) - success_count
            # 只写规范状态：全部成功 = completed，其余一律 failed。
            # 唤醒是直接拿返回值判定，不存在陈旧 result 问题；
            # 但仍要求至少有一个成功目标，避免「无目标」被算成完成。
            overall = "completed" if (success_count > 0 and fail_count == 0) else "failed"
            history.append({
                "time": now_iso,
                "status": overall,
                "success_count": success_count,
                "fail_count": fail_count,
                "total": len(target_results),
                "target_results": target_results,
            })
            if len(history) > 3:
                t["execution_history"] = history[-3:]
            # 任务级状态跟随实际结果，否则失败任务永远停在初始值
            t["status"] = overall
            t["finished_at"] = now_iso
        except Exception as e:
            error(f"[唤醒调度] 任务 {t.get('task_id')} 执行异常: {e}", ctx=ctx)
            t.setdefault("execution_history", []).append({
                "time": now_iso, "status": "failed", "error": str(e)[:100]
            })
            t["status"] = "failed"
            t["finished_at"] = now_iso

    save_wakeups(remaining)


async def wakeup_scheduler_loop():
    """后台唤醒调度循环，由 core.py 启动"""
    info("[唤醒调度] 定时唤醒调度器已启动", ctx=None)
    while True:
        try:
            await check_and_execute_wakeups()
        except asyncio.CancelledError:
            info("[唤醒调度] 调度器已停止", ctx=None)
            break
        except Exception as e:
            error(f"[唤醒调度] 检查异常: {e}", ctx=None)
        await asyncio.sleep(CHECK_INTERVAL)