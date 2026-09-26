# -*- coding: utf-8 -*-
# scheduler.py — 定时推送任务调度器（支持一次性/每天/间隔调度，记录消息ID）
import json
import time
import uuid
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from log import info, warn, error, LogCtx
from config import BASE_DIR, get_bots, get_bot_allow_manage_all_push, \
    get_bot_allow_cross_push, get_bot_allow_cross_push_incoming
from client import BotClient, send_with_policy
from memory import load_user_map, load_mirror, get_user_name, get_group_name_from_mirror, \
    append_message


SCHEDULE_FILE = BASE_DIR / "scheduled_push.json"
CHECK_INTERVAL = 5  # 秒，后台检查周期

# 共享核心：合法类型 / 字段白名单 / 参数归一化。见 task_core.py。
from task_core import (  # noqa: E402
    VALID_SCHEDULE_TYPES, PUSH_UPDATE_FIELDS,
    norm_schedule_type, apply_push_updates, normalize_push_task,
)


# ==================== 时间工具 ====================
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


# ==================== 存储 ====================
def load_tasks() -> List[Dict]:
    """加载所有定时推送任务，迁移旧数据（无 schedule_type 的视为 one_time）"""
    if not SCHEDULE_FILE.exists():
        return []
    try:
        with open(SCHEDULE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        tasks = data if isinstance(data, list) else []
        # 自动迁移旧数据
        needs_save = False
        for t in tasks:
            if "schedule_type" not in t:
                t["schedule_type"] = "one_time"
                needs_save = True
            # 迁移早期 API 写入的扁平媒体字段 -> 执行器认识的嵌套 media
            if not t.get("media") and t.get("media_source"):
                normalize_push_task(t)
                needs_save = True
            # 迁移旧版 push_message 记录的 details（缺少 ok/error 字段）
            result = t.get("result")
            if result and isinstance(result.get("details"), list):
                for d in result["details"]:
                    if "ok" not in d:
                        msg_ids = d.get("message_ids", [])
                        d["ok"] = bool(msg_ids and any(m.get("msg_id") for m in msg_ids))
                        needs_save = True
                    if "error" not in d:
                        d["error"] = ""
                        needs_save = True
        if needs_save:
            save_tasks(tasks)
            info("[调度] 已自动迁移旧版推送任务数据（补全 ok/error 字段）", ctx=None)
        return tasks
    except Exception as e:
        warn(f"[调度] 加载定时任务文件失败: {e}", ctx=None)
        return []


def save_tasks(tasks: List[Dict]):
    """保存所有定时推送任务"""
    try:
        SCHEDULE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SCHEDULE_FILE, "w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)
    except Exception as e:
        error(f"[调度] 保存定时任务文件失败: {e}", ctx=None)


# ==================== 任务管理 ====================
def add_task(task_data: Dict) -> str:
    """添加一条定时推送任务，返回 task_id。

    schedule_type 会在这里统一校验；非法值抛 ValueError，
    避免脏数据进库后只在后台刷「未知类型」告警。
    """
    tasks = load_tasks()
    task_id = str(uuid.uuid4())[:8]
    task_data["task_id"] = task_id
    task_data["status"] = "pending"
    task_data["schedule_type"] = norm_schedule_type(task_data.get("schedule_type"))
    task_data["created_at"] = _now_bj_iso()
    task_data.setdefault("execution_history", [])
    tasks.append(task_data)
    save_tasks(tasks)
    schedule_type = task_data.get("schedule_type", "one_time")
    schedule_desc = task_data.get("schedule_time", "未知")
    if schedule_type == "interval":
        schedule_desc = f"每{task_data.get('interval_seconds', 3600)}秒"
    elif schedule_type == "daily":
        schedule_desc = f"每天 {task_data.get('schedule_time', '未知')}"
    info(f"[调度] 添加定时推送任务 {task_id}，类型={schedule_type}，时间={schedule_desc}",
         ctx=LogCtx(app_id=task_data.get("app_id", "")))
    return task_id


def delete_task(task_id: str, caller_app_id: str) -> Tuple[bool, str]:
    """删除指定任务。返回 (成功, 消息)"""
    tasks = load_tasks()
    for i, t in enumerate(tasks):
        if t.get("task_id") == task_id:
            if t.get("app_id") != caller_app_id and not get_bot_allow_manage_all_push(caller_app_id):
                return False, f"无权限删除机器人 {t.get('app_id')} 发起的定时推送"
            removed = tasks.pop(i)
            save_tasks(tasks)
            info(f"[调度] 删除定时推送任务 {task_id} (app_id={removed.get('app_id')})",
                 ctx=LogCtx(app_id=removed.get("app_id", "")))
            return True, f"已删除推送任务 {task_id}"
    return False, f"未找到任务 {task_id}"


def update_task(task_id: str, caller_app_id: str, updates: Dict) -> Tuple[bool, str]:
    """更新指定任务。返回 (成功, 消息)"""
    tasks = load_tasks()
    for t in tasks:
        if t.get("task_id") == task_id:
            if t.get("app_id") != caller_app_id and not get_bot_allow_manage_all_push(caller_app_id):
                return False, f"无权限修改机器人 {t.get('app_id')} 发起的定时推送"
            # 字段白名单、schedule_type 校验、targets 归一化统一走共享核心，
            # 保证 API 与 AI 工具改同一条任务得到完全相同的结果
            ok_u, err_u = apply_push_updates(t, updates)
            if not ok_u:
                return False, err_u
            save_tasks(tasks)
            info(f"[调度] 更新定时推送任务 {task_id}", ctx=LogCtx(app_id=t.get("app_id", "")))
            return True, f"已更新推送任务 {task_id}"
    return False, f"未找到任务 {task_id}"


def list_tasks(caller_app_id: str) -> List[Dict]:
    """列出当前机器人可见的所有定时推送任务（含全部状态）。
    可见规则：
    - 自己的任务始终可见
    - 其他机器人的任务：需要同时满足 caller 的 ALLOW_CROSS_BOT_PUSH=1 和对方的 ALLOW_CROSS_BOT_PUSH_INCOMING=1
    - ALLOW_MANAGE_ALL_PUSH=1 可见所有
    """
    tasks = load_tasks()
    can_manage_all = get_bot_allow_manage_all_push(caller_app_id)
    if can_manage_all:
        return tasks
    caller_can_push = get_bot_allow_cross_push(caller_app_id)
    result = []
    for t in tasks:
        t_app_id = t.get("app_id", "")
        if t_app_id == caller_app_id:
            result.append(t)
        elif caller_can_push and get_bot_allow_cross_push_incoming(t_app_id):
            result.append(t)
    return result


# ==================== 后台执行 ====================
async def execute_task(task: Dict, bot_cache: Dict[str, BotClient]):
    """
    执行一次定时推送任务，执行结果和消息ID写入 task。
    bot_cache: 已创建的 BotClient 缓存 {app_id: BotClient}
    """
    app_id = task.get("app_id", "")
    targets = task.get("targets", [])
    content = task.get("content", "")
    # 兼容早期 API 写的扁平 media_source/file_type/file_name
    media = normalize_push_task(task).get("media")  # {source, file_type, file_name} 或 None

    # 该任务的标识（原 set_log_context 已废弃，改为显式传 ctx）
    task_ctx = LogCtx(app_id=app_id)

    if not targets:
        warn(f"[调度] 任务 {task.get('task_id')} 无目标，跳过", ctx=task_ctx)
        return

    # 构建定时推送 footer
    initiator = task.get("initiator_info", {})
    created_raw = task.get("created_at", "")
    created_fmt = ""
    if created_raw:
        try:
            dt = datetime.fromisoformat(created_raw)
            created_fmt = dt.strftime("%Y.%m.%d %H:%M")
        except Exception:
            created_fmt = created_raw
    if initiator.get("type") == "group":
        g_name = initiator.get("group_name", "")
        g_id = initiator.get("group_id", "")
        i_name = initiator.get("name", "未知")
        i_id = initiator.get("id", "")
        footer = f"\n\n—— 来自 群 {g_name}（{g_id}）的 {i_name}（用户 {i_id}）发起的定时推送（{created_fmt}发起）"
    elif initiator.get("type") == "user":
        i_name = initiator.get("name", "未知")
        i_id = initiator.get("id", "")
        footer = f"\n\n—— 来自 {i_name}（用户 {i_id}）发起的定时推送（{created_fmt}发起）"
    elif initiator.get("type") == "system":
        footer = f"\n\n—— 来自系统发起的定时推送（{created_fmt}发起）"
    else:
        footer = ""
    full_content = content + footer if footer else content

    # ---------- Markdown 支持 ----------
    # 任务可通过 message_type="markdown" + markdown 字段指定富文本推送，
    # 正文取 markdown 内容；未指定时按普通文本处理。
    task_mtype = str(task.get("message_type") or task.get("msg_type") or "text").strip().lower()
    use_md = task_mtype in ("markdown", "md", "2")
    md_body = (task.get("markdown") or content) if use_md else None
    md_full = ((md_body + footer) if footer else md_body) if use_md else None

    # 缓存 bot 凭证
    all_bots = {b["APP_ID"]: b["APP_SECRET"] for b in get_bots() if b.get("APP_ID")}

    success_count = 0
    fail_count = 0
    details = []  # 记录每个目标的发送结果，含消息ID
    for t in targets:
        t_type = t.get("type", "")
        t_id = t.get("id", "")
        t_app_id = t.get("app_id", app_id)
        api_msg_type = "c2c" if t_type == "user" else t_type
        # 目标可能属于不同机器人，按目标重新构造 ctx
        ctx = task_ctx.with_(app_id=t_app_id)

        # 获取或创建 BotClient
        if t_app_id not in bot_cache:
            secret = all_bots.get(t_app_id)
            if not secret:
                error(f"[调度] 未找到机器人 {t_app_id} 的凭证，跳过目标 {t_id}", ctx=ctx)
                details.append({"type": t_type, "id": t_id, "app_id": t_app_id, "ok": False, "error": "无凭证", "message_ids": []})
                fail_count += 1
                continue
            bot_cache[t_app_id] = BotClient(t_app_id, secret)
        sender = bot_cache[t_app_id]

        target_ok = False
        target_error = ""
        target_msg_ids = []  # 记录该目标所有发送的消息ID
        # 发送（支持媒体）
        try:
            thread_key = f"{'c2c' if t_type == 'user' else 'group'}_{t_id}"
            ms = media.get("source") if media else None
            ft = media.get("file_type") if media else None
            fn = media.get("file_name") if media else None

            # 双发策略与发送编排统一由 client.send_with_policy 处理。
            # 注意：这里传裸正文（content / md_body），footer 由 helper 统一追加，
            # 避免与 full_content / md_full 里已含的 footer 重复拼接。
            sent = await send_with_policy(
                sender, api_msg_type, t_id, content,
                media_source=ms, file_type=ft, file_name=fn,
                markdown=md_body if use_md else None,
                footer=footer,
            )
            target_msg_ids.extend(sent["message_ids"])
            target_ok = sent["ok"]
            if not target_ok:
                target_error = sent["error"] or "发送失败"

            if target_ok:
                success_count += 1
                # 媒体条用「媒体：<正文>（<URL>）」，文本条只带正文，两者分开记录
                hist_text = md_full if use_md else full_content
                _media_src = media.get("source") if media else None
                # 逐条写入历史并带上各自 msg_id/msg_idx，供撤回与 /api/history 使用
                if sent["message_ids"]:
                    for _m in sent["message_ids"]:
                        _is_media = _m.get("type") in ("media", "media_combined")
                        _txt = (f"媒体：{content}（{_media_src}）"
                                if (_is_media and _media_src) else hist_text)
                        append_message(thread_key, "assistant", _txt,
                                       msg_id=_m.get("msg_id"),
                                       msg_idx=_m.get("msg_idx"),
                                       is_markdown=use_md and not _is_media,
                                       media_url=_media_src if _is_media else None)
                else:
                    append_message(thread_key, "assistant", hist_text,
                                   is_markdown=use_md)
            else:
                fail_count += 1
            await asyncio.sleep(0.3)
        except Exception as e:
            error(f"[调度] 任务 {task.get('task_id')} 发送异常: {e}", ctx=ctx)
            target_ok = False
            target_error = str(e)[:80]
            fail_count += 1

        details.append({
            "type": t_type,
            "id": t_id,
            "app_id": t_app_id,
            "ok": target_ok,
            "error": target_error,
            "message_ids": target_msg_ids,
        })

    # 存储执行结果到任务
    task["result"] = {
        "success_count": success_count,
        "fail_count": fail_count,
        "total": len(targets),
        "details": details,
    }
    info(f"[调度] 任务 {task.get('task_id')} 执行完成: 成功 {success_count}, 失败 {fail_count}", ctx=task_ctx)


async def check_and_execute():
    """检查并执行到期的定时推送任务（由后台循环调用）"""
    tasks = load_tasks()
    now = _now_bj()
    bot_cache: Dict[str, BotClient] = {}

    due = []
    remaining = []

    for t in tasks:
        status = t.get("status", "pending")
        schedule_type = t.get("schedule_type", "one_time")
        task_id = t.get("task_id", "?")
        # 每个任务的标识可能不同，因此在循环内按任务构造 ctx
        ctx = LogCtx(app_id=t.get("app_id", ""))

        if schedule_type == "one_time":
            if status == "pending":
                sched = _parse_rfc3339(t.get("schedule_time", ""))
                if sched and now >= sched:
                    info(f"[调度] 一次性任务 {task_id} 到期，准备执行", ctx=ctx)
                    due.append(t)
                    t["status"] = "sent"
                    t["executed_at"] = _now_bj_iso()
                    remaining.append(t)
                else:
                    remaining.append(t)
            else:
                # 已完成的 also kept
                remaining.append(t)

        elif schedule_type == "daily":
            if status != "pending":
                remaining.append(t)
                continue
            sched = _parse_rfc3339(t.get("schedule_time", ""))
            if sched:
                history = t.get("execution_history", [])
                if not history:
                    if now >= sched:
                        info(f"[调度] 每日任务 {task_id} 首次到期（{sched}），准备执行", ctx=ctx)
                        due.append(t)
                else:
                    today_sched = now.replace(hour=sched.hour, minute=sched.minute, second=sched.second, microsecond=0)
                    today_str = now.strftime("%Y-%m-%d")
                    executed_today = any(
                        h.get("time", "").startswith(today_str) for h in history
                    )
                    if not executed_today and now >= today_sched:
                        info(f"[调度] 每日任务 {task_id} 到期（每天 {today_sched.time()}），准备执行", ctx=ctx)
                        due.append(t)
                remaining.append(t)
            else:
                warn(f"[调度] 每日任务 {task_id} schedule_time 无效: {t.get('schedule_time')}", ctx=ctx)
                remaining.append(t)

        elif schedule_type == "interval":
            if status != "pending":
                remaining.append(t)
                continue
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
                start_time = _parse_rfc3339(t.get("schedule_time", ""))
                if start_time is None:
                    info(f"[调度] 间隔任务 {task_id} 无起始时间，立即执行首次", ctx=ctx)
                    due.append(t)
                elif now >= start_time:
                    info(f"[调度] 间隔任务 {task_id} 首次到期（{start_time}），准备执行", ctx=ctx)
                    due.append(t)
                remaining.append(t)
            else:
                next_time = last_time + timedelta(seconds=interval)
                if now >= next_time:
                    info(f"[调度] 间隔任务 {task_id} 到期（上次 {last_time.isoformat()}，间隔 {interval}s），准备执行", ctx=ctx)
                    due.append(t)
                remaining.append(t)
        else:
            warn(f"[调度] 任务 {task_id} 未知类型: {schedule_type}", ctx=ctx)
            remaining.append(t)

    if not due:
        return

    info(f"[调度] 发现 {len(due)} 个待执行任务", ctx=None)

    # 执行到期任务
    for t in due:
        now_iso = _now_bj_iso()
        # 每个待执行任务的标识可能不同，循环内重新构造 ctx
        ctx = LogCtx(app_id=t.get("app_id", ""))
        try:
            await execute_task(t, bot_cache)
            # 记录执行历史（含每个目标的执行情况）
            history = t.setdefault("execution_history", [])
            result = t.get("result", {})
            sc = result.get("success_count", 0)
            fc = result.get("fail_count", 0)
            to = result.get("total", sc + fc)
            # 提取每个目标的结果（不含 message_ids，旧记录不需要撤回）
            target_results = []
            for d in result.get("details", []):
                target_results.append({
                    "type": d.get("type", ""),
                    "id": d.get("id", ""),
                    "ok": d.get("ok", False),
                    "error": d.get("error", ""),
                })
            history.append({
                "time": now_iso,
                "status": "success" if fc == 0 else "partial" if sc > 0 else "failed",
                "success_count": sc,
                "fail_count": fc,
                "total": to,
                "target_results": target_results,
            })
            if len(history) > 3:
                t["execution_history"] = history[-3:]
        except Exception as e:
            error(f"[调度] 任务 {t.get('task_id')} 执行异常: {e}", ctx=ctx)
            t.setdefault("execution_history", []).append({
                "time": now_iso, "status": "failed", "error": str(e)[:100]
            })

    save_tasks(remaining)


async def scheduler_loop():
    """后台调度循环，由 core.py 启动"""
    info("[调度] 定时推送调度器已启动", ctx=None)
    while True:
        try:
            await check_and_execute()
        except asyncio.CancelledError:
            info("[调度] 调度器已停止", ctx=None)
            break
        except Exception as e:
            error(f"[调度] 检查异常: {e}", ctx=None)
        await asyncio.sleep(CHECK_INTERVAL)