# -*- coding: utf-8 -*-
# scheduler.py — 定时推送任务调度器（支持一次性/每天/间隔调度，记录消息ID）
import json
import time
import uuid
import asyncio
from utils import atomic_write_json
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from log import info, warn, error, debug, LogCtx
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
    TASK_STATUSES, task_status_of, task_status_of_raw,
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


def _executed_on_date(history: List[Dict], day: datetime) -> bool:
    """判断执行历史里是否存在"发生在 day 这一天（北京时间）"的记录。

    原先用 h["time"].startswith("2026-09-30") 做前缀比较，隐含假设 time 一定是
    "YYYY-MM-DD..." 开头且不带时区换算。写入侧目前是北京时间 ISO 字符串，
    但一旦改成 UTC 存储、换成 "2026/09/30" 分隔符、或前补空格，前缀比较就会
    静默判错 —— 表现为"每日任务在同一天被重复执行"或"当天不再执行"。

    这里改为解析成 datetime 再按北京时间比较日期，兼容带时区（含 Z）、
    空格分隔、纯日期等写法；无法解析的记录一律忽略（不参与"今天已执行"判断，
    宁可多执行一次，也不要因为一条脏记录导致当天彻底不执行）。
    """
    want = day.astimezone(timezone(timedelta(hours=8))).date()
    for h in history or []:
        if not isinstance(h, dict):
            continue
        dt = _parse_rfc3339(str(h.get("time") or ""))
        if dt and dt.date() == want:
            return True
    return False


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
            # 迁移旧版状态拼写（sent -> completed 等）到规范值，
            # 一次性写回，避免旧值长期滞留、每次读取都要现场归类。
            # 必须用 task_status_of_raw：task_status_of 对周期任务会返回
            # last_run_status，拿它回写会把 status 覆盖成上次的执行结果。
            norm = task_status_of_raw(t)
            if t.get("status") != norm and norm in TASK_STATUSES:
                t["status"] = norm
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
    """保存所有定时推送任务（整表覆盖）。

    ★ 仅供「本就在同一次同步读改写内」的调用方使用（add_task / update_task /
      delete_task 等）：它们 load 后立刻改、立刻 save，中间没有 await，
      因此不存在覆盖他人写入的窗口。

      调度器的执行循环**不能**用这个函数 —— 它跨越 await 持有分钟级旧快照，
      整表写回会抹掉期间新建/修改的任务，请改用 merge_task_updates。
    """
    try:
        # 原子写：原先的 open(SCHEDULE_FILE,"w") 是先截断再写，
        # 中途失败/被杀会留下半截 JSON，下次读取解析不回 → 全部定时任务消失。
        atomic_write_json(SCHEDULE_FILE, tasks)
        return True
    except Exception as e:
        error(f"[调度] 保存定时任务文件失败: {e}", ctx=None)
        return False


def merge_task_updates(updates: Dict[str, Dict], removed_ids=None):
    """把执行结果**定点合并**回磁盘，而不是整表覆盖。

    为什么需要它：调度器 check_and_execute 在开头 load_tasks() 读入快照，
    随后对每个到期任务 await execute_task(...)（LLM 往返 + 发送重试，可达
    数十秒）。在这个 await 窗口里，API 或 AI 工具完全可能新建/修改/删除
    定时任务。若结束时用开头那份旧快照整表写回，这些改动会被**静默抹掉** ——
    接口已返回 200 和 task_id，任务却不存在了。

    因此这里以「磁盘现状」为基准，只把本次确实执行过的任务（updates 的键）
    的字段覆盖上去；磁盘上新增的条目原样保留，磁盘上已删除的不会被复活。

    updates:     {task_id: {字段: 新值}}  仅含本次执行真正改动过的字段
    removed_ids: 本次执行后应当移除的 task_id 集合（如一次性任务执行完毕）
    """
    try:
        fresh = load_tasks()
    except Exception as e:
        error(f"[调度] 合并任务状态前读取失败，放弃本次写入: {e}", ctx=None)
        return False

    if removed_ids:
        removed_ids = set(removed_ids)
        fresh = [t for t in fresh if t.get("task_id") not in removed_ids]

    changed = 0
    for t in fresh:
        upd = updates.get(t.get("task_id"))
        if not upd:
            continue
        # 只覆盖本次执行改动的字段 —— 磁盘上的其它字段（用户刚改过的
        # 内容/时间/目标）必须原样保留，否则同样等于回滚用户的修改
        for k, v in upd.items():
            if t.get(k) != v:
                t[k] = v
                changed += 1

    try:
        atomic_write_json(SCHEDULE_FILE, fresh)
        debug(f"[调度] 已合并 {len(updates)} 个任务的执行状态（{changed} 处变更）",
              ctx=None)
        return True
    except Exception as e:
        error(f"[调度] 合并写入定时任务文件失败: {e}", ctx=None)
        return False


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

    可见规则（按「谁在问」分两种口径，别混淆）：

    1) 运维视角 —— 全局密钥
       调用方以空 caller_app_id 表达自己是全局密钥（见 api_server
       的 _resolve_app_id 与 /api/push/list 的 caller 计算）。此时返回全量，
       供管理面板一把查看。

    2) 机器人密钥 —— 一律只回自己的任务
       **即使该机器人开了 ALLOW_MANAGE_ALL_PUSH=1 也不并入他人任务**：
       那个开关的语义是「允许它对别人的任务执行管理动作」
       （delete/update/revoke 走各自的 handler），而不是「允许它读取
       别人的任务内容」。任务体里含 targets（他人群/用户 openid）与
       content（推送正文），一旦并入即泄露他人会话标识与消息内容。

       同理，对方 ALLOW_CROSS_BOT_PUSH_INCOMING=1 只表示「允许被推送」，
       不表示允许读取任务列表 —— 原实现据此并入，在本项目默认配置
       （9 个 ALLOW_CROSS_* 全为 1）下等于默认泄露。
    """
    tasks = load_tasks()

    # 全局密钥（无以归属的 caller）→ 运维全量视图
    if not caller_app_id:
        return tasks

    # 机器人密钥：只回自己的任务
    return [t for t in tasks if t.get("app_id", "") == caller_app_id]


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
        # 必须显式落一个「失败」结果再返回。
        # 否则 result 会保持上一轮的旧值（或压根不存在），
        # 调用方读到 fail_count=0 就误判成「执行完成」——
        # 一个目标都没有、什么都没发出去，绝不算完成。
        task["result"] = {
            "success_count": 0,
            "fail_count": 0,
            "total": 0,
            "details": [],
            "no_targets": True,
        }
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

    # 本循环只负责挑出「到期该执行」的任务（due）。
    # 不再构建 remaining 列表 —— 它是旧版 save_tasks(remaining) 整表覆盖的残留：
    # 自从改为 merge_task_updates 定点合并后，未到期任务的落盘状态一律以磁盘
    # 现状为准，磁盘上的条目既不会被丢弃也不会被复活，无需再收集一份副本。

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
                    # 这里不再预置 "sent"：最终状态由下面的执行循环按实际结果
                    # 写成 completed / failed，避免「还没执行就显示完成」
                    t["executed_at"] = _now_bj_iso()

        elif schedule_type == "daily":
            if status != "pending":
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
                    # 按日期比较（不再用字符串前缀），见 _executed_on_date
                    executed_today = _executed_on_date(history, now)
                    if not executed_today and now >= today_sched:
                        info(f"[调度] 每日任务 {task_id} 到期（每天 {today_sched.time()}），准备执行", ctx=ctx)
                        due.append(t)
            else:
                warn(f"[调度] 每日任务 {task_id} schedule_time 无效: {t.get('schedule_time')}", ctx=ctx)

        elif schedule_type == "interval":
            if status != "pending":
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
            else:
                next_time = last_time + timedelta(seconds=interval)
                if now >= next_time:
                    info(f"[调度] 间隔任务 {task_id} 到期（上次 {last_time.isoformat()}，间隔 {interval}s），准备执行", ctx=ctx)
                    due.append(t)
        else:
            warn(f"[调度] 任务 {task_id} 未知类型: {schedule_type}", ctx=ctx)

    if not due:
        return

    info(f"[调度] 发现 {len(due)} 个待执行任务", ctx=None)

    # 执行到期任务。
    # ★ 这里**不**写回开头读到的 tasks 快照 —— 执行过程跨越 await（LLM 往返 +
    #   发送重试，可达数十秒），期间 API/AI 工具可能已新建或修改任务。改为把
    #   本次真正改动的字段收集到 task_updates，最后 merge_task_updates 定点合并。
    task_updates: Dict[str, Dict] = {}
    for t in due:
        now_iso = _now_bj_iso()
        task_id = t.get("task_id")
        schedule_type = t.get("schedule_type", "one_time")
        # 每个待执行任务的标识可能不同，循环内重新构造 ctx
        ctx = LogCtx(app_id=t.get("app_id", ""))
        updates: Dict[str, Any] = {}
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
            # 只写规范状态。三态判定（M10）：
            #   全成功 → completed
            #   有成功也有失败 → partial（此前被误记为 completed，掩盖了
            #                        多目标里有一半没发出去的事实）
            #   全失败/无目标 → failed
            # 判定必须同时看 total 与 sc，不能只看 fail_count == 0——
            # 无目标或结果缺失时 fc 也是 0，只看它会把「什么都没发出去」
            # 误判成「执行完成」（这正是之前失败被算进完成的根因）。
            if to > 0 and sc > 0 and fc == 0:
                run_status = "completed"
            elif sc > 0 and fc > 0:
                run_status = "partial"
            else:
                run_status = "failed"
            history.append({
                "time": now_iso,
                "status": run_status,
                "success_count": sc,
                "fail_count": fc,
                "total": to,
                "target_results": target_results,
            })
            updates["execution_history"] = history[-3:]
            # 周期性任务（daily/interval）执行后必须把 status 复位为 pending，
            # 否则会永久停摆：上方 407/430 行的到期判定要求 status=="pending"，
            # 一旦写成 completed/failed 就在下一轮被直接跳过，再也不执行 ——
            # 表现为「每日推送跑了一次就没了」，且状态显示为「已结束」而非出错。
            # 一次性任务（one_time）保持 run_status，因为它本就只跑一次。
            if schedule_type in ("daily", "interval"):
                updates["status"] = "pending"
            else:
                updates["status"] = run_status
            # 最近一次执行结果单独留档，供列表展示与「执行失败」筛选使用
            # （status 复位后不能再承载这个信息，故必须有独立字段）
            updates["last_run_status"] = run_status
            updates["finished_at"] = now_iso
        except Exception as e:
            error(f"[调度] 任务 {task_id} 执行异常: {e}", ctx=ctx)
            hist = t.setdefault("execution_history", [])
            hist.append({
                "time": now_iso, "status": "failed", "error": str(e)[:100]
            })
            updates["execution_history"] = hist[-3:]
            # 同成功路径：周期任务即便本轮异常也要复位，否则一次网络抖动
            # 就会让该任务此后永久不再执行
            if schedule_type in ("daily", "interval"):
                updates["status"] = "pending"
            else:
                updates["status"] = "failed"
            updates["last_run_status"] = "failed"
            updates["finished_at"] = now_iso

        if task_id:
            task_updates[task_id] = updates
        else:
            warn("[调度] 到期任务缺少 task_id，其执行结果无法回写", ctx=ctx)

    # 定点合并回磁盘（磁盘上新增的任务保留，已删除的不复活）
    merge_task_updates(task_updates)


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