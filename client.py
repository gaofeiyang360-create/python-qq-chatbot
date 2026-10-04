# -*- coding: utf-8 -*-
# client.py — WebSocket 客户端 & QQ API 调用（BotClient、鉴权、发消息、群禁言、WS连接）
import json
import time
import asyncio
import requests
import websockets
import os
from typing import Optional, Dict, Any, Tuple, List

from config import (get_executor, get_bot_name, set_bot_name, get_bots,
                    get_bot_enabled, get_media_block)
from log import info, warn, error, debug, LogCtx
from utils import infer_file_type, needs_dual_send


# 正在处理的消息任务集合（持有引用，防止任务被 GC 提前回收）。
# 见 ws 接收循环：消息处理改为 create_task 派发，不再阻塞收包。
_message_tasks: set = set()

# 长期后台任务（心跳等）的强引用集合。
# create_task 的返回值若不持有，任务可能在运行中被 GC 回收 —— 心跳一旦
# 被回收，连接就不会再发 op=1，服务端会在心跳超时后判定掉线并断开。
_background_tasks: set = set()


def _spawn_background(coro, name: str, ctx=None):
    """把协程派发为长期后台任务，并持有强引用直到它结束。

    与 _message_tasks 分开管理：那些是短命的消息处理任务，这些是随连接
    存活的常驻任务。两者都用 add_done_callback 在结束时移除引用，
    同时**读取一次异常** —— 只 discard 而不取异常，任务内的报错会以
    "Task exception was never retrieved" 的形式丢到 stderr，
    既不进日志也不易排查。
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _done(t: asyncio.Task):
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            error(f"[后台任务 {name}] 异常退出: {type(exc).__name__}: {exc}", ctx=ctx)

    task.add_done_callback(_done)
    return task


def _log_message_task_result(t: asyncio.Task):
    """消息处理任务结束时的回调：释放引用 + 取出异常写进日志。

    取消（CancelledError）是正常路径 —— 同会话新消息到达时会取消上一条的
    处理任务，因此不算错误、不记 error。
    """
    _message_tasks.discard(t)
    if t.cancelled():
        return
    exc = t.exception()
    if exc is not None:
        error(f"[处理消息错误] 任务异常退出: {type(exc).__name__}: {exc}", ctx=None)


# ==================== BotClient：Token 管理 & QQ API 调用 ====================
class BotClient:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_info = {"access_token": None, "expires_at": 0}
        self.bot_name = "蓝狼"
        # 用于消息去重的 msg_seq 计数器
        self._msg_seq_map = {}  # key: msg_id, value: int
        # 最近一次发送消息的响应元数据，供外部读取
        self._last_send_meta: Dict[str, Any] = {}
        # 本实例发送过的全部消息（按发送顺序），供一次调用产生多条消息时读取。
        # 与 _last_send_meta 的区别：后者只保留最后一次，双发场景会覆盖前一条。
        self.sent_messages: List[Dict[str, Any]] = []
        # 上次 send_message 调用产生的消息（每次调用前清空，只含本次）
        self._call_sent: List[Dict[str, Any]] = []

    def begin_send_batch(self):
        """开始一次发送批次，重置本次调用的消息记录。"""
        self._call_sent = []
        return self._call_sent

    def get_call_sent(self) -> List[Dict[str, Any]]:
        """获取上一次 send_message 调用产生的全部消息 [{"msg_id","msg_idx"}]。

        注意：send_with_policy 会连续调用多次 send_message，需配合
        begin_send_batch() 在每次调用前重置，否则只反映最后一次。
        """
        return list(self._call_sent)

    def get_access_token(self, force_refresh: bool = False) -> str:
        ctx = LogCtx(app_id=self.app_id)
        if force_refresh or not self.token_info["access_token"] or time.time() >= self.token_info["expires_at"] - 60:
            url = "https://bots.qq.com/app/getAppAccessToken"
            headers = {"Content-Type": "application/json"}
            payload = {"appId": self.app_id, "clientSecret": self.app_secret}
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            self.token_info["access_token"] = data["access_token"]
            self.token_info["expires_at"] = time.time() + int(data["expires_in"])
            info(f"[Token] {self.app_id} 获取成功，有效期 {data['expires_in']} 秒", ctx=ctx)
        return self.token_info["access_token"]

    def get_websocket_url(self) -> str:
        token = self.get_access_token()
        url = "https://api.sgroup.qq.com/gateway"
        headers = {"Authorization": f"QQBot {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()["url"]

    # ========== 异步包装（避免阻塞事件循环） ==========
    # get_access_token / get_websocket_url 内部用的是**同步 requests**，
    # 直接从协程里调用会把整个事件循环卡住（本项目所有机器人在同一个
    # 循环上跑）：取 token 与取网关地址都是跨公网请求，一旦网络抖动，
    # 期间所有机器人的收包、定时任务、API 请求全部停摆。
    # 因此协程路径一律改用下面两个包装 —— 把阻塞调用丢进
    # config._executor 线程池，与项目里其它 requests 调用保持一致。
    async def get_access_token_async(self, force_refresh: bool = False) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            get_executor(), lambda: self.get_access_token(force_refresh)
        )

    async def get_websocket_url_async(self) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(get_executor(), self.get_websocket_url)

    # ========== 媒体上传（仅支持 URL 直接上传） ==========
    async def upload_media_by_url(self, msg_type: str, recipient_id: str,
                                  source: str, file_type: Optional[int] = None,
                                  file_name: Optional[str] = None) -> Optional[str]:
        """
        使用官方 /files 接口直接传入 URL 上传。
        返回 file_info，失败返回 None。
        """
        # 日志标识：app_id + 会话（msg_type=group_xxx / c2c_xxx）
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}")
        info(f"[上传URL] 收到 file_name: {file_name}, file_type: {file_type}", ctx=ctx)
        if not (source.startswith('http://') or source.startswith('https://')):
            warn(f"[上传URL] 错误：仅支持 HTTP/HTTPS URL，收到: {source[:80]}", ctx=ctx)
            return None

        # 确定 file_type（若未指定则自动推断）；实现见 utils.infer_file_type
        if file_type is None:
            file_type = infer_file_type(file_name, source)

        token = await self.get_access_token_async()
        endpoint = "users" if msg_type == "c2c" else "groups"
        url = f"https://api.sgroup.qq.com/v2/{endpoint}/{recipient_id}/files"
        headers = {
            "Authorization": f"QQBot {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "file_type": file_type,
            "url": source,
            "srv_send_msg": False
        }
        if file_name:
            payload["file_name"] = file_name

        info(f"[上传URL] 类型 {file_type}，目标 {source[:80]}...", ctx=ctx)
        debug(f"[上传URL] 完整 payload: {json.dumps(payload, ensure_ascii=False)}", ctx=ctx)
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=30)
            )
        except Exception as e:
            error(f"[上传URL] 请求异常: {e}", ctx=ctx)
            return None

        if resp.status_code != 200:
            error(f"[上传URL] 失败，状态码 {resp.status_code}，响应: {resp.text[:200]}", ctx=ctx)
            return None

        resp_json = resp.json()
        file_info = resp_json.get('file_info')
        if not file_info:
            warn(f"[上传URL] 响应无 file_info: {resp_json}", ctx=ctx)
            return None
        info(f"[上传URL] 成功，file_info: {file_info[:20]}...", ctx=ctx)
        return file_info

    def _parse_send_response(self, resp) -> Optional[str]:
        """从API响应中解析消息ID，同时存储 ext_info（如 msg_idx = ref_idx）到 self._last_send_meta。
        ref_idx 是这条发送消息自己在系统中的索引，其他消息引用它时会用到这个值。
        返回消息ID字符串，失败返回None。"""
        try:
            resp_json = resp.json()
            msg_id_resp = resp_json.get("id") or resp_json.get("message_id")
            # 存储完整的发送响应元数据
            ext_info = resp_json.get("ext_info") or {}
            ref_idx = ext_info.get("ref_idx") if isinstance(ext_info, dict) else None
            self._last_send_meta = {
                "id": msg_id_resp,
                "msg_idx": ref_idx,  # ref_idx = 本消息的 msg_idx
                "ref_idx": ref_idx,  # 保留旧字段名兼容
            }
            # 追加到本实例的发送记录，使"一次调用产生多条消息"可被完整读取
            if msg_id_resp:
                entry = {"msg_id": msg_id_resp, "msg_idx": ref_idx}
                self.sent_messages.append(entry)
                self._call_sent.append(entry)
            return msg_id_resp if msg_id_resp else None
        except Exception:
            self._last_send_meta = {}
            return None

    def get_last_send_msg_idx(self) -> Optional[str]:
        """获取最近一次发送消息的 msg_idx（其他消息引用本消息时使用的索引）。"""
        return self._last_send_meta.get("msg_idx")

    def get_last_send_ref_idx(self) -> Optional[str]:
        """获取最近一次发送消息的 ref_idx（等同于 msg_idx，为兼容保留）。"""
        return self._last_send_meta.get("ref_idx")

    def get_last_send_id(self) -> Optional[str]:
        """获取最近一次发送消息的 id。"""
        return self._last_send_meta.get("id")

    # ========== 发送消息（支持 msg_seq 自动去重） ==========
    async def send_message(self, msg_type: str, recipient_id: str,
                           content: Optional[str] = None,
                           msg_id: Optional[str] = None,
                           media: Optional[str] = None,
                           media_source: Optional[str] = None,
                           embed: Optional[Dict] = None,
                           file_type: Optional[int] = None,
                           file_name: Optional[str] = None,
                           msg_seq: Optional[int] = None,   # 新增可选参数
                           markdown: Optional[Any] = None,  # Markdown 消息（msg_type=2）
                           max_retries: int = 3) -> Optional[str]:
        """
        发送消息，支持文本、富媒体、Markdown。
        media_source 必须是 HTTP/HTTPS URL（不支持本地文件）。
        若上传失败，直接返回 None，无自动回退。
        如果提供了 msg_id 且未提供 msg_seq，则自动分配递增序列号。

        markdown 参数支持三种写法（优先级高于 content）：
          - str：自定义 markdown 文本 → {"markdown": {"content": "..."}}
          - dict：完整透传（可含 content / custom_template_id / params）
          - 模板：{"custom_template_id": "...", "params": [...]}

        注意：富媒体(msg_type=7)与 Markdown(msg_type=2)互斥，同时传入时富媒体优先。
        返回: 消息ID（str）表示成功，None 表示失败。
        """
        # 日志标识：app_id + 会话（msg_type=group_xxx / c2c_xxx）+ 触发本次发送的 msg_id
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}",
                     msg_id=msg_id or "")
        # ---------- 1. 获取 file_info ----------
        file_info = None
        if media_source:
            if not (media_source.startswith('http://') or media_source.startswith('https://')):
                warn(f"[发送] 错误：media_source 必须为 HTTP/HTTPS URL，收到: {media_source[:80]}", ctx=ctx)
                return None

            info(f"[发送] 调用上传，file_name={file_name}, file_type={file_type}", ctx=ctx)
            file_info = await self.upload_media_by_url(msg_type, recipient_id,
                                                       media_source, file_type, file_name)
            if not file_info:
                error("[发送] URL 上传失败，放弃发送", ctx=ctx)
                return None

        if not media_source and media:
            file_info = media

        # ---------- 2. 确定 msg_seq ----------
        if msg_id:
            if msg_seq is None:
                # 自动分配序列号
                self._msg_seq_map[msg_id] = self._msg_seq_map.get(msg_id, 0) + 1
                msg_seq = self._msg_seq_map[msg_id]
        else:
            msg_seq = None  # 主动消息可不填

        # ---------- 3. 发送消息（重试） ----------
        is_media_msg = (file_info is not None)
        media_obj = {"file_info": file_info} if is_media_msg else None

        for attempt in range(max_retries):
            token = await self.get_access_token_async()
            if msg_type == "c2c":
                url = f"https://api.sgroup.qq.com/v2/users/{recipient_id}/messages"
            else:
                url = f"https://api.sgroup.qq.com/v2/groups/{recipient_id}/messages"
            headers = {
                "Authorization": f"QQBot {token}",
                "Content-Type": "application/json"
            }
            payload: Dict[str, Any] = {}

            # ---------- 归一化 markdown 参数 ----------
            md_obj = None
            if markdown is not None and not is_media_msg:
                if isinstance(markdown, str):
                    md_obj = {"content": markdown}
                elif isinstance(markdown, dict):
                    md_obj = dict(markdown)
                else:
                    warn(f"[发送] markdown 参数类型不支持: {type(markdown).__name__}，已忽略", ctx=ctx)
                if md_obj is not None and not md_obj:
                    md_obj = None

            if md_obj is not None:
                # Markdown 消息：msg_type=2，内容放在 markdown 对象里（不再用 content）
                payload["markdown"] = md_obj
            elif content:
                payload["content"] = content

            if msg_id:
                payload["msg_id"] = msg_id
            if msg_seq is not None:
                payload["msg_seq"] = msg_seq
            if embed:
                payload["embed"] = embed
            if is_media_msg:
                payload["msg_type"] = 7
                payload["media"] = media_obj
            elif md_obj is not None:
                payload["msg_type"] = 2
            else:
                payload["msg_type"] = 0

            debug(f"[发送] 最终 payload: {json.dumps(payload, ensure_ascii=False)[:500]}", ctx=ctx)
            loop = asyncio.get_event_loop()
            try:
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                )
                if resp.status_code == 200:
                    msg_id_resp = self._parse_send_response(resp)
                    if is_media_msg:
                        info(f"[Reply] {msg_type} 富媒体消息发送成功: {content[:50] if content else '媒体'} msg_id={msg_id_resp}", ctx=ctx)
                    elif md_obj is not None:
                        preview = str(md_obj.get("content", ""))[:50]
                        kind = "模板" if md_obj.get("custom_template_id") else "自定义"
                        info(f"[Reply] {msg_type} Markdown({kind})消息发送成功: {preview} msg_id={msg_id_resp}", ctx=ctx)
                    else:
                        info(f"[Reply] {msg_type} 发送成功: {content[:50] if content else '空'} msg_id={msg_id_resp}", ctx=ctx)
                    return msg_id_resp
                else:
                    error(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                    if resp.status_code in (401, 500):
                        warn("[Reply] 强制刷新 Token", ctx=ctx)
                        # 必须用 async 包装：这是跨公网请求，同步调用会
                        # 在重试路径上卡住整个事件循环（所有机器人共用）
                        await self.get_access_token_async(force_refresh=True)
                        continue
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        error("[Reply] 发送最终失败，放弃消息", ctx=ctx)
                        # ----- 被动回复超限 → 自动切主动重试 -----
                        if msg_id:
                            try:
                                resp_json = resp.json()
                                err_code = resp_json.get("code") or resp_json.get("err_code")
                            except Exception:
                                err_code = None
                            if err_code == 40034128:
                                warn("[Reply] 检测到被动回复超限（40034128），尝试主动发送...", ctx=ctx)
                                # 去掉 msg_id 以主动发送
                                payload.pop("msg_id", None)
                                payload.pop("msg_seq", None)
                                active_resp = await loop.run_in_executor(
                                    get_executor(),
                                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                                )
                                if active_resp.status_code == 200:
                                    msg_id_resp = self._parse_send_response(active_resp)
                                    info(f"[Reply] 主动发送成功: {content[:50] if content else '空'} msg_id={msg_id_resp}", ctx=ctx)
                                    return msg_id_resp
                                else:
                                    error(f"[Reply] 主动发送也失败，状态码 {active_resp.status_code}", ctx=ctx)
                        return None
            except Exception as e:
                error(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 异常: {e}", ctx=ctx)
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    error("[Reply] 发送最终失败，放弃消息", ctx=ctx)
                    return None
        return None

    # ========== 撤回消息 ==========
    async def revoke_message(self, msg_type: str, recipient_id: str, message_id: str) -> bool:
        """
        撤回一条已发送的消息。
        msg_type: "c2c" 或 "group"
        recipient_id: 用户 openid 或群 openid
        message_id: 要撤回的消息 ID
        返回 True 表示撤回成功，False 表示失败。
        注意：发送超过2分钟的消息不可撤回。
        """
        # 日志标识：app_id + 会话（msg_type=group_xxx / c2c_xxx）
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}")
        token = await self.get_access_token_async()
        if msg_type == "c2c":
            url = f"https://api.sgroup.qq.com/v2/users/{recipient_id}/messages/{message_id}"
        else:
            url = f"https://api.sgroup.qq.com/v2/groups/{recipient_id}/messages/{message_id}"
        headers = {
            "Authorization": f"QQBot {token}",
        }
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.delete(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                info(f"[撤回] {msg_type} 消息撤回成功: {message_id} (target={recipient_id})", ctx=ctx)
                return True
            else:
                error(f"[撤回] 失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return False
        except Exception as e:
            error(f"[撤回] 异常: {e}", ctx=ctx)
            return False

    # ========== 群禁言相关 API ==========
    async def get_group_mute_status(self, group_openid: str) -> Optional[Dict]:
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/restrict_chat_setting"
        headers = {"Authorization": f"QQBot {token}"}
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.get(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                warn(f"[群禁言] 查询失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return None
        except Exception as e:
            warn(f"[群禁言] 查询异常: {e}", ctx=ctx)
            return None

    async def get_group_info(self, group_openid: str) -> Optional[Dict]:
        """
        获取群基本信息。
        GET /v2/groups/{group_openid}/info
        返回: {"group_openid": "...", "group_name": "...", ...} 或 None
        """
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/info"
        headers = {"Authorization": f"QQBot {token}"}
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.get(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                data = resp.json()
                info(f"[群信息] 获取成功: {data.get('group_name', '未知')} ({group_openid})", ctx=ctx)
                return data
            else:
                error(f"[群信息] 获取失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return None
        except Exception as e:
            error(f"[群信息] 获取异常: {e}", ctx=ctx)
            return None

    async def get_bot_state(self, group_openid: str) -> Optional[Dict]:
        """
        获取机器人在指定群中的状态信息。
        GET /v2/groups/{group_openid}/bot_state
        返回: 包含 member_role, allow_proactive_msg, recv_msg_setting 等
        """
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/bot_state"
        headers = {"Authorization": f"QQBot {token}"}
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.get(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                data = resp.json()
                info(f"[机器人状态] 获取成功: role={data.get('member_role')}, proactive={data.get('allow_proactive_msg')} ({group_openid})", ctx=ctx)
                return data
            else:
                error(f"[机器人状态] 获取失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return None
        except Exception as e:
            error(f"[机器人状态] 获取异常: {e}", ctx=ctx)
            return None

    async def set_group_mute(self, group_openid: str, op: str, member_openid: str,
                             mute_expire_at: str = "") -> Tuple[bool, int]:
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/restrict_chat_setting"
        headers = {
            "Authorization": f"QQBot {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "members": [
                {
                    "op": op,
                    "member_openid": member_openid,
                    "mute_expire_at": mute_expire_at
                }
            ]
        }
        debug(f"[群禁言] 发送请求: POST {url}", ctx=ctx)
        debug(f"[群禁言] payload: {json.dumps(payload, ensure_ascii=False)}", ctx=ctx)
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                return True, 0
            else:
                error_code = 0
                try:
                    err_data = resp.json()
                    error_code = err_data.get("err_code", err_data.get("code", 0))
                except Exception:
                    pass
                error(f"[群禁言] 设置失败，状态码 {resp.status_code}, 错误码 {error_code}, 响应: {resp.text[:500]}", ctx=ctx)
                return False, error_code
        except Exception as e:
            error(f"[群禁言] 设置异常: {e}", ctx=ctx)
            return False, -1


# ==================== WebSocket 主连接 ====================
async def main_connection(bot_client: BotClient):
    from msg import handle_message, handle_event

    # 日志标识：整个连接生命周期内的日志都带本机器人的 app_id
    ctx = LogCtx(app_id=bot_client.app_id)

    info(f"[启动] 机器人 {bot_client.app_id} 开始连接...", ctx=ctx)
    # 用 async 包装：内部是同步 requests 的跨公网请求，直接调用会卡住
    # 整个事件循环（所有机器人在同一个循环上）
    ws_url = await bot_client.get_websocket_url_async()
    info(f"[启动] 地址: {ws_url}", ctx=ctx)

    async with websockets.connect(ws_url) as ws:
        hello = await ws.recv()
        hello_data = json.loads(hello)
        debug(f"[收到 Hello] {hello}", ctx=ctx)
        heartbeat_interval = hello_data.get("d", {}).get("heartbeat_interval", 30000) / 1000.0

        token = await bot_client.get_access_token_async()
        identify = {
            "op": 2,
            "d": {
                "token": f"QQBot {token}",
                "intents": (1 << 25) | (1 << 30) | (1 << 24),
                "shard": [0, 1],
                "properties": {"os": "Linux", "browser": "MyBot", "device": "MyBot"}
            }
        }
        await ws.send(json.dumps(identify))
        debug("[鉴权] 已发送 Identify", ctx=ctx)

        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if data.get("op") == 0 and data.get("t") == "READY":
                ready_data = data.get("d", {})
                user_info = ready_data.get("user", {})
                bot_username = user_info.get("username", "蓝狼")
                bot_client.bot_name = bot_username
                set_bot_name(bot_username)
                info(f"[收到 Ready] 机器人名字: {bot_username}", ctx=ctx)
                break
            else:
                debug(f"[收到] {msg}", ctx=ctx)

        info(f"[准备就绪] 机器人 {bot_client.bot_name} 已上线", ctx=ctx)

        async def heartbeat():
            while True:
                await asyncio.sleep(heartbeat_interval)
                try:
                    await ws.send(json.dumps({"op": 1, "d": int(time.time() * 1000)}))
                    debug("[心跳] 发送", ctx=ctx)
                except websockets.ConnectionClosed:
                    info("[心跳] 连接已关闭", ctx=ctx)
                    break
                except Exception as e:
                    warn(f"[心跳错误] {e}", ctx=ctx)
                    break

                # ★ 使用时主动检测：bot 是否仍存在于配置且启用
                still_valid = False
                for b in get_bots():
                    if b.get("APP_ID") == bot_client.app_id and get_bot_enabled(bot_client.app_id):
                        still_valid = True
                        break
                if not still_valid:
                    info(f"[心跳] 机器人 {bot_client.app_id} 已被移除或禁用，关闭连接", ctx=ctx)
                    await ws.close()
                    break

        _spawn_background(heartbeat(), f"heartbeat-{bot_client.app_id}", ctx=ctx)
        info("[监听] 开始接收消息...", ctx=ctx)

        async for raw in ws:
            try:
                data = json.loads(raw)
                op = data.get("op")
                if op not in (1, 11):
                    debug(f"[收到] {raw[:200]}...", ctx=ctx)
                if op == 0:
                    t = data.get("t")
                    if t in ("C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
                        if get_media_block() == 1:
                            # 阻塞模式（旧逻辑）：串行等待，识别期间后续消息读不进来。
                            # 实时读取配置，改完即生效。
                            await handle_message(data, bot_client)
                        else:
                            # 非阻塞模式（默认）：handle_message 内部可能包含媒体识别
                            # （耗时数秒），若在此串行等待，后续消息就读不进来，媒体识别
                            # 也就永远等不到"新消息"来打断它。改为派发成任务，让接收循环
                            # 持续收消息。
                            #
                            # 并发安全性由 handle_message 自身保证：
                            #   - 每条消息先 bump_media_gen（打断同会话正在进行的识别）
                            #   - 会话级的队列/任务调度（pending_queues / pending_process_tasks）
                            #     负责同一会话的串行与合并，不同会话互不影响。
                            task = asyncio.create_task(handle_message(data, bot_client))
                            _message_tasks.add(task)
                            # 结束时移除引用，并读一次异常：
                            # 只 discard 不取异常的话，handle_message 里的报错
                            # 只会以 "Task exception was never retrieved" 落到
                            # stderr，不进日志，事后无从排查。
                            task.add_done_callback(_log_message_task_result)
                    elif t in ("GROUP_JOIN_REQUEST", "GROUP_MEMBER_ADD", "GROUP_MEMBER_REMOVE"):
                        await handle_event(data, bot_client)
                    else:
                        pass
            except websockets.ConnectionClosed:
                info("[连接] 服务器关闭连接", ctx=ctx)
                raise
            except Exception as e:
                error(f"[处理消息错误] {e}", ctx=ctx)


# ==================== 媒体发送编排（单一事实来源） ====================
async def send_with_policy(sender: BotClient, msg_type: str, recipient_id: str,
                           content: str, *, media_source: str = None,
                           file_type: int = None, file_name: str = None,
                           markdown: str = None, footer: str = "",
                           msg_id: str = None, interval: float = 0.3) -> Dict[str, Any]:
    """
    按媒体类型策略发送消息，统一 push_message / schedule_push 等多处推送逻辑。

    策略（由 utils.needs_dual_send 决定）：
      - 图片(1)：content 文字随媒体一起显示 → 一条媒体消息即可
      - 视频/语音/文件(2/3/4)：content 会被 QQ 客户端忽略 → 先发媒体，再单独发文本
        仅当确实有文字（caption/markdown/footer 任一非空）时才双发，否则单发。

    失败语义：
      - 媒体发送失败 → 立即中止，**不再发送文本**，整条消息按失败返回
        （推送场景媒体是主体，只留一条说明文字会造成语义残缺）；
      - 媒体成功、文本失败 → 仍算成功（媒体已送达）。

    参数：
      content:   媒体消息的正文（不带 footer）
      markdown:  非空且 use_md 时走 msg_type=2 富文本；媒体消息本身不能用 Markdown
      footer:    追加到文本消息末尾的说明（如"来自 XX 发起的推送"）
      msg_id:    传入则作为被动回复（QQ 要求被动回复带 msg_id/msg_seq）。
                 双发场景下仅第一条带 msg_id，第二条为主动消息。
      interval:  双发之间的间隔秒数

    返回：{"ok": bool, "error": str, "message_ids": [{"msg_id","msg_idx","type"}]}
      message_ids 按实际发送顺序排列，双发场景含 2 条，调用方据此向模型暴露全部消息ID。
    """
    # 日志标识：app_id 来自 sender，thread_key 由本次发送目标构造
    ctx = LogCtx(app_id=sender.app_id,
                 thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}",
                 msg_id=msg_id or "")
    result = {"ok": False, "error": "", "message_ids": []}

    def _full(base: str) -> str:
        return (base + footer) if footer else base

    try:
        # 双发判定：类型需要双发，且确实有文字要展示（caption/markdown/footer 任一非空）。
        # 无文字时单发，避免产生一条空的文本消息。
        _has_text = bool((content or "").strip() or (markdown or "").strip()
                         or (footer or "").strip())
        if media_source and needs_dual_send(file_type, file_name, media_source) and _has_text:
            # ---- 双发：先媒体，后独立文本 ----
            # 媒体消息本身只带正文（不带 footer），避免装饰文字被媒体消息吞掉；
            # footer 统一由随后的独立文本消息承载。
            sender.begin_send_batch()
            msg_id_media = await sender.send_message(
                msg_type=msg_type, recipient_id=recipient_id,
                content=content,
                media_source=media_source, file_type=file_type, file_name=file_name,
                msg_id=msg_id,
            )
            media_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None

            # 媒体发送失败 → 立刻中止，不再发送文本。
            # 推送场景下媒体是主体，只留下一条"说明文字"会造成语义残缺，
            # 因此整条消息按失败处理，由调用方计入 fail_count。
            if not msg_id_media:
                result["ok"] = False
                result["error"] = "媒体发送失败，已中止发送文本"
                return result

            result["message_ids"].append(
                {"msg_id": msg_id_media, "msg_idx": media_idx, "type": "media"})

            await asyncio.sleep(interval)

            text_kwargs = {"msg_type": msg_type, "recipient_id": recipient_id, "msg_id": None}
            if markdown:
                text_kwargs["markdown"] = _full(markdown)
            else:
                text_kwargs["content"] = _full(content)
            sender.begin_send_batch()
            msg_id_text = await sender.send_message(**text_kwargs)
            text_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None

            if msg_id_text:
                result["message_ids"].append({
                    "msg_id": msg_id_text,
                    "msg_idx": text_idx,
                    "type": "markdown" if markdown else "text",
                })
            # 媒体已成功，文本失败不影响整体成功（媒体本身已送达）
            result["ok"] = True

        elif media_source:
            # ---- 图片：一条媒体消息带正文（图片的正文会正常显示，footer 也随之一并带上）----
            sender.begin_send_batch()
            msg_id_sent = await sender.send_message(
                msg_type=msg_type, recipient_id=recipient_id,
                content=_full(content),
                media_source=media_source, file_type=file_type, file_name=file_name,
                msg_id=msg_id,
            )
            sent_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None
            if msg_id_sent:
                result["message_ids"].append(
                    {"msg_id": msg_id_sent, "msg_idx": sent_idx, "type": "media_combined"})
            result["ok"] = bool(msg_id_sent)
            if not result["ok"]:
                result["error"] = "发送失败"

        else:
            # ---- 纯文本 / Markdown ----
            kwargs = {"msg_type": msg_type, "recipient_id": recipient_id, "msg_id": None}
            if markdown:
                kwargs["markdown"] = _full(markdown)
            else:
                kwargs["content"] = _full(content)
            sender.begin_send_batch()
            msg_id_sent = await sender.send_message(**kwargs)
            sent_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None
            if msg_id_sent:
                result["message_ids"].append({
                    "msg_id": msg_id_sent,
                    "msg_idx": sent_idx,
                    "type": "markdown" if markdown else "text",
                })
            result["ok"] = bool(msg_id_sent)
            if not result["ok"]:
                result["error"] = "发送失败"

    except Exception as e:
        error(f"[发送编排] 异常: {e}", ctx=ctx)
        result["error"] = f"发送异常: {e}"

    return result