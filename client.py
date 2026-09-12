# -*- coding: utf-8 -*-
# client.py — WebSocket 客户端 & QQ API 调用（BotClient、鉴权、发消息、群禁言、WS连接）
import json
import time
import asyncio
import requests
import websockets
import os
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlparse
from pathlib import Path

from config import get_executor, get_bot_name, set_bot_name, get_bots, get_bot_enabled
from log import info, warn, error, debug


# ==================== BotClient：Token 管理 & QQ API 调用 ====================
class BotClient:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_info = {"access_token": None, "expires_at": 0}
        self.bot_name = "灵泽集AI"
        # 用于消息去重的 msg_seq 计数器
        self._msg_seq_map = {}  # key: msg_id, value: int

    def get_access_token(self, force_refresh: bool = False) -> str:
        if force_refresh or not self.token_info["access_token"] or time.time() >= self.token_info["expires_at"] - 60:
            url = "https://bots.qq.com/app/getAppAccessToken"
            headers = {"Content-Type": "application/json"}
            payload = {"appId": self.app_id, "clientSecret": self.app_secret}
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            self.token_info["access_token"] = data["access_token"]
            self.token_info["expires_at"] = time.time() + int(data["expires_in"])
            info(f"[Token] {self.app_id} 获取成功，有效期 {data['expires_in']} 秒")
        return self.token_info["access_token"]

    def get_websocket_url(self) -> str:
        token = self.get_access_token()
        url = "https://api.sgroup.qq.com/gateway"
        headers = {"Authorization": f"QQBot {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()["url"]

    # ========== 媒体上传（仅支持 URL 直接上传） ==========
    async def upload_media_by_url(self, msg_type: str, recipient_id: str,
                                  source: str, file_type: Optional[int] = None,
                                  file_name: Optional[str] = None) -> Optional[str]:
        """
        使用官方 /files 接口直接传入 URL 上传。
        返回 file_info，失败返回 None。
        """
        info(f"[上传URL] 收到 file_name: {file_name}, file_type: {file_type}")
        if not (source.startswith('http://') or source.startswith('https://')):
            info(f"[上传URL] 错误：仅支持 HTTP/HTTPS URL，收到: {source[:80]}")
            return None

        # 确定 file_type（若未指定则自动推断）
        if file_type is None:
            ext = ""
            if file_name:
                ext = Path(file_name).suffix.lower()
            if not ext:
                url_path = urlparse(source).path
                ext = Path(url_path).suffix.lower()
            if ext in ['.jpg', '.jpeg', '.png']:
                file_type = 1
            elif ext in ['.mp4']:
                file_type = 2
            elif ext in ['.silk']:
                file_type = 3
            else:
                file_type = 4

        token = self.get_access_token()
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

        info(f"[上传URL] 类型 {file_type}，目标 {source[:80]}...")
        info(f"[上传URL] 完整 payload: {json.dumps(payload, ensure_ascii=False)}")
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=30)
            )
        except Exception as e:
            error(f"[上传URL] 请求异常: {e}")
            return None

        if resp.status_code != 200:
            error(f"[上传URL] 失败，状态码 {resp.status_code}，响应: {resp.text[:200]}")
            return None

        resp_json = resp.json()
        file_info = resp_json.get('file_info')
        if not file_info:
            info(f"[上传URL] 响应无 file_info: {resp_json}")
            return None
        info(f"[上传URL] 成功，file_info: {file_info[:20]}...")
        return file_info

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
                           max_retries: int = 3) -> bool:
        """
        发送消息，支持文本、富媒体。
        media_source 必须是 HTTP/HTTPS URL（不支持本地文件）。
        若上传失败，直接返回 False，无自动回退。
        如果提供了 msg_id 且未提供 msg_seq，则自动分配递增序列号。
        """
        # ---------- 1. 获取 file_info ----------
        file_info = None
        if media_source:
            if not (media_source.startswith('http://') or media_source.startswith('https://')):
                info(f"[发送] 错误：media_source 必须为 HTTP/HTTPS URL，收到: {media_source[:80]}")
                return False

            info(f"[发送] 调用上传，file_name={file_name}, file_type={file_type}")
            file_info = await self.upload_media_by_url(msg_type, recipient_id,
                                                       media_source, file_type, file_name)
            if not file_info:
                error("[发送] URL 上传失败，放弃发送")
                return False

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
            token = self.get_access_token()
            if msg_type == "c2c":
                url = f"https://api.sgroup.qq.com/v2/users/{recipient_id}/messages"
            else:
                url = f"https://api.sgroup.qq.com/v2/groups/{recipient_id}/messages"
            headers = {
                "Authorization": f"QQBot {token}",
                "Content-Type": "application/json"
            }
            payload: Dict[str, Any] = {}
            if content:
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
            else:
                payload["msg_type"] = 0

            info(f"[发送] 最终 payload: {json.dumps(payload, ensure_ascii=False)[:500]}")
            loop = asyncio.get_event_loop()
            try:
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                )
                if resp.status_code == 200:
                    if is_media_msg:
                        info(f"[Reply] {msg_type} 富媒体消息发送成功: {content[:50] if content else '媒体'}")
                    else:
                        info(f"[Reply] {msg_type} 发送成功: {content[:50] if content else '空'}")
                    return True
                else:
                    error(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}")
                    if resp.status_code in (401, 500):
                        info("[Reply] 强制刷新 Token")
                        self.get_access_token(force_refresh=True)
                        continue
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        error("[Reply] 发送最终失败，放弃消息")
                        # ----- 被动回复超限 → 自动切主动重试 -----
                        if msg_id:
                            try:
                                resp_json = resp.json()
                                err_code = resp_json.get("code") or resp_json.get("err_code")
                            except Exception:
                                err_code = None
                            if err_code == 40034128:
                                info("[Reply] 检测到被动回复超限（40034128），尝试主动发送...")
                                # 去掉 msg_id 以主动发送
                                payload.pop("msg_id", None)
                                payload.pop("msg_seq", None)
                                active_resp = await loop.run_in_executor(
                                    get_executor(),
                                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                                )
                                if active_resp.status_code == 200:
                                    info(f"[Reply] 主动发送成功: {content[:50] if content else '空'}")
                                    return True
                                else:
                                    error(f"[Reply] 主动发送也失败，状态码 {active_resp.status_code}")
                        return False
            except Exception as e:
                error(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 异常: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    error("[Reply] 发送最终失败，放弃消息")
                    return False
        return False

    # ========== 群禁言相关 API ==========
    async def get_group_mute_status(self, group_openid: str) -> Optional[Dict]:
        token = self.get_access_token()
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
                error(f"[群禁言] 查询失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}")
                return None
        except Exception as e:
            error(f"[群禁言] 查询异常: {e}")
            return None

    async def set_group_mute(self, group_openid: str, op: str, member_openid: str,
                             mute_expire_at: str = "") -> Tuple[bool, int]:
        token = self.get_access_token()
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
        info(f"[群禁言] 发送请求: POST {url}")
        info(f"[群禁言] payload: {json.dumps(payload, ensure_ascii=False)}")
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
                error(f"[群禁言] 设置失败，状态码 {resp.status_code}, 错误码 {error_code}, 响应: {resp.text[:500]}")
                return False, error_code
        except Exception as e:
            error(f"[群禁言] 设置异常: {e}")
            return False, -1


# ==================== WebSocket 主连接 ====================
async def main_connection(bot_client: BotClient):
    from msg import handle_message, handle_event

    info(f"[启动] 机器人 {bot_client.app_id} 开始连接...")
    ws_url = bot_client.get_websocket_url()
    info(f"[启动] 地址: {ws_url}")

    async with websockets.connect(ws_url) as ws:
        hello = await ws.recv()
        hello_data = json.loads(hello)
        info(f"[收到 Hello] {hello}")
        heartbeat_interval = hello_data.get("d", {}).get("heartbeat_interval", 30000) / 1000.0

        token = bot_client.get_access_token()
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
        info("[鉴权] 已发送 Identify")

        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if data.get("op") == 0 and data.get("t") == "READY":
                ready_data = data.get("d", {})
                user_info = ready_data.get("user", {})
                bot_username = user_info.get("username", "灵泽集AI")
                bot_client.bot_name = bot_username
                set_bot_name(bot_username)
                info(f"[收到 Ready] 机器人名字: {bot_username}")
                break
            else:
                info(f"[收到] {msg}")

        info(f"[准备就绪] 机器人 {bot_client.bot_name} 已上线")

        async def heartbeat():
            while True:
                await asyncio.sleep(heartbeat_interval)
                try:
                    await ws.send(json.dumps({"op": 1, "d": int(time.time() * 1000)}))
                    info("[心跳] 发送")
                except websockets.ConnectionClosed:
                    info("[心跳] 连接已关闭")
                    break
                except Exception as e:
                    info(f"[心跳错误] {e}")
                    break

                # ★ 使用时主动检测：bot 是否仍存在于配置且启用
                still_valid = False
                for b in get_bots():
                    if b.get("APP_ID") == bot_client.app_id and get_bot_enabled(bot_client.app_id):
                        still_valid = True
                        break
                if not still_valid:
                    info(f"[心跳] 机器人 {bot_client.app_id} 已被移除或禁用，关闭连接")
                    await ws.close()
                    break

        asyncio.create_task(heartbeat())
        info("[监听] 开始接收消息...")

        async for raw in ws:
            try:
                data = json.loads(raw)
                op = data.get("op")
                if op not in (1, 11):
                    info(f"[收到] {raw[:200]}...")
                if op == 0:
                    t = data.get("t")
                    if t in ("C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
                        await handle_message(data, bot_client)
                    elif t in ("GROUP_JOIN_REQUEST", "GROUP_MEMBER_ADD", "GROUP_MEMBER_REMOVE"):
                        await handle_event(data, bot_client)
                    else:
                        pass
            except websockets.ConnectionClosed:
                info("[连接] 服务器关闭连接")
                raise
            except Exception as e:
                info(f"[处理消息错误] {e}")