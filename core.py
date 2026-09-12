# -*- coding: utf-8 -*-
# core.py — 主逻辑（机器人启动、多机器人调度、断线重连、动态配置热加载）
import sys
import asyncio
import websockets

from config import get_bots, get_bot_enabled
from client import BotClient, main_connection
from log import info, warn, error, debug

# 新增 bot 检测间隔（秒）
SYNC_INTERVAL = 3


async def run_bot_for_client(bot_client: BotClient):
    """
    单个机器人的运行循环，含断线自动重连。
    重连前检查 bot 是否仍被启用/存在于配置中，否则退出。
    """
    while True:
        # 每次（重）连接前检查 bot 是否仍有效
        if not _is_bot_still_valid(bot_client.app_id):
            info(f"[管理器] 机器人 {bot_client.app_id} 已被移除或禁用，停止运行")
            return

        try:
            await main_connection(bot_client)
        except (websockets.ConnectionClosedError, websockets.ConnectionClosed,
                asyncio.TimeoutError, ConnectionError) as e:
            warn(f"[连接断开] {e}，5秒后重连...")
            await asyncio.sleep(5)
        except _BotStopped:
            info(f"[管理器] 机器人 {bot_client.app_id} 已响应停止信号")
            return
        except Exception as e:
            error(f"[致命错误] {e}，10秒后重连...")
            await asyncio.sleep(10)


def _is_bot_still_valid(app_id: str) -> bool:
    """检查 bot 是否仍存在于配置中且已启用"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id and get_bot_enabled(app_id):
            return True
    return False


class _BotStopped(Exception):
    """连接层主动停止机器人时抛出的内部异常"""
    pass


class BotManager:
    """
    动态机器人管理器。
    定期扫描配置，自动启动配置中新增的机器人。
    已运行机器人的停止/配置变更由各自连接层在使用时主动检测。
    """

    def __init__(self):
        self._bot_tasks: dict = {}       # app_id -> asyncio.Task
        self._bot_clients: dict = {}     # app_id -> BotClient
        self._running = True

    async def _start_bot(self, app_id: str, app_secret: str):
        """启动一个机器人"""
        if app_id in self._bot_tasks:
            return
        client = BotClient(app_id, app_secret)
        self._bot_clients[app_id] = client

        async def wrapped_run():
            try:
                await run_bot_for_client(client)
            except asyncio.CancelledError:
                info(f"[管理器] 机器人 {app_id} 已停止")
                raise

        task = asyncio.create_task(wrapped_run(), name=f"bot_{app_id}")
        self._bot_tasks[app_id] = task
        info(f"[管理器] 机器人 {app_id} 已启动")

    async def sync_bots(self):
        """
        同步配置与运行状态：仅启动新增的机器人。
        已运行机器人的停止由 client/heartbeat 在使用时自行检测。
        """
        # 收集配置中已启用的 app_id
        configured = set()
        for bot in get_bots():
            aid = bot.get("APP_ID")
            if aid and get_bot_enabled(aid):
                configured.add(aid)

        running = set(self._bot_tasks.keys())
        to_start = configured - running
        to_stop = running - configured

        for app_id in to_start:
            for bot in get_bots():
                if bot.get("APP_ID") == app_id:
                    await self._start_bot(app_id, bot.get("APP_SECRET", ""))
                    break

        for app_id in to_stop:
            task = self._bot_tasks.pop(app_id, None)
            self._bot_clients.pop(app_id, None)
            if task and not task.done():
                task.cancel()
                info(f"[管理器] 机器人 {app_id} 已被移除/禁用，发送停止信号")

        # 清理已自然结束的 task
        for app_id in list(self._bot_tasks.keys()):
            task = self._bot_tasks[app_id]
            if task.done():
                del self._bot_tasks[app_id]
                self._bot_clients.pop(app_id, None)
                info(f"[管理器] 机器人 {app_id} 已自然结束")

    async def start(self):
        """启动管理器主循环"""
        info("[管理器] 机器人动态管理器已启动")

        await self.sync_bots()

        if not self._bot_tasks:
            error("配置文件中没有已启用的机器人，程序退出。")
            sys.exit(1)

        while self._running:
            try:
                await asyncio.sleep(SYNC_INTERVAL)
                await self.sync_bots()
            except asyncio.CancelledError:
                break
            except Exception as e:
                error(f"[管理器] 同步异常: {e}")

        # 停止所有机器人
        for app_id in list(self._bot_tasks.keys()):
            task = self._bot_tasks.pop(app_id, None)
            self._bot_clients.pop(app_id, None)
            if task and not task.done():
                task.cancel()
                info(f"[管理器] 机器人 {app_id} 已发送停止信号")

    def stop(self):
        """请求停止管理器"""
        self._running = False


async def run_bots():
    """启动动态机器人管理器"""
    manager = BotManager()
    await manager.start()