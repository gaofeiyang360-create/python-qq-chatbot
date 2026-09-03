# -*- coding: utf-8 -*-
# core.py — 主逻辑（机器人启动、多机器人调度、断线重连）
import sys
import asyncio
import websockets

from config import BOTS
from client import BotClient, main_connection


async def run_bot_for_client(bot_client: BotClient):
    """单个机器人的运行循环，含断线自动重连"""
    while True:
        try:
            await main_connection(bot_client)
        except (websockets.ConnectionClosedError, websockets.ConnectionClosed,
                asyncio.TimeoutError, ConnectionError) as e:
            print(f"[连接断开] {e}，5秒后重连...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[致命错误] {e}，10秒后重连...")
            await asyncio.sleep(10)


async def run_bots():
    """启动所有配置的机器人，并发运行"""
    tasks = []
    for bot_info in BOTS:
        app_id = bot_info.get("APP_ID")
        app_secret = bot_info.get("APP_SECRET")
        if not app_id or not app_secret:
            print("警告：配置中缺少 APP_ID 或 APP_SECRET，跳过该机器人")
            continue
        bot_client = BotClient(app_id, app_secret)
        task = asyncio.create_task(run_bot_for_client(bot_client))
        tasks.append(task)

    if not tasks:
        print("没有可用的机器人配置，程序退出。")
        sys.exit(1)

    await asyncio.gather(*tasks)
