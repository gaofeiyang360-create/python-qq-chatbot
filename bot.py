# -*- coding: utf-8 -*-
# bot.py — 主程序入口
# CODE BY 飞扬nb
# 模块化设计：config(配置) / memory(记忆) / ai(AI请求) / msg(消息处理) / client(WS&API) / core(启动逻辑)
import asyncio
from core import run_bots


if __name__ == "__main__":
    asyncio.run(run_bots())
