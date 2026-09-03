# QQ 机器人

基于 QQ 官方机器人 API（bots.qq.com）构建的智能 AI 机器人，支持多机器人并发、群聊/私聊对话、多媒体识别、工具调用（Function Calling）、长期记忆、群管理等功能。

## 项目结构

```
.
├── bot.py          # 程序入口，启动所有机器人
├── core.py         # 核心逻辑：机器人启动、多机器人调度、断线重连
├── client.py       # WebSocket 客户端 & QQ API 调用（鉴权、发消息、群禁言）
├── config.py       # 配置管理：实时读取 config.json、线程池、数据目录初始化
├── memory.py       # 记忆系统：全局/群/私聊/机器人记忆、聊天记录、媒体缓存、记忆整理
├── ai.py           # AI 交互：对话调用、媒体识别、群聊判定、回复生成、摘要、网页抓取
├── msg.py          # 消息处理：消息解析、冷却队列、事件处理（加群/退群）
├── tool.py         # 工具系统：工具定义（function calling）、HTTP 请求、音乐搜索/播放、群管理
├── config.json     # 配置文件（首次运行自动生成）
├── memory.json     # 全局记忆存储
├── mirror.json     # 用户 QQ 号与用户名映射
├── history/        # 聊天历史记录（每个会话一个 JSON 文件）
├── media_cache/    # 媒体识别结果缓存
├── qun_memory/     # 群聊长期记忆
├── c2c_memory/     # 私聊长期记忆
├── bot_memory/     # 机器人专属记忆
└── qun_memory_backup/  # 群记忆整理备份
```

## 快速开始

### 1. 环境要求

- Python 3.8+
- 依赖库：`requests`, `websockets`

### 2. 安装依赖

```bash
pip install requests websockets
```

### 3. 配置

首次运行会自动生成 `config.json`，编辑该文件填入你的机器人信息：

```json
{
  "bots": [
    {
      "APP_ID": "你的APP_ID",
      "APP_SECRET": "你的APP_SECRET",
      "SYSTEM_PROMPT": "你是一个幽默风趣的AI助手",
      "ISOLATE_GLOBAL_MEMORY": 0,
      "ENABLED": 1,
      "AUTO_WELCOME": 1,
      "GROUP_MANAGE_WHITELIST": [],
      "ENABLE_TOOLS": 1,
      "MAX_TOOL_ROUNDS": 6
    }
  ],
  "models": {
    "judge": {
      "base_url": "https://api.example.com/v1",
      "api_key": "your-api-key",
      "model_name": "your-judge-model"
    },
    "main": {
      "base_url": "https://api.example.com/v1",
      "api_key": "your-api-key",
      "model_name": "your-main-model"
    },
    "vision": {
      "base_url": "https://api.example.com/v1",
      "api_key": "your-api-key",
      "model_name": "your-vision-model"
    }
  }
}
```

### 4. 运行

```bash
python bot.py
```

## 核心功能

### 多机器人并发

在 `config.json` 的 `bots` 数组中配置多个机器人，每个机器人使用独立的 App ID 和 App Secret，通过 `asyncio` 并发运行，支持断线自动重连。

### 聊天模式

| 模式 | 说明 |
|------|------|
| **私聊 (C2C)** | 直接与机器人对话，全部消息自动回复 |
| **群聊 @ 模式** | 在群中 @ 机器人时触发回复 |
| **群聊被动模式** | 机器人通过 AI 判断是否介入群聊对话 |

### 三级 AI 模型

| 模型 | 用途 | 特点 |
|------|------|------|
| **judge** | 判定是否回复、管理记忆、摘要生成 | 轻量模型，低温度（0.2-0.3） |
| **main** | 主对话生成、工具调用 | 主力模型，支持 Function Calling |
| **vision** | 图片/视频识别 | 多模态模型，低温度（0.3） |

### 记忆系统

系统支持四级记忆体系，自动管理和相关性检索：

| 记忆类型 | 作用域 | 说明 |
|----------|--------|------|
| **全局记忆** | 所有会话 | 跨群/跨用户的通用知识 |
| **群聊记忆** | 特定群 | 该群聊特有的信息和上下文 |
| **私聊记忆** | 特定用户 | 该用户的个性化信息 |
| **机器人记忆** | 特定机器人 | 该机器人实例的专属记忆 |

- **相似度检索**：通过 Jaccard 相似度计算，自动检索与当前消息最相关的记忆注入系统提示
- **自动整理**：当记忆条数超过 15 条时，自动调用 AI 进行精简合并
- **AI 驱动管理**：每次对话后自动评估是否需要增删改查记忆

### 上下文压缩

当聊天历史超过阈值（默认 25 条）时，自动生成摘要插入到历史中，后续检索从摘要之后开始，有效控制 token 消耗。

### 消息队列与冷却

- 同群/同用户的连续消息自动合并，进入冷却队列（默认 2 秒）
- 冷却结束后统一处理，避免重复请求
- 新消息到达时，自动取消旧处理任务并重新开始

### 工具系统（Function Calling）

基于 OpenAI 兼容的 Function Calling 实现，AI 可在对话中主动调用以下工具：

| 工具 | 功能 |
|------|------|
| `http_request` | 发起任意 HTTP 请求（GET/POST/PUT/DELETE/PATCH） |
| `send_media` | 发送图片、视频或文件 |
| `send_text` | 发送纯文本消息 |
| `recognize_media` | 主动识别媒体内容 |
| `search_music` | 搜索网易云音乐 |
| `play_music` | 播放音乐（非 VIP 歌曲） |
| `mute_member` | 禁言群成员（白名单群） |
| `unmute_member` | 解除群成员禁言（白名单群） |
| `skip_reply` | 终止工具循环，避免重复回复 |

### 多媒体处理

- **图片/视频识别**：自动通过视觉 AI 模型生成摘要描述
- **语音消息**：自动转写为文字（利用 QQ 平台 ASR 结果）
- **文本文件**：自动下载并提取内容
- **网页链接**：自动抓取网页内容并生成摘要
- **聊天记录转发**：解析转发格式中的媒体和文件

### 群管理事件

- **加群申请**：自动记录申请人信息
- **成员加入**：可选自动发送欢迎语
- **成员退出**：AI 判断是否回复告别消息

## 配置说明

### 配置文件 `config.json`

| 字段 | 说明 |
|------|------|
| `bots[].APP_ID` | 机器人 App ID（QQ 开放平台获取） |
| `bots[].APP_SECRET` | 机器人 App Secret |
| `bots[].SYSTEM_PROMPT` | 机器人个性设定 |
| `bots[].ISOLATE_GLOBAL_MEMORY` | 是否隔离全局记忆（0=共享，1=隔离） |
| `bots[].ENABLED` | 是否启用（1=启用，0=禁用） |
| `bots[].AUTO_WELCOME` | 新人入群是否自动欢迎 |
| `bots[].GROUP_MANAGE_WHITELIST` | 群管理白名单（群 openid 列表） |
| `bots[].ENABLE_TOOLS` | 是否启用工具调用 |
| `bots[].MAX_TOOL_ROUNDS` | 工具调用最大循环次数 |
| `AI_MAX_MSG_LEN` | 单条消息最大字符数 |
| `CONTEXT_LIMIT` | 保留的上下文对话轮数 |
| `JUDGE_CONTEXT_LIMIT` | 判断模型使用的上下文轮数 |
| `COOLDOWN_SECONDS` | 用户请求冷却时间 |
| `COMPRESS_THRESHOLD` | 触发上下文压缩的轮数 |
| `MAX_WORKERS` | 并发工作线程数 |
| `SYSTEM_PROMPT` | 全局系统提示词 |

### 数据文件

- `memory.json`：全局记忆数据
- `mirror.json`：QQ 号与用户名的映射表
- `history/*.json`：每个会话的聊天历史记录
- `media_cache/*.json`：媒体识别结果的缓存
- `qun_memory/*.json`：每个群的长期记忆
- `c2c_memory/*.json`：每个用户的私聊记忆
- `bot_memory/*.json`：每个机器人的专属记忆

## 数据迁移

程序会自动检测旧版 `threads.json` 文件，并将其迁移到新的分文件存储结构：

- `global_memory` → `memory.json`
- `user_mapping` → `mirror.json`
- `threads[*].history` → `history/{key}.json`

旧文件会被备份为 `threads.json.bak`。

## 错误处理

- **断线重连**：WebSocket 断开后自动重连（5 秒延迟，致命错误 10 秒延迟）
- **Token 刷新**：Token 过期前 60 秒自动刷新，401 错误强制刷新
- **被动回复超限**：自动切换为主动发送消息
- **配置文件损坏**：自动备份损坏文件并生成默认配置
- **JSON 文件损坏**：自动备份并重建

## 代码架构

```
bot.py
  └─ core.run_bots()
       └─ core.run_bot_for_client()
            └─ client.main_connection()
                 ├─ 建立 WebSocket 连接
                 ├─ 发送心跳
                 └─ 接收消息
                      ├─ handle_message() → msg.py
                      │    ├─ parse_message() → 解析消息
                      │    ├─ 处理附件/媒体/网页
                      │    ├─ 冷却队列合并
                      │    └─ process_queue() → handle_processed_message()
                      │         └─ ai.generate_reply()
                      │              ├─ call_ai() / call_ai_with_tools()
                      │              ├─ tool.execute_tool_call()
                      │              └─ client.send_message()
                      └─ handle_event() → msg.py
                           └─ 加群/退群事件处理
```

## 版权信息

- 代码作者：飞扬nb
