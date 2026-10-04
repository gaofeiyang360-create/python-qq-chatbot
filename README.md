# QQ Bot — 多机器人 AI 管理框架

一套基于 **Python asyncio** 的多机器人 QQ 服务端框架：同时托管多个 QQ 机器人，
接入大模型做智能回复，自带记忆系统、定时推送/唤醒、媒体处理、HTTP API 与
内置 Web 管理面板。

- **作者**：飞扬nb
- **运行环境**：CPython 3.14.6（Windows x64）实测通过，最低要求 3.10
- **代码规模**：14 个模块，约 17,900 行
- **当前配置**：18 个机器人

---

## 目录

- [功能概览](#功能概览)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [配置说明](#配置说明)
- [HTTP API](#http-api)
- [管理面板](#管理面板)
- [数据文件](#数据文件)
- [设计要点](#设计要点)
- [常见问题](#常见问题)

---

## 功能概览

| 能力 | 说明 |
|---|---|
| **多机器人托管** | 一份配置托管多个机器人，每个机器人独立 asyncio 任务、独立配置、独立记忆 |
| **AI 智能回复** | 接入 OpenAI 兼容接口；群聊是否回复由模型判定；支持多模型轮换与故障切换 |
| **记忆系统** | 四级记忆：全局 / 群 / 私聊 / 机器人；自动整理压缩；可搜索、可开关 |
| **工具调用** | 机器人可调用工具（发送消息、查询、管理定时任务等），支持多轮工具循环 |
| **定时推送** | 一次性 / 每天 / 间隔三种调度，可指定目标与消息模板 |
| **定时唤醒** | 到点唤醒机器人，由 AI 自主决定行动（区别于推送的固定内容） |
| **媒体处理** | 图片/视频/语音/文件的收发与缓存；支持多模态识别 |
| **HTTP API** | 35 个接口，覆盖消息、群管理、任务、配置、记忆、日志 |
| **Web 管理面板** | 单文件 HTML，零外部依赖，浏览器直接打开即可管理 |

---

## 快速开始

### 1. 安装依赖

```bash
python -m pip install -r requirements.txt
```

依赖已全部精确锁定（`==`），共 18 个包，见 [requirements.txt](requirements.txt)。
全部有 Windows 预编译 wheel，**无需编译工具链**。

### 2. 配置机器人

编辑 `config.json`：

```jsonc
{
  "bots": [
    {
      "APP_ID": "你的机器人AppID",
      "APP_SECRET": "你的机器人AppSecret",
      "ENABLED": 1,
      "desc": "给这个机器人起个名字（面板里显示）",
      "SYSTEM_PROMPT": "这个机器人的系统提示词",
      "ENABLE_TOOLS": 1,
      "MAX_TOOL_ROUNDS": 30
    }
  ],
  "models": {
    "main":   [{ "base_url": "https://...", "api_key": "sk-...", "model_name": "..." }],
    "judge":  [{ "base_url": "https://...", "api_key": "sk-...", "model_name": "..." }],
    "vision": [{ "base_url": "https://...", "api_key": "sk-...", "model_name": "..." }]
  }
}
```

三个模型用途不同，都可以配多个做轮换：

- `main` — 主对话模型
- `judge` — 判定模型（判断群里这条消息要不要回，用便宜快的）
- `vision` — 视觉模型（识别图片内容）

### 3. 启动

```bash
python bot.py
```

启动后会看到：

```
[INFO] 启动 18 个机器人
[INFO] API 服务已启动 http://0.0.0.0:8148
[INFO] 定时推送调度器已启动
[INFO] 定时唤醒调度器已启动
```

### 4. 打开管理面板

浏览器直接打开 `admin3.0.html`，填服务地址 `http://127.0.0.1:8148` 和 API 密钥登录。

> 也可以直接访问 `http://127.0.0.1:8148/` 查看服务状态。

---

## 项目结构

```
bot.py                  主程序入口（10 行，调用 core.run_bots）
│
├─ core.py              启动逻辑：多机器人调度、断线重连、配置热加载
│
├─ client.py            WebSocket 长连接 + QQ 开放平台 API 封装（BotClient）
├─ msg.py               消息解析：媒体/引用/记录、冷却队列、事件处理
├─ ai.py                模型请求：对话、视觉识别、群聊判定、摘要、网页抓取
├─ memory.py            记忆系统：四级记忆、聊天记录、媒体缓存、自动整理
├─ tool.py              AI 工具集（4618 行，项目最大模块）
│
├─ scheduler.py         定时推送调度器
├─ wakeup_scheduler.py  定时唤醒调度器
├─ task_core.py         推送/唤醒的共享核心（API 与工具共用一份实现）
│
├─ api_server.py        HTTP API 服务（35 个接口）
├─ config.py            配置读写（实时生效，无需重启）
├─ log.py               日志（基于 loguru，配置热更新）
└─ utils.py             通用工具（零项目依赖，避免循环导入）
```

### 分层依赖

```
utils.py          ← 最底层，只依赖标准库，绝不 import 本项目其他模块
  ↑
log.py            ← 日志 + spawn_background（放这里是为了避免循环导入）
config.py         ← 配置
  ↑
memory.py / client.py / ai.py / msg.py
  ↑
tool.py / task_core.py / scheduler.py / wakeup_scheduler.py
  ↑
api_server.py / core.py
```

**`utils.py` 为什么零项目依赖**：`ai.py` / `tool.py` / `msg.py` 之间存在相互依赖，
任何业务模块放进公共依赖链都可能触发循环导入。保持 `utils.py` 只依赖标准库，
它才能被任何模块安全导入。

**`spawn_background` 为什么在 `log.py` 而不是 `utils.py`**：它需要写日志，
而 `utils.py` 不能依赖 `log.py`（否则又变成业务依赖）。

---

## 配置说明

### 顶层配置

| 键 | 默认 | 说明 |
|---|---|---|
| `AI_MAX_MSG_LEN` | 99500 | 单条消息最大长度 |
| `API_BIND_HOST` | `0.0.0.0` | API 监听地址 |
| `API_BIND_PORT` | 8148 | API 监听端口 |
| `COMPRESS_THRESHOLD` | 25 | 记忆整理触发阈值 |
| `CONTEXT_LIMIT` | 20 | 对话上下文条数 |
| `COOLDOWN_SECONDS` | 2 | 同一会话回复冷却（秒） |
| `MAX_WORKERS` | 20 | 线程池大小 |
| `MEDIA_BLOCK` | 0 | 是否屏蔽媒体 |
| `TOOL_CHOICE` | 1 | 是否启用工具调用 |
| `SYSTEM_PROMPT` | — | 全局系统提示词 |
| `enable_log` | 1 | 是否写日志文件（0 = 仅控制台） |
| `log_file` | `log.txt` | 日志路径 |
| `log_level` | `info` | 文件日志等级 |
| `log_console_level` | `info` | 控制台日志等级 |
| `max_log_length` | 5000 | 日志文件行数上限，超出自动轮转 |

### 单机器人配置

每个机器人可独立覆盖行为：

| 键 | 说明 |
|---|---|
| `APP_ID` / `APP_SECRET` | QQ 开放平台凭据 |
| `ENABLED` | 是否启动这个机器人 |
| `desc` | 显示名（管理面板里显示） |
| `SYSTEM_PROMPT` | 该机器人的系统提示词（覆盖全局） |
| `ENABLE_TOOLS` | 是否允许工具调用 |
| `MAX_TOOL_ROUNDS` | 工具调用最大轮数（默认 30） |
| `ISOLATE_GLOBAL_MEMORY` | 是否与全局记忆隔离 |
| `AUTO_WELCOME` | 自动欢迎新成员 |
| `DISABLE_AI_REPLY` | 完全关闭 AI 回复（只留 API/推送） |
| `API_ENABLED` | 是否开放该机器人的独立 API |
| `API_KEYS` | 该机器人的独立密钥列表（空则用全局密钥） |
| `GROUP_MANAGE_WHITELIST` | 群管理白名单 |

**跨机器人权限**（都是 `*_INCOMING` 配对，前者管「我能否操作别人」，后者管「别人能否操作我」）：

| 键 | 说明 |
|---|---|
| `ALLOW_CROSS_BOT_PUSH` / `_INCOMING` | 跨机器人推送 |
| `ALLOW_CROSS_BOT_WAKEUP` / `_INCOMING` | 跨机器人唤醒 |
| `ALLOW_CROSS_BOT_GET_LIST` / `_INCOMING` | 跨机器人查看任务列表 |
| `ALLOW_CROSS_BOT_HISTORY` / `_INCOMING` | 跨机器人读取聊天记录 |
| `ALLOW_MANAGE_ALL_PUSH` | 管理**操作**权限（注意：只管操作，不管可见性） |

---

## HTTP API

服务默认监听 `0.0.0.0:8148`，共 **35 个接口**。

### 鉴权

两种密钥，任选其一：

- **全局密钥**（`GLOBAL_API_KEYS`）— 可访问全部机器人
- **机器人密钥**（`bots[].API_KEYS`）— 只能访问自己

两种传法：

```http
X-API-Key: your_key
Authorization: Bearer your_key
```

> 未鉴权的请求访问 `/health`、`/info`（精简版）可以成功；
> 访问业务接口一律返回 **401**。

### 接口清单

| 分组 | 接口 |
|---|---|
| **状态** | `GET /` `GET /health` `GET /info` |
| **机器人** | `GET /api/bots` |
| **会话** | `GET /api/groups` `GET /api/members` `GET /api/users` `GET /api/contacts` |
| **消息** | `GET /api/history` `POST /api/send` `POST /api/revoke` `POST /api/batch_revoke` `POST /api/message/hide` |
| **群管理** | `POST /api/mute` `GET /api/mute/status` `GET /api/bot_state` |
| **推送** | `POST /api/push/create` `GET /api/push/list` `POST /api/push/update` `POST /api/push/delete` |
| **唤醒** | `POST /api/wakeup/create` `GET /api/wakeup/list` `POST /api/wakeup/update` `POST /api/wakeup/delete` `POST /api/wakeup/trigger` |
| **配置** | `GET /api/config/get` `GET /api/config/list` `POST /api/config/set` |
| **记忆** | `GET /api/memory/list` `POST /api/memory/add` `POST /api/memory/update` `POST /api/memory/delete` `POST /api/memory/toggle` `GET /api/memory/search` |
| **日志** | `GET /api/logs` |

### 示例

发送消息：

```bash
curl -X POST http://127.0.0.1:8148/api/send \
  -H "X-API-Key: your_key" \
  -H "Content-Type: application/json" \
  -d '{
        "app_id": "1905417157",
        "target_type": "group",
        "target_id": "群openid",
        "content": "你好"
      }'
```

列出机器人：

```bash
curl http://127.0.0.1:8148/api/bots -H "X-API-Key: your_key"
```

> `app_id` 可省略（用全局密钥时），此时后端按各接口规则自行解析或聚合。

---

## 管理面板

三个版本，**推荐用 `admin3.0.html`**：

| 文件 | 大小 | 说明 |
|---|---|---|
| `admin.html` | 151 KB | 早期版本 |
| `admin2.0.html` | 179 KB | 中间版本 |
| **`admin3.0.html`** | **267 KB** | **当前版本，功能最全** |

**特点**：单文件 HTML，零外部依赖，无需构建，浏览器直接打开。

**功能**：聊天记录查看（含筛选、Markdown 渲染）、消息发送/撤回、群成员与禁言、
推送与唤醒任务管理、记忆管理、配置编辑、日志查看。

**安全说明**：

- 所有动态内容统一经 `esc()` 转义（覆盖 `& < > " '`），Markdown 走
  「先抽代码块 → 转义 → 再套结构规则」的正确顺序
- API 密钥默认**只存 `sessionStorage`**（关标签页即失效）；勾选「记住密钥」
  才写入 `localStorage`
- 完整审查报告见 [`admin3.0-审查报告.md`](admin3.0-审查报告.md)

---

## 数据文件

运行期数据都在项目根目录，**全部是纯 JSON**（便于直接查看和手工修复）：

| 文件 / 目录 | 内容 |
|---|---|
| `config.json` | 主配置（机器人、模型、系统参数） |
| `mirror.json` | 群名/昵称映射缓存 |
| `user_map.json` | openid ↔ 用户/群 映射 |
| `memory.json` | 全局记忆 |
| `qun_memory/` | 各群记忆 |
| `c2c_memory/` | 各私聊记忆 |
| `bot_memory/` | 各机器人自身记忆 |
| `history/` | 聊天记录（**注意：是数组，不是对象**） |
| `media_cache/` | 媒体文件缓存 |
| `scheduled_push.json` | 定时推送任务 |
| `scheduled_wakeup.json` | 定时唤醒任务 |
| `relay_config.json` | 转发配置 |
| `log.txt` | 日志文件 |

### 写入安全

所有 JSON 写入统一走 `utils.atomic_write_json()`：

```
写临时文件 → fsync 落盘 → os.replace 原子替换 → 保留 .bak 备份
```

**进程中途被杀不会留下半个损坏的 JSON。** 排障时如怀疑数据损坏，
可直接查看同目录的 `.bak`。

### 关于 `member_openid`

**同一个真人在同一个机器人下的不同群里，`member_openid` 是相同的。**
这是刻意的设计：用户级记忆（`c2c_memory/`）和用户映射（`user_map.json`）
都依赖这个性质，才能在多个群里认出同一个人。

---

## 设计要点

以下是这套代码里几个容易被改坏的地方，修 bug 前建议先读：

### 1. 记忆写入必须原子

任何直接 `open(..., "w")` 写 JSON 的改动都会破坏原子性保证。
**始终用 `atomic_write_json()`。**

### 2. `history/*.json` 是数组

不是 `{msg_id: msg}` 这种对象。按字典处理会直接崩。

### 3. 日志配置热更新

`log.py` 每次写日志都会检查 `config.json` 的 mtime，变了就重建 logger。
所以**改日志等级不需要重启**。

### 4. 配置同样热更新

`config.py` 支持实时读写，改完即生效。但 `bots` 列表的增删需要重启
（启动时按列表创建 asyncio 任务）。

### 5. 推送 vs 唤醒的区别

| | 定时推送 | 定时唤醒 |
|---|---|---|
| 行为 | 到点发送**固定内容** | 到点唤起 AI，由 AI **自主决定**做什么 |
| 成本 | 0 次模型调用 | 1+ 次模型调用 |
| 适用 | 公告、提醒、固定文案 | 需要判断/组织语言的场景 |

唤醒任务的 `targets` 是**唤醒谁**；如果要 AI 往某些群发消息，需要在
`description` 里写清楚，AI 会自己调 `push_message` 工具。

### 6. 接口实现的单一来源

`task_core.py` 是推送/唤醒的核心实现，`api_server.py`（HTTP）和
`tool.py`（AI 工具）都只调它。**不要在两边各写一遍** —— 历史上就是
因为各写一遍导致参数不统一、字段白名单互有缺失。

### 7. 前端转义顺序

`admin3.0.html` 的 `renderMarkdown()` 顺序是：

```
抽出代码块 → 抽出行内代码 → esc() 转义 → 套用 Markdown 结构规则
```

**先转义再套结构规则**。顺序颠倒就是经典 XSS。

---

## 常见问题

### 启动后机器人不在线

1. 检查 `config.json` 里对应机器人的 `ENABLED` 是否为 `1`
2. 检查 `APP_ID` / `APP_SECRET` 是否正确
3. 看日志里的鉴权错误 —— QQ 的 access_token 有有效期，过期会自动刷新，
   但 `APP_SECRET` 填错会一直失败

### 管理面板登录报「密钥无效或未授权」

1. 确认服务已启动（`http://127.0.0.1:8148/health` 应返回 200）
2. 确认密钥在 `GLOBAL_API_KEYS` 里，或该机器人的 `API_KEYS` 里
3. 若面板通过 `file://` 打开，注意浏览器对本地文件的 CORS 限制

### 面板数据不自动刷新

已在 `admin3.0.html` 修复（轮询统一由 `enterApp()` 启动）。
如果你用的是 `admin.html` / `admin2.0.html`，这两个老版本可能仍有此问题。

### 日志不写文件

检查 `config.json`：`enable_log` 必须为 `1`。为 `0` 时只输出到控制台
（这是正常行为，不是 bug）。

### 端口被占用

改 `config.json` 的 `API_BIND_PORT`，重启即可。

---

## 许可与致谢

- 作者：**飞扬nb**
- 依赖：[aiohttp](https://github.com/aio-libs/aiohttp) ·
  [websockets](https://github.com/python-websockets/websockets) ·
  [requests](https://github.com/psf/requests) ·
  [loguru](https://github.com/Delgan/loguru)

---

*本文档基于当前代码状态（14 模块 / 约 17,900 行 / 35 个 API / 18 个机器人）编写。
改动代码后请同步更新对应章节。*