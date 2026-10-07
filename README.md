# QQ Bot — 多机器人 AI 管理框架

基于 **Python asyncio** 的多机器人 QQ 服务端框架：一份配置同时托管多个机器人，
接入大模型做智能回复，自带记忆系统、定时推送 / 定时唤醒、媒体处理、
HTTP API 与内置 Web 管理面板。

- **作者**：飞扬nb
- **运行环境**：Python **3.10+**（Windows / Linux 均可）
- **规模**：14 个模块 · 约 16,300 行 · 35 个 HTTP 接口

---

## 特性

| 能力 | 说明 |
|---|---|
| **多机器人托管** | 一份配置托管多个机器人，各自独立 asyncio 任务、独立配置、独立记忆 |
| **AI 智能回复** | 接入 OpenAI 兼容接口；群聊是否回复由判定模型决定；支持多模型轮换与故障切换 |
| **四级记忆** | 全局 / 群 / 私聊 / 机器人四层记忆，自动整理压缩，可搜索、可开关 |
| **工具调用** | 机器人可调用工具（发消息、查记录、管理定时任务、群管理等），支持多轮工具循环 |
| **定时推送** | 一次性 / 每天 / 间隔三种调度，到点发送固定内容 |
| **定时唤醒** | 到点唤起 AI，由 AI 自主决定做什么（区别于推送的固定内容） |
| **媒体处理** | 图片 / 视频 / 语音 / 文件的收发与缓存，支持多模态识别 |
| **HTTP API** | 35 个接口，覆盖消息、群管理、推送、唤醒、配置、记忆、日志 |
| **Web 管理面板** | 单文件 HTML，零外部依赖、无需构建，浏览器直接打开 |

---

## 快速开始

### 1. 安装依赖

```bash
python -m pip install -r requirements.txt
```

依赖全部精确锁定（`==`），且都有 Windows 预编译 wheel，**不需要编译工具链**。

### 2. 配置机器人

```bash
cp config.example.json config.json        # Windows: copy config.example.json config.json
```

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
    "main":   [{ "base_url": "https://你的接口地址", "api_key": "sk-你的密钥", "model_name": "你的模型" }],
    "judge":  [{ "base_url": "https://你的接口地址", "api_key": "sk-你的密钥", "model_name": "你的模型" }],
    "vision": [{ "base_url": "https://你的接口地址", "api_key": "sk-你的密钥", "model_name": "你的模型" }]
  },
  "GLOBAL_API_KEYS": ["你自己的管理密钥"]
}
```

三个模型用途不同，都可以配多个做轮换：

- `main` — 主对话模型
- `judge` — 判定模型（判断群里这条消息要不要回，用便宜快的）
- `vision` — 视觉模型（识别图片内容）

> ⚠️ `config.json` 里全是明文密钥。仓库已附带 `.gitignore` 将其排除，
> **请不要把它提交到版本库，也不要把真实密钥贴进 issue 或截图。**

### 3. 启动

```bash
python bot.py
```

启动后会看到：

```
[INFO] 启动 N 个机器人
[INFO] API 服务已启动 http://0.0.0.0:8148
[INFO] 定时推送调度器已启动
[INFO] 定时唤醒调度器已启动
```

### 4. 打开管理面板

浏览器直接打开 `admin3.0.html`，填服务地址 `http://127.0.0.1:8148` 和 API 密钥登录。

> 也可以直接访问 `http://127.0.0.1:8148/` 查看服务状态与接口清单。

---

## 项目结构

```
bot.py                  主程序入口（调用 core.run_bots）
│
├─ core.py              启动逻辑：多机器人调度、断线重连、配置热加载
│
├─ client.py            WebSocket 长连接 + QQ 开放平台 API 封装
├─ msg.py               消息解析：媒体 / 引用 / 记录、冷却队列、事件处理
├─ ai.py                模型请求：对话、视觉识别、群聊判定、摘要、网页抓取
├─ memory.py            记忆系统：四级记忆、聊天记录、媒体缓存、自动整理
├─ tool.py              AI 工具集（项目最大模块）
│
├─ scheduler.py         定时推送调度器
├─ wakeup_scheduler.py  定时唤醒调度器
├─ task_core.py         推送 / 唤醒的共享核心（API 与工具共用一份实现）
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

`utils.py` 保持零项目依赖是刻意的：`ai.py` / `tool.py` / `msg.py` 之间存在相互依赖，
任何业务模块放进公共依赖链都可能触发循环导入，只有纯标准库才能被任何模块安全导入。
同理，`spawn_background` 需要写日志，所以放在 `log.py` 而不是 `utils.py`。

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
| `log_level` / `log_console_level` | `info` | 文件 / 控制台日志等级 |
| `max_log_length` | 5000 | 日志文件行数上限，超出自动轮转（`<= 0` 表示不限制） |

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
| `DISABLE_AI_REPLY` | 完全关闭 AI 回复（只留 API / 推送） |
| `API_ENABLED` / `API_KEYS` | 是否开放该机器人的独立 API 及其密钥（为空则用全局密钥） |
| `GROUP_MANAGE_WHITELIST` | 群管理白名单 |

**跨机器人权限**（均为 `*_INCOMING` 配对：前者管「我能否操作别人」，后者管「别人能否操作我」）：

| 键 | 说明 |
|---|---|
| `ALLOW_CROSS_BOT_PUSH` / `_INCOMING` | 跨机器人推送 |
| `ALLOW_CROSS_BOT_WAKEUP` / `_INCOMING` | 跨机器人唤醒 |
| `ALLOW_CROSS_BOT_GET_LIST` / `_INCOMING` | 跨机器人查看任务列表 |
| `ALLOW_CROSS_BOT_HISTORY` / `_INCOMING` | 跨机器人读取聊天记录 |
| `ALLOW_MANAGE_ALL_PUSH` | 管理**操作**权限（只管操作，不管可见性） |

---

## HTTP API

服务默认监听 `0.0.0.0:8148`，共 **35 个接口**，全部参数见下文逐条说明。

### 统一约定

**参数位置**（最容易搞错的一点）

| 请求方法 | 参数位置 |
|---|---|
| `GET` | URL query string |
| `POST` | JSON 请求体（建议带 `Content-Type: application/json`） |
| `app_id` | **两处皆可**：query 优先，其次 body |
| `key` | 三处皆可：`Authorization` 头、`X-API-Key` 头、query |

**鉴权**

两种密钥，任选其一：

- **全局密钥**（`GLOBAL_API_KEYS`）— 可访问全部机器人，`role = "global"`
- **机器人密钥**（`bots[].API_KEYS`）— 只能访问自己，`role = "bot"`

密钥传法（同时命中多个来源时按全局 > 机器人的优先级取权限更高的）：

```http
Authorization: Bearer your_key     ← 也兼容直接放裸 key
X-API-Key: your_key
GET /api/bots?key=your_key         ← 同时兼容 api_key / apikey
```

免鉴权与可选鉴权的例外：

| 路径 | 鉴权行为 |
|---|---|
| `/`、`/health` | 完全免鉴权 |
| `/info` | **可选鉴权**：未带密钥只回 `{alive:true}`，带有效密钥才回机器人清单与绑定地址 |

若 `GLOBAL_API_ENABLED=1` 且 `GLOBAL_API_KEYS` 为空数组 → 无需密钥且拥有全部权限；
若某机器人 `API_ENABLED=1` 且 `API_KEYS` 为空数组 → 无需密钥，但仅限该机器人。

**`app_id` 解析规则**（所有业务接口通用）

- 机器人密钥通道：锁定为自己的 `app_id`；显式传别人的会直接报错（**不会静默返回自己的数据**，避免调用方误以为操作了目标机器人）
- 全局密钥通道：可显式指定；缺省时若**只有一个已启用的机器人**则自动选用，否则报 `请通过 app_id 参数指定要操作的机器人`

**响应信封**

```jsonc
// 成功
{ "ok": true, "data": { /* 各接口不同 */ } }

// 失败
{ "ok": false, "error": "错误原因，中文" }
```

**HTTP 状态码策略**（重要）

| 后端语义码 | 实际返回 | 说明 |
|---|---|---|
| `400` / `401` / `403` / `404` | 原样返回 | 调用方用错了 |
| `500` | **HTTP 422** | body 里保留 `http_status: 500` |
| `502` 及其它 5xx | **HTTP 409** | body 里保留 `http_status: 502` |

之所以不直接返回 5xx：经 Cloudflare 之类反代时，**5xx 的响应体会被反代自己的错误页覆盖**，
调用方只能看到纯文本 `error code: 502`，JSON 里的错误文案全部丢失，
表现为「接口永远 502、看不到任何原因、无法排查」。选 422/409 而不是统一 400，
是为了保留下区分度：一眼看出是「后端炸了」（422）还是「QQ 侧拒绝了这次操作」（409）。

> 判错请以 body 里的 `ok === false` 为准，不要依赖状态码。

**参数名严格校验**

各接口只接受下表列出的规范参数名，传别的（例如旧的 `group_id` / `member_openid`）
**会直接报错而不是静默忽略** —— 静默忽略会让调用方以为筛选生效了，其实拿到的是全部数据。
错误文案形如 `不支持的参数: xxx`，并对常见旧名给出改名提示：

| 旧名 | 换成 |
|---|---|
| `group_id` / `group_openid` / `id` | `target_id` |
| `member_openid` / `user_id` | `member_id` |
| `message_id` / `msg_id` / `msg_ids` | `message_ids` |
| `type` | `target_type` |

**其它全局行为**

- **请求体上限 1MB**：`Content-Length` 超过 1MB 时请求体被当作空对象，因此会表现为「缺少某参数」的 400
- **时间格式**：统一 RFC3339 带时区，北京时间，如 `2026-09-26T20:00:00+08:00`；列表接口的时间筛选也接受纯日期 `2026-09-21`
- **异常脱敏**：异常文本里的部署绝对路径会被替换成 `<path>`，不会泄露服务端目录结构与用户名（完整异常仍写日志）
- **CORS**：允许任意来源，并**回显请求的 `Origin`**（含 `file://` 场景下的 `null`，因此在本地双击打开管理面板也能用）；`OPTIONS` 预检由中间件直接返回 204，不参与鉴权

### 接口总览

| 分组 | 接口 |
|---|---|
| **状态** | `GET /` `GET /health` `GET /info` |
| **机器人** | `GET /api/bots` |
| **会话** | `GET /api/groups` `GET /api/members` `GET /api/users` `GET /api/contacts` |
| **消息** | `GET /api/history` `POST /api/send` `POST /api/revoke` `POST /api/message/hide` `POST /api/batch_revoke` |
| **群管理** | `POST /api/mute` `GET /api/mute/status` `GET /api/bot_state` |
| **推送** | `POST /api/push/create` `GET /api/push/list` `POST /api/push/update` `POST /api/push/delete` |
| **唤醒** | `POST /api/wakeup/create` `GET /api/wakeup/list` `POST /api/wakeup/update` `POST /api/wakeup/delete` `POST /api/wakeup/trigger` |
| **配置** | `GET /api/config/get` `GET /api/config/list` `POST /api/config/set` |
| **记忆** | `GET /api/memory/list` `POST /api/memory/add` `POST /api/memory/update` `POST /api/memory/delete` `POST /api/memory/toggle` `GET /api/memory/search` |
| **日志** | `GET /api/logs` |

---

### 状态

#### `GET /` — 服务信息与接口清单

无参数，完全免鉴权。返回自描述文档，`data` 字段：

| 键 | 说明 |
|---|---|
| `service` | 固定 `"qqbot-api"` |
| `version` | 固定 `"1.0"` |
| `time` | 当前北京时间 ISO 字符串 |
| `endpoints` | 接口清单字符串数组（列业务接口，不含 `/`、`/health`、`/info` 与配置类接口） |
| `logs_api` / `memory_api` / `task_list_query` / `task_list_response` | 对日志、记忆、任务列表筛选与响应的内嵌说明 |

#### `GET /health` — 健康检查

无参数，免鉴权。`data` = `{"status": "healthy", "time": "<北京时间ISO>"}`。

#### `GET /info` — 服务与开关状态（按身份分级返回）

无参数，可选鉴权。响应内容随是否携带有效密钥而不同：

| 场景 | `data` |
|---|---|
| 未鉴权 | `{service: "dsh-qqbot-api", alive: true, authenticated: false}` |
| 已鉴权 | 上述字段（`authenticated: true`）+ `global_api_enabled`、`global_has_keys`、`bind`（`"host:port"`）、`bots` |

`bots` 数组每项为 `{app_id, api_enabled, has_keys}`，只回布尔值不回显密钥本身。

> 未鉴权时故意不返回机器人清单与绑定地址：`app_id` 本身不是秘密，
> 但「有哪些机器人、哪个开了 API」是攻击者最想要的第一步侦察信息。

---

### 机器人

#### `GET /api/bots` — 可用机器人列表

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `search` | string | 否 | — | 关键词，匹配 `APP_ID` 或 `desc` 的子串（不区分大小写），任一命中即保留 |
| `app_id` | — | **不要传** | — | 不在白名单内，传了会 `400 不支持的参数: app_id` |

`data`：`total`、`bots[]`、`role`、`can_manage_all`、`scope_app_id`。
`bots[]` 每项为 `{app_id, enabled, api_enabled, desc, has_keys}`；`has_keys` 只给布尔值。

机器人密钥调用时只返回自己；`role` 取 `global` / `bot` / `none`。

---

### 会话

#### `GET /api/groups` — 群列表

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `app_id` | string | 条件 | — | 见「`app_id` 解析规则」 |
| `search` | string | 否 | — | 匹配群名或 `group_id` 子串（不区分大小写） |

`data`：`app_id`、`total`、`groups[]`。
`groups[]` 每项为 `{group_id, group_name, member_count, members, member_ids}`，
其中 `members` 为 `[{member_id, member_name}]`（名称缺失时为空串），`member_ids` 为纯 ID 数组（兼容旧字段）。

#### `GET /api/members` — 群成员列表

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_id` | string | 否 | — | 群 openid；不传则返回**全部群**的成员 |
| `app_id` | string | 条件 | — | 同上 |
| `search` | string | 否 | — | 匹配成员昵称或 `member_id` 子串（不区分大小写） |
| `all` | bool | 否 | `false` | 为真时每个成员额外附 `openid` 字段（与 `member_id` 同值）。**只认 `1` / `true` / `yes`**，`on` 不算真 |

`data`：`app_id`、`group_filter`（未指定时为 `null`）、`total`（所有群成员数之和）、`group_count`、`groups[]`。
`groups[]` 每项为 `{group_id, group_name, member_count, total_recorded, members}`；
`member_count` 是过滤后成员数，`total_recorded` 是本地记录的原始成员数；
`members[]` 为 `{member_id, member_name, has_name, group_id}`（`all` 为真时多 `openid`）。

指定了 `target_id` 但该群没有本地记录时返回 `404 未找到群 {target_id} 的成员记录（该群没有本地记录，请先同步成员）`。

#### `GET /api/users` — 私聊用户列表

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `app_id` | string | 条件 | — | 同上 |
| `search` | string | 否 | — | 匹配 `user_name` 或 `user_id` 子串（不区分大小写） |

`data`：`app_id`、`total`、`users[]`，`users[]` 每项为 `{user_id, user_name}`。

#### `GET /api/contacts` — 群 + 私聊总览

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `app_id` | string | 条件 | — | 同上 |
| `search` | string | 否 | — | 同时作用于群列表与用户列表 |

`data`：`app_id`、`groups`、`users`、`group_total`、`user_total`。
内部直接复用 `/api/groups` 与 `/api/users`，任一子请求非 200 即原样返回该响应。

---

### 消息

#### `GET /api/history` — 读取聊天记录

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_type` | string | **是** | — | 只能为 `group` 或 `c2c` |
| `target_id` | string | **是** | — | 会话 ID（群 openid 或用户 openid） |
| `app_id` | string | 条件 | — | 同上 |
| `keyword` | string | 否 | — | 子串匹配正文，**区分大小写** |
| `limit` | int | 否 | `50` | 取最新 N 条；无法解析时回落 50；`<= 0` 表示返回全部 |
| `revoked` | string | 否 | — | `1`/`true`/`yes` 只看已撤回；`0`/`false`/`no` 只看未撤回；其它值不过滤 |
| `markdown` | string | 否 | — | 同上，筛 Markdown 消息 |
| `tools` | string | 否 | — | **只有** `1`/`true`/`yes` 生效：只保留工具调用记录（`tool_calls` 非空或 `role=tool`）。**没有 `0` 分支**，传 `0`/`false` 等同不过滤 |
| `wakeup` | string | 否 | — | 同 `revoked`，筛唤醒消息 |
| `is_hide` | string | 否 | — | 同 `revoked`，筛已隐藏消息 |
| `raw` | string | 否 | — | 在白名单里但**代码从未读取**，行为与不传一致 |

过滤顺序：先 `keyword` → 再按 `limit` 截取 → 然后依次 `revoked` → `markdown` → `tools` → `wakeup` → `is_hide`。

`data`：`app_id`、`thread_key`（`"{target_type}_{target_id}"`）、`total`（关键词过滤后、`limit` 与其它过滤**之前**的条数）、
`returned`、`revoked_count`、`markdown_count`、`tool_call_count`、`wakeup_count`、`is_hide_count`、`messages[]`。

`messages[]` 在原始记录字段（`msg_id` / `msg_idx` / `ref_msg_idx` / `tool_calls` / `tool_call_id` / `is_summary` 等）基础上补齐：
`revoked`、`is_markdown`、`is_wakeup`、`is_hide`、`media_url`（非媒体为 `null`）、`is_tool_call`、
`tool_names`（发起方才有）、`is_thinking`（assistant 且无 `msg_id` 时为 1）、`ts` / `username` / `user_id`（缺失时为空串）、
`content`（已撤回且正文未以 `[已撤回]` 开头时自动补前缀）。

#### `POST /api/send` — 主动发送消息

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_type` | string | **是** | — | 只能为 `group` 或 `c2c` |
| `target_id` | string | **是** | — | 目标群 openid 或用户 openid，且必须登记在该机器人名下 |
| `content` | string | 条件 | — | 纯文本正文。非 Markdown 模式下与 `media_source` **至少给一个** |
| `message_type` | string | 否 | — | 声明格式。`markdown` / `md` / `2` 视为富文本，其余一概按纯文本 |
| `markdown` | string \| object | 条件 | — | Markdown 正文。传了它即进入 Markdown 模式；对象形式支持 `{"content": "..."}` 或 `{"custom_template_id": "...", "params": [...]}` |
| `markdown_content` | string | 否 | — | 历史兼容名。**只传它、不声明 `message_type` 也会按 Markdown 处理** |
| `media_source` | string | 条件 | — | 媒体 URL，必须以 `http://` 或 `https://` 开头 |
| `file_type` | int | 否 | — | 媒体类型，透传底层 |
| `file_name` | string | 否 | — | 媒体文件名，透传底层 |
| `quote_msg_idx` | string | 否 | — | 引用回复。填被引用消息的 **`msg_idx`**（形如 `REFIDX_xxxxxx==`），**不是 `msg_id`** —— 官方接口收的是索引，传 `msg_id` 会被判为无效引用。取值来自 `/api/history` 的 `msg_idx` 字段 |
| `app_id` | string | 条件 | — | 同上 |

正文取值优先级：`markdown` > `markdown_content` > `content`。
**媒体与 Markdown 不同发**：同时给了 `media_source` 时媒体优先，Markdown 被忽略。

`data`：`app_id`、`thread_key`、`msg_id`、`msg_idx`、`quote_msg_idx`、`media_url`。

#### `POST /api/revoke` — 撤回消息

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_type` | string | **是** | — | 只能为 `group` 或 `c2c` |
| `target_id` | string | **是** | — | 会话 ID |
| `message_ids` | array | **是** | — | 消息 ID 数组；单个字符串会被自动包成单元素数组；空数组报错 |
| `app_id` | string | 条件 | — | 同上 |

返回结构随数量变化：

- **单条**成功：`{app_id, message_id, revoked: true, thread_key, history_marked}`
- **多条**：`{app_id, thread_key, total, revoked_count, failed_count, results[]}`
  - `results[]` 成功项 `{message_id, revoked: true, history_marked}`
  - 异常项 `{message_id, revoked: false, error: "<异常原文>"}`
  - 失败项 `{message_id, revoked: false, error: "撤回失败（消息可能已过期或无权限）"}`

> 超过 2 分钟的消息无法撤回。

#### `POST /api/message/hide` — 隐藏 / 取消隐藏消息

**纯本地操作**：只改本地记录的 `is_hide`，不调用 QQ 接口，也不影响 `revoked`；隐藏的消息不会再喂给 AI。

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_type` | string | **是** | — | 只能为 `group` 或 `c2c` |
| `target_id` | string | **是** | — | 会话 ID |
| `message_ids` | array | **是** | — | 消息 ID 数组，单个字符串自动包成数组 |
| `is_hide` | bool | 否 | `1`（= 隐藏） | 不传即隐藏；传 `0` / `false` 表示取消隐藏。真值判定为 `1`/`true`/`yes`/`on` |
| `app_id` | string | 条件 | — | 同上 |

返回：单条为 `{app_id, thread_key, message_id, is_hide, updated}`；
多条为 `{app_id, thread_key, total, is_hide, updated_count, failed_count, results[]}`，
失败项的错误文案为 `未找到该 message_id（或值未发生变化）`，单条未命中时整体返回 `404`。

#### `POST /api/batch_revoke` — 按关键词 / 时间范围批量撤回

**默认只预览**，必须显式 `confirm=1` 才真正执行。

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_type` | string | **是** | — | 只能为 `group` 或 `c2c` |
| `target_id` | string | **是** | — | 会话 ID |
| `keywords` | array | 条件 | `[]` | 关键词数组，任一命中即可（不区分大小写子串）。与时间范围**至少要有一个** |
| `start_time` | string | 条件 | — | RFC3339，`Z` 结尾会自动归一；解析失败报错 |
| `end_time` | string | 条件 | — | 同上。只给时间范围时，**无法判断时间的消息一律排除** |
| `confirm` | int | 否 | `0` | 必须显式 `1` 且 `dry_run` 为 0 才真正执行 |
| `dry_run` | int | 否 | `0` | 兼容旧参数；显式 `1` 时同样只预览 |
| `app_id` | string | 条件 | — | 同上 |

匹配时会跳过：无 `msg_id` 的记录、已撤回的记录、正文以 `[已撤回]` 开头的记录。关键词为空时不做关键词过滤。

预览返回：`{app_id, thread_key, preview: true, executed: false, matched, messages[], hint}`，
`messages[]` 每项 `{msg_id, preview（正文前 80 字符）, keyword, timestamp, role}`。
执行返回：`{app_id, thread_key, preview: false, executed: true, matched, success, failed, results[]}`。

> 不指定条件就撤回整个会话历史是很危险的，因此 `keywords` 与时间范围至少要给一个。

---

### 群管理

#### `POST /api/mute` — 禁言 / 解禁 / 更改时长

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_id` | string | **是** | — | 群 openid |
| `op` | string | **是** | — | `add`=禁言 / `del`=解除 / `update`=更改时长 |
| `members` | array | 条件 | — | 三种写法都接受：`[{"member_id":"x","mute_expire_at":"..."}]`、`["x","y"]`、单个 `"x"` 或 `{"member_id":"x"}`。传一个即单个、传多个即批量；重复 `member_id` 会去重，单次最多 50 个 |
| `member_id` | string | 条件 | — | `members` 未给时回退读它，自动包成数组 |
| `seconds` | int | 条件 | — | 相对秒数，整批共用一个时长。`op=add/update` 且既无 `mute_expire_at` 也无成员自带到期时间时**必填**；必须为正整数；超过官方上限 30 天会被**静默截断**为 30 天；`op=del` 时被强制置 0 |
| `mute_expire_at` | string | 否 | `""` | 绝对到期时间（RFC3339），**给定时优先于 `seconds`**。`op=del` 时被强制清空 |
| `app_id` | string | 条件 | — | 同上 |

`members[]` 元素可自带 `mute_expire_at`，**优先于外层**的统一值。未给外层时间时，`seconds` 会换算成北京时间的 RFC3339。
官方对部分时间格式返回错误码 `10007` 时，会自动改为「1 小时后」重试一次，成功则回显修正后的到期时间。

返回：**单个成员**为扁平结构 `{app_id, target_id, group_id, member_id, op, seconds, mute_expire_at}`；
**多个成员**为 `{app_id, target_id, group_id, op, seconds, total, success, failed, results[]}`，
`results[]` 成功项 `{member_id, ok: true, mute_expire_at}`，失败项 `{member_id, ok: false, error_code, error}`。

#### `GET /api/bot_state` — 查询机器人自身在群内的角色

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_id` | string | **是** | — | 群 openid |
| `app_id` | string | 条件 | — | 同上 |

`data`：`app_id`、`group_id`、`group_name`（取自本地映射，取不到为空串）、`member_role`（官方原值）、
`role_label`（`member`→普通成员 / `admin`→管理员 / `owner`→群主 / 其它原样 / 空为「未知」）、
`can_manage`（`admin` 或 `owner` 时为 true）、`allow_proactive_msg`、`recv_msg_setting`、`joined_at`、`member_openid`、`bot_state`（官方原始对象）。

> 机器人的群管理能力取决于 `member_role`：只有 `admin` / `owner` 才能禁言解禁。

#### `GET /api/mute/status` — 查询群禁言状态

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_id` | string | **是** | — | 群 openid |
| `app_id` | string | 条件 | — | 同上 |
| `with_names` | string | 否 | — | 在白名单里但**代码从未读取**；昵称始终会补全，传不传结果相同 |

`data`：`app_id`、`group_id`、`mute_status`（官方原始对象）、`members[]`（官方成员数组，就地补 `member_name` / `has_name` / `is_muted`）、
`muted_members[]`（精简列表，每项 `{member_id, member_name, has_name, mute_expire_at, is_muted, username}`）、`muted_count`、`global_mode`（无规则时为 `none`）。

与 `/api/bot_state` 不同，本接口**不检查机器人是否被禁用**。

`is_muted` 的判定口径：成员对象里**存在 `mute_expire_at` 键**时取该值的非空性，
不存在该键时一律视为 true（即视为被禁言）。

---

### 定时推送

#### `POST /api/push/create` — 创建推送任务

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `content` | string | **是** | — | 推送正文，空或纯空白报 `缺少 content` |
| `targets` | array | 条件 | — | 与下面的单目标简写二选一。元素键：`type`（`user`/`group`）、`id`（必填），其它键（如 `name`）原样保留 |
| `target_type` | string | 条件 | — | 单目标简写。**只能是 `user` 或 `group`，不接受 `c2c`** |
| `target_id` | string | 条件 | — | 单目标简写，与 `target_type` 同时给才生效 |
| `schedule_type` | string | 否 | `one_time` | `one_time` / `daily` / `interval` |
| `schedule_time` | string | 条件 | `""` | `one_time` 与 `daily` 时**必填** |
| `interval_seconds` | int | 否 | — | 仅 `interval` 且非空时落库。给了就必须能转 int 且 > 0；`interval` 类型下**不强制**提供 |
| `media_source` | string | 否 | — | 媒体来源。给了会构造嵌套 `media`，并把 `file_type` / `file_name` 一并放进去（执行器只读嵌套 `media`） |
| `file_type` | 任意 | 否 | — | 仅当 `media_source` 非空时进入 `media.file_type` |
| `file_name` | string | 否 | — | 仅当 `media_source` 非空且去空白后非空时进入 `media.file_name` |
| `message_type` / `markdown` / `is_markdown` | 任意 | 否 | — | Markdown 相关，非空即落库。创建时**不能传嵌套 `media`**（只有 update 接口接受） |
| `app_id` | string | 条件 | — | 同上，并作为 `targets[].app_id` 的默认值（不覆盖元素自带的） |

任务落库时自动写入 `initiator_info`（`{"app_id", "type": "system", "id": "", "name": "API", "created_at"}`）；
`task_id`、`status="pending"`、`created_at`、`execution_history` 由调度器补齐。

`data`：`task_id`（uuid4 前 8 位）、`app_id`、`schedule_type`。

#### `GET /api/push/list` — 列出推送任务

`app_id`（仅用于解析与回显）+ 下文的**共享筛选参数** + `with_counts`（在白名单里但**代码从未读取**，`counts` 始终返回）。

> **可见范围由调用方身份决定，与 `app_id` 取值无关**：全局密钥取全量视图，机器人密钥只取自己的任务
> （即使开了 `ALLOW_MANAGE_ALL_PUSH` 也不并入他人任务）。

`data`：`app_id`、`total`（**过滤后**条数）、`tasks[]`（原样返回，不裁剪字段）、
`counts`（**过滤前**全量统计，键固定为 `pending` / `completed` / `partial` / `failed` 加 `total`）、
`filters`（仅在至少一条筛选生效时出现）。

#### `POST /api/push/delete` — 删除推送任务

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `task_id` | string | **是** | — | 任务 ID |
| `app_id` | string | 条件 | — | 同时作为删除的归属校验口径 |

`data`：`{task_id, message}`。未找到或无权限时返回 `404`。

#### `POST /api/push/update` — 更新推送任务

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `task_id` | string | **是** | — | 任务 ID |
| `updates` | object | 条件 | — | 嵌套写法，只采纳白名单字段，`task_id` / `app_id` 等会被丢弃 |
| 平铺字段 | 同下 | 条件 | — | 与 `updates` **同时出现时，`updates` 内的同名字段优先** |
| `app_id` | string | 条件 | — | 同上 |

可更新字段：`content`、`targets`、`schedule_type`、`schedule_time`、`interval_seconds`、
`media_source`、`file_type`、`file_name`、`media`、`message_type`、`markdown`、`is_markdown`。

两点要注意：

- **`updates` 与平铺至少要给一个**，否则报 `缺少要更新的字段（可用 updates 嵌套或直接平铺）`
- 字段值为 `null` 表示**删除该字段**（唤醒接口的 `null` 则是直接写入）

`data`：`{task_id, message, updated}`，`updated` 为本次实际提交的字段名排序数组。

> ⚠️ 本接口的**校验错误也统一返回 `404`**，不是 400：`updates 必须为对象`、
> `updates 不能为空`、`schedule_type 无效`、`interval_seconds 必须为整数`、
> `targets[...]` 系列错误，以及「未找到任务」「无权限修改」都走 `404`。
> 排查时不要只按 400 找参数问题。

---

### 定时唤醒

#### `POST /api/wakeup/create` — 创建唤醒任务

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `description` | string | **是** | — | 唤醒提示词正文，空报 `缺少 description` |
| `schedule_type` | string | **是** | 无默认 | `one_time` / `daily` / `interval`。**与推送不同，这里没有默认值**，缺省即报错 |
| `schedule_time` | string | 条件 | `""` | `one_time` 与 `daily` 时必填 |
| `interval_seconds` | int | 否 | — | 仅 `interval` 且非空时落库；给了就必须能转 int 且 > 0 |
| `targets` | array | 条件 | — | 元素键 `type`（`user`/`group`）、`id`（必填） |
| `target_type` / `target_id` | string | 条件 | — | 单目标简写，`target_type` 只能是 `user` 或 `group` |
| `initiator` | object | 否 | system 占位 | **只读 `id` 与 `name`，两者都非空才采用**；否则整体替换为 `{"app_id", "type": "system", "id": "", "name": "API", "created_at"}`。其余键原样落库 |
| `isolation_mode` | int | 否 | 不落库（调度器补 0） | 给了就转 int，失败报 `isolation_mode 必须为整数`。**本接口不校验取值范围**（`trigger` 才校验 0/1） |
| `app_id` | string | 条件 | — | 同上，并作为 `targets[].app_id` 默认值 |

`data`：`task_id`、`app_id`、`schedule_type`。

#### `GET /api/wakeup/list` — 列出唤醒任务

参数与 `/api/push/list` **完全一致**（共用同一套筛选实现），可见范围规则也相同。
`data` 同为 `app_id`、`total`、`tasks[]`、`counts`（键同推送列表）、`filters`。

#### `POST /api/wakeup/delete` — 删除唤醒任务

`task_id`（必填）+ `app_id`。`data` = `{task_id, message}`，未找到或无权限返回 `404`。

#### `POST /api/wakeup/update` — 更新唤醒任务

`task_id`（必填）+ `updates`（嵌套）或平铺字段，`updates` 优先，至少要给一个。
可更新字段：`schedule_type`、`schedule_time`、`interval_seconds`、`targets`、`initiator`、`description`、`isolation_mode`。
**不含 `status`** —— 状态由调度器写入，外部不可直接改。

> 与推送不同：字段值为 `null` 时**不会被当作删除**，而是直接写入 `null`。
>
> 与推送相同：**校验错误也统一返回 `404`**（`updates 必须为对象`、`updates 不能为空`、
> `schedule_type 无效`、`interval_seconds 必须为整数`、`targets[...]` 系列），
> 「未找到任务」「无权限修改」同样是 `404`。

#### `POST /api/wakeup/trigger` — 立即唤醒（API 模拟用户输入）

向指定会话注入一段提示词，模拟用户输入，立即触发一轮 AI 回复。**这是唯一会同步阻塞并真的调用模型的接口。**

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `target_type` | string | **是** | — | 只能为 `group` 或 `c2c`（落库时 `c2c` → `user`） |
| `target_id` | string | **是** | — | 群 openid 或用户 openid，且必须登记在该机器人名下 |
| `description` | string | **是** | — | 要注入的提示词正文。**参数名是 `description`，不是 `prompt`** |
| `isolation_mode` | int | 否 | `0` | `1`=隔离模式：唤醒期间到达的用户消息**排队等待**，唤醒结束后再处理，两者互不干扰。只能为 0 或 1 |
| `initiator` | object | 否 | API 调用方 | 仅当是对象且 `id` 非空时采用；否则替换为 `{"id": caller, "name": "API(caller)", "type": "system"}` |
| `target_name` | string | 否 | 不落库 | 给了就写入 `targets[0].name` |
| `app_id` | string | 条件 | — | 同上 |

成功时 `data`：`app_id`、`thread_key`、`target_type`、`target_id`、`isolation_mode`、`ok: true`、`error: ""`、
`wakeup_message`（实际注入的文本）。
失败时返回 `409`（body 内 `http_status: 502`，`ok_result: false`），文案为 `唤醒未成功：{原因}`，并附带 `error_detail`。

> 唤醒任务的 `targets` 是**唤醒谁**。如果要 AI 往某些群发消息，需要在 `description` 里写清楚，
> AI 会自己调 `push_message` 工具完成。

---

### 共享的列表筛选参数

以下参数适用于 `GET /api/push/list` 与 `GET /api/wakeup/list`，**全部可选、可任意组合**。

**状态 `status`**

| 值 | 含义 |
|---|---|
| `pending` | 未开始（也接受中文 `未开始`，历史拼写 `active`） |
| `completed` | 执行完成（也接受中文 `执行完成`，历史拼写 `sent`） |
| `partial` | **部分失败**（也接受中文 `部分失败`） |
| `failed` | 执行失败（也接受中文 `执行失败`） |

- 多值用逗号、中文逗号、分号、竖线分隔，或重复同名参数；多个之间取「或」
- **`status=failed` 会同时命中 `partial`**（部分失败显然属于「没发成功」），反向不成立：筛 `partial` 不带 `failed`
- 无法识别的值**不报错**，被忽略并写入响应的 `filters.status_ignored`；若全部无法识别，结果直接为空数组
- 别名 `state` 内部支持，但**不在 HTTP 白名单里**，传了会被拒绝

> 为什么有 4 个状态：一个推送任务可以带多个目标。早期只有 completed/failed 二选一，
> 判定为「成功数 > 0 且失败数 == 0」，于是「10 个目标成功 3 个、失败 7 个」被记成 completed，
> 调用方完全看不出有一半没发出去。新增 `partial` 后：全成功=completed、部分成功=partial、全失败=failed。

**关键词**

| 参数 | 说明 |
|---|---|
| `keywords` | 多关键词，取值时**任一命中**即算匹配 |
| `keyword` / `q` / `search` | 单关键词的等价写法 |
| `match_all` | `1` / `true` / `yes` 时要求**全部**关键词都命中 |

⚠️ 这四个关键词参数不是合并关系，而是**优先级替换**：只取第一个非空的
（`keywords` > `keyword` > `q` > `search`），其余被忽略。也就是说
`?keywords=天气&keyword=新闻` 只会按 `天气` 筛选。

关键词匹配的是任务里汇总的可搜索文本，包括 `task_id`、`app_id`、`content`、`description`、
`schedule_time`、`status`、发起人的 `id`/`name`/`app_id`/`type`、各 `targets[]` 的
`id`/`type`/`name`/`app_id`，以及 `execution_history` 里的错误信息与各目标执行结果。比较前统一转小写。

**其它筛选**

| 参数 | 类型 | 说明 |
|---|---|---|
| `task_id` | string | 任务 ID **包含**匹配 |
| `task_app_id` | string | 只保留该机器人的任务（按任务 `app_id` **精确**相等） |
| `target_type` | string | `group` / `user`（`c2c` 兼容为 `user`）；无枚举校验 |
| `target_id` | string | 对 `targets[].id` **包含**匹配 |
| `isolation_mode` | int | 按 `isolation_mode` 过滤。**推送任务没有该字段，传了会筛空** |
| `start` / `start_time` | string | 时间下界（含）。二者取第一个非空键。支持 ISO（含 `Z`）、`2026-09-21 20:40`、`2026/09/21`、纯日期等格式；无时区按 +08:00 |
| `end` / `end_time` | string | 时间上界（含）。**只给日期时会自动补到当天 23:59:59** |
| `time_field` | string | 用哪个字段做时间比较，默认依次尝试 `schedule_time` → `created_at`；**传了就只用该字段** |
| `order` | string | `asc` / `desc`。其它值忽略即不排序 |
| `limit` | int | 排序后取**末尾** `limit` 条（即最近 N 条）。必须能转 int 且 > 0，否则静默忽略 |

两个容易踩的坑：

- **`order` 的排序依据固定是 `schedule_time` → `created_at`**，与 `time_field` 无关
- 时间格式解析失败**不会报错**，而是写入响应的 `filters.time_error`，且该边界不生效（另一端仍然生效）

---

### 配置

> 三个配置接口**仅限全局密钥**调用，机器人密钥或无密钥模式一律 `403 配置管理仅限全局密钥操作`。

#### `GET /api/config/get` — 读取配置（已脱敏）

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `path` | string | 否 | `""` | 配置路径，**用点号 `.` 分隔**，如 `API_BIND_PORT`、`bots.0.API_ENABLED`。支持任意层嵌套 dict 与数组下标；键名**大小写不敏感**。省略则返回整份配置 |
| `app_id` | string | 否 | — | 在白名单里但**代码从未读取** |

脱敏规则：顶层 `models`（含模型 `base_url` 与明文 `api_key`）**整项剔除**；
凭证类键变为 `"****"`；`API_KEYS` 类字段变为 `["****", ...]` 保留元素个数；
`path` 的最后一段若本身是敏感键，直接返回字符串 `"****"`（避免用裸字符串绕过脱敏）。

`data`：`{path, value}`。

#### `POST /api/config/set` — 修改配置（原子写盘）

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `set` | object | 条件 | — | 批量写法：`{"API_BIND_PORT": 8149, "bots.0.API_ENABLED": 1}`，逐项独立处理 |
| `path` | string | 条件 | — | 单条写法，点号分隔，空段被忽略 |
| `value` | 与原值同类型 | 条件 | — | 新值。**类型必须与原值一致**，否则报 `需要布尔值(true/false)` / `需要整数` / `需要数字` / `需要字符串` / `需要数组` / `需要对象` |
| `app_id` | string | 否 | — | 在白名单里但**代码从未读取**（权限只看密钥角色） |

`set` 与 `path`+`value` **二选一**，都没有则报 `请提供 path+value 或 set 对象`。

被禁止的写入（需直接编辑配置文件）：

- `models`（服务端内部配置）
- 敏感键（`APP_SECRET` / `APP_TOKEN` / `SECRET` / `TOKEN` / `PASSWORD`，大小写不敏感）
- `APP_ID`（机器人标识）
- **不允许新增配置项**，只能改已存在的

`data`：`{applied: [{path, old, value}], errors: [...], note}`。
**部分成功即返回 200**，失败项在 `errors` 里；只有全部失败才返回 400。

> 配置改动实时生效，无需重启；监听地址/端口变更会自动重载。

#### `GET /api/config/list` — 扁平列出可配置项

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `scope` | string | 否 | `global` | `global` 列顶层配置项；`bot` 只列该机器人的配置，路径形如 `bots.{索引}.{键}`。**`global` / `bot` 之外的任何取值都落入 global 分支** |
| `app_id` | string | 条件 | — | `scope=bot` 时**必填**，缺失报 `scope=bot 时需要 app_id`；找不到机器人报 `404` |

`items[]` 每项：`{path, value, type, type_name, sensitive, api_key, readonly}`。
其中 `sensitive=true` 表示凭证字段（完全只读），`api_key=true` 表示 API 密钥字段（可写但不回显），
`readonly = sensitive 或 键名为 APP_ID`。

`global` 分支下 `bots` 数组不展开，只给一条占位项（`hidden: true`，提示改用 `scope=bot` 查看）；
顶层 `models` 则完全不出现在结果里。

---

### 记忆

四级记忆与 `identifier` 的对应关系是这套接口的核心，先看清再接：

| `level` | 含义 | `identifier` 填什么 |
|---|---|---|
| `global` | 全局记忆 | 机器人 **APP_ID** |
| `bot` | 机器人专属记忆 | 机器人 **APP_ID** |
| `group` | 群聊记忆 | 群 openid |
| `c2c` | 私聊记忆 | 用户 openid |

`level` 的合法值唯一集合是 `global` / `bot` / `group` / `c2c`（`/api/memory/search` 额外接受 `all`）。
非法值报 `未知记忆级别 X，可选：global/bot/group/c2c`。

这些接口与 AI 侧的 `view_memory` / `add_memory` / `modify_memory` / `delete_memory` /
`enable_memory` / `disable_memory` / `search_memory` 工具**共用同一套底层函数与归属校验口径**，
所以「记忆里看不到、却能往里发消息」这种不一致不会出现。

**归属校验**：`group` / `c2c` 级要求目标群/用户确实登记在该机器人名下；
`global` / `bot` 级要求 `identifier` 是本次请求的机器人。跨机器人的操作一律 `403`。

#### `GET /api/memory/list` — 读取某级记忆

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `level` | string | 否 | `global` | 见上表（不含 `all`） |
| `identifier` | string | **是** | — | **本接口必填**，不做任何默认推断。缺失报 `缺少 identifier（{level} 级记忆需填{提示}）` |
| `app_id` | string | 条件 | — | 同上 |

`data`：`level`、`identifier`、`app_id`、`enabled`（该级记忆是否启用）、`total`、
`items[]`（每项 `{index, content}`，`index` 从 0 开始，可直接用于 update / delete）。

#### `POST /api/memory/add` — 追加记忆

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `items` | array | 否 | — | **批量模式开关**。是数组即走批量分支，元素键为 `level` / `identifier` / `content`。**优先于单条参数**：只要 `items` 是数组，顶层的 `level`/`identifier`/`content` 一律被忽略，也不做顶层缺失校验 |
| `level` | string | 条件 | 无默认 | 单条模式必填（不传即空串，报 `未知记忆级别 (空)`） |
| `identifier` | string | 条件 | — | 单条模式必填 |
| `content` | string | 条件 | — | 单条模式必填，去空白后为空报 `缺少 content` |
| `app_id` | string | 条件 | — | 同上 |

单条返回：`{mode: "single", level, identifier, message: "已添加"}`。
批量返回：`{mode: "batch", total, success_count, failed_count, success[], failed[]}`，
并在顶层额外带 `ok_result`（= 无任何失败）。

**批量语义**：逐条独立、**部分失败不回滚**；`items: []` 时计数全为 0、`ok_result` 为 true。
批量里单条失败不产生 HTTP 错误，错误落在对应 `failed[].error`。

> 若该级记忆当前被禁用，添加会返回 `409 {level} 级记忆当前已禁用，请先启用（/api/memory/toggle）`。

#### `POST /api/memory/update` — 按索引替换

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `items` | array | 否 | — | 批量模式开关，元素键 `level` / `identifier` / `index` / `content`，优先于单条参数 |
| `level` | string | 条件 | 无默认 | 单条模式必填 |
| `identifier` | string | 条件 | — | 单条模式必填 |
| `index` | int | 条件 | — | 单条模式必填，从 0 开始。负数与布尔值一律视为无效 |
| `content` | string | 条件 | — | 单条模式必填 |
| `app_id` | string | 条件 | — | 同上 |

`index` 越界或无效返回 `404 修改失败：索引 {index} 越界或无效`。
**修改不检查该级是否被禁用**（禁用只影响注入，内容仍可整理）。

单条返回 `{mode, level, identifier, index, message: "已修改"}`（`index` 原样回显传入值）。

#### `POST /api/memory/delete` — 按索引删除

参数同 update，但**没有 `content`**（`items` 元素为 `level` / `identifier` / `index`）。
越界返回 `404 删除失败：索引 {index} 越界或无效`；删除同样不检查该级是否被禁用。
单条返回 `{mode, level, identifier, index, message: "已删除"}`。

#### `POST /api/memory/toggle` — 启用 / 禁用某级记忆

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `level` | string | **是** | 无默认 | 只能为 `global` / `bot` / `group` / `c2c`（**不接受 `all`**） |
| `identifier` | string | **是** | — | 见上表 |
| `enabled` | bool | **是** | — | 真值判定为 `1` / `true` / `yes` / `on`，其余一律为 false。取值顺序：body `enabled` → body `value` → **query** `enabled`；三处都无则报 `缺少 enabled`。注意 query 的 `value` **不会被读取** |
| `value` | bool | 否 | — | `enabled` 的别名（仅 body） |
| `app_id` | string | 条件 | — | 同上 |

**不支持 `items` 批量**。`data`：`{level, identifier, enabled, message: "已启用"/"已禁用"}`。

#### `GET /api/memory/search` — 跨层级检索

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `keywords` | string \| array | **是** | — | 与 `keyword` / `q` 三者任一即可，支持重复传参；按逗号、中文逗号、分号、竖线拆分，大小写不敏感去重保序。全空报 `缺少 keywords` |
| `level` | string | 否 | `all` | `all` / `global` / `bot` / `group` / `c2c`。非法报 `未知记忆级别 X，可选：all/global/bot/group/c2c` |
| `identifier` | string | 否 | `""` | 作用随 `level`：`global`/`bot` 时作为目标机器人 APP_ID；`group` 时为群 openid；`c2c` 时为用户 openid；`level=all` 时**被忽略** |
| `group_id` | string | 否 | `""` | 群 openid。**在 `level=all` 时也会生效** |
| `c2c_user_id` | string | 否 | `""` | 私聊用户 openid，同样在 `level=all` 时生效 |
| `app_id` | string | 条件 | — | 同上 |

`data`：`keywords`、`level`、`total`、`matches[]`、`text`。
`matches[]` 每项 `{level, identifier, index, content, enabled}`；
`text` 是底层返回的拼接文本。**本接口不回显 `app_id`。**

---

### 日志

#### `GET /api/logs` — 读取日志

| 参数 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `lines` | int | 否 | `200` | 返回末尾多少**条**日志（按「条」不按物理行，多行堆栈不会被拆开）。必须能转 int 且 > 0；**上限 5000**，超出静默截断 |
| `keyword` | string | 否 | — | 子串过滤（不区分大小写）。语义是**先过滤再取尾部最后 N 条** |
| `level` | string | 否 | — | **日志等级**过滤（不是记忆级别），如 `INFO` / `WARNING` / `ERROR`，不区分大小写。只看该条日志的首行，不看正文 |
| `app_id` | string | 否 | — | 按 `[appid={app_id}` 子串过滤。**机器人密钥只能读自己的日志**：传别人的报 `403`，不传也会被强制收窄为自己的 |
| `reverse` | bool | 否 | `false` | `1`/`true`/`yes`/`on` = 最新在前，其余为正序 |

`data`：`file_name`（**只有文件名，不含完整路径**，不暴露服务端目录结构）、`exists`、`enabled`、
`level`（配置里的日志等级）、`app_id`、`total`、`matched`、`returned`、`max_log_length`、
`unlimited`（`max_log_length <= 0` 时为 true，表示不裁剪）、`lines[]`（日志文本数组）。

日志文件不存在**不算错误**：仍返回 200，`exists: false`、`lines: []`，并附
`hint: "日志文件不存在：请检查配置 enable_log 是否为 1（为 0 时只输出控制台，不写文件）"`（该分支没有 `level` 字段）。

---

## 管理面板

`admin3.0.html` 是当前版本，单文件、零外部依赖、无需构建，浏览器直接打开即可。

**功能**：聊天记录查看（含筛选、Markdown 渲染）、消息发送 / 撤回、群成员与禁言、
推送与唤醒任务管理、记忆管理、配置编辑、日志查看。

**安全设计**：

- 所有动态内容统一经 `esc()` 转义（覆盖 `& < > " '`）
- API 密钥默认**只存 `sessionStorage`**（关标签页即失效），勾选「记住密钥」才写入 `localStorage`
- Markdown 渲染顺序为「先抽代码块 → 转义 → 再套结构规则」，颠倒顺序就是经典 XSS

---

## 数据文件

运行期数据都在项目根目录，**全部是纯 JSON**（便于直接查看和手工修复）：

| 文件 / 目录 | 内容 |
|---|---|
| `config.json` | 主配置（机器人、模型、系统参数） |
| `mirror.json` | 群名 / 昵称映射缓存 |
| `user_map.json` | openid ↔ 用户 / 群 映射 |
| `memory.json` | 全局记忆 |
| `qun_memory/` `c2c_memory/` `bot_memory/` | 群 / 私聊 / 机器人自身记忆 |
| `history/` | 聊天记录（**注意：是数组，不是对象**） |
| `media_cache/` | 媒体文件缓存 |
| `scheduled_push.json` `scheduled_wakeup.json` | 定时推送 / 唤醒任务 |
| `relay_config.json` | 转发配置 |
| `log.txt` | 日志文件 |

### 写入安全

所有 JSON 写入统一走 `utils.atomic_write_json()`：

```
写临时文件 → fsync 落盘 → os.replace 原子替换 → 保留 .bak 备份
```

**进程中途被杀不会留下半个损坏的 JSON。** 排障时若怀疑数据损坏，可直接查看同目录的 `.bak`。

### 关于 `member_openid`

**同一个真人在同一个机器人下的不同群里，`member_openid` 是相同的。**
这是刻意的设计：用户级记忆（`c2c_memory/`）和用户映射（`user_map.json`）
都依赖这个性质，才能在多个群里认出同一个人。

---

## 设计约定

以下是这套代码里最容易被改坏的地方，动手前建议先读：

1. **记忆写入必须原子** —— 任何直接 `open(..., "w")` 写 JSON 的改动都会破坏原子性保证，始终用 `atomic_write_json()`。
2. **`history/*.json` 是数组**，不是 `{msg_id: msg}` 这种对象，按字典处理会直接崩。
3. **配置与日志均热更新** —— `log.py` 每次写日志都检查 `config.json` 的 mtime，改了等级不用重启；但 `bots` 列表的增删需要重启（启动时按列表创建 asyncio 任务）。
4. **推送与唤醒是两回事** —— 推送到点发**固定内容**、零模型调用；唤醒到点唤起 AI、由 AI **自主决定**做什么，成本 1+ 次模型调用。
5. **接口实现只有一份** —— `task_core.py` 是推送 / 唤醒的核心实现，HTTP 层（`api_server.py`）和 AI 工具层（`tool.py`）都只调它。两边各写一遍会导致参数不统一、字段白名单互有缺失。
6. **状态定义只有一份** —— 任务状态常量在 `task_core.py`，`scheduler` / `wakeup_scheduler` / `tool` / `api_server` 都从这里导入。放在 `tool.py` 会与「tool 延迟导入 scheduler」形成循环导入。
7. **参数名不再有别名** —— 业务参数只保留一个规范名（`target_id` / `member_id` / `message_ids` / `content` / `description`），传旧名一律报错并提示改名，不静默兼容。

### 已知的文档与代码不一致

`api_server.py` 里几处 docstring 与自描述内容已经落后于实现，**以代码为准**：

| 位置 | 文档说 | 实际 |
|---|---|---|
| `GET /` 的 `task_list_query.status` | 「只接受任务真实产生的**三种**状态」，只列 pending/completed/failed | 实际有 **4** 种，含 `partial`（部分失败） |
| `/api/push/list` docstring | 中文状态还写着 `运行中` / `已取消` | 这两个状态**已被彻底删除**，传了会被忽略并记入 `status_ignored`；且实际还接受 `部分失败` |
| `/api/wakeup/create` docstring | 说接受 `prompt` 作为提示词、`type + id` 作为单目标简写 | 实际只读 `description`，单目标简写只认 `target_type` / `target_id`，传 `prompt` 会报 `不支持的参数` |
| `/api/wakeup/trigger` docstring | 参数写作 `prompt` | 实际参数名是 `description` |

另有 4 个参数在白名单里但**代码从未读取**，传了不报错也不生效：
`/api/history` 的 `raw`、`/api/mute/status` 的 `with_names`、
`/api/push/list` 与 `/api/wakeup/list` 的 `with_counts`（`counts` 始终返回）、
`/api/config/get` 与 `/api/config/set` 的 `app_id`。

---

## 常见问题

**启动后机器人不在线**
检查 `config.json` 里该机器人的 `ENABLED` 是否为 `1`、`APP_ID` / `APP_SECRET` 是否正确。
QQ 的 access_token 有有效期并会自动刷新，但 `APP_SECRET` 填错会一直失败。

**管理面板登录报「密钥无效或未授权」**
确认服务已启动（`http://127.0.0.1:8148/health` 返回 200），
确认密钥在 `GLOBAL_API_KEYS` 或该机器人的 `API_KEYS` 里。
若通过 `file://` 打开面板，注意浏览器对本地文件的 CORS 限制。

**调用接口报「不支持的参数: xxx」**
参数名不是规范名。见「参数名严格校验」的改名对照表 —— 注意这套 API
**不做别名兼容**，旧的 `group_id` / `msg_id` 之类必须换成新名。

**返回 409 但错误码写着 502**
这是刻意设计：5xx 会被反代覆盖响应体，所以统一映射成 4xx 返回，真实语义码在
body 的 `http_status` 里。见「HTTP 状态码策略」。

**日志不写文件**
检查 `config.json` 的 `enable_log` 必须为 `1`；为 `0` 时只输出到控制台（正常行为）。

**端口被占用**
改 `config.json` 的 `API_BIND_PORT` 后重启即可。

---

## 许可

本项目暂未附带开源许可证文件。如需转载、二次分发或商用，请先联系作者。

- 作者：**飞扬nb**
- 依赖：[aiohttp](https://github.com/aio-libs/aiohttp) ·
  [websockets](https://github.com/python-websockets/websockets) ·
  [requests](https://github.com/psf/requests) ·
  [loguru](https://github.com/Delgan/loguru)
