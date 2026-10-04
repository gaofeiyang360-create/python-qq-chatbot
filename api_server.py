# -*- coding: utf-8 -*-
# api_server.py — HTTP API 服务（aiohttp）
# 提供群/私聊列表查看、消息查看、主动发送、撤回、禁言管理、定时推送与定时唤醒配置
"""
鉴权模型（三条通道，互不干扰）：

1) 机器人密钥  —— 每个 bot 的 API_KEYS
   只能操作"该机器人自己"的数据与动作（app_id 被锁定为该机器人）。

2) 全局密钥    —— 顶层配置 GLOBAL_API_KEYS
   拥有全部权限，可以通过 app_id 参数管理任意机器人。
   由顶层开关 GLOBAL_API_ENABLED 独立控制；关闭它只停用本通道，
   各机器人自己的 API_KEYS 通道照常工作。

3) 无密钥模式  —— 对应密钥数组为空数组时，该通道不需要密钥即可访问。

密钥传递方式（三种都支持）：
   - Authorization: Bearer <key>
   - X-API-Key: <key>
   - URL 查询参数 ?key=<key>

实时配置：所有开关与密钥每次请求都重新读取 config.json，改完即生效，无需重启。
"""
import asyncio
import json
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

from aiohttp import web

from config import (
    get_bots, get_bot_enabled,
    get_bot_api_enabled, get_bot_api_keys,
    get_global_api_enabled, get_global_api_keys,
    get_api_bind_host, get_api_bind_port,
    get_bot_allow_cross_push, get_bot_allow_cross_push_incoming,
    get_bot_allow_manage_all_push,
    get_bot_allow_cross_wakeup, get_bot_allow_cross_wakeup_incoming,
    get_bot_allow_cross_get_list, get_bot_allow_cross_get_list_incoming,
    get_bot_allow_cross_history, get_bot_allow_cross_history_incoming,
    get_config, write_config, is_sensitive_key, is_api_key_field,
    DEFAULT_CONFIG, deep_patch_defaults,
    get_log_file, get_enable_log, get_log_level, get_max_log_length,   # /api/logs
)
from log import info, warn, error, debug, LogCtx
from log import _split_log_entries   # 日志按「条」切分（与写入端同一套口径）
from memory import (
    load_user_map, load_mirror, load_history,
    get_user_name, get_group_name_from_mirror,
    append_message, mark_message_revoked,
    set_message_hidden,   # 隐藏消息（is_hide=1，不发给 AI），与撤回独立
    parse_history_ts,   # 从结构化 ts 字段解析消息时间
    search_memory,      # 记忆关键词搜索（与 AI 工具共用同一份实现）
)
from client import BotClient
from utils import parse_message_type

# 角色标识
ROLE_GLOBAL = "global"   # 全局密钥通道：全权
ROLE_BOT = "bot"         # 机器人密钥通道：仅限本机器人
ROLE_NONE = "none"       # 无密钥模式


# ==================== 鉴权 ====================
def _extract_keys(request: web.Request) -> List[str]:
    """从请求中提取候选密钥（可能同时带多个来源，全部收集用于匹配）"""
    keys: List[str] = []

    # 1) Authorization: Bearer <key>
    auth = request.headers.get("Authorization", "") or ""
    if auth:
        parts = auth.split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            keys.append(parts[1].strip())
        else:
            # 兼容直接放裸 key 的写法
            keys.append(auth.strip())

    # 2) X-API-Key
    xk = request.headers.get("X-API-Key", "") or ""
    if xk.strip():
        keys.append(xk.strip())

    # 3) URL 查询参数 ?key=  （同时兼容 api_key / apikey）
    for pname in ("key", "api_key", "apikey"):
        qv = request.query.get(pname, "")
        if qv and qv.strip():
            keys.append(qv.strip())

    # 去重保序
    seen = set()
    out = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def authenticate(request: web.Request) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    鉴权。返回 (auth_info, error_message)。
    auth_info: {"role":..., "app_id": app_id or None, "can_manage_all": bool}

    优先级：全局密钥 > 机器人密钥（同一请求若同时命中，按权限更高的处理）
    """
    presented = _extract_keys(request)

    global_enabled = get_global_api_enabled()
    global_keys = get_global_api_keys()

    # ---- 通道 1：全局密钥 ----
    if global_enabled:
        if not global_keys:
            # 全局密钥为空数组 → 无需密钥且拥有全部权限
            return {"role": ROLE_GLOBAL, "app_id": None, "can_manage_all": True}, None
        for pk in presented:
            if pk in global_keys:
                return {"role": ROLE_GLOBAL, "app_id": None, "can_manage_all": True}, None

    # ---- 通道 2：各机器人密钥 ----
    for bot in get_bots():
        aid = bot.get("APP_ID", "")
        if not aid:
            continue
        if not get_bot_api_enabled(aid):
            continue
        bkeys = get_bot_api_keys(aid)
        if not bkeys:
            # 该机器人密钥为空数组 → 无需密钥，但仅限该机器人
            return {"role": ROLE_BOT, "app_id": aid, "can_manage_all": False}, None
        for pk in presented:
            if pk in bkeys:
                return {"role": ROLE_BOT, "app_id": aid, "can_manage_all": False}, None

    # ---- 都未通过 ----
    if not global_enabled and not any(get_bot_api_enabled(b.get("APP_ID", "")) for b in get_bots()):
        return None, "API 服务未开启"
    return None, "鉴权失败：密钥无效或缺失"


def _resolve_app_id(auth: Dict[str, Any], request: web.Request) -> Tuple[Optional[str], Optional[str]]:
    """
    解析本次请求要操作的机器人 app_id。
    - 机器人密钥通道：锁定为自己的 app_id；若请求显式指定了别的 app_id，直接报权限错误
      （而不是静默返回自己的数据，避免调用方误以为操作了目标机器人）
    - 全局密钥通道：可以显式指定 app_id，缺省时若只有一个可用机器人则用它
    """
    requested = (request.query.get("app_id") or "").strip()
    if not requested:
        body = request.get("json_body")
        if isinstance(body, dict):
            requested = str(body.get("app_id") or "").strip()

    if auth["role"] == ROLE_BOT:
        own = auth["app_id"]
        if requested and requested != own:
            return None, f"权限不足：本密钥只能操作机器人 {own}"
        return own, None

    if requested:
        for b in get_bots():
            if b.get("APP_ID") == requested:
                return requested, None
        return None, f"未找到机器人 {requested}"

    # 未指定：自动选择唯一启用的机器人
    ids = [b.get("APP_ID") for b in get_bots() if b.get("APP_ID") and get_bot_enabled(b.get("APP_ID"))]
    if len(ids) == 1:
        return ids[0], None
    return None, "请通过 app_id 参数指定要操作的机器人"


def _check_cross_allowed(auth: Dict[str, Any], caller_app_id: str, target_app_id: str) -> Optional[str]:
    """机器人密钥通道下，禁止操作其他机器人（全局密钥不受限）"""
    if auth["role"] == ROLE_GLOBAL:
        return None
    if target_app_id != caller_app_id:
        return f"权限不足：本密钥只能操作机器人 {caller_app_id}"
    return None


# ==================== 响应辅助 ====================
def ok(data: Any = None, **extra) -> web.Response:
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    payload.update(extra)
    return web.json_response(payload, dumps=lambda o: json.dumps(o, ensure_ascii=False, default=str))


def fail(message: str, status: int = 400, **extra) -> web.Response:
    """构造失败响应。

    HTTP 状态码策略（重要，改动原因见下）：

      400 / 403 / 404 / 401 —— 原样返回，表示「调用方用错了」。

      500 / 502 / 任意 5xx —— 映射为 4xx 后返回，真实语义码放进 body 的
                           http_status。映射关系：
                             500      → HTTP 422
                             502      → HTTP 409
                             其它 5xx → HTTP 409（保守，避免任何 5xx 漏出去）

    为什么不能直接返回 500/502（线上实测，经 Cloudflare 反代）：

        后端返回 200/400/401/403/404 → Cloudflare 原样透传，能看到 JSON
        后端返回 5xx                 → Cloudflare 用自己的错误页**覆盖响应体**，
                                       调用方只能看到纯文本 "error code: 502"，
                                       JSON 里的 error 文案全部丢失

    而禁言/撤回这类操作失败时必然返回 502，于是表现为
    「接口永远 502、看不到任何错误原因，无法排查」。

    为什么选 422 / 409 而不是统一 400：
        422 Unprocessable Entity —— 语义是「请求本身合法，但服务器无法处理」，
                                    正好对应 500 那类「后端处理时出错」。
        409 Conflict            —— 语义是「与当前资源状态冲突」，
                                    正好对应 502 那类「QQ 侧拒绝了这次操作」
                                    （如成员是管理员不能禁言、消息已过期）。
        保留区分度，排查时一眼能看出是「后端炸了」还是「业务被拒」。

    前端判错看的是 body 里的 ok === false，与 HTTP 状态码无关，
    因此改动后前端的错误提示行为完全不变。
    真实语义码保留在 http_status，日志与排查不受影响。
    """
    payload = {"ok": False, "error": message}
    if 500 <= status <= 599:
        payload["http_status"] = status
        status = 422 if status == 500 else 409
    payload.update(extra)
    return web.json_response(
        payload, status=status,
        dumps=lambda o: json.dumps(o, ensure_ascii=False, default=str)
    )


# ==================== 异常信息脱敏（M15） ====================
# 请求体大小上限（M14）。1MB 与 aiohttp 默认 client_max_size 对齐。
_MAX_BODY_BYTES = 1024 * 1024

# 异常文本里需要抹掉的绝对路径：
#   Windows  D:\dsh-workspace\xxx.json / C:\Users\name\...
#   POSIX    /home/user/app/xxx.json
# 以及常见的环境变量展开痕迹。
#
# 为什么必须脱敏：约 30 处 fail(f"...异常: {e}", 500) 会把原始异常直接回给
# 调用方。FileNotFoundError / PermissionError 这类异常的 str() 里**含部署
# 绝对路径**，等于把服务端目录结构、用户名、盘符免费送给任何能调用 API 的人
# （在无密钥模式下更是对所有人开放）。完整异常仍然写进日志，排查不受影响。
_PATH_PATTERNS = (
    re.compile(r"[A-Za-z]:\\[^\s'\"<>|]+"),      # Windows 盘符路径
    re.compile(r"\\\\[^\s'\"<>|]+"),              # UNC 路径
    re.compile(r"/(?:home|root|usr|var|opt|tmp|etc|Users)/[^\s'\"<>|]+"),  # POSIX
)


def _safe_error(exc: Exception, action: str, *, ctx=None, status: int = 500) -> web.Response:
    """把异常转成给调用方的响应：**保留异常类型与语义，去掉路径等敏感细节**。

    - 日志记录完整异常（含堆栈上下文），排查照旧；
    - 响应只给「动作 + 异常类型 + 脱敏后的消息」，让调用方知道发生了什么，
      但拿不到部署路径。
    """
    raw = f"{type(exc).__name__}: {exc}"
    error(f"[API] {action}失败: {raw}", ctx=ctx)          # 完整信息只进日志

    safe = str(exc)
    for pat in _PATH_PATTERNS:
        safe = pat.sub("<path>", safe)
    # 兜底：万一还有别的盘符/长路径痕迹，压掉连续反斜杠后的内容
    safe = re.sub(r"[A-Za-z]:\\\S*", "<path>", safe)
    if len(safe) > 300:
        safe = safe[:300] + "…"

    return fail(f"{action}失败（{type(exc).__name__}）：{safe}", status)


async def _read_json(request: web.Request) -> Dict:
    """安全读取 JSON body，并缓存到 request['json_body'] 供 app_id 解析使用。

    ★ 读之前先按 Content-Length 拒绝超大请求体（M14）：
      aiohttp 默认的 client_max_size 是 1MB，但它在**读满时**才抛错，且
      本中间件链会先调用本函数把整个 body 读进内存，等于用一个无上限的
      read 去挡一个本该在入口拒绝的请求。显式比对 Content-Length 可以在
      进入读取前就返回 413，避免大 body 占用内存。
      Content-Length 缺失时（chunked）退回 aiohttp 自身的上限保护。
    """
    declared = request.headers.get("Content-Length")
    if declared:
        try:
            if int(declared) > _MAX_BODY_BYTES:
                request["json_body"] = {}
                return {}
        except (TypeError, ValueError):
            request["json_body"] = {}
            return {}

    body: Dict = {}
    if request.can_read_body:
        try:
            raw = await request.text()
            if raw and raw.strip():
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    body = parsed
        except Exception:
            body = {}
    request["json_body"] = body
    return body


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


# 各接口允许的参数名（规范名，无别名）。
# 传了不在名单里的参数（例如旧的 group_id / member_openid）直接报错，
# 而不是静默忽略——静默忽略会让调用方以为生效了，其实取的是全部数据。
# 任务列表接口（推送 / 唤醒）允许的全部筛选参数
_LIST_FILTER_PARAMS = (
    "status", "keywords", "keyword", "q", "search", "match_all",
    "task_id", "task_app_id", "target_type", "target_id", "isolation_mode",
    "start", "start_time", "end", "end_time", "time_field", "order", "limit",
)

STRICT_PARAMS = {
    "/api/members":       {"target_id", "app_id", "search", "all", "key"},
    "/api/bots":          {"search", "key"},
    "/api/contacts":      {"app_id", "search", "key"},
    "/api/groups":        {"app_id", "search", "key"},
    "/api/users":         {"app_id", "search", "key"},
    "/api/send":          {"target_type", "target_id", "app_id",
                           "content", "message_type", "markdown", "markdown_content",
                           "media_source", "file_type", "file_name", "key"},
    "/api/bot_state":     {"target_id", "app_id", "key"},
    "/api/mute/status":   {"target_id", "app_id", "with_names", "key"},
    "/api/revoke":        {"target_type", "target_id", "message_ids", "app_id", "key"},
    # 隐藏消息（改 is_hide，不发给 AI，与撤回独立）
    "/api/message/hide":  {"target_type", "target_id", "message_ids",
                           "is_hide", "app_id", "key"},
    "/api/mute":          {"target_id", "members", "member_id", "op", "seconds",
                           "mute_expire_at", "app_id", "key"},
    "/api/push/create":   {"targets", "target_type", "target_id", "app_id",
                           "content", "schedule_type", "schedule_time",
                           "interval_seconds", "media_source", "file_type",
                           "file_name", "message_type", "markdown", "is_markdown", "key"},
    "/api/wakeup/create": {"targets", "target_type", "target_id", "app_id",
                           "description", "schedule_type", "schedule_time",
                           "interval_seconds", "initiator", "isolation_mode", "key"},
    "/api/batch_revoke":  {"keywords", "start_time", "end_time", "target_type",
                           "target_id", "dry_run", "confirm", "app_id", "key"},
    "/api/push/update":   {"task_id", "app_id", "key", "updates",
                           "content", "targets", "schedule_type", "schedule_time",
                           "interval_seconds", "message_type", "markdown",
                           "is_markdown", "media", "media_source", "file_type", "file_name"},
    "/api/push/delete":   {"task_id", "app_id", "key"},
    "/api/wakeup/update": {"task_id", "app_id", "key", "updates",
                           "description", "targets", "schedule_type", "schedule_time",
                           "interval_seconds", "initiator", "isolation_mode"},
    "/api/wakeup/delete": {"task_id", "app_id", "key"},
    "/api/wakeup/trigger":{"target_type", "target_id", "target_name", "description",
                           "initiator", "isolation_mode", "app_id", "key"},
    "/api/history":       {"target_type", "target_id", "app_id", "key",
                           "keyword", "limit", "revoked", "markdown", "tools",
                           "wakeup", "is_hide", "raw"},
    "/api/push/list":     set(_LIST_FILTER_PARAMS) | {"app_id", "key", "with_counts"},
    "/api/wakeup/list":   set(_LIST_FILTER_PARAMS) | {"app_id", "key", "with_counts"},
    "/api/config/get":    {"path", "app_id", "key"},
    "/api/config/list":   {"app_id", "scope", "key"},
    "/api/config/set":    {"path", "set", "value", "app_id", "key"},
    # 记忆管理（与 AI 工具 view_memory / add_memory / modify_memory /
    # delete_memory / enable_memory / disable_memory / search_memory 对应）
    "/api/memory/list":   {"level", "identifier", "app_id", "key"},
    "/api/memory/add":    {"level", "identifier", "content", "items", "app_id", "key"},
    "/api/memory/update": {"level", "identifier", "index", "content", "items", "app_id", "key"},
    "/api/memory/delete": {"level", "identifier", "index", "items", "app_id", "key"},
    "/api/memory/toggle": {"level", "identifier", "enabled", "value", "app_id", "key"},
    "/api/memory/search": {"keywords", "keyword", "q", "level", "identifier",
                           "group_id", "c2c_user_id", "app_id", "key"},
    "/api/logs":          {"lines", "keyword", "level", "app_id", "reverse", "key"},
}


def _reject_unknown_params(request, path: str, body: dict):
    """校验参数名是否都是规范名；返回错误响应或 None。

    只对列入 STRICT_PARAMS 的接口生效，且只在「关键参数缺失」时提示，
    避免影响 key 等鉴权参数的多种传法。
    """
    allowed = STRICT_PARAMS.get(path)
    if not allowed:
        return None
    given = set(body.keys()) if body else set(request.query.keys())
    unknown = {k for k in given if k.lower() not in {a.lower() for a in allowed}}
    unknown = {k for k in unknown if k.lower() != "key"}
    if not unknown:
        return None
    hint = {
        "group_id": "target_id", "group_openid": "target_id", "id": "target_id",
        "member_openid": "member_id", "user_id": "member_id",
        "message_id": "message_ids", "msg_id": "message_ids", "msg_ids": "message_ids",
        "type": "target_type",
    }
    parts = []
    for k in sorted(unknown):
        parts.append(f"{k}（请改用 {hint[k]}）" if k in hint else k)
    return fail("不支持的参数: " + "、".join(parts), 400)


# 通过 API 完全不暴露的顶层配置键。
# models 数组含模型 base_url 与明文 api_key，属于服务端内部配置，
# 任何 /api/config/* 读取路径都不应返回它。
API_HIDDEN_CONFIG_KEYS = {"MODELS"}


def _is_hidden_cfg_key(key) -> bool:
    """该顶层配置键是否禁止通过 API 读取"""
    return str(key).strip().upper() in API_HIDDEN_CONFIG_KEYS


# ==================== 任务列表过滤（推送 / 唤醒共用） ====================
# 具体实现在 tool.py 里（与 AI 工具共用同一份逻辑），这里只做别名导入。
from tool import (                  # noqa: E402
    _TaskParams as _NormParams,
    _filter_tasks as _filter_tasks,
    _task_counts as _task_counts,
    _norm_task_status as _norm_status,
    _split_multi as _split_multi,
    _parse_task_ts as _parse_ts,
    _task_search_text as _task_search_text,
    TASK_STATUSES as _TASK_STATUSES,
    _TASK_STATUS_LABELS as _STATUS_LABELS,
)

# 记忆管理：实现在 memory.py 的「记忆管理统一入口」一节，
# 与 AI 工具的 view/add/modify/delete/enable/disable_memory 共用同一份函数对象。
# 直接从 memory 导入（而非经 tool 转发），避免为用记忆而依赖工具定义模块。
from memory import (                # noqa: E402
    MEMORY_LEVELS as _MEMORY_LEVELS,
    _memory_get as _memory_get,
    _memory_add as _memory_add,
    _memory_replace as _memory_replace,
    _memory_remove as _memory_remove,
    _memory_set_enabled as _memory_set_enabled,
    _memory_batch as _memory_batch,
    MemoryError_ as _MemoryError,
    MemoryDisabledError as _MemoryDisabledError,
    MemoryLevelError as _MemoryLevelError,
)


# ==================== CORS ====================
# 允许的来源列表。"*" 表示允许所有来源（回显请求的 Origin，以兼容携带凭证的场景）。
# 注意：Origin 为 "null" 的情况见于用 file:// 直接打开本地 HTML，
# 必须原样回显 "null" 才能让本地管理页正常工作。
#
# ★ 这是**有意的设计决定**：本项目支持直接用 file:// 双击打开管理页
#   （本地单机运维场景，免起 http 服务）。因此不对 "null" 做拒绝处理 ——
#   它带来的风险（本地任意 HTML 可跨域调用本 API）由部署方通过
#   API_BIND_HOST / API_KEYS 自行控制，不属于代码层面的缺陷。
CORS_ALLOW_ORIGIN = "*"

# 预检响应中允许的请求头（覆盖管理页用到的全部自定义头）
CORS_ALLOW_HEADERS = (
    "Authorization, X-API-Key, Content-Type, Accept, Origin, "
    "X-Requested-With, Cache-Control, Pragma"
)

# 允许的方法
CORS_ALLOW_METHODS = "GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD"

# 预检结果缓存时间（秒）
CORS_MAX_AGE = "86400"


def _cors_origin(request: web.Request) -> Optional[str]:
    """
    计算应当回显的 Access-Control-Allow-Origin 值。
    - 配置为 "*"：回显请求的 Origin（含 "null"，支持 file:// 本地管理页）；无 Origin 时返回 "*"
    - 配置为具体域名列表：命中则回显，否则返回 None（不加 CORS 头）
    """
    origin = request.headers.get("Origin")

    if CORS_ALLOW_ORIGIN.strip() == "*":
        # 回显 Origin 而非直接返回 "*"，这样 Origin: null（file://）也能被允许
        return origin if origin else "*"

    allowed = [o.strip() for o in CORS_ALLOW_ORIGIN.split(",") if o.strip()]
    if origin and origin in allowed:
        return origin
    # 无 Origin 的请求（同源、curl 等）不需要 CORS，但也无害
    if not origin:
        return "*"
    return None


@web.middleware
async def cors_middleware(request: web.Request, handler):
    """
    统一的 CORS 处理：
    1) 预检请求（OPTIONS）直接在此返回 204，不进入鉴权中间件
       —— 浏览器预检不会携带 Authorization，若交给鉴权必然失败
    2) 其余请求在响应上附加 CORS 头
    """
    origin = _cors_origin(request)

    # ---------- 预检请求 ----------
    if request.method == "OPTIONS":
        resp = web.Response(status=204)
        if origin:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Methods"] = CORS_ALLOW_METHODS
            req_headers = request.headers.get("Access-Control-Request-Headers")
            resp.headers["Access-Control-Allow-Headers"] = req_headers or CORS_ALLOW_HEADERS
            resp.headers["Access-Control-Max-Age"] = CORS_MAX_AGE
            resp.headers["Access-Control-Expose-Headers"] = "*"
        # 回显 Origin 时必须声明 Vary，避免缓存串味
        resp.headers["Vary"] = "Origin"
        return resp

    # ---------- 普通请求 ----------
    try:
        resp = await handler(request)
    except web.HTTPException as ex:
        resp = ex

    if origin:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Methods"] = CORS_ALLOW_METHODS
        resp.headers["Access-Control-Allow-Headers"] = CORS_ALLOW_HEADERS
        resp.headers["Access-Control-Expose-Headers"] = "*"
        resp.headers["Vary"] = "Origin"
    return resp


# ==================== BotClient 缓存 ====================
_client_cache: Dict[str, BotClient] = {}


def get_client(app_id: str) -> Optional[BotClient]:
    """获取（或创建）指定机器人的 BotClient"""
    for b in get_bots():
        if b.get("APP_ID") == app_id:
            if app_id not in _client_cache:
                _client_cache[app_id] = BotClient(app_id, b.get("APP_SECRET", ""))
            return _client_cache[app_id]
    return None


# ==================== 统一鉴权中间件 ====================
@web.middleware
async def auth_middleware(request: web.Request, handler):
    # 预检请求不应鉴权（正常情况下已被 cors_middleware 短路，此处为兜底）
    if request.method == "OPTIONS":
        return web.Response(status=204)

    # 健康检查/根路径不需要鉴权
    if request.path in ("/", "/health"):
        return await handler(request)

    # /info：**尝试**鉴权但即使失败也放行（LOW 修复）。
    #   它既要能被未鉴权的前端探活，又要在带了有效密钥时返回完整信息
    #   （见 h_info：无 auth 时只回 {alive:true}，不泄露机器人清单与绑定地址）。
    #   因此这里不能像其它接口那样在鉴权失败时 401，只能把结果放进 request。
    if request.path == "/info":
        try:
            auth_opt, _err = authenticate(request)
        except Exception:
            auth_opt = None
        if auth_opt is not None:
            request["auth"] = auth_opt
        return await handler(request)

    # 先读取 body（供 app_id 解析），GET 请求无 body
    if request.method in ("POST", "PUT", "PATCH"):
        await _read_json(request)

    auth, err = authenticate(request)
    if auth is None:
        # 鉴权尚未通过，拿不到任何 app_id / 会话标识，只带请求本身可辨认的信息
        ctx = LogCtx()
        warn(f"[API] 鉴权失败 {request.method} {request.path}: {err}", ctx=ctx)
        return fail(err, status=401)

    # 鉴权通过后即可确定机器人；请求体里的 app_id 供全局密钥通道解析
    ctx = LogCtx(app_id=str(auth.get("app_id") or ""))

    request["auth"] = auth
    return await handler(request)


# ==================== 1. 列表查看 ====================
async def h_groups(request: web.Request) -> web.Response:
    """群列表：/api/groups[?app_id=][&search=]"""
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)
    if not get_bot_enabled(app_id):
        return fail(f"机器人 {app_id} 已禁用", 403)

    if (e := _reject_unknown_params(request, "/api/groups", {})):
        return e

    search = (request.query.get("search") or "").strip().lower()
    user_map = load_user_map()
    mirror = load_mirror()

    raw = user_map.get(app_id, {}).get("group", {})
    if isinstance(raw, list):
        groups = {gid: [] for gid in raw}
    elif isinstance(raw, dict):
        groups = raw
    else:
        groups = {}

    names = mirror.get("groups", {}).get(app_id, {})
    # 成员名称映射：mirror.users[app_id][openid] = 昵称
    user_names = mirror.get("users", {}).get(app_id, {})
    result = []
    for gid, members in groups.items():
        name = names.get(gid) or get_group_name_from_mirror(app_id, gid) or ""
        mlist = members if isinstance(members, list) else []
        # 成员带名称返回（名称缺失时为空串，前端回退显示 ID）
        member_objs = [
            {
                "member_id": mid,
                "member_name": user_names.get(mid) or get_user_name(mid, app_id) or "",
            }
            for mid in mlist
        ]
        item = {
            "group_id": gid,
            "group_name": name,
            "member_count": len(mlist),
            "members": member_objs,
            # 兼容旧字段：纯 ID 列表
            "member_ids": mlist,
        }
        if search and search not in (name or "").lower() and search not in gid.lower():
            continue
        result.append(item)

    return ok({"app_id": app_id, "total": len(result), "groups": result})


async def h_members(request: web.Request) -> web.Response:
    """群成员列表：/api/members?target_id=xxx[&app_id=][&search=][&all=1]

    参数：target_id（群 openid）。
    返回带昵称的群成员列表。
    - target_id 指定群时只返回该群成员；不指定则返回全部群的成员。
    - search 可按昵称或 openid 过滤。
    - all=1 时同时附带每个成员的原始 openid 字段（与 member_id 相同，便于兼容）。
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)
    if not get_bot_enabled(app_id):
        return fail(f"机器人 {app_id} 已禁用", 403)

    if (e := _reject_unknown_params(request, "/api/members", {})):
        return e

    group_id = request.query.get("target_id", "").strip()
    search = (request.query.get("search") or "").strip().lower()
    want_all = (request.query.get("all") or "").strip().lower() in ("1", "true", "yes")

    user_map = load_user_map()
    mirror = load_mirror()

    raw = user_map.get(app_id, {}).get("group", {})
    if isinstance(raw, list):
        groups = {gid: [] for gid in raw}
    elif isinstance(raw, dict):
        groups = raw
    else:
        groups = {}

    if group_id:
        if group_id not in groups:
            return fail(f"未找到群 {group_id} 的成员记录（该群没有本地记录，请先同步成员）", 404)
        groups = {group_id: groups[group_id]}

    group_names = mirror.get("groups", {}).get(app_id, {})
    user_names = mirror.get("users", {}).get(app_id, {})

    result = []
    for gid, members in groups.items():
        mlist = members if isinstance(members, list) else []
        objs = []
        for mid in mlist:
            mname = user_names.get(mid) or get_user_name(mid, app_id) or ""
            if search and search not in (mname or "").lower() and search not in mid.lower():
                continue
            obj = {
                "member_id": mid,
                "member_name": mname,
                "has_name": bool(mname),
                "group_id": gid,
            }
            if want_all:
                obj["openid"] = mid
            objs.append(obj)

        # 未指定 group_id 时按群分组返回
        result.append({
            "group_id": gid,
            "group_name": group_names.get(gid) or get_group_name_from_mirror(app_id, gid) or "",
            "member_count": len(objs),
            "total_recorded": len(mlist),
            "members": objs,
        })

    total = sum(g["member_count"] for g in result)
    return ok({
        "app_id": app_id,
        "group_filter": group_id or None,
        "total": total,
        "group_count": len(result),
        "groups": result,
    })


async def h_users(request: web.Request) -> web.Response:
    """私聊用户列表：/api/users[?app_id=][&search=]"""
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)
    if not get_bot_enabled(app_id):
        return fail(f"机器人 {app_id} 已禁用", 403)

    if (e := _reject_unknown_params(request, "/api/users", {})):
        return e

    search = (request.query.get("search") or "").strip().lower()
    user_map = load_user_map()
    mirror = load_mirror()

    uids = user_map.get(app_id, {}).get("user", []) or []
    if not isinstance(uids, list):
        uids = []
    names = mirror.get("users", {}).get(app_id, {})

    result = []
    for uid in uids:
        name = names.get(uid) or get_user_name(uid, app_id) or ""
        if search and search not in (name or "").lower() and search not in uid.lower():
            continue
        result.append({"user_id": uid, "user_name": name})

    return ok({"app_id": app_id, "total": len(result), "users": result})


async def h_contacts(request: web.Request) -> web.Response:
    """总览：/api/contacts[?app_id=]  同时返回群与私聊"""
    if (e := _reject_unknown_params(request, "/api/contacts", {})):
        return e

    g = await h_groups(request)
    u = await h_users(request)
    if g.status != 200:
        return g
    if u.status != 200:
        return u
    gj = json.loads(g.body.decode("utf-8"))
    uj = json.loads(u.body.decode("utf-8"))
    return ok({
        "app_id": gj["data"]["app_id"],
        "groups": gj["data"]["groups"],
        "users": uj["data"]["users"],
        "group_total": gj["data"]["total"],
        "user_total": uj["data"]["total"],
    })


async def h_bots(request: web.Request) -> web.Response:
    """可用机器人列表：/api/bots[?search=] （全局密钥或机器人密钥均可看自己的）

    search 可按 APP_ID 或描述（desc）关键词过滤，与工具 get_targets 的 search 行为一致。

    额外返回 role / can_manage_all：调用方（管理面板）据此决定要不要提供
    「全局」选项 —— 全局密钥可以不带 app_id 直接查（此时由后端按各接口规则
    自行解析或聚合），机器人密钥则必须锁定在自己身上。
    """
    auth = request["auth"]
    if (e := _reject_unknown_params(request, "/api/bots", {})):
        return e

    search = (request.query.get("search") or "").strip().lower()
    out = []
    for b in get_bots():
        aid = b.get("APP_ID", "")
        if not aid:
            continue
        if auth["role"] == ROLE_BOT and aid != auth["app_id"]:
            continue
        desc = b.get("desc", "")
        if search and search not in aid.lower() and search not in str(desc).lower():
            continue
        out.append({
            "app_id": aid,
            "enabled": get_bot_enabled(aid),
            "api_enabled": get_bot_api_enabled(aid),
            "desc": desc,
            # 是否配了本机器人独立密钥。只给布尔值，不回显密钥本身。
            # 管理面板的机器人视图据此显示「独立密钥 / 全局密钥」，
            # 避免为了这一个字段去调 /info（后者未鉴权时不返回 bots）。
            "has_keys": len(get_bot_api_keys(aid)) > 0,
        })
    return ok({
        "total": len(out),
        "bots": out,
        # 当前密钥的角色：管理面板用它决定是否显示「全局」选项
        "role": auth["role"],
        "can_manage_all": bool(auth.get("can_manage_all")),
        # 本密钥被限定的 app_id（机器人密钥才有；全局密钥为 None）
        "scope_app_id": auth.get("app_id") or None,
    })


# ==================== 2. 消息查看 ====================
async def h_history(request: web.Request) -> web.Response:
    """消息查看：/api/history?target_type=group|c2c&target_id=xxx[&app_id=][&limit=20][&keyword=][&revoked=][&markdown=][&tools=][&wakeup=][&is_hide=]

    返回内容：原样保留聊天记录 json 的全部字段
    （role / content / ts / username / user_id / msg_id / msg_idx / ref_msg_idx /
     tool_calls / tool_call_id / is_summary / revoked / is_hide ...）
    在此基础上额外统一补充以下状态字段：
    - revoked      是否已撤回（bool）
    - is_hide      是否已隐藏「不发给 AI」（1/0），与 revoked 完全无关
    - is_markdown  是否 Markdown 消息（1/0）
    - is_wakeup    是否唤醒消息，含定时唤醒与 API 唤醒（1/0）
    - is_tool_call 是否属于工具调用流程，发起与返回都为 1（1/0）
    - tool_names   仅发起方有，工具名数组
    - is_thinking  是否为思考消息：assistant 且未真正发出（无 msg_id）（1/0）

    说明：记录已是结构化格式 —— content 只含正文，时间戳/昵称/用户ID 分别
    由 ts / username / user_id 字段承载，前端直接取字段即可，无需正则反解。

    过滤参数：revoked / markdown / tools / wakeup / is_hide（1=只看，0=只看否）
    raw=1 为兼容别名，行为与默认一致。
    """
    if (e := _reject_unknown_params(request, "/api/history", {})):
        return e

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    target_type = (request.query.get("target_type") or "").strip().lower()
    target_id = (request.query.get("target_id") or "").strip()
    if target_type not in ("group", "c2c"):
        return fail("target_type 必须为 group 或 c2c")
    if not target_id:
        return fail("缺少 target_id")

    thread_key = f"{target_type}_{target_id}"
    ctx = LogCtx(app_id=app_id, thread_key=thread_key)
    hist = load_history(thread_key)

    # 关键词过滤
    keyword = (request.query.get("keyword") or "").strip()
    if keyword:
        hist = [m for m in hist if keyword in str(m.get("content", ""))]

    # 数量限制（取最新的 limit 条）
    try:
        limit = int(request.query.get("limit", 50))
    except (TypeError, ValueError):
        limit = 50
    total = len(hist)
    selected = hist[-limit:] if limit > 0 else hist

    # 按撤回状态过滤：revoked=1 只看已撤回，revoked=0 只看未撤回，不传则全部
    revoked_filter = (request.query.get("revoked") or "").strip().lower()
    if revoked_filter in ("1", "true", "yes"):
        selected = [m for m in selected if m.get("revoked")]
    elif revoked_filter in ("0", "false", "no"):
        selected = [m for m in selected if not m.get("revoked")]

    # 按消息类型过滤：markdown=1 只看 Markdown 消息，markdown=0 只看非 Markdown
    md_filter = (request.query.get("markdown") or "").strip().lower()
    if md_filter in ("1", "true", "yes"):
        selected = [m for m in selected if m.get("is_markdown")]
    elif md_filter in ("0", "false", "no"):
        selected = [m for m in selected if not m.get("is_markdown")]

    # 是否只保留工具调用相关的记录
    tools_filter = (request.query.get("tools") or "").strip().lower()
    if tools_filter in ("1", "true", "yes"):
        selected = [m for m in selected
                    if m.get("tool_calls") or m.get("role") == "tool"]

    # 按唤醒状态过滤：wakeup=1 只看唤醒消息，wakeup=0 只看非唤醒消息
    wakeup_filter = (request.query.get("wakeup") or "").strip().lower()
    if wakeup_filter in ("1", "true", "yes"):
        selected = [m for m in selected if m.get("is_wakeup")]
    elif wakeup_filter in ("0", "false", "no"):
        selected = [m for m in selected if not m.get("is_wakeup")]

    # 按「隐藏」状态过滤：is_hide=1 只看已隐藏的，is_hide=0 只看参与上下文的。
    # 注意这与 revoked 完全无关：撤回是消息在 QQ 侧被撤回，is_hide 是人工隐藏。
    ishide_filter = (request.query.get("is_hide") or "").strip().lower()
    if ishide_filter in ("1", "true", "yes"):
        selected = [m for m in selected if m.get("is_hide")]
    elif ishide_filter in ("0", "false", "no"):
        selected = [m for m in selected if not m.get("is_hide")]

    # raw=1 作为兼容别名保留（默认模式现在已返回全部字段，两者行为一致）
    msgs = []
    revoked_count = 0
    markdown_count = 0
    tool_call_count = 0
    wakeup_count = 0
    is_hide_count = 0
    for m in selected:
        item = {"role": m.get("role", ""), "content": m.get("content", "")}
        # 原样保留聊天记录 json 的全部字段（msg_id / msg_idx / ref_msg_idx /
        # tool_calls / tool_call_id / is_summary / revoked ... 一律不丢）
        item.update({k: v for k, v in m.items() if k not in ("role", "content")})

        # 统一补齐状态字段（始终返回，便于调用方判断；与原始值一致）
        item["revoked"] = bool(m.get("revoked"))
        item["is_markdown"] = 1 if m.get("is_markdown") else 0
        item["is_wakeup"] = 1 if m.get("is_wakeup") else 0
        # is_hide：1=该条已隐藏、不发给 AI（默认 0）。与 revoked 无关，各自独立。
        item["is_hide"] = 1 if m.get("is_hide") else 0
        # 媒体消息原始 URL（非媒体消息为 None）
        item["media_url"] = m.get("media_url") or None

        # ---- AI 工具调用记录 ----
        # assistant 发起：is_tool_call=1，并附上工具名数组
        if m.get("tool_calls"):
            item["is_tool_call"] = 1
            names = [tc.get("function", {}).get("name", "")
                     for tc in m["tool_calls"] if isinstance(tc, dict)]
            item["tool_names"] = [n for n in names if n]
        elif m.get("role") == "tool":
            # 工具返回也属于工具调用流程的一环
            item["is_tool_call"] = 1
        else:
            item["is_tool_call"] = 0

        # ---- 思考消息（模型中间轮，未真正发给用户）----
        # 判据：assistant 且没有 msg_id。
        # 有 msg_id 说明这条已作为真实消息发送出去；没有则是纯推理文本，
        # 或"推理 + 工具调用"的中间产物。由后端统一判定并下发，
        # 前端不再各自用「无 msg_id」这类经验规则去猜。
        item["is_thinking"] = 1 if (m.get("role") == "assistant" and not m.get("msg_id")) else 0

        # ---- 展示用结构化字段（结构性字段随 item 一起原样返回）----
        # ts / username / user_id 已由存储层独立保存（见 memory.append_message），
        # 前端直接取用即可，不必再从 content 里正则反解时间戳与昵称前缀。
        item["ts"] = m.get("ts") or ""
        item["username"] = m.get("username") or ""
        item["user_id"] = m.get("user_id") or ""

        # content 已是纯正文（不含时间戳/昵称前缀）。
        # 兼容期兜底：极端情况下（如外部直接写入的文件未被归一）仍可能带前缀，
        # 此时用结构化字段拼一份等价的展示文本，保证前端始终拿到同样的东西。
        body = str(m.get("content", "") or "")
        if m.get("revoked") and not body.startswith("[已撤回]"):
            body = "[已撤回]" + body
        item["content"] = body

        if item.get("revoked"):
            revoked_count += 1
        if item.get("is_markdown"):
            markdown_count += 1
        if item.get("tool_calls"):
            tool_call_count += 1
        if item.get("is_wakeup"):
            wakeup_count += 1
        if item.get("is_hide"):
            is_hide_count += 1
        msgs.append(item)

    return ok({
        "app_id": app_id,
        "thread_key": thread_key,
        "total": total,
        "returned": len(msgs),
        "revoked_count": revoked_count,
        "markdown_count": markdown_count,
        "tool_call_count": tool_call_count,
        "wakeup_count": wakeup_count,
        "is_hide_count": is_hide_count,
        "messages": msgs,
    })


# ==================== 3. 主动发送消息 ====================
async def h_send(request: web.Request) -> web.Response:
    """主动发送：POST /api/send
    body: {target_type, target_id, content, media_source?, file_type?, file_name?, app_id?}
    支持 Markdown，正文三种传法均可（优先级 markdown > markdown_content > content）：
      {target_type, target_id, message_type:"markdown", markdown:"# 标题..."}
      {target_type, target_id, message_type:"markdown", markdown_content:"# 标题..."}
      {target_type, target_id, message_type:"markdown", content:"# 标题..."}   ← 与普通消息一致
      markdown 也可传对象：{"content": "..."} 或 {"custom_template_id":"...","params":[...]}
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or {}
    if not body:
        body = await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/send", body)):
        return e

    target_type = str(body.get("target_type") or "").strip().lower()
    target_id = str(body.get("target_id") or "").strip()
    content = body.get("content")
    media_source = body.get("media_source")
    file_type = body.get("file_type")
    file_name = body.get("file_name")

    # ---------- Markdown ----------
    markdown = body.get("markdown")
    # 统一解析：content 为准，兼容 markdown_content；markdown 字段（对象/模板）仍支持
    use_md, _body_text = parse_message_type(body, allow_markdown_only=True)
    if use_md and markdown is None:
        if _body_text is not None and str(_body_text).strip():
            markdown = _body_text
        elif content is not None:
            markdown = content

    if target_type not in ("group", "c2c"):
        return fail("target_type 必须为 group 或 c2c")
    if not target_id:
        return fail("缺少 target_id")

    # ---------- 目标会话归属校验（M16）----------
    # _check_cross_allowed 只校验「URL 上的 app_id 是否等于本密钥的 app_id」，
    # 它管的是「能不能操作这个机器人」，**管不到 target_id**。而 target_id
    # 完全由调用方给出且此前从不校验 —— 于是任何机器人密钥都能以自己的身份
    # 往别人名下的群/私聊发消息（甚至借媒体 URL 触发远端抓取）。
    # 这里补上会话级归属：目标必须确实登记在该机器人名下。
    if not _target_owned_by(app_id, target_type, target_id):
        return fail(
            f"权限不足：{'群' if target_type == 'group' else '用户'} {target_id} "
            f"不属于机器人 {app_id}（无本地记录，请先同步）", 403)

    thread_key = f"{target_type}_{target_id}"
    ctx = LogCtx(app_id=app_id, thread_key=thread_key)

    if use_md:
        if not markdown:
            return fail("message_type 为 markdown 时必须提供 markdown 内容"
                        "（markdown / markdown_content / content 任一均可）")
        if not isinstance(markdown, (str, dict)):
            return fail("markdown 必须是字符串或对象")
        if isinstance(markdown, dict) and not markdown:
            return fail("markdown 对象不能为空")
    else:
        if (not content or not str(content).strip()) and not media_source:
            return fail("缺少 content（纯文本）或 media_source（媒体URL）")

    if not get_bot_enabled(app_id):
        return fail(f"机器人 {app_id} 已禁用", 403)

    client = get_client(app_id)
    if client is None:
        return fail(f"未找到机器人 {app_id} 的凭证", 400)

    try:
        # 主动消息：msg_id=None
        send_kwargs = {
            "msg_type": target_type,
            "recipient_id": target_id,
            "media_source": media_source or None,
            "file_type": file_type,
            "file_name": file_name,
            "msg_id": None,
        }
        if use_md:
            send_kwargs["markdown"] = markdown
            # 富媒体与 Markdown 不同发；若同时给了媒体，媒体优先（markdown 会被忽略）
            if media_source:
                send_kwargs["content"] = str(content) if content else None
        else:
            send_kwargs["content"] = str(content) if content else None

        sent = await client.send_message(**send_kwargs)
    except Exception as e:
        error(f"[API] 发送异常: {e}", ctx=ctx)
        return _safe_error(e, "发送")

    if not sent:
        return fail("发送失败（请检查目标ID、权限或媒体URL）", 502)

    send_id = client.get_last_send_id()
    send_idx = client.get_last_send_msg_idx()

    # 记录到聊天历史
    # 是否标记为 Markdown 消息（媒体优先，媒体消息不算 markdown）
    hist_is_md = bool(use_md and not media_source)
    if hist_is_md:
        if isinstance(markdown, str):
            desc = markdown
        else:
            desc = str(markdown.get("content") or "") or f"Markdown 模板消息（{markdown.get('custom_template_id', '')}）"
    elif media_source:
        # 媒体消息用「媒体：<正文>（<URL>）」格式，与 send_media / play_music 等统一
        desc = f"媒体：{content if content else '媒体内容'}（{media_source}）"
    elif content:
        desc = str(content)
    else:
        desc = ""
    try:
        append_message(thread_key, "assistant", desc, msg_id=send_id, msg_idx=send_idx,
                       is_markdown=hist_is_md,
                       media_url=media_source or None)
    except Exception as e:
        warn(f"[API] 写入历史失败: {e}", ctx=ctx)

    info(f"[API] 主动发送成功 app={app_id} {thread_key} msg_id={send_id}", ctx=ctx)
    return ok({
        "app_id": app_id,
        "thread_key": thread_key,
        "msg_id": send_id,
        "msg_idx": send_idx,
        "media_url": media_source or None,
    })


# ==================== 4. 撤回消息 ====================
async def h_revoke(request: web.Request) -> web.Response:
    """撤回：POST /api/revoke

    参数（统一规范名）：
      target_type   group / c2c
      target_id     会话 ID
      message_ids   消息 ID 数组，如 ["xxx", "yyy"]
      app_id?
    给多个 msg_id 时会逐个撤回，返回每条的结果。
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/revoke", body)):
        return e

    target_type = str(body.get("target_type") or "").strip().lower()
    target_id = str(body.get("target_id") or "").strip()
    # 统一为数组：只接受 message_ids（可传数组或单个字符串）
    ids = body.get("message_ids")
    if isinstance(ids, str):
        ids = [ids] if ids.strip() else []
    if not isinstance(ids, list) or not ids:
        return fail("缺少 message_ids（数组形式，如 [\"xxx\"]）")
    ids = [str(x).strip() for x in ids if str(x).strip()]
    if not ids:
        return fail("message_ids 不能为空")

    if target_type not in ("group", "c2c"):
        return fail("target_type 必须为 group 或 c2c")
    if not target_id:
        return fail("缺少 target_id")
    if not ids:
        return fail("缺少 message_ids（数组形式，如 [\"xxx\", \"yyy\"]）")

    thread_key = f"{target_type}_{target_id}"
    ctx = LogCtx(app_id=app_id, thread_key=thread_key)

    client = get_client(app_id)
    if client is None:
        return fail(f"未找到机器人 {app_id} 的凭证", 400)

    results = []
    for message_id in ids:
        try:
            success = await client.revoke_message(target_type, target_id, message_id)
        except Exception as e:
            error(f"[API] 撤回异常: {e}", ctx=ctx)
            results.append({"message_id": message_id, "revoked": False, "error": str(e)})
            continue
        if success:
            # 撤回成功后，在聊天记录中标记该消息为已撤回，
            # 使 /api/history 能反映消息状态（与 AI 工具撤回的行为保持一致）。
            marked = mark_message_revoked(thread_key, message_id)
            info(f"[API] 撤回成功 app={app_id} {thread_key} msg={message_id} 记录已标记={marked}", ctx=ctx)
            results.append({"message_id": message_id, "revoked": True, "history_marked": marked})
        else:
            results.append({"message_id": message_id, "revoked": False,
                            "error": "撤回失败（消息可能已过期或无权限）"})

    ok_n = sum(1 for r in results if r["revoked"])
    # 单条撤回时保持原有的扁平返回结构，避免老调用方取不到字段
    if len(ids) == 1:
        r0 = results[0]
        if r0["revoked"]:
            return ok({
                "app_id": app_id,
                "message_id": r0["message_id"],
                "revoked": True,
                "thread_key": thread_key,
                "history_marked": r0.get("history_marked", False),
            })
        return fail(r0.get("error") or "撤回失败（消息可能已过期或无权限）", 502)

    return ok({
        "app_id": app_id,
        "thread_key": thread_key,
        "total": len(ids),
        "revoked_count": ok_n,
        "failed_count": len(ids) - ok_n,
        "results": results,
    })


async def h_message_hide(request: web.Request) -> web.Response:
    """隐藏 / 取消隐藏消息：POST /api/message/hide

    body: {
      target_type, target_id,        # 必填，会话（group / c2c）
      message_ids: [...],            # 必填，要修改的消息 ID 数组（也接受单个字符串）
      is_hide: 0 | 1,                # 可选，默认 1（隐藏，即不发给 AI）
      app_id?                        # 全局密钥通道可指定
    }

    作用：修改聊天记录里的 is_hide 字段。is_hide=1（已隐藏）的条目在构建
    发给 AI 的上下文时被整条剔除（见 memory.get_history / filter_hidden_for_ai）。

    与 /api/revoke 的区别（两者**完全独立**，互不影响）：
      /api/revoke        —— 真的去 QQ 侧撤回消息，并写 revoked 状态；
                            撤回后的消息仍会进上下文，只是带 [已撤回] 前缀。
      /api/message/hide  —— 纯本地隐藏，不调用任何 QQ 接口、不碰 revoked；
                            命中后模型彻底看不到这条。

    因此本接口不要求消息未过期，也不校验机器人权限 —— 它只改本地 JSON。
    返回每条消息的修改结果；未找到该 message_id 记为该条失败（不影响其它条）。
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/message/hide", body)):
        return e

    target_type = str(body.get("target_type") or "").strip().lower()
    target_id = str(body.get("target_id") or "").strip()
    if target_type not in ("group", "c2c"):
        return fail("target_type 必须为 group 或 c2c")
    if not target_id:
        return fail("缺少 target_id")

    # 统一为数组：只接受 message_ids（可传数组或单个字符串）
    ids = body.get("message_ids")
    if isinstance(ids, str):
        ids = [ids] if ids.strip() else []
    if not isinstance(ids, list) or not ids:
        return fail("缺少 message_ids（数组形式，如 [\"xxx\"]）")
    ids = [str(x).strip() for x in ids if str(x).strip()]
    if not ids:
        return fail("message_ids 不能为空")

    # is_hide 默认 1：接口名就是「隐藏」，不传即按「隐藏」处理。
    # 传 0/false 表示取消隐藏，该条重新参与 AI 上下文。
    raw_flag = body.get("is_hide")
    if raw_flag is None:
        is_hide = True
    else:
        is_hide = str(raw_flag).strip().lower() in ("1", "true", "yes", "on")

    thread_key = f"{target_type}_{target_id}"
    ctx = LogCtx(app_id=app_id, thread_key=thread_key)

    results = []
    for message_id in ids:
        changed = set_message_hidden(thread_key, message_id, is_hide)
        if changed:
            results.append({"message_id": message_id, "is_hide": 1 if is_hide else 0,
                            "updated": changed})
        else:
            # 找不到 msg_id（或该条已是目标值）——分开说明，便于排查
            results.append({"message_id": message_id, "is_hide": 1 if is_hide else 0,
                            "updated": 0,
                            "error": "未找到该 message_id（或值未发生变化）"})

    ok_n = sum(1 for r in results if r["updated"])
    info(f"[API] 隐藏消息 app={app_id} {thread_key} is_hide={1 if is_hide else 0} "
         f"成功={ok_n}/{len(ids)}", ctx=ctx)

    # 单条时保持扁平返回，与 /api/revoke 的写法一致
    if len(ids) == 1:
        r0 = results[0]
        if r0["updated"]:
            return ok({
                "app_id": app_id,
                "thread_key": thread_key,
                "message_id": r0["message_id"],
                "is_hide": r0["is_hide"],
                "updated": r0["updated"],
            })
        return fail(r0.get("error") or "未找到该 message_id", 404)

    return ok({
        "app_id": app_id,
        "thread_key": thread_key,
        "total": len(ids),
        "is_hide": 1 if is_hide else 0,
        "updated_count": ok_n,
        "failed_count": len(ids) - ok_n,
        "results": results,
    })


async def h_batch_revoke(request: web.Request) -> web.Response:
    """批量撤回：POST /api/batch_revoke

    body: {
      target_type, target_id,          # 必填，会话
      keywords?: [str],                # 关键词，任一命中即撤回
      start_time?, end_time?,          # RFC3339 时间范围
      confirm?: 1,                     # 必须显式传 1 才会真正撤回；否则只做预览
      app_id?
    }

    安全设计（默认安全）：
      - 不传 confirm 或 confirm != 1  => 预览模式，只返回将要撤回的消息，不做任何修改
      - confirm = 1                   => 真正执行撤回

    与 AI 工具 batch_revoke 的匹配规则保持一致：
      - keywords 与时间范围至少要有一个，否则等于无条件撤整个会话
      - 指定时间范围时，无时间戳前缀的消息一律排除（宁少勿错）
      - 已撤回的消息跳过
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/batch_revoke", body)):
        return e
    target_type = str(body.get("target_type") or "").strip().lower()
    target_id = str(body.get("target_id") or "").strip()
    if target_type not in ("group", "c2c"):
        return fail("target_type 必须为 group 或 c2c")
    if not target_id:
        return fail("缺少 target_id")

    raw_kw = body.get("keywords") or []
    if not isinstance(raw_kw, list):
        return fail("keywords 必须为数组")
    keywords = [str(k).strip() for k in raw_kw if k and str(k).strip()]

    start_time_str = str(body.get("start_time") or "").strip()
    end_time_str = str(body.get("end_time") or "").strip()

    # 默认安全：只有显式 confirm=1 才执行，否则一律预览。
    # 兼容旧的 dry_run 参数：显式传 dry_run=1 时同样只预览。
    try:
        confirm = int(body.get("confirm") or 0)
    except (TypeError, ValueError):
        return fail("confirm 必须为 0 或 1")
    try:
        legacy_dry = int(body.get("dry_run") or 0)
    except (TypeError, ValueError):
        return fail("dry_run 必须为 0 或 1")
    dry_run = not (confirm == 1 and legacy_dry == 0)

    if not keywords and not start_time_str and not end_time_str:
        return fail("至少需要指定 keywords 或时间范围（start_time/end_time）之一，"
                    "以免无条件撤回整个会话的历史消息")

    thread_key = f"{target_type}_{target_id}"
    ctx = LogCtx(app_id=app_id, thread_key=thread_key)

    # 解析时间范围（统一转北京时间）
    bj_tz = timezone(timedelta(hours=8))
    start_dt = end_dt = None
    if start_time_str:
        try:
            start_dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00")).astimezone(bj_tz)
        except Exception as e:
            return fail(f"start_time 格式无效 '{start_time_str}': {e}")
    if end_time_str:
        try:
            end_dt = datetime.fromisoformat(end_time_str.replace("Z", "+00:00")).astimezone(bj_tz)
        except Exception as e:
            return fail(f"end_time 格式无效 '{end_time_str}': {e}")

    client = get_client(app_id)
    if client is None:
        return fail(f"未找到机器人 {app_id} 的凭证", 400)

    hist = load_history(thread_key)
    matched: List[Dict[str, Any]] = []

    for m in hist:
        content = str(m.get("content") or "")
        msg_id_val = m.get("msg_id") or ""
        if not msg_id_val:
            continue
        if m.get("revoked"):
            continue
        if content.startswith("[已撤回]"):
            continue

        # 时间从结构化 ts 字段解析（旧记录由 load_history 归一后同样可用），
        # 不再从正文里正则抠 "[YYYY-MM-DD HH:MM]" 前缀。
        raw_content = content
        msg_dt = parse_history_ts(m)

        # 指定时间范围时，无法判断时间的条目一律排除
        if start_dt or end_dt:
            if msg_dt is None:
                continue
            if start_dt and msg_dt < start_dt:
                continue
            if end_dt and msg_dt > end_dt:
                continue

        lower = raw_content.lower()
        hit_kw = None
        for kw in keywords:
            if kw.lower() in lower:
                hit_kw = kw
                break
        if not (hit_kw or not keywords):
            continue

        matched.append({
            "msg_id": msg_id_val,
            "preview": raw_content[:80],
            "keyword": hit_kw,
            "timestamp": msg_dt.strftime("%Y-%m-%d %H:%M") if msg_dt else "",
            "role": m.get("role") or "",
        })

    if dry_run:
        info(f"[API] 批量撤回预览：匹配 {len(matched)} 条（未执行，等待 confirm=1）", ctx=ctx)
        return ok({
            "app_id": app_id, "thread_key": thread_key,
            "preview": True, "executed": False,
            "matched": len(matched), "messages": matched,
            "hint": "这是预览结果，未做任何撤回。确认无误后请带上 confirm=1 再次调用以真正执行。",
        })

    results = []
    ok_count = fail_count = 0
    for it in matched:
        mid = it["msg_id"]
        try:
            success = await client.revoke_message(target_type, target_id, mid)
        except Exception as e:
            results.append({"msg_id": mid, "ok": False, "error": str(e)})
            fail_count += 1
            continue
        if success:
            mark_message_revoked(thread_key, mid)
            results.append({"msg_id": mid, "ok": True})
            ok_count += 1
        else:
            results.append({"msg_id": mid, "ok": False, "error": "失败（可能超时或无权限）"})
            fail_count += 1

    info(f"[API] 批量撤回完成：成功 {ok_count} 失败 {fail_count}（匹配 {len(matched)}）", ctx=ctx)
    return ok({
        "app_id": app_id, "thread_key": thread_key,
        "preview": False, "executed": True,
        "matched": len(matched),
        "success": ok_count, "failed": fail_count, "results": results,
    })


# ==================== 5. 禁言管理 ====================
async def h_mute(request: web.Request) -> web.Response:
    """禁言/解禁/改禁言：POST /api/mute

    参数（统一规范名，成员一律用数组）：
      target_id        群 openid
      members          成员数组，传一个即单个、传多个即批量：
                         [{"member_id": "xxx", "mute_expire_at": "..."}, ...]
                       简写也接受：["xxx", "yyy"] 或 直接给 member_id 字符串
      op               add=禁言 / update=更改时长 / del=解除
      seconds          相对秒数（整批共用一个时长）；也可传 mute_expire_at 绝对时间
      mute_expire_at   绝对到期时间 RFC3339，给定时优先于 seconds
      app_id?

    单个成员时返回扁平结构；多个成员时返回 {total, success, failed, results}。
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/mute", body)):
        return e

    group_id = str(body.get("target_id") or "").strip()
    op = str(body.get("op") or "").strip().lower()
    seconds = body.get("seconds")
    # 工具侧传的是绝对到期时间；给了就以此为准（秒数只是给内部日志/回显）
    mute_expire_at = str(body.get("mute_expire_at") or "").strip()

    if not group_id:
        return fail("缺少 target_id")
    if op not in ("add", "del", "update"):
        return fail("op 必须为 add（禁言）/ del（解除）/ update（更改时长）")

    # 成员列表：统一用 members 数组（传一个=单个，传多个=批量）
    # 也接受直接给 member_id（自动包成数组）
    from task_core import normalize_members, mute_result_summary
    members_src = body.get("members")
    if members_src is None:
        members_src = body.get("member_id")
    members, merr = normalize_members(members_src, mute_expire_at)
    if merr:
        return fail(merr)
    if not members:
        return fail("缺少 members（数组）或 member_id")

    # 秒数处理：给了 mute_expire_at 就直接用，没给才按 seconds 换算。
    # 批量时每个成员可自带 mute_expire_at，此时外层可以一个都不给。
    has_per_member_expire = any(m.get("mute_expire_at") for m in members)
    if op in ("add", "update"):
        if mute_expire_at:
            # 工具风格：绝对到期时间，校验格式后直接用
            if not _parse_task_ts(mute_expire_at):
                return fail("mute_expire_at 格式无效，应为 RFC3339（如 2026-09-26T20:00:00+08:00）")
            if seconds is None:
                seconds = 0  # 仅用于回显，不参与计算
        elif has_per_member_expire and seconds is None:
            seconds = 0      # 时长由各成员自带，外层不用给
        else:
            if seconds is None:
                return fail(f"op={op} 时必须提供 seconds 或 mute_expire_at")
            try:
                seconds = int(seconds)
            except (TypeError, ValueError):
                return fail("seconds 必须为整数")
            if seconds <= 0:
                return fail("seconds 必须为正整数")
            # 官方上限：30 天
            max_sec = 30 * 24 * 3600
            if seconds > max_sec:
                seconds = max_sec
    else:
        seconds = 0
        mute_expire_at = ""

    ctx = LogCtx(app_id=app_id, thread_key=f"group_{group_id}")

    client = get_client(app_id)
    if client is None:
        return fail(f"未找到机器人 {app_id} 的凭证", 400)

    # seconds → mute_expire_at（RFC3339，北京时间）
    def _sec_to_rfc3339(sec: int) -> str:
        bj = timezone(timedelta(hours=8))
        return (datetime.now(bj) + timedelta(seconds=sec)).replace(
            microsecond=0).isoformat(timespec="seconds")

    if op == "del":
        default_expire = ""
    elif mute_expire_at:
        default_expire = mute_expire_at   # 直接用调用方给的绝对时间
    else:
        default_expire = _sec_to_rfc3339(seconds)

    op_name = {"add": "禁言", "del": "解除禁言", "update": "更改禁言"}[op]
    results: List[Dict[str, Any]] = []

    for m in members:
        mid = m["member_id"]
        # 每个成员可用自己的 mute_expire_at，缺省则用外层统一值
        if op == "del":
            expire_at = ""
        elif m.get("mute_expire_at"):
            expire_at = m["mute_expire_at"]
        else:
            expire_at = default_expire

        try:
            success, error_code = await client.set_group_mute(group_id, op, mid, expire_at)
            # 官方对部分时间格式返回 10007，自动修正为 1 小时后重试
            if not success and error_code == 10007 and op in ("add", "update"):
                retry_at = _sec_to_rfc3339(3600)
                success, error_code = await client.set_group_mute(group_id, op, mid, retry_at)
                if success:
                    expire_at = retry_at
                    warn("[API] 禁言时间被官方修正为1小时后成功", ctx=ctx)
        except Exception as e:
            error(f"[API] 禁言操作异常: {e}", ctx=ctx)
            results.append({"member_id": mid, "ok": False, "error": str(e)})
            continue

        if success:
            info(f"[API] {op_name}成功 app={app_id} group={group_id} member={mid} expire={expire_at}", ctx=ctx)
            results.append({"member_id": mid, "ok": True, "mute_expire_at": expire_at})
        else:
            results.append({"member_id": mid, "ok": False, "error_code": error_code,
                            "error": f"{op_name}失败（错误码 {error_code}，请检查机器人在群内权限）"})

    summary = mute_result_summary(results)

    # 单个成员：保持原来的扁平返回结构，向后兼容
    if len(results) == 1:
        r0 = results[0]
        if r0["ok"]:
            return ok({
                "app_id": app_id, "target_id": group_id, "group_id": group_id,
                "member_id": r0["member_id"],
                "op": op, "seconds": seconds, "mute_expire_at": r0.get("mute_expire_at", ""),
            })
        return fail(r0["error"], 502, error_code=r0.get("error_code"))

    # 多个成员：返回汇总
    return ok({
        "app_id": app_id, "target_id": group_id, "group_id": group_id,
        "op": op, "seconds": seconds, **summary,
    })


async def h_bot_state(request: web.Request) -> web.Response:
    """查询机器人在群中的状态：/api/bot_state?target_id=xxx[&app_id=]

    参数：target_id（群 openid）。

    对应官方 GET /v2/groups/{group_openid}/bot_state
    返回机器人自身在该群的角色与接收消息设置：
    - member_role: member(普通成员) / admin(管理员) / owner(群主)
    - allow_proactive_msg: 是否允许主动推送
    - recv_msg_setting: 接收消息设置
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    if (e := _reject_unknown_params(request, "/api/bot_state", {})):
        return e

    group_id = request.query.get("target_id", "").strip()
    if not group_id:
        return fail("缺少 target_id")

    if not get_bot_enabled(app_id):
        return fail(f"机器人 {app_id} 已禁用", 403)

    client = get_client(app_id)
    if client is None:
        return fail(f"未找到机器人 {app_id} 的凭证", 400)

    try:
        state = await client.get_bot_state(group_id)
    except Exception as e:
        return _safe_error(e, "查询")

    if state is None:
        return fail("查询失败（可能机器人不在该群，或接口无权限）", 502)

    role = state.get("member_role", "")
    can_manage = role in ("admin", "owner")   # 是否具备群管理权限

    # 群名（mirror.json 里可能已有记录）
    gname = ""
    try:
        mirror = load_mirror()
        gname = (mirror.get("groups", {}).get(app_id, {}) or {}).get(group_id) \
            or get_group_name_from_mirror(app_id, group_id) or ""
    except Exception:
        pass

    return ok({
        "app_id": app_id,
        "group_id": group_id,
        "group_name": gname,
        "member_role": role,
        "role_label": {"member": "普通成员", "admin": "管理员", "owner": "群主"}.get(role, role or "未知"),
        "can_manage": can_manage,
        "allow_proactive_msg": state.get("allow_proactive_msg"),
        "recv_msg_setting": state.get("recv_msg_setting"),
        "joined_at": state.get("joined_at"),
        "member_openid": state.get("member_openid"),
        "bot_state": state,
    })


async def h_mute_status(request: web.Request) -> web.Response:
    """查询群禁言状态：/api/mute/status?target_id=xxx[&app_id=][&with_names=1]

    参数：target_id（群 openid）。

    返回官方 restrict_chat_setting 的原始结构，另外补充：
    - members[].member_name：用 mirror.json 里的昵称补全
    - members[].mute_expire_at / is_muted：便于前端直接判断
    - muted_members：精简后的「当前被禁言成员」列表，前端可直接渲染
    """
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    if (e := _reject_unknown_params(request, "/api/mute/status", {})):
        return e

    group_id = request.query.get("target_id", "").strip()
    if not group_id:
        return fail("缺少 target_id")

    client = get_client(app_id)
    if client is None:
        return fail(f"未找到机器人 {app_id} 的凭证", 400)

    try:
        status = await client.get_group_mute_status(group_id)
    except Exception as e:
        return _safe_error(e, "查询")

    if status is None:
        return fail("查询失败（可能机器人不是群管理员，或群不存在）", 502)

    # ---- 补全成员昵称，并整理出「当前被禁言成员」列表 ----
    mirror = load_mirror()
    user_names = mirror.get("users", {}).get(app_id, {})
    members = status.get("members")
    muted_members = []
    if isinstance(members, list):
        for m in members:
            if not isinstance(m, dict):
                continue
            mid = m.get("member_openid") or m.get("member_id") or ""
            mname = user_names.get(mid) or get_user_name(mid, app_id) or ""
            expire = m.get("mute_expire_at") or ""
            # 有 expire_at 视为仍被禁言；无该字段时以接口返回为准
            is_muted = bool(expire) if "mute_expire_at" in m else True
            m["member_name"] = mname
            m["has_name"] = bool(mname)
            m["is_muted"] = is_muted
            muted_members.append({
                "member_id": mid,
                "member_name": mname,
                "has_name": bool(mname),
                "mute_expire_at": expire,
                "is_muted": is_muted,
                "username": m.get("username", ""),
            })

    global_rule = status.get("global_rule") or {}
    return ok({
        "app_id": app_id,
        "group_id": group_id,
        "mute_status": status,
        "members": members if isinstance(members, list) else [],
        "muted_members": muted_members,
        "muted_count": len(muted_members),
        "global_mode": global_rule.get("mode", "none"),
    })


# ==================== 6. 定时推送配置 ====================
async def h_push_create(request: web.Request) -> web.Response:
    """创建定时推送：POST /api/push/create
    body: {content, targets:[{type,id,name,app_id?}], schedule_type,
           schedule_time?, interval_seconds?, media_source?, file_type?,
           file_name?, app_id?}

    与 AI 工具 schedule_push 完全对齐，两种写法都接受：
      targets: [{type,id}, ...]        或   target_type + target_id（单个目标）
      content + message_type/markdown  （Markdown 推送）
      media_source/file_type/file_name （媒体推送，工具同名）
    """
    import scheduler as sched
    from task_core import (build_targets, norm_schedule_type, push_create_fields,
                           validate_schedule_fields)

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/push/create", body)):
        return e

    content = body.get("content")
    schedule_time = body.get("schedule_time", "")
    interval_seconds = body.get("interval_seconds")

    # targets：数组，或 target_type + target_id 指定单个目标
    targets, terr = build_targets(body.get("targets"), body.get("target_type"),
                                  body.get("target_id"))
    if terr:
        return fail(terr)

    try:
        schedule_type = norm_schedule_type(body.get("schedule_type"))
    except ValueError as e:
        return fail(str(e))

    # 日志标识：多个目标时不存在唯一会话，取首个目标作代表
    _t0 = targets[0]
    _primary = f"{'c2c' if _t0['type'] == 'user' else 'group'}_{_t0['id']}"
    ctx = LogCtx(app_id=app_id, thread_key=_primary)

    if not content or not str(content).strip():
        return fail("缺少 content")
    if (serr := validate_schedule_fields(schedule_type, schedule_time, interval_seconds)):
        return fail(serr)

    for t in targets:
        t.setdefault("app_id", app_id)

    task_data = {
        "app_id": app_id,
        "content": str(content),
        "targets": targets,
        "schedule_type": schedule_type,
        "schedule_time": schedule_time,
        "initiator_info": {
            "app_id": app_id, "type": "system", "id": "", "name": "API",
            "created_at": _now_iso(),
        },
    }
    if schedule_type == "interval" and interval_seconds is not None:
        task_data["interval_seconds"] = int(interval_seconds)
    # 媒体 + Markdown 字段：与工具同一批字段名，避免「工具能建、API 改不动」
    task_data.update(push_create_fields(body))

    try:
        task_id = sched.add_task(task_data)
    except ValueError as e:
        return fail(str(e))
    except Exception as e:
        error(f"[API] 创建推送异常: {e}", ctx=ctx)
        return _safe_error(e, "创建推送")

    info(f"[API] 创建定时推送 {task_id} app={app_id}", ctx=ctx)
    return ok({"task_id": task_id, "app_id": app_id, "schedule_type": schedule_type})


async def h_push_list(request: web.Request) -> web.Response:
    """列出定时推送：/api/push/list[?app_id=][&status=][&keywords=][&start=][&end=]

    查询参数（全部可选，可任意组合）：
      status       状态筛选，支持多值（逗号分隔或重复参数）
                   取值：pending(未开始)/completed(执行完成)/failed(执行失败)
                   也接受中文：未开始/运行中/执行完成/执行失败/已取消
      keywords     关键词数组，多值（逗号分隔或重复参数），任一命中即可
      keyword/q    单关键词（等价于 keywords 只有一个）
      match_all=1  关键词需全部命中
      task_id      任务 id 包含匹配
      task_app_id  仅保留该机器人的任务
      target_type  目标类型 group / user(c2c)
      target_id    目标 id 包含匹配
      start / end  时间范围（含），格式 2026-09-21 或 2026-09-21T20:40:00+08:00
      time_field   用哪个字段做时间比较（默认 schedule_time，回退 created_at）
      order        asc / desc 按时间排序
      with_counts  1 时附带各状态数量统计
    """
    if (e := _reject_unknown_params(request, "/api/push/list", {})):
        return e

    import scheduler as sched
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    try:
        # 用「调用方身份」而不是 URL 上的 app_id 决定可见范围：
        #   · 全局密钥（ROLE_GLOBAL）→ 传空串，取运维全量视图；
        #   · 机器人密钥 → 传自己的 app_id，只拿自己的任务。
        # 若这里传 app_id，全局密钥显式指定 app_id 时会被列表逻辑误判成
        # 机器人密钥而收窄结果；反之若恒传空串，机器人密钥就会拿到全量 ——
        # 两个方向都会错，必须按角色区分（F4：跨机器人任务列表泄露）。
        caller = "" if auth["role"] == ROLE_GLOBAL else auth["app_id"]
        tasks = sched.list_tasks(caller)
    except Exception as e:
        return _safe_error(e, "读取推送列表")

    # 先统计全量状态分布（在过滤前），便于前端展示筛选标签
    all_counts = _task_counts(tasks)

    try:
        tasks, fmeta = _filter_tasks(tasks, request)
    except Exception as e:
        return _safe_error(e, "过滤推送列表")

    resp: Dict[str, Any] = {
        "app_id": app_id,
        "total": len(tasks),
        "tasks": tasks,
        "counts": all_counts,
    }
    if fmeta.get("filters"):
        resp["filters"] = fmeta["filters"]
    return ok(resp)


async def h_push_delete(request: web.Request) -> web.Response:
    """删除定时推送：POST /api/push/delete  body: {task_id, app_id?}"""
    import scheduler as sched
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/push/delete", body)):
        return e
    task_id = str(body.get("task_id") or "").strip()
    if not task_id:
        return fail("缺少 task_id")

    try:
        success, msg = sched.delete_task(task_id, app_id)
    except Exception as e:
        return _safe_error(e, "删除推送")
    if success:
        return ok({"task_id": task_id, "message": msg})
    return fail(msg, 404)


async def h_push_update(request: web.Request) -> web.Response:
    """更新定时推送：POST /api/push/update

    两种写法都接受（与 AI 工具 update_scheduled_push 对齐）：
      {task_id, updates:{content:...}}    ← API 风格（嵌套）
      {task_id, content:..., targets:...} ← 工具风格（平铺）
    同时出现时 updates 内的字段优先。
    """
    import scheduler as sched
    from task_core import PUSH_UPDATE_FIELDS, split_updates

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/push/update", body)):
        return e
    task_id = str(body.get("task_id") or "").strip()
    if not task_id:
        return fail("缺少 task_id")

    updates = split_updates(body, PUSH_UPDATE_FIELDS)
    if not updates:
        return fail("缺少要更新的字段（可用 updates 嵌套或直接平铺）")

    try:
        success, msg = sched.update_task(task_id, app_id, updates)
    except Exception as e:
        return _safe_error(e, "更新推送")
    if success:
        return ok({"task_id": task_id, "message": msg, "updated": sorted(updates.keys())})
    return fail(msg, 404)


# ==================== 7. 定时唤醒配置 ====================
async def h_wakeup_create(request: web.Request) -> web.Response:
    """创建定时唤醒：POST /api/wakeup/create
    body: {description, targets:[...], initiator:{id,name,...}, schedule_type,
           schedule_time?, interval_seconds?, isolation_mode?, app_id?}

    与 AI 工具 create_scheduled_wakeup 对齐，两种写法都接受：
      targets: [{type,id}, ...]   或   type + id（单个目标）
      description 或 prompt        （提示词正文，工具侧叫 description）
    initiator 缺省时自动用 system 占位，省去无上下文调用方的麻烦。
    """
    import wakeup_scheduler as ws
    from task_core import build_targets, norm_schedule_type, validate_schedule_fields

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/wakeup/create", body)):
        return e
    description = str(body.get("description") or "").strip()
    schedule_time = body.get("schedule_time", "")
    interval_seconds = body.get("interval_seconds")

    targets, terr = build_targets(body.get("targets"), body.get("target_type"),
                                  body.get("target_id"))
    if terr:
        return fail(terr)

    try:
        schedule_type = norm_schedule_type(body.get("schedule_type"), default="")
    except ValueError as e:
        return fail(str(e))

    if not schedule_type:
        return fail("缺少 schedule_type")
    if (serr := validate_schedule_fields(schedule_type, schedule_time, interval_seconds)):
        return fail(serr)
    if not description:
        return fail("缺少 description")

    # initiator 缺省时用 system 占位（工具侧由上下文提供，API 侧不必强制）
    initiator = body.get("initiator") or {}
    if not isinstance(initiator, dict) or not initiator.get("id") or not initiator.get("name"):
        initiator = {"app_id": app_id, "type": "system", "id": "", "name": "API",
                     "created_at": _now_iso()}

    for t in targets:
        t.setdefault("app_id", app_id)

    ctx = LogCtx(app_id=app_id)

    task_data = {
        "app_id": app_id,
        "schedule_type": schedule_type,
        "schedule_time": schedule_time,
        "targets": targets,
        "initiator": initiator,
        "description": description,
    }
    if schedule_type == "interval" and interval_seconds is not None:
        task_data["interval_seconds"] = int(interval_seconds)
    if body.get("isolation_mode") is not None:
        try:
            task_data["isolation_mode"] = int(body["isolation_mode"])
        except (TypeError, ValueError):
            return fail("isolation_mode 必须为整数")

    try:
        task_id = ws.add_wakeup(task_data)
    except ValueError as e:
        return fail(str(e))
    except Exception as e:
        error(f"[API] 创建唤醒异常: {e}", ctx=ctx)
        return _safe_error(e, "创建唤醒")

    info(f"[API] 创建定时唤醒 {task_id} app={app_id}", ctx=ctx)
    return ok({"task_id": task_id, "app_id": app_id, "schedule_type": schedule_type})


async def h_wakeup_list(request: web.Request) -> web.Response:
    """列出定时唤醒：/api/wakeup/list[?app_id=][&status=][&keywords=][&start=][&end=]

    查询参数与 /api/push/list 完全一致（见该接口说明）：
      status / keywords / keyword / q / match_all / task_id / task_app_id /
      target_type / target_id / start / end / time_field / order
    """
    if (e := _reject_unknown_params(request, "/api/wakeup/list", {})):
        return e

    import wakeup_scheduler as ws
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    try:
        # 同 /api/push/list：按调用方角色决定可见范围（F4）
        caller = "" if auth["role"] == ROLE_GLOBAL else auth["app_id"]
        tasks = ws.list_wakeups(caller)
    except Exception as e:
        return _safe_error(e, "读取唤醒列表")

    all_counts = _task_counts(tasks)

    try:
        tasks, fmeta = _filter_tasks(tasks, request)
    except Exception as e:
        return _safe_error(e, "过滤唤醒列表")

    resp: Dict[str, Any] = {
        "app_id": app_id,
        "total": len(tasks),
        "tasks": tasks,
        "counts": all_counts,
    }
    if fmeta.get("filters"):
        resp["filters"] = fmeta["filters"]
    return ok(resp)


async def h_wakeup_delete(request: web.Request) -> web.Response:
    """删除定时唤醒：POST /api/wakeup/delete  body: {task_id, app_id?}"""
    import wakeup_scheduler as ws
    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/wakeup/delete", body)):
        return e
    task_id = str(body.get("task_id") or "").strip()
    if not task_id:
        return fail("缺少 task_id")

    try:
        success, msg = ws.delete_wakeup(task_id, app_id)
    except Exception as e:
        return _safe_error(e, "删除唤醒")
    if success:
        return ok({"task_id": task_id, "message": msg})
    return fail(msg, 404)


async def h_wakeup_update(request: web.Request) -> web.Response:
    """更新定时唤醒：POST /api/wakeup/update

    两种写法都接受（与 AI 工具 update_scheduled_wakeup 对齐）：
      {task_id, updates:{description:...}}   ← API 风格（嵌套）
      {task_id, description:...}             ← 工具风格（平铺）
    """
    import wakeup_scheduler as ws
    from task_core import WAKEUP_UPDATE_FIELDS, split_updates

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/wakeup/update", body)):
        return e
    task_id = str(body.get("task_id") or "").strip()
    if not task_id:
        return fail("缺少 task_id")

    updates = split_updates(body, WAKEUP_UPDATE_FIELDS)
    if not updates:
        return fail("缺少要更新的字段（可用 updates 嵌套或直接平铺）")

    try:
        success, msg = ws.update_wakeup(task_id, app_id, updates)
    except Exception as e:
        return _safe_error(e, "更新唤醒")
    if success:
        return ok({"task_id": task_id, "message": msg, "updated": sorted(updates.keys())})
    return fail(msg, 404)


# ==================== 8. 立即唤醒（API 模拟用户输入） ====================
async def h_wakeup_trigger(request: web.Request) -> web.Response:
    """立即唤醒：POST /api/wakeup/trigger

    通过 API 向指定会话注入一段提示词，模拟用户输入，立即触发 AI 回复。
    body:
      {
        "target_type": "group" | "c2c",      # 必填
        "target_id":   "...",                # 必填，群ID 或 用户 openid
        "prompt":      "提示词内容",          # 必填，作为【API唤醒】的唤醒说明
        "isolation_mode": 0 | 1,             # 可选，默认 0；1=隔离模式
        "initiator":   {"id":..,"name":..},  # 可选，模拟的发起人（默认 API 调用方）
        "app_id":      "..."                 # 可选，目标机器人
      }
    隔离模式（isolation_mode=1）：唤醒期间用户消息不会打断本次唤醒处理，两者互不干扰；
        唤醒期间到达的用户消息会排队等待，唤醒结束后再处理。默认 0。
    """
    import wakeup_scheduler as ws

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return fail(e, 403)

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/wakeup/trigger", body)):
        return e

    target_type = str(body.get("target_type") or "").strip().lower()
    target_id = str(body.get("target_id") or "").strip()
    # 提示词统一用 description
    prompt = body.get("description")
    prompt = str(prompt or "").strip()

    isolation_mode = body.get("isolation_mode", 0)

    if target_type not in ("group", "c2c"):
        return fail("target_type 必须为 group 或 c2c")
    if not target_id:
        return fail("缺少 target_id")

    # ---------- 目标会话归属校验（M16）----------
    # 与 /api/send 同理：_check_cross_allowed 只看 URL 上的 app_id，管不到
    # target_id。缺了这一步，任何机器人密钥都能向别人名下的会话注入提示词、
    # 触发一轮 AI 回复 —— 后果比单纯发消息更大（会消耗对方额度并写入对方历史）。
    if not _target_owned_by(app_id, target_type, target_id):
        return fail(
            f"权限不足：{'群' if target_type == 'group' else '用户'} {target_id} "
            f"不属于机器人 {app_id}（无本地记录，请先同步）", 403)

    if not prompt:
        return fail("缺少 description（要注入的提示词内容）")
    try:
        isolation_mode = int(isolation_mode or 0)
    except (TypeError, ValueError):
        return fail("isolation_mode 必须为整数")
    if isolation_mode not in (0, 1):
        return fail("isolation_mode 只能为 0 或 1")

    if not get_bot_enabled(app_id):
        return fail(f"机器人 {app_id} 已禁用", 403)

    # 目标会话类型与机器人类型对齐（c2c 用于私聊，group 用于群）
    initiator = body.get("initiator")
    if not isinstance(initiator, dict) or not initiator.get("id"):
        # 默认发起人：API 调用方
        caller = auth.get("app_id") or app_id
        initiator = {"id": caller, "name": f"API({caller})", "type": "system"}

    target = {"type": "user" if target_type == "c2c" else "group",
              "id": target_id, "app_id": app_id}
    if body.get("target_name"):
        target["name"] = str(body["target_name"])

    # 目标会话已确定，构造带 thread_key 的 ctx 供本处理器全部日志复用
    ctx = LogCtx(app_id=app_id, thread_key=f"{target_type}_{target_id}")

    task_data = {
        "app_id": app_id,
        "targets": [target],
        "initiator": initiator,
        "description": prompt,
        "isolation_mode": isolation_mode,
        "schedule_type": "immediate",
        "source": "api",
        # 不进调度队列，仅供本次执行使用的临时任务
        "task_id": "api",
    }

    try:
        bot_cache = {}
        result = await ws.run_wakeup_on_target(task_data, target, bot_cache, tag="API唤醒")
    except Exception as e:
        error(f"[API] 立即唤醒异常: {e}", ctx=ctx)
        return _safe_error(e, "立即唤醒")

    result = result or {}
    thread_key = f"{target_type}_{target_id}"

    if result.get("ok"):
        info(f"[API] 立即唤醒成功 app={app_id} {thread_key} 隔离模式={isolation_mode}", ctx=ctx)
        return ok({
            "app_id": app_id,
            "thread_key": thread_key,
            "target_type": target_type,
            "target_id": target_id,
            "isolation_mode": isolation_mode,
            "ok": True,
            "error": "",
            "wakeup_message": ws.build_wakeup_message(task_data, tag="API唤醒"),
        })

    info(f"[API] 立即唤醒失败 app={app_id} {thread_key}：{result.get('error')}", ctx=ctx)
    return fail(
        f"唤醒未成功：{result.get('error') or '未知原因'}",
        502,
        app_id=app_id,
        thread_key=thread_key,
        target_type=target_type,
        target_id=target_id,
        isolation_mode=isolation_mode,
        ok_result=False,
        error_detail=result.get("error", ""),
    )


# ==================== 9. 记忆管理 ====================
# 与 AI 工具的 view_memory / add_memory / modify_memory / delete_memory /
# enable_memory / disable_memory / search_memory 共用底层函数
# （实现都在 memory.py，本段只做参数解析与 JSON 组装）：
#
#     /api/memory/*  ==>  memory.py 的 _memory_*  <==  AI 工具 execute_tool_call
#
# 四个记忆级别（level）：
#   global 全局记忆   identifier = app_id（各机器人隔离时按 app_id 分文件）
#   bot    机器人记忆 identifier = app_id
#   group  群聊记忆   identifier = 群 openid
#   c2c    私聊记忆   identifier = 用户 openid
#
# identifier 一律必填、不做默认推断：global/bot 猜 app_id 尚可用 _resolve_app_id，
# 但 group/c2c 的 identifier 是 openid，猜不出来 —— 与其对一半级别静默兜底、
# 另一半报错，不如统一要求显式传入（缺省时给出的报错里会提示该填什么）。


def _memory_ident_hint(level: str) -> str:
    """identifier 缺失/为空时，按级别给出该填什么的提示。"""
    return {"global": "机器人 APP_ID", "bot": "机器人 APP_ID",
            "group": "群 openid", "c2c": "用户 openid"}.get(level, "记忆标识符")


async def _memory_precheck(request: web.Request, path: str) -> Tuple[Optional[Dict], Optional[web.Response]]:
    """记忆接口公共前置：参数名校验 + 鉴权 + 机器人权限校验。

    返回 (body, None) 或 (None, 错误响应)。
    """
    auth = request["auth"]
    body = request.get("json_body") or await _read_json(request)
    if request.method in ("POST", "PUT", "PATCH"):
        if (e := _reject_unknown_params(request, path, body)):
            return None, e
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return None, fail(err, 403 if auth["role"] == ROLE_BOT else 400)
    if (e := _check_cross_allowed(auth, auth["app_id"], app_id)):
        return None, fail(e, 403)
    return body, None


def _memory_pick_level(body: Dict, request: web.Request, default: str = "all") -> str:
    """取 level 参数（GET 走 query，POST 走 body）。"""
    raw = body.get("level") if body else None
    if raw is None:
        raw = request.query.get("level")
    return str(raw or default).strip().lower()


def _memory_pick_identifier(body: Dict, request: web.Request, level: str = "") -> str:
    """取 identifier 参数。

    按级别优先取对应的专用参数名，便于调用方写出更像 REST 的请求：
      group 级可传 group_id，c2c 级可传 c2c_user_id，二者都是 identifier 的别名。
    """
    if body:
        if level == "group" and body.get("group_id"):
            return str(body["group_id"]).strip()
        if level == "c2c" and body.get("c2c_user_id"):
            return str(body["c2c_user_id"]).strip()
        if body.get("identifier"):
            return str(body["identifier"]).strip()
    if level == "group" and request.query.get("group_id"):
        return request.query["group_id"].strip()
    if level == "c2c" and request.query.get("c2c_user_id"):
        return request.query["c2c_user_id"].strip()
    return (request.query.get("identifier") or "").strip()


def _memory_check_level(level: str) -> Optional[web.Response]:
    """level 合法性检查；不合法返回错误响应。"""
    if level not in _MEMORY_LEVELS:
        return fail(f"未知记忆级别 {level or '(空)'}，可选：{'/'.join(_MEMORY_LEVELS)}")
    return None


def _memory_level_app_id(level: str, identifier: str) -> str:
    """global / bot 两级记忆归属于某个机器人，返回其 app_id（其余级别返回空串）。"""
    return identifier if level in ("global", "bot") else ""


def _group_owned_by(app_id: str, group_id: str) -> bool:
    """该群是否属于这个机器人（依据 user_map[app_id].group）。

    与 /api/groups 同源，保证「列表里看得到」和「记忆里读得到」口径一致。
    """
    try:
        raw = load_user_map().get(app_id, {}).get("group", {})
    except Exception:
        return False
    if isinstance(raw, list):
        return group_id in raw
    if isinstance(raw, dict):
        return group_id in raw
    return False


def _user_owned_by(app_id: str, user_id: str) -> bool:
    """该私聊用户是否属于这个机器人（依据 user_map[app_id].user）。

    兼容 user 为 dict（键即 openid）的旧结构，避免历史数据格式差异导致误判。
    """
    try:
        raw = load_user_map().get(app_id, {}).get("user", [])
    except Exception:
        return False
    if isinstance(raw, dict):
        return user_id in raw
    if isinstance(raw, list):
        return user_id in raw
    return False


def _target_owned_by(app_id: str, target_type: str, target_id: str) -> bool:
    """目标会话是否属于该机器人（按类型分派）。

    与 tool.py 中同名函数同源同义：记忆接口与发送/唤醒接口共用同一套
    归属口径，避免「记忆里看不到、却能往里发消息」这种不一致。
    """
    if not app_id or not target_id:
        return False
    if target_type == "c2c":
        return _user_owned_by(app_id, target_id)
    return _group_owned_by(app_id, target_id)


def _memory_owner_app_id(request: web.Request, body: Optional[Dict] = None) -> str:
    """取本次记忆操作归属的 app_id（用于 group/c2c 归属校验）。

    _memory_precheck 已经解析并校验过 app_id，这里直接复用它的结果，
    避免各处重新解析导致口径不一致（例如有的地方看 body、有的看 query）。
    """
    auth = request["auth"]
    app_id = auth.get("app_id") or ""
    if app_id:
        return app_id
    # 无密钥模式下 auth 不带 app_id，此时以请求参数为准（已被 precheck 校验过）
    try:
        body = body or request.get("json_body") or {}
    except Exception:
        body = {}
    return str(body.get("app_id") or request.query.get("app_id") or "").strip()


def _memory_check_ownership(app_id: str, level: str, identifier: str) -> Optional[web.Response]:
    """校验记忆目标是否属于当前机器人（group / c2c / global / bot 四级全覆盖）。

    为什么必须校验：group / c2c 记忆文件是**按 openid 全局存放**的
    （qun_memory/<gid>.json、c2c_memory/<uid>.json），文件本身不记 app_id。
    若不校验，任何一个机器人（或持有该机器人密钥的调用方）只要知道 openid，
    就能读写别的机器人的群/私聊记忆 —— 越权且会串数据。

    global / bot 级同样要校验，且**必须按 identifier 而不是按 URL 上的 app_id**：
    bot_memory/<app_id>.json 与 memory_<app_id>.json 都以 app_id 为文件名，
    调用方完全可以传 app_id=<自己> 通过 _check_cross_allowed，
    再用 identifier=<他人 app_id> 把读取目标指向别人的记忆文件。
    因此这里比对的是 identifier 本身。
    """
    if level == "group":
        if not _group_owned_by(app_id, identifier):
            return fail(f"群 {identifier} 不属于机器人 {app_id}（无本地记录，请先同步该群）", 403)
    elif level == "c2c":
        if not _user_owned_by(app_id, identifier):
            return fail(f"用户 {identifier} 不属于机器人 {app_id}（无本地记录，请先与该用户私聊）", 403)
    elif level in ("global", "bot"):
        # identifier 即目标 app_id。global 记忆还受 ISOLATE_GLOBAL_MEMORY 影响，
        # 这里只做「不得指向其它机器人」的归属判定，不改变隔离语义。
        if identifier and identifier != app_id:
            # 目标机器人必须存在，避免把「写错 app_id」与「越权」混为一谈
            known = any(b.get("APP_ID") == identifier for b in get_bots())
            if known:
                return fail(
                    f"权限不足：identifier（{identifier}）指向其它机器人的 {level} 记忆，"
                    f"本请求的 app_id 为 {app_id}", 403)
            return fail(f"未知的机器人 app_id：{identifier}", 400)
    return None


def _memory_check_item_ownership(app_id: str, item: Any) -> Optional[str]:
    """批量操作用：校验单个 item 的归属，越权时返回错误文案，正常返回 None。

    批量条目的 level/identifier 是调用方逐条给的，所以必须逐条校验 ——
    否则「批量」就成了绕过单条校验的后门。
    """
    if not isinstance(item, dict):
        return None                      # 结构错误交给底层统一报
    lv = str(item.get("level") or "").strip().lower()
    if lv not in ("group", "c2c"):
        return None
    ident = str(item.get("identifier") or item.get("group_id")
                or item.get("c2c_user_id") or "").strip()
    if not ident:
        return None                      # 缺 identifier 由底层报「缺少」
    e = _memory_check_ownership(app_id, lv, ident)
    return None if e is None else f"{lv}({ident}) 不属于机器人 {app_id}"


async def h_memory_list(request: web.Request) -> web.Response:
    """读取某级记忆：GET /api/memory/list?level=&identifier=[&app_id=]

    返回该级的记忆条目（带索引，可直接用于 update/delete）与启用状态。
    对应 AI 工具 view_memory。level 省略时默认 global。
    """
    # GET 无 body，_memory_precheck 只看 body 会漏掉查询参数里的拼写错误，
    # 故与 /api/push/list 等 GET 接口一样，在 handler 开头显式带 {} 校验一次
    if (e := _reject_unknown_params(request, "/api/memory/list", {})):
        return e

    body, e = await _memory_precheck(request, "/api/memory/list")
    if e:
        return e

    level = _memory_pick_level(body, request, default="global")
    if (e := _memory_check_level(level)):
        return e
    identifier = _memory_pick_identifier(body, request, level)
    if not identifier:
        return fail(f"缺少 identifier（{level} 级记忆需填{_memory_ident_hint(level)}）")
    if (e := _memory_check_ownership(_memory_owner_app_id(request, body), level, identifier)):
        return e

    try:
        mem_list, enabled = _memory_get(level, identifier)
    except _MemoryError as ex:
        return fail(str(ex), 400)
    except Exception as ex:
        return fail(f"读取记忆异常: {ex}", 500)

    # 带索引返回：调用方拿到后可直接拿去 update/delete，不必自己数下标
    items = [{"index": i, "content": c} for i, c in enumerate(mem_list)]
    return ok({
        "level": level,
        "identifier": identifier,
        "app_id": _memory_level_app_id(level, identifier),
        "enabled": enabled,
        "total": len(items),
        "items": items,
    })


async def h_memory_add(request: web.Request) -> web.Response:
    """添加记忆：POST /api/memory/add

    单条：{level, identifier, content}
    批量：{items: [{level, identifier, content}, ...]}
    对应 AI 工具 add_memory（同样支持 items 批量）。

    注意：该级记忆被禁用时添加会被拒绝（与工具侧一致）；
    如需先启用请调 /api/memory/toggle。
    """
    body, e = await _memory_precheck(request, "/api/memory/add")
    if e:
        return e

    items = body.get("items")
    if isinstance(items, list):
        owner = _memory_owner_app_id(request, body)
        # 逐条校验归属：「批量」不能成为绕过单条校验的后门
        unauthorized = [x for x in (_memory_check_item_ownership(owner, it) for it in items) if x]
        if unauthorized:
            return fail("批量中存在不属于当前机器人的目标：" + "；".join(unauthorized[:5]), 403)
        res = _memory_batch("add", items)
        return ok({
            "mode": "batch",
            "total": len(items),
            "success_count": len(res["success"]),
            "failed_count": len(res["failed"]),
            "success": res["success"],
            "failed": res["failed"],
        }, ok_result=not res["failed"])

    level = _memory_pick_level(body, request, default="")
    if (e := _memory_check_level(level)):
        return e
    identifier = _memory_pick_identifier(body, request, level)
    content = str(body.get("content") or "")
    if not identifier:
        return fail(f"缺少 identifier（{level} 级记忆需填{_memory_ident_hint(level)}）")
    if not content.strip():
        return fail("缺少 content")
    if (e := _memory_check_ownership(_memory_owner_app_id(request, body), level, identifier)):
        return e

    try:
        _memory_add(level, identifier, content)
    except _MemoryDisabledError:
        return fail(f"{level} 级记忆当前已禁用，请先启用（/api/memory/toggle）", 409)
    except _MemoryError as ex:
        return fail(str(ex), 400)
    except Exception as ex:
        return fail(f"添加记忆异常: {ex}", 500)

    info(f"[API] 记忆添加 {level}({identifier}): {content[:80]}", ctx=LogCtx(app_id=_memory_level_app_id(level, identifier)))
    return ok({"mode": "single", "level": level, "identifier": identifier, "message": "已添加"})


async def h_memory_update(request: web.Request) -> web.Response:
    """修改某条记忆：POST /api/memory/update

    单条：{level, identifier, index, content}
    批量：{items: [{level, identifier, index, content}, ...]}
    对应 AI 工具 modify_memory。索引从 0 开始，越界即失败（部分失败不回滚）。
    """
    body, e = await _memory_precheck(request, "/api/memory/update")
    if e:
        return e

    items = body.get("items")
    if isinstance(items, list):
        owner = _memory_owner_app_id(request, body)
        unauthorized = [x for x in (_memory_check_item_ownership(owner, it) for it in items) if x]
        if unauthorized:
            return fail("批量中存在不属于当前机器人的目标：" + "；".join(unauthorized[:5]), 403)
        res = _memory_batch("update", items)
        return ok({
            "mode": "batch",
            "total": len(items),
            "success_count": len(res["success"]),
            "failed_count": len(res["failed"]),
            "success": res["success"],
            "failed": res["failed"],
        }, ok_result=not res["failed"])

    level = _memory_pick_level(body, request, default="")
    if (e := _memory_check_level(level)):
        return e
    identifier = _memory_pick_identifier(body, request, level)
    if not identifier:
        return fail(f"缺少 identifier（{level} 级记忆需填{_memory_ident_hint(level)}）")
    index = body.get("index")
    if index is None:
        return fail("缺少 index")
    content = str(body.get("content") or "")
    if not content.strip():
        return fail("缺少 content")
    if (e := _memory_check_ownership(_memory_owner_app_id(request, body), level, identifier)):
        return e

    try:
        done = _memory_replace(level, identifier, index, content)
    except _MemoryError as ex:
        return fail(str(ex), 400)
    except Exception as ex:
        return fail(f"修改记忆异常: {ex}", 500)

    if not done:
        return fail(f"修改失败：索引 {index} 越界或无效", 404)
    info(f"[API] 记忆修改 {level}({identifier})[{index}]", ctx=LogCtx(app_id=_memory_level_app_id(level, identifier)))
    return ok({"mode": "single", "level": level, "identifier": identifier,
               "index": index, "message": "已修改"})


async def h_memory_delete(request: web.Request) -> web.Response:
    """删除某条记忆：POST /api/memory/delete

    单条：{level, identifier, index}
    批量：{items: [{level, identifier, index}, ...]}
    对应 AI 工具 delete_memory。
    """
    body, e = await _memory_precheck(request, "/api/memory/delete")
    if e:
        return e

    items = body.get("items")
    if isinstance(items, list):
        owner = _memory_owner_app_id(request, body)
        unauthorized = [x for x in (_memory_check_item_ownership(owner, it) for it in items) if x]
        if unauthorized:
            return fail("批量中存在不属于当前机器人的目标：" + "；".join(unauthorized[:5]), 403)
        res = _memory_batch("delete", items)
        return ok({
            "mode": "batch",
            "total": len(items),
            "success_count": len(res["success"]),
            "failed_count": len(res["failed"]),
            "success": res["success"],
            "failed": res["failed"],
        }, ok_result=not res["failed"])

    level = _memory_pick_level(body, request, default="")
    if (e := _memory_check_level(level)):
        return e
    identifier = _memory_pick_identifier(body, request, level)
    if not identifier:
        return fail(f"缺少 identifier（{level} 级记忆需填{_memory_ident_hint(level)}）")
    index = body.get("index")
    if index is None:
        return fail("缺少 index")
    if (e := _memory_check_ownership(_memory_owner_app_id(request, body), level, identifier)):
        return e

    try:
        done = _memory_remove(level, identifier, index)
    except _MemoryError as ex:
        return fail(str(ex), 400)
    except Exception as ex:
        return fail(f"删除记忆异常: {ex}", 500)

    if not done:
        return fail(f"删除失败：索引 {index} 越界或无效", 404)
    info(f"[API] 记忆删除 {level}({identifier})[{index}]", ctx=LogCtx(app_id=_memory_level_app_id(level, identifier)))
    return ok({"mode": "single", "level": level, "identifier": identifier,
               "index": index, "message": "已删除"})


async def h_memory_toggle(request: web.Request) -> web.Response:
    """启用/禁用某级记忆：POST /api/memory/toggle  body: {level, identifier, enabled}

    对应 AI 工具的 enable_memory / disable_memory。
    禁用只影响「是否注入系统提示」，记忆内容保留。
    """
    body, e = await _memory_precheck(request, "/api/memory/toggle")
    if e:
        return e

    level = _memory_pick_level(body, request, default="")
    if (e := _memory_check_level(level)):
        return e
    identifier = _memory_pick_identifier(body, request, level)
    if not identifier:
        return fail(f"缺少 identifier（{level} 级记忆需填{_memory_ident_hint(level)}）")
    if (e := _memory_check_ownership(_memory_owner_app_id(request, body), level, identifier)):
        return e

    raw = body.get("enabled")
    if raw is None:
        raw = body.get("value")
    if raw is None:
        raw = request.query.get("enabled")
    if raw is None:
        return fail("缺少 enabled（true/false，也可用 value 或查询参数 enabled=1/0）")
    enabled = str(raw).strip().lower() in ("1", "true", "yes", "on")

    try:
        _memory_set_enabled(level, identifier, enabled)
    except _MemoryError as ex:
        return fail(str(ex), 400)
    except Exception as ex:
        return fail(f"切换记忆状态异常: {ex}", 500)

    info(f"[API] 记忆 {level}({identifier}) 已{'启用' if enabled else '禁用'}",
         ctx=LogCtx(app_id=_memory_level_app_id(level, identifier)))
    return ok({"level": level, "identifier": identifier, "enabled": enabled,
               "message": "已启用" if enabled else "已禁用"})


async def h_memory_search(request: web.Request) -> web.Response:
    """检索记忆：GET /api/memory/search?keywords=&level=&identifier=[&group_id=][&c2c_user_id=]

    多关键词（逗号分隔或重复参数），命中任意一个即返回；跨层级搜索。
    对应 AI 工具 search_memory —— 底层同为 memory.search_memory，
    故这里额外提供结构化 matches（工具侧拿到的是拼好的中文文本）。
    level: global / bot / group / c2c / all（默认 all、不区分大小写）
    """
    if (e := _reject_unknown_params(request, "/api/memory/search", {})):
        return e

    body, e = await _memory_precheck(request, "/api/memory/search")
    if e:
        return e

    # 多关键词：keywords 支持逗号分隔与重复参数，keyword/q 为单数别名
    raw_keywords: List[str] = []
    for src in (request.query.getall("keywords", []), request.query.getall("keyword", []),
                request.query.getall("q", [])):
        for chunk in src:
            raw_keywords.extend(_split_multi(chunk))
    for key in ("keywords", "keyword", "q"):
        val = body.get(key)
        if val is not None:
            for chunk in (val if isinstance(val, list) else [val]):
                raw_keywords.extend(_split_multi(chunk))
    seen: set = set()
    keywords = [k for k in raw_keywords if not (k.lower() in seen or seen.add(k.lower()))]
    if not keywords:
        return fail("缺少 keywords（多个关键词用逗号分隔，或重复传该参数）")

    level = _memory_pick_level(body, request, default="all")
    if level not in ("all",) + tuple(_MEMORY_LEVELS):
        return fail(f"未知记忆级别 {level}，可选：all/{'/'.join(_MEMORY_LEVELS)}")

    auth = request["auth"]
    app_id, err = _resolve_app_id(auth, request)
    if err:
        return fail(err, 403 if auth["role"] == ROLE_BOT else 400)

    identifier = _memory_pick_identifier(body, request, level)
    group_id = (body.get("group_id") or request.query.get("group_id") or "").strip()
    c2c_id = (body.get("c2c_user_id") or request.query.get("c2c_user_id") or "").strip()

    # 各层级所需标识符：global/bot 用 app_id，group/c2c 用 openid
    search_app_id = identifier if level in ("global", "bot") else app_id
    search_group_id = group_id or (identifier if level == "group" else "") or None
    search_c2c_id = c2c_id or (identifier if level == "c2c" else "") or None

    # 归属校验：搜索同样能读出记忆内容，不校验就等于用搜索绕过 list 的限制
    if search_group_id and level in ("group", "all"):
        if (e := _memory_check_ownership(app_id, "group", search_group_id)):
            return e
    if search_c2c_id and level in ("c2c", "all"):
        if (e := _memory_check_ownership(app_id, "c2c", search_c2c_id)):
            return e
    # global / bot 级：identifier 可能被用来指向别的机器人的记忆文件
    # （search_app_id 在 2672 行取自 identifier），必须一并校验。
    if level in ("global", "bot", "all"):
        if (e := _memory_check_ownership(app_id, "global", search_app_id)):
            return e

    try:
        text = search_memory(
            keywords=keywords,
            layer=level,
            app_id=search_app_id,
            group_id=search_group_id,
            c2c_user_id=search_c2c_id,
        )
    except Exception as ex:
        return fail(f"搜索记忆异常: {ex}", 500)

    # 结构化结果：逐层列出匹配条目（工具侧只用 text，API 侧两者都给）
    matches: List[Dict[str, Any]] = []

    def _collect(lv: str, ident: Optional[str]):
        if not ident:
            return
        try:
            mem_list, enabled = _memory_get(lv, ident)
        except Exception:
            return
        for i, c in enumerate(mem_list):
            low = str(c).lower()
            if any(k.lower() in low for k in keywords):
                matches.append({"level": lv, "identifier": ident, "index": i,
                                "content": c, "enabled": enabled})

    if level in ("global", "all"):
        _collect("global", search_app_id)
    if level in ("bot", "all"):
        _collect("bot", search_app_id)
    if level in ("group", "all"):
        _collect("group", search_group_id)
    if level in ("c2c", "all"):
        _collect("c2c", search_c2c_id)

    info(f"[API] 记忆搜索 keywords={keywords} level={level} 命中 {len(matches)} 条",
         ctx=LogCtx(app_id=search_app_id or ""))
    return ok({
        "keywords": keywords,
        "level": level,
        "total": len(matches),
        "matches": matches,
        "text": text,
    })


# ==================== 10. 日志读取 ====================
async def h_logs(request: web.Request) -> web.Response:
    """读取日志：GET /api/logs[?lines=200][&keyword=][&level=][&app_id=][&reverse=1]

    对应日志文件（config 的 log_file，默认 log.txt），由 log.py 的
    LineRotatingFileSink 单文件滚动写入，超过 max_log_length 行会裁掉头部旧行。

    参数：
      lines    返回末尾多少**条**日志（默认 200，上限 5000）。日志文件可能几十万条，
               一次全返会撑爆响应，所以默认只给尾部。
               注意单位是「条」而不是物理行：一条日志的正文里可能含换行
               （多行堆栈等），按行返回会把一条拆成好几项。
      keyword  子串过滤（不区分大小写）。先过滤再取尾部 —— 与 /api/history 的
               keyword 语义一致：过滤结果里取最后 N 行，而不是"最后 N 行里再过滤"
               （后者在关键词稀疏时会频繁返回空）。
      level    按日志等级过滤（INFO / WARNING / ERROR 等，不区分大小写）。
      reverse  1=按时间倒序（最新在前），便于直接看最近发生了什么。
      app_id   日志是全机器人共写的单文件，可用它筛出某个机器人相关的行
               （按 [appid=xxx 前缀匹配）。不传则返回全部。
               权限：全局密钥可读全部；机器人密钥**只能读自己** —— 传别人的
               app_id 会被 403 拒绝，不传也会被强制收窄到自己。

    响应里的 unlimited=true 表示 config 的 max_log_length <= 0（不限制文件大小，
    只追加不裁剪）；此时 total 会随运行持续增长。

    响应只返回日志的**文件名**（file_name），不返回完整路径 ——
    路径会暴露服务端的目录结构，而调用方通常只需要知道"日志在哪个文件"。
    """
    if (e := _reject_unknown_params(request, "/api/logs", {})):
        return e

    try:
        lines = int(request.query.get("lines") or 200)
    except (TypeError, ValueError):
        return fail("lines 必须是整数")
    if lines <= 0:
        return fail("lines 必须大于 0")
    lines = min(lines, 5000)             # 上限，避免一次拉爆响应体

    keyword = (request.query.get("keyword") or "").strip().lower()
    level = (request.query.get("level") or "").strip().upper()
    app_id = (request.query.get("app_id") or "").strip()
    reverse = (request.query.get("reverse") or "").strip().lower() in ("1", "true", "yes", "on")

    # ---------- 权限：日志是「全体机器人共写一个文件」，必须显式收口 ----------
    # 本接口没有一个天然的 app_id 归属（不像 /api/history 有 target_id），
    # 所以不能套用 _resolve_app_id 那套「机器人只看自己」的逻辑，
    # 而要按角色区分：
    #   全局密钥：日志本来就是给它做运维排查的，可读全部，并可用 app_id 过滤
    #   机器人密钥：只能读自己相关的行。若允许读全量，等于任何一个机器人
    #               都能看到别的机器人的群 openid、私聊内容、用户昵称 —— 越权。
    auth = request["auth"]
    if auth["role"] == ROLE_BOT:
        own = auth["app_id"] or ""
        if app_id and app_id != own:
            return fail(f"权限不足：本密钥只能查看机器人 {own} 的日志", 403)
        # 即使不传 app_id，也强制只看自己的行（而不是返回全量）
        app_id = own

    log_file = get_log_file()
    # 只对外暴露文件名，不返回完整路径：
    # 路径会泄露服务端的目录结构（如 D:\dsh-workspace\...），
    # 调用方真正需要的只是"日志存在哪个文件"，文件名就够了。
    log_name = os.path.basename(log_file)
    _max_len = get_max_log_length()
    # max_log_length <= 0 表示不限制文件大小：显式给出布尔值，
    # 免得调用方看到 max_log_length=0 误以为"一行都不留"
    _unlimited = _max_len <= 0
    if not os.path.exists(log_file):
        # 文件不存在不算错误：可能是 enable_log=0（只输出控制台）或还没写过日志
        return ok({
            "file_name": log_name,
            "exists": False,
            "enabled": bool(get_enable_log()),
            "app_id": app_id or None,
            "total": 0, "matched": 0, "returned": 0,
            "max_log_length": _max_len,
            "unlimited": _unlimited,
            "lines": [],
            "hint": "日志文件不存在：请检查配置 enable_log 是否为 1（为 0 时只输出控制台，不写文件）",
        })

    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as ex:
        return fail(f"读取日志失败: {ex}", 500)

    # 按「日志条」而不是物理行处理：一条日志的正文里可能含换行
    # （多行堆栈、模型返回的整段文本），按行切会把它拆成好几条，
    # 前端无法还原，lines 的语义也会和 max_log_length 对不上。
    # 复用 log.py 的切分器，保证「写入端怎么算一条」和「读取端怎么算一条」一致。
    entries = _split_log_entries(content)
    total = len(entries)

    def _keep(entry: str) -> bool:
        low = entry.lower()
        if keyword and keyword not in low:
            return False
        # 等级匹配：只看该条日志的头部（"时间 | LEVEL | ..."），
        # 避免正文里恰好出现 "ERROR" 字样被误命中
        head = entry.split("\n", 1)[0].upper()
        if level and f"| {level}" not in head:
            return False
        if app_id and f"[appid={app_id}" not in entry:
            return False
        return True

    matched = [e.rstrip("\n") for e in entries if _keep(e)]
    tail = matched[-lines:]
    if reverse:
        tail = list(reversed(tail))

    return ok({
        "file_name": log_name,
        "exists": True,
        "enabled": bool(get_enable_log()),
        "level": get_log_level(),
        # 实际生效的 app_id 过滤：机器人密钥通道下它会被强制收窄为自己，
        # 回显出来调用方才能确认「我到底看的是谁的日志」
        "app_id": app_id or None,
        "total": total,                  # 文件总条数
        "matched": len(matched),         # 过滤后条数
        "returned": len(tail),           # 实际返回条数
        "max_log_length": _max_len,
        "unlimited": _unlimited,         # true = 不限制文件大小（max_log_length <= 0）
        "lines": tail,
    })


# ==================== 11. 服务信息 ====================
async def h_root(request: web.Request) -> web.Response:
    """根路径：无需鉴权的服务信息"""
    return ok({
        "service": "qqbot-api",
        "version": "1.0",
        "time": _now_iso(),
        "endpoints": [
            "GET  /api/bots",
            "GET  /api/groups",
            "GET  /api/members",
            "GET  /api/users",
            "GET  /api/contacts",
            "GET  /api/history",
            "POST /api/send",
            "POST /api/revoke",
            "POST /api/message/hide",
            "POST /api/batch_revoke",
            "POST /api/mute",
            "GET  /api/mute/status",
            "GET  /api/bot_state",
            "POST /api/push/create",
            "GET  /api/push/list",
            "POST /api/push/update",
            "POST /api/push/delete",
            "POST /api/wakeup/create",
            "GET  /api/wakeup/list",
            "POST /api/wakeup/update",
            "POST /api/wakeup/delete",
            "POST /api/wakeup/trigger",
            "GET  /api/memory/list",
            "POST /api/memory/add",
            "POST /api/memory/update",
            "POST /api/memory/delete",
            "POST /api/memory/toggle",
            "GET  /api/memory/search",
            "GET  /api/logs",
        ],
        "logs_api": {
            "note": "读取日志文件（config 的 log_file，与 log.py 写入端同源）",
            "params": {
                "lines": "返回末尾多少条日志，默认 200，上限 5000（按「条」不按物理行）",
                "keyword": "子串过滤（不区分大小写）；先过滤再取尾部",
                "level": "按等级过滤，如 INFO / WARNING / ERROR",
                "app_id": "只看某个机器人相关的行（匹配 [appid=xxx 前缀）",
                "reverse": "1=最新在前",
            },
            "permission": "全局密钥可读全部；机器人密钥只能读自己的日志",
            "unlimited": "config 的 max_log_length <= 0 时不限制文件大小，响应里 unlimited=true",
            "file_name": "只返回日志文件名，不返回完整路径（不暴露服务端目录结构）",
        },
        "memory_api": {
            "note": "记忆管理接口，与机器人工具 view_memory / add_memory / modify_memory / "
                    "delete_memory / enable_memory / disable_memory / search_memory 共用同一套底层函数",
            "levels": {
                "global": "全局记忆，identifier = 机器人 APP_ID",
                "bot": "机器人专属记忆，identifier = 机器人 APP_ID",
                "group": "群聊记忆，identifier = 群 openid",
                "c2c": "私聊记忆，identifier = 用户 openid",
            },
            "batch": "add / update / delete 三个接口都可用 items 数组一次操作多条",
            "items_example": {
                "items": [
                    {"level": "group", "identifier": "群openid", "content": "要添加的内容"},
                    {"level": "c2c", "identifier": "用户openid", "index": 0, "content": "替换后的内容"},
                ]
            },
            "index_semantics": "index 从 0 开始；delete 为按下标删除，多条批量按数组顺序依次执行",
        },
        "task_list_query": {
            "applies_to": ["GET /api/push/list", "GET /api/wakeup/list"],
            "note": "以下查询参数全部可选，可任意组合；多个值可用逗号分隔或重复同名参数",
            "status": {
                "desc": "按状态筛选，可多值（或关系）。只接受任务真实产生的三种状态",
                "values": {
                    "pending": "未开始",
                    "completed": "执行完成",
                    "failed": "执行失败",
                },
                "also_accepts": ["未开始", "执行完成", "执行失败"],
                "example": "?status=pending,completed 或 ?status=未开始",
                "note": "无法识别的值会被忽略，并在响应的 meta.filters.status_ignored 中回显",
            },
            "keywords": {
                "desc": "关键词数组，任一命中即可（默认）",
                "example": "?keywords=天气,新闻 或 ?keywords=天气&keywords=新闻",
            },
            "keyword / q / search": "单关键词，等价于 keywords 只有一个",
            "match_all": "1 时要求全部关键词都命中",
            "task_id": "任务 id 包含匹配",
            "task_app_id": "仅保留该机器人（app_id）的任务",
            "target_type": "目标类型：group / user（c2c 亦可）",
            "target_id": "目标 id 包含匹配",
            "start / end": "时间范围（含），支持 2026-09-21 或 2026-09-21T20:40:00+08:00；只给日期时 end 补到当天 23:59:59",
            "start_time / end_time": "start / end 的别名",
            "time_field": "用哪个字段做时间比较，默认 schedule_time 回退 created_at",
            "order": "asc / desc，按时间排序",
        },
        "task_list_response": {
            "total": "过滤后的任务数",
            "tasks": "任务列表",
            "counts": "过滤前的全量状态统计（total/pending/completed/failed）",
            "filters": "本次实际生效的过滤条件回显（仅在有过滤时出现）",
        },
    })


async def h_health(request: web.Request) -> web.Response:
    """健康检查"""
    return ok({"status": "healthy", "time": _now_iso()})


async def h_info(request: web.Request) -> web.Response:
    """服务与开关状态。

    ★ LOW 修复：本接口在中间件里被列为免鉴权路径（原意是给前端探活），
      但它返回了**全部机器人的 app_id、绑定地址、以及各密钥有无**。
      在无密钥模式下等于对任何人开放一份完整的资产清单 ——
      app_id 本身不是秘密，但「有哪些机器人、哪个开了 API」是攻击者
      最想要的第一步侦察信息（配合 C1 的无鉴权状态可直接逐个调用）。

      现在按调用方身份分级返回：
        - 已通过鉴权：维持原样（运维/管理页需要这些信息）
        - 未鉴权：只给「服务是否存活 + 版本」，不含任何机器人标识与绑定地址
      前端探活只需 ok=true，不受影响。
    """
    auth = request.get("auth")

    if not auth:
        # 未鉴权：最小可用信息
        return ok({
            "service": "dsh-qqbot-api",
            "alive": True,
            "authenticated": False,
        })

    bots = []
    for b in get_bots():
        aid = b.get("APP_ID", "")
        bots.append({
            "app_id": aid,
            "api_enabled": get_bot_api_enabled(aid),
            "has_keys": len(get_bot_api_keys(aid)) > 0,
        })
    return ok({
        "service": "dsh-qqbot-api",
        "alive": True,
        "authenticated": True,
        "global_api_enabled": get_global_api_enabled(),
        "global_has_keys": len(get_global_api_keys()) > 0,
        "bind": f"{get_api_bind_host()}:{get_api_bind_port()}",
        "bots": bots,
    })


# ==================== 配置管理 ====================
# 仅全局密钥可操作配置：机器人密钥不得读取/修改全局或他人配置。


def _cfg_guard(auth) -> Optional[web.Response]:
    """配置类接口的统一权限校验：仅全局密钥可用"""
    if auth["role"] != ROLE_GLOBAL:
        return fail("配置管理仅限全局密钥操作", 403)
    return None


def _mask_sensitive(obj):
    """
    递归脱敏：
      - APP_SECRET 等凭证字段：以 **** 返回，且禁止写入
      - API_KEYS / GLOBAL_API_KEYS：以 **** 返回（隐藏明文），但允许写入
      - models 数组：整项剔除，不返回任何内容
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if _is_hidden_cfg_key(k):
                continue          # models 等内部配置：直接不出现
            if is_sensitive_key(k):
                out[k] = "****"
            elif is_api_key_field(k) and isinstance(v, list):
                out[k] = ["****"] * len(v) if v else []
            else:
                out[k] = _mask_sensitive(v)
        return out
    if isinstance(obj, list):
        return [_mask_sensitive(x) for x in obj]
    return obj


def _resolve_parent(root, parts: List[str]):
    """
    逐层定位到「最后一个键的父容器」。
    返回 (parent, actual_last_key_or_index, error)。
    键名大小写不敏感；数组用数字下标。
    """
    cur = root
    for i, part in enumerate(parts[:-1]):
        if isinstance(cur, list):
            if not part.isdigit():
                return None, None, f"路径 {'.'.join(parts[:i+1])} 需要数组下标"
            idx = int(part)
            if idx < 0 or idx >= len(cur):
                return None, None, f"数组下标越界: {'.'.join(parts[:i+1])}"
            cur = cur[idx]
        elif isinstance(cur, dict):
            actual = None
            for k in cur.keys():
                if isinstance(k, str) and k.upper() == part.upper():
                    actual = k
                    break
            if actual is None:
                return None, None, f"配置项不存在: {'.'.join(parts[:i+1])}"
            cur = cur[actual]
        else:
            return None, None, f"路径 {'.'.join(parts[:i+1])} 不是对象或数组"

    last = parts[-1]
    if isinstance(cur, list):
        if not last.isdigit():
            return None, None, "数组写入需要下标"
        idx = int(last)
        if idx < 0 or idx >= len(cur):
            return None, None, "数组下标越界"
        return cur, idx, ""
    if isinstance(cur, dict):
        actual = None
        for k in cur.keys():
            if isinstance(k, str) and k.upper() == last.upper():
                actual = k
                break
        if actual is None:
            return None, None, f"配置项不存在: {'.'.join(parts)}（API 不允许新增配置项）"
        return cur, actual, ""
    return None, None, "路径不可写"


def _type_ok(old, value) -> Optional[str]:
    """校验新值与原值类型一致，返回错误信息或 None"""
    if isinstance(old, bool):
        return None if isinstance(value, bool) else "需要布尔值(true/false)"
    if isinstance(old, int):
        if isinstance(value, bool) or not isinstance(value, int):
            return "需要整数"
        return None
    if isinstance(old, float):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return "需要数字"
        return None
    if isinstance(old, str):
        return None if isinstance(value, str) else "需要字符串"
    if isinstance(old, list):
        return None if isinstance(value, list) else "需要数组"
    if isinstance(old, dict):
        return None if isinstance(value, dict) else "需要对象"
    return None


async def h_config_get(request: web.Request) -> web.Response:
    """
    读取配置：GET /api/config/get[?path=a.b.c]
    - path 省略时返回整个配置（密钥字段已脱敏）
    - path 形如 bots.0.API_ENABLED 或 API_BIND_PORT
    """
    if (e := _reject_unknown_params(request, "/api/config/get", {})):
        return e

    auth = request["auth"]
    if (e := _cfg_guard(auth)):
        return e

    cfg = get_config()
    path_raw = (request.query.get("path") or "").strip()
    if not path_raw:
        data = _mask_sensitive(cfg)
        return ok({"path": "", "value": data})

    parts = [p for p in path_raw.split(".") if p != ""]
    # 顶层隐藏键（models）任何形式都不许读：models、models.0、models.main...
    if parts and _is_hidden_cfg_key(parts[0]):
        return fail(f"配置项 {parts[0]} 不对外开放", 403)

    cur = cfg
    for i, part in enumerate(parts):
        if isinstance(cur, list):
            if not part.isdigit():
                return fail(f"路径 {'.'.join(parts[:i+1])} 需要数组下标")
            idx = int(part)
            if idx < 0 or idx >= len(cur):
                return fail(f"数组下标越界: {'.'.join(parts[:i+1])}")
            cur = cur[idx]
        elif isinstance(cur, dict):
            actual = None
            for k in cur.keys():
                if isinstance(k, str) and k.upper() == part.upper():
                    actual = k
                    break
            if actual is None:
                return fail(f"配置项不存在: {'.'.join(parts[:i+1])}", 404)
            cur = cur[actual]
        else:
            return fail(f"路径 {'.'.join(parts[:i+1])} 不是对象或数组")

    # 按路径读取时，最后一段键名同样要参与脱敏判定。
    # 否则 /api/config/get?path=bots.0.APP_SECRET 会把裸字符串原样返回
    # （_mask_sensitive 只看键名，对裸值无能为力）。
    last_key = parts[-1] if parts else ""
    if not last_key.isdigit() and (is_sensitive_key(last_key) or is_api_key_field(last_key)):
        return ok({"path": path_raw, "value": "****"})

    return ok({"path": path_raw, "value": _mask_sensitive(cur)})


async def h_config_set(request: web.Request) -> web.Response:
    """
    修改配置：POST /api/config/set
    body: {path: "API_BIND_PORT", value: 8149}
      或  {set: {"API_BIND_PORT": 8149, "bots.0.API_ENABLED": 1}}

    安全保证：
      - 仅全局密钥可调用
      - 不允许新增配置项（防止拼错键名产生垃圾配置）
      - 新值类型必须与原值一致
      - 密钥字段禁止修改
      - 写入前自校验 + 原子替换，失败不破坏原文件
    """
    auth = request["auth"]
    if (e := _cfg_guard(auth)):
        return e

    body = request.get("json_body") or await _read_json(request)
    if (e := _reject_unknown_params(request, "/api/config/set", {})):
        return e


    pairs: List[tuple] = []
    if isinstance(body.get("set"), dict):
        for k, v in body["set"].items():
            pairs.append((str(k), v))
    if "path" in body:
        pairs.append((str(body.get("path") or ""), body.get("value")))
    if not pairs:
        return fail("请提供 path+value 或 set 对象")

    cfg = get_config()
    applied, errors = [], []

    for path_raw, value in pairs:
        parts = [p for p in str(path_raw).split(".") if p != ""]
        if not parts:
            errors.append(f"{path_raw}: 路径为空")
            continue

        # models 是服务端内部配置：既不开放读，也不允许通过 API 改
        if _is_hidden_cfg_key(parts[0]):
            errors.append(f"{path_raw}: {parts[0]} 为服务端内部配置，禁止通过 API 读写，请直接编辑配置文件")
            continue

        parent, key, err = _resolve_parent(cfg, parts)
        if err:
            errors.append(f"{path_raw}: {err}")
            continue

        last = parts[-1]
        if is_sensitive_key(last):
            errors.append(f"{path_raw}: 敏感字段（密钥）禁止通过 API 修改，请直接编辑配置文件")
            continue
        # APP_ID 是机器人标识：改写它等于把配置指向另一个机器人，
        # 且会与凭证/记忆/任务里的 app_id 全部失配，故禁止通过 API 修改。
        if str(last).strip().upper() == "APP_ID":
            errors.append(f"{path_raw}: APP_ID 为机器人标识，禁止通过 API 修改，请直接编辑配置文件")
            continue

        old = parent[key]
        if (terr := _type_ok(old, value)):
            errors.append(f"{path_raw}: {terr}")
            continue

        parent[key] = value
        applied.append({"path": path_raw, "old": _mask_sensitive(old),
                        "value": _mask_sensitive(value)})

    if errors and not applied:
        return fail("；".join(errors))

    deep_patch_defaults(cfg, DEFAULT_CONFIG)
    if not write_config(cfg):
        return fail("配置写入失败（已保留原配置，详见日志）", 500)

    # 配置管理仅限全局密钥通道（auth["app_id"] 为 None），无会话概念故不传 ctx
    info(f"[API] 配置已修改: {[a['path'] for a in applied]}", ctx=None)
    return ok({
        "applied": applied,
        "errors": errors,
        "note": "配置已实时生效，无需重启（监听地址/端口变更会自动重载）",
    })


async def h_config_list(request: web.Request) -> web.Response:
    """
    列出可配置项：GET /api/config/list[?scope=global|bot][&app_id=]

    返回扁平的「路径 + 当前值 + 类型」，便于前端生成表单。

    scope=bot   —— 只列出该机器人的配置项
       · APP_SECRET 等凭证字段：值以 **** 返回，且二次标记 sensitive（禁止写入）
       · APP_ID：正常显示真实值（它是标识符不是密钥），仅禁止改写
       · API_KEYS 等密钥数组：脱敏为 ["****", ...]，保留元素个数
       · 其余普通数组：完整返回真实内容

    scope=global（默认）—— 列出顶层配置项
       · 跳过 bots 数组（用 scope=bot&app_id=... 单独查看，避免把全部凭证铺开）
       · 跳过 models 数组（含 API 密钥，不便逐条展示；如需查看用 /api/config/get）
       · 其余 dict 展开成 "父.子" 路径
    """
    if (e := _reject_unknown_params(request, "/api/config/list", {})):
        return e

    auth = request["auth"]
    if (e := _cfg_guard(auth)):
        return e

    cfg = get_config()
    scope = (request.query.get("scope") or "global").strip().lower()
    app_id = (request.query.get("app_id") or "").strip()
    items: List[Dict[str, Any]] = []

    # 全局 scope 下不展开的顶层键（体积大或含凭证）
    GLOBAL_SKIP = {"bots"}

    def add(path, value, key_name=""):
        kn = key_name or path.split(".")[-1]
        upper = str(kn).strip().upper()
        is_sens = is_sensitive_key(kn)
        is_keyf = is_api_key_field(kn)

        if is_sens:
            # 凭证：完全脱敏，且标记只读
            masked: Any = "****"
        elif is_keyf and isinstance(value, list):
            # 密钥数组：保留长度信息，逐个脱敏（前端可据此提示"共 N 个"）
            masked = ["****"] * len(value)
        else:
            # 普通值（含普通数组）：递归脱敏，数组会原样保留真实内容
            masked = _mask_sensitive(value)

        items.append({
            "path": path,
            "value": masked,
            # 数组/对象用真实类型名，避免显示成 str
            "type": type(value).__name__,
            "type_name": type(value).__name__,
            # 凭证字段：完全只读；API 密钥字段：可写但不回显
            "sensitive": is_sens,
            "api_key": is_keyf,
            # APP_ID 是标识符：显示真实值，但禁止改写（改它等于换一个机器人）
            "readonly": is_sens or upper == "APP_ID",
        })

    # ---------------- scope=bot ----------------
    if scope == "bot":
        if not app_id:
            return fail("scope=bot 时需要 app_id")
        bots = cfg.get("bots") or []
        target = None
        for i, b in enumerate(bots):
            if str(b.get("APP_ID")) == app_id:
                target = (i, b)
                break
        if target is None:
            return fail(f"未找到机器人 {app_id}", 404)
        i, b = target
        for k, v in b.items():
            # APP_ID 传真实值（不再写成 ****），其余交给 add() 统一处理
            add(f"bots.{i}.{k}", v, k)
        return ok({
            "scope": "bot", "app_id": app_id, "bot_index": i,
            "total": len(items), "items": items,
        })

    # ---------------- scope=global ----------------
    for k, v in cfg.items():
        # models 完全不出现（连占位都不给），避免暴露有几组模型配置
        if _is_hidden_cfg_key(k):
            continue
        if k in GLOBAL_SKIP:
            # 只给出概览占位，不展开内容
            items.append({
                "path": "bots",
                "value": f"<{len(v)} 个机器人，请用 scope=bot&app_id=... 查看>",
                "type": "array", "type_name": "array",
                "sensitive": False, "api_key": False, "readonly": True,
                "hidden": True,
            })
        elif isinstance(v, dict):
            for k2, v2 in v.items():
                add(f"{k}.{k2}", v2, k2)
        else:
            add(k, v, k)

    return ok({"scope": "global", "total": len(items), "items": items})


# ==================== 应用装配 ====================
def create_app() -> web.Application:
    # CORS 必须放在鉴权之前：浏览器预检请求不带 Authorization，需先被短路处理
    app = web.Application(middlewares=[cors_middleware, auth_middleware])
    app.add_routes([
        web.get("/", h_root),
        web.get("/health", h_health),
        web.get("/info", h_info),

        web.get("/api/bots", h_bots),
        web.get("/api/groups", h_groups),
        web.get("/api/members", h_members),
        web.get("/api/users", h_users),
        web.get("/api/contacts", h_contacts),
        web.get("/api/history", h_history),

        web.post("/api/send", h_send),
        web.post("/api/revoke", h_revoke),
        web.post("/api/message/hide", h_message_hide),
        web.post("/api/batch_revoke", h_batch_revoke),
        web.post("/api/mute", h_mute),
        web.get("/api/mute/status", h_mute_status),
        web.get("/api/bot_state", h_bot_state),

        web.post("/api/push/create", h_push_create),
        web.get("/api/push/list", h_push_list),
        web.post("/api/push/update", h_push_update),
        web.post("/api/push/delete", h_push_delete),

        web.post("/api/wakeup/create", h_wakeup_create),
        web.get("/api/wakeup/list", h_wakeup_list),
        web.post("/api/wakeup/update", h_wakeup_update),
        web.post("/api/wakeup/delete", h_wakeup_delete),
        web.post("/api/wakeup/trigger", h_wakeup_trigger),

        web.get("/api/config/get", h_config_get),
        web.get("/api/config/list", h_config_list),
        web.post("/api/config/set", h_config_set),

        # 记忆管理（与 AI 工具的 view/add/modify/delete/enable/disable/search_memory 共用底层）
        web.get("/api/memory/list", h_memory_list),
        web.post("/api/memory/add", h_memory_add),
        web.post("/api/memory/update", h_memory_update),
        web.post("/api/memory/delete", h_memory_delete),
        web.post("/api/memory/toggle", h_memory_toggle),
        web.get("/api/memory/search", h_memory_search),

        # 日志读取（读 config 的 log_file，与 log.py 写入端同源）
        web.get("/api/logs", h_logs),
    ])
    return app


async def api_server_loop():
    """
    API 服务主循环（由 core.py 作为后台任务启动）。
    实时监听配置中的监听地址/端口：若配置变化会自动重启服务。
    """
    current = None  # (host, port, runner)

    while True:
        try:
            host = get_api_bind_host()
            port = get_api_bind_port()
            want = (host, port)

            if current is None or current[0] != want[0] or current[1] != want[1]:
                # 关闭旧服务
                if current is not None:
                    info(f"[API] 监听配置变更，重启服务 {current[0]}:{current[1]} → {host}:{port}", ctx=None)
                    try:
                        await current[2].cleanup()
                    except Exception as e:
                        warn(f"[API] 关闭旧服务失败: {e}", ctx=None)
                    current = None

                # 启动新服务
                runner = web.AppRunner(create_app())
                try:
                    await runner.setup()
                    site = web.TCPSite(runner, host, port)
                    await site.start()
                except OSError as e:
                    error(f"[API] 启动失败 {host}:{port} — {e}（5秒后重试）", ctx=None)
                    try:
                        await runner.cleanup()
                    except Exception:
                        pass
                    await asyncio.sleep(5)
                    continue

                current = (host, port, runner)
                info(f"[API] 服务已启动，监听 http://{host}:{port}/", ctx=None)
        except asyncio.CancelledError:
            info("[API] 服务停止", ctx=None)
            if current is not None:
                try:
                    await current[2].cleanup()
                except Exception:
                    pass
            raise
        except Exception as e:
            error(f"[API] 服务异常: {e}", ctx=None)

        await asyncio.sleep(3)