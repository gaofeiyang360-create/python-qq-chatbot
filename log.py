# -*- coding: utf-8 -*-
# log.py — 日志模块（基于 loguru 第三方库）
# 提供日志分级：info / warn / error / debug
# 配置项（来自 config.json）：
#   enable_log    — 是否启用文件日志，0=禁用（仅控制台输出），1=启用
#   log_file      — 日志文件路径，默认 "log.txt"
#   max_log_length — 日志文件最大行数，达到后自动轮转，默认 800
#
# ★ 日志配置支持实时读取：
#   每次写日志时自动检查 config.json 的修改时间（mtime），
#   若有变更则自动根据新配置重建 logger，无需重启程序。
#
# ★ 日志标识必须显式传入（本文件的核心约定）：
#   每次调用 info/warn/error/debug 都必须传入 ctx（LogCtx），
#   其中携带 app_id / thread_key / msg_id / thread_display。
#   刻意不使用 contextvars 之类的隐式上下文：隐式上下文会把"上一条消息"
#   的标识带到"下一条消息"的日志里（尤其在 asyncio 任务复用、任务取消、
#   或异常路径漏清理时），导致日志串味、难以排查。
#   漏传时打印 appid=? 占位符，而不是回退到任何历史值。
#
#   用法：
#     from log import info, LogCtx
#     ctx = LogCtx(app_id="123", thread_key="group_abc", msg_id="m1")
#     info("[发送] 完成", ctx=ctx)
#     info("[发送] 完成", ctx=ctx, thread_display="某群")   # 临时覆盖显示名
import sys
import os
import threading
from dataclasses import dataclass, replace
from typing import Optional


# ==================== 日志标识（显式传递，无隐式上下文） ====================
@dataclass(frozen=True)
class LogCtx:
    """一次日志所需的全部标识。

    不可变（frozen）：避免调用方持有同一个 ctx 对象后被意外改写，
    从而在后续日志里带上不属于它的标识。

    字段：
      app_id:         机器人 APP_ID
      thread_key:     会话标识，如 group_xxx / c2c_xxx
      msg_id:         触发本次处理的原始消息 ID
      thread_display: 会话显示名（群名/用户名），仅用于人类阅读
    """
    app_id: str = ""
    thread_key: str = ""
    msg_id: str = ""
    thread_display: str = ""

    def with_(self, **kwargs) -> "LogCtx":
        """派生一个只覆盖部分字段的新 ctx（原对象不变）。

        用于在已有 ctx 基础上补/换某个字段，例如补上群名：
          ctx2 = ctx.with_(thread_display="测试群")
        """
        return replace(self, **kwargs)


# 允许在 ctx 位置直接传字符串作为 app_id 的简写（ctx="123" 等价于 LogCtx(app_id="123")）
CtxLike = Optional[object]


def _coerce_ctx(ctx: CtxLike) -> Optional[LogCtx]:
    """把传入的 ctx 归一化为 LogCtx；无法识别时返回 None。"""
    if ctx is None:
        return None
    if isinstance(ctx, LogCtx):
        return ctx
    if isinstance(ctx, str):
        # 简写：只关心 app_id 的场景
        return LogCtx(app_id=ctx)
    # 兼容 dict
    if isinstance(ctx, dict):
        return LogCtx(
            app_id=str(ctx.get("app_id") or ""),
            thread_key=str(ctx.get("thread_key") or ""),
            msg_id=str(ctx.get("msg_id") or ""),
            thread_display=str(ctx.get("thread_display") or ""),
        )
    return None


def _build_prefix(ctx: Optional[LogCtx], *,
                  app_id: str = "", thread_key: str = "", msg_id: str = "",
                  thread_display: str = "") -> str:
    """构建日志前缀。

    规则（关键）：
      - 只使用本次调用显式传入的字段，绝不读取任何隐式/历史上下文；
      - ctx 中缺失的字段一律显示 '?' 占位符，便于一眼看出哪个调用点没传标识；
      - thread_display 作为额外显示名，仅在 thread_key 存在时附带。

    产出形如：
      [appid=123 | thread=group_abc [测试群] | msg_id=m1]
      [appid=? | thread=?]                       ← 漏传时的样子
    """
    c = _coerce_ctx(ctx)
    a = str(app_id or (c.app_id if c else "") or "")
    t = str(thread_key or (c.thread_key if c else "") or "")
    m = str(msg_id or (c.msg_id if c else "") or "")
    d = str(thread_display or (c.thread_display if c else "") or "")

    parts = [f"appid={a or '?'}"]
    t_part = f"thread={t or '?'}"
    if t and d:
        t_part += f" [{d}]"
    parts.append(t_part)
    if _show_msg_id:
        parts.append(f"msg_id={m or '?'}")

    return f"[{' | '.join(parts)}] "


# 延迟导入 loguru
_loguru_imported = False
logger = None

# 记录 config.json 最后已知的 mtime，用于实时检测变更
_last_config_mtime: float = 0.0
# 标记 setup_logger() 是否至少被调用过一次
_logger_initialized = False
# 是否在日志前缀中显示 msg_id（按 config.json log_show_msg_id 刷新）
_show_msg_id = 1

# --- 可重入防护 ---
# setup_logger() → get_config() → 日志补全 → info() → 死循环
# 使用线程锁 + 重入计数器阻断
_logger_reentry_count = 0
_logger_reentry_lock = threading.Lock()


def _get_logger():
    """懒加载 loguru logger"""
    global _loguru_imported, logger
    if not _loguru_imported:
        from loguru import logger as _loguru_logger
        logger = _loguru_logger
        _loguru_imported = True
    return logger


def _get_config_mtime() -> float:
    """获取 config.json 文件的最后修改时间，若文件不存在返回 0.0"""
    try:
        from config import CONFIG_FILE
        if CONFIG_FILE.exists():
            return os.path.getmtime(str(CONFIG_FILE))
    except Exception:
        pass
    return 0.0


def _check_log_config():
    """
    检查 config.json 是否有变更（通过比较 mtime），
    若有变更则重新初始化 logger。
    每次写日志前由 info/warn/error/debug 函数调用。
    内含可重入保护：当 setup_logger 再次触发日志时直接跳过。
    """
    global _last_config_mtime, _logger_initialized, _logger_reentry_count

    # 可重入检测：已在 setup_logger → config 路径中，直接返回
    with _logger_reentry_lock:
        if _logger_reentry_count > 0:
            return
        current_mtime = _get_config_mtime()
        needs_setup = not _logger_initialized or current_mtime != _last_config_mtime
        if not needs_setup:
            return
        _last_config_mtime = current_mtime
        # 增加重入计数，保护 setup_logger 内部
        _logger_reentry_count += 1

    try:
        setup_logger()
        _logger_initialized = True
    finally:
        with _logger_reentry_lock:
            _logger_reentry_count -= 1


def setup_logger():
    """
    根据配置初始化日志系统。
    可以被多次调用，每次都会重置 handler。
    """
    global _show_msg_id
    from config import get_enable_log, get_log_file, get_max_log_length, get_disable_print_in_console, get_log_level, get_log_show_msg_id, get_log_console_level

    # 刷新 msg_id 显示开关
    _show_msg_id = get_log_show_msg_id()

    _logger = _get_logger()
    _logger.remove()  # 清空所有已有 handler

    enabled = get_enable_log()

    if enabled:
        log_file = get_log_file()
        max_lines = get_max_log_length()
        level = get_log_level()
        # 校验等级有效性，无效时回退 INFO
        valid_levels = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
        if level not in valid_levels:
            level = "INFO"

        # 自定义 sink：单文件滚动，超过行数删除头顶旧行
        class LineRotatingFileSink:
            def __init__(self, path: str, max_lines: int):
                self.path = path
                self.max_lines = max_lines

            def write(self, message):
                # 追加写入
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(message)
                # 超出行数 → 裁掉头顶
                if self.max_lines > 0:
                    self._trim()

            def _trim(self):
                try:
                    with open(self.path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                    if len(lines) > self.max_lines:
                        with open(self.path, "w", encoding="utf-8") as f:
                            f.writelines(lines[-self.max_lines:])
                except Exception:
                    pass

        fmt = "{time:YYYY-MM-DD HH:mm:ss} | {level:<5} | {name}:{function}:{line} - {message}\n"

        _logger.add(
            LineRotatingFileSink(log_file, max_lines),
            level=level,
            format=fmt,
            backtrace=True,
            diagnose=False,
        )

    # 控制台日志：根据 disable_print_in_console 决定是否输出
    if not get_disable_print_in_console():
        console_level = get_log_console_level()
        # 校验等级有效性
        valid_levels = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}
        if console_level not in valid_levels:
            console_level = "DEBUG"
        _logger.add(
            sys.stderr,
            level=console_level,
            format=(
                "<green>{time:HH:mm:ss}</green> "
                "| <level>{level:<5}</level> "
                "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
                "- <level>{message}</level>"
            ),
            colorize=True,
        )


# ==================== 便捷导出函数（带实时配置检测） ====================
# 每个函数都要求显式传入 ctx（或 app_id/thread_key/msg_id 关键字），
# 缺失的字段显示 '?'，绝不回退到任何隐式历史上下文。

# ctx 之外可用的字段级覆盖关键字（从 kwargs 里摘出来，不传给 loguru）
_FIELD_KEYS = ("app_id", "thread_key", "msg_id", "thread_display")


def _emit(level_method, msg, ctx=None, *args, **kwargs):
    """统一出口：拼前缀后交给 loguru。

    字段级覆盖（app_id=... / thread_key=... / msg_id=... / thread_display=...）
    会优先于 ctx 中的同名字段；这些键不会透传给 loguru，
    其余 kwargs（如 loguru 的 exception=、depth=）原样透传。
    """
    _check_log_config()
    overrides = {k: kwargs.pop(k) for k in _FIELD_KEYS if k in kwargs}
    prefix = _build_prefix(ctx, **overrides)
    _get_logger().__getattribute__(level_method)(prefix + msg, *args, **kwargs)


def info(msg, ctx=None, *args, **kwargs):
    """输出 INFO 级别日志。

    ctx 必传（LogCtx / app_id 字符串 / dict）；也可用关键字覆盖单个字段：
      info("...", ctx=ctx)
      info("...", ctx=ctx, thread_display="群名")
      info("...", app_id="123", thread_key="group_x")   # 不用 ctx 的写法
    """
    _emit("info", msg, ctx, *args, **kwargs)


def warn(msg, ctx=None, *args, **kwargs):
    """输出 WARNING 级别日志（用法同 info）。"""
    _emit("warning", msg, ctx, *args, **kwargs)


def error(msg, ctx=None, *args, **kwargs):
    """输出 ERROR 级别日志（用法同 info）。"""
    _emit("error", msg, ctx, *args, **kwargs)


def debug(msg, ctx=None, *args, **kwargs):
    """输出 DEBUG 级别日志（用法同 info）。"""
    _emit("debug", msg, ctx, *args, **kwargs)