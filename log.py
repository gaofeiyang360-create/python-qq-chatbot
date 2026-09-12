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
import sys
import os
import threading

# 延迟导入 loguru
_loguru_imported = False
logger = None

# 记录 config.json 最后已知的 mtime，用于实时检测变更
_last_config_mtime: float = 0.0
# 标记 setup_logger() 是否至少被调用过一次
_logger_initialized = False

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
    from config import get_enable_log, get_log_file, get_max_log_length, get_disable_print_in_console, get_log_level

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

        # 使用匿名闭包实现按行数轮转
        def make_line_rotator(limit: int):
            count = [0]
            def rotator(message, file):
                count[0] += 1
                return count[0] >= limit
            return rotator

        _logger.add(
            log_file,
            level=level,
            encoding="utf-8",
            rotation=make_line_rotator(max_lines),
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<5} | {message}",
            backtrace=True,
            diagnose=False,
        )

    # 控制台日志：根据 disable_print_in_console 决定是否输出
    if not get_disable_print_in_console():
        _logger.add(
            sys.stderr,
            level="DEBUG",
            format=(
                "<green>{time:HH:mm:ss}</green> "
                "| <level>{level:<5}</level> "
                "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
                "- <level>{message}</level>"
            ),
            colorize=True,
        )


# ==================== 便捷导出函数（带实时配置检测） ====================

def info(msg, *args, **kwargs):
    """输出 INFO 级别日志（自动检测配置变更）"""
    _check_log_config()
    _get_logger().info(msg, *args, **kwargs)


def warn(msg, *args, **kwargs):
    """输出 WARNING 级别日志（自动检测配置变更）"""
    _check_log_config()
    _get_logger().warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """输出 ERROR 级别日志（自动检测配置变更）"""
    _check_log_config()
    _get_logger().error(msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """输出 DEBUG 级别日志（自动检测配置变更）"""
    _check_log_config()
    _get_logger().debug(msg, *args, **kwargs)