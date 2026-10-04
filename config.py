# -*- coding: utf-8 -*-
# config.py — 配置文件实时读取写入
import sys
import json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List

from log import info, warn, error, setup_logger, LogCtx

# ==================== 路径与常量 ====================
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

CONFIG_FILE = BASE_DIR / "config.json"
MIRROR_FILE = BASE_DIR / "mirror.json"

DEFAULT_CONFIG = {
    # ==================== 日志配置 ====================
    "enable_log": 0,                 # 0=禁用文件日志（仅控制台输出），1=启用文件日志
    "log_file": "log.txt",           # 日志文件路径
    "max_log_length": 800,           # 日志文件最大行数，达到后自动轮转裁掉旧行；
                                     # 0（或负数）= 不限制文件大小，只追加不裁剪（自行留意磁盘占用）
    "disable_print_in_console": 0,   # 0=控制台输出，1=关闭控制台显示（文件日志仍可启用）
    "log_level": "DEBUG",            # 文件日志记录等级：DEBUG/INFO/WARNING/ERROR/CRITICAL
    "log_console_level": "DEBUG",    # 控制台输出等级：默认 DEBUG（全部显示），可设为 INFO 过滤调试噪音
    "log_show_msg_id": 1,            # 日志前缀是否显示 msg_id：1=显示，0=隐藏（只显示 appid 和 thread）

    "bots": [
        {
            "APP_ID": "YOUR_APP_ID",                    # 替换为实际的机器人 App ID
            "APP_SECRET": "YOUR_APP_SECRET",            # 替换为实际的机器人 App Secret
            "SYSTEM_PROMPT": "你是一个幽默风趣的AI助手",  # 可根据需要自定义
            "ISOLATE_GLOBAL_MEMORY": 0,                 # 0 表示共享全局记忆，1 表示隔离
            "ENABLED": 1,                               # 1 启用，0 禁用
            "AUTO_WELCOME": 1,                          # 1 自动发送欢迎语
            "ENABLE_TOOLS": 1,                          # 是否启用工具调用
            "MAX_TOOL_ROUNDS": 6,                       # 工具调用最大轮数
            "ALLOW_CROSS_BOT_PUSH": 1,                  # 1=允许本机器人跨机器人推送（向其他机器人的用户/群发消息），0=禁止
            "ALLOW_CROSS_BOT_PUSH_INCOMING": 1,          # 1=允许其他机器人向本机器人的用户/群推送消息，0=禁止
            "ALLOW_MANAGE_ALL_PUSH": 1,                  # 1=允许管理所有定时推送（包括其他机器人发起的），0=只能管理自己的
            "ALLOW_CROSS_BOT_WAKEUP": 1,                  # 1=允许本机器人跨机器人定时唤醒，0=禁止
            "ALLOW_CROSS_BOT_WAKEUP_INCOMING": 1,          # 1=允许其他机器人向本机器人的用户/群发起定时唤醒，0=禁止
            "ALLOW_CROSS_BOT_GET_LIST": 1,                  # 1=允许本机器人获取跨机器人用户和群列表，0=禁止
            "ALLOW_CROSS_BOT_GET_LIST_INCOMING": 1,          # 1=允许其他机器人获取本机器人的用户和群列表，0=禁止
            "ALLOW_CROSS_BOT_HISTORY": 1,                    # 1=允许本机器人跨机器人读写聊天记录（query_history/revoke/batch_revoke），0=禁止
            "ALLOW_CROSS_BOT_HISTORY_INCOMING": 1,           # 1=允许其他机器人读写本机器人的聊天记录，0=禁止
            "DISABLE_AI_REPLY": 0,                           # 0=正常AI回复（推荐），1=关闭AI回复与工具调用，仅记录消息/群/用户/群员信息和摘要
            "API_ENABLED": 1,                                # 1=开启本机器人的 HTTP API 服务（默认开），0=关闭
            "API_KEYS": []                                   # 本机器人的 API 服务密钥数组，空数组=不需要密钥即可访问
        }
    ],
    "AI_MAX_MSG_LEN": 1500,          # 单条消息最大长度（字符数）
    "CONTEXT_LIMIT": 15,             # 上下文保留对话轮数
    "JUDGE_CONTEXT_LIMIT": 10,       # 判断模型使用的上下文轮数
    "COOLDOWN_SECONDS": 2,           # 用户请求冷却时间（秒）
    "COMPRESS_THRESHOLD": 25,        # 触发上下文压缩的阈值（轮数）
    "MAX_WORKERS": 20,               # 并发工作线程数
    # 图片阻塞：控制媒体识别是否阻塞消息接收（实时读取，改完即生效）
    #   1 = 阻塞（旧逻辑）：识别期间该机器人的接收循环被占用，后续消息要排队等识别结束
    #   0 = 不阻塞（默认）：识别不占用接收循环；若期间来了新消息，本轮改用占位（只留 URL），
    #       识别转后台继续跑完并写缓存，AI 可自行调 recognize_media 命中缓存
    "MEDIA_BLOCK": 0,
    "TOOL_CHOICE": 1,                         # 工具调用策略：1=每次必须调用工具（required），0=不强制（auto）
    "SYSTEM_PROMPT": "you are a helpful ai",  # 全局系统提示词（可作为后备）

    # ==================== 全局 API 服务 ====================
    # 全局开关与各机器人的 API_ENABLED / API_KEYS 互不干扰：
    #   - 全局开关关闭时，只停用「全局密钥」这条管理通道，各机器人自己的 API 仍照常工作
    #   - 全局密钥拥有全部权限，可以管理（查看/操作）任意机器人
    "GLOBAL_API_ENABLED": 1,          # 1=开启全局 API 管理通道（默认开），0=关闭
    "GLOBAL_API_KEYS": [],            # 全局 API 密钥数组，空数组=不需要密钥且拥有全部权限
    "API_BIND_HOST": "0.0.0.0",       # API 服务监听地址
    "API_BIND_PORT": 8148,            # API 服务监听端口

    "models": {
        "judge": [                   # 判断模型（用于意图识别/路由），支持多Key故障切换
            {
                "base_url": "YOUR_JUDGE_BASE_URL",
                "api_key": "YOUR_JUDGE_API_KEY",
                "model_name": "your-judge-model"
            }
        ],
        "main": [                    # 主对话模型，支持多Key故障切换
            {
                "base_url": "YOUR_MAIN_BASE_URL",
                "api_key": "YOUR_MAIN_API_KEY",
                "model_name": "your-main-model"
            }
        ],
        "vision": [                  # 视觉多模态模型，支持多Key故障切换
            {
                "base_url": "YOUR_VISION_BASE_URL",
                "api_key": "YOUR_VISION_API_KEY",
                "model_name": "your-vision-model"
            }
        ]
    }
}

# 机器人名称（运行时可被 WebSocket Ready 事件更新）
BOT_NAME = "蓝狼"


# ==================== 安全 JSON 读取（自动修复损坏文件） ====================
def safe_load_json(file_path: Path, default_data, ctx=None):
    """
    尝试读取 JSON 文件，若文件不存在或格式损坏，则备份损坏文件并创建新文件写入 default_data，
    然后返回 default_data。

    ctx 可选：本函数是通用工具，但调用方往往知道这份数据属于哪个机器人
    （例如 bot_memory/<app_id>.json 的 app_id 就在手上）。传进来日志里就能
    直接定位到是哪个机器人的数据损坏了，而不是一行 appid=? 的无主记录。
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        if file_path.exists():
            backup_name = file_path.with_name(f"[broken]{file_path.name}")
            if backup_name.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = file_path.with_name(f"[broken]{timestamp}_{file_path.name}")
            try:
                file_path.rename(backup_name)
                warn(f"文件 {file_path} 损坏，已备份为 {backup_name}", ctx=ctx)
            except Exception as be:
                error(f"备份文件失败: {be}", ctx=ctx)
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(default_data, f, ensure_ascii=False, indent=2)
            info(f"已创建新的 {file_path} 使用默认数据", ctx=ctx)
        except Exception as we:
            error(f"创建新文件失败: {we}", ctx=ctx)
        return default_data


# ==================== 大小写不敏感的键查找 ====================
def _ci_find_key(container: dict, key: str) -> Optional[str]:
    """
    在 dict 中按大小写不敏感方式查找键，返回实际存在的键名；找不到返回 None。
    优先精确匹配，其次忽略大小写匹配。
    用于兼容用户手写的不同大小写写法（配置项统一以大写自动生成）。
    """
    if not isinstance(container, dict) or not isinstance(key, str):
        return None
    if key in container:
        return key
    target = key.upper()
    for k in container.keys():
        if isinstance(k, str) and k.upper() == target:
            return k
    return None


# ==================== 自动补全缺省配置项 ====================
def deep_patch_defaults(loaded: dict, defaults: dict, path: str = "") -> bool:
    """
    递归对比 loaded 与 defaults，将 loaded 中缺失的 key 用 defaults 的值补充。
    返回 True 表示有改动（已补全），False 表示无改动。
    规则：
      - 对于 dict 类型的值，递归合并缺失的子 key
      - 对于 list 类型的值（如 bots），不合并到现有 item 内部，但如果列表为空且默认不空，
        将默认模板中缺失的 key 补到每个现有 item
      - 其他类型（str/int/bool），缺失则直接补
      - 永远不覆盖 loaded 中已有的值
    """
    changed = False
    for key, default_val in defaults.items():
        full_key = f"{path}.{key}" if path else key
        # 大小写不敏感查找：配置项统一以大写自动生成，但用户手写其它大小写也应视为已存在，
        # 避免在同一 item 中同时出现 DISABLE_AI_REPLY 与 disable_ai_reply 两个键
        actual_key = key if key in loaded else _ci_find_key(loaded, key)
        if actual_key is None:
            loaded[key] = __import__('copy').deepcopy(default_val)
            info(f"配置补全: 缺少 '{full_key}'，已添加默认值", ctx=None)
            changed = True
        elif isinstance(default_val, dict) and isinstance(loaded[actual_key], dict):
            # 递归合并嵌套 dict
            sub_changed = deep_patch_defaults(loaded[actual_key], default_val, full_key)
            if sub_changed:
                changed = True
        elif isinstance(default_val, list) and isinstance(loaded[actual_key], list):
            # list 类型：确保每个现有 item （dict）也补上默认模板中缺失的 key
            if default_val and isinstance(default_val[0], dict):
                template = default_val[0]
                for idx, item in enumerate(loaded[actual_key]):
                    if isinstance(item, dict):
                        item_path = f"{full_key}[{idx}]"
                        for tk, tv in template.items():
                            existing_tk = tk if tk in item else _ci_find_key(item, tk)
                            if existing_tk is None:
                                item[tk] = __import__('copy').deepcopy(tv)
                                info(f"配置补全: 缺少 '{item_path}.{tk}'，已添加默认值", ctx=None)
                                changed = True
                        # list内补全结束后，再递归处理该item（应对嵌套结构）
                        sub_changed = deep_patch_defaults(item, template, item_path)
                        if sub_changed:
                            changed = True
        elif isinstance(default_val, list) and isinstance(loaded[actual_key], dict):
            # --- 旧版 models 迁移：dict → list[dict] ---
            # loaded[key] 是旧格式 dict（如 {"base_url":..., "api_key":..., "model_name":...}）
            # 将其包装为 list，并用默认模板填充缺失字段
            info(f"配置迁移: '{full_key}' 从旧格式 dict 迁移为 list[dict]（多Key支持）", ctx=None)
            old_dict = loaded[actual_key]
            # 用默认模板补全旧dict中可能缺失的字段
            if default_val and isinstance(default_val[0], dict):
                template = default_val[0]
                for tk, tv in template.items():
                    if _ci_find_key(old_dict, tk) is None:
                        old_dict[tk] = __import__('copy').deepcopy(tv)
            loaded[actual_key] = [old_dict]
            changed = True
    return changed


# ==================== 配置文件加载（实时读取 + 自动补全） ====================
def get_config():
    if not CONFIG_FILE.exists():
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        info("配置文件 config.json 已生成，请编辑后重新运行。", ctx=None)
        sys.exit(0)
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        backup_name = CONFIG_FILE.with_name(f"[broken]{CONFIG_FILE.name}")
        if backup_name.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = CONFIG_FILE.with_name(f"[broken]{timestamp}_{CONFIG_FILE.name}")
        try:
            CONFIG_FILE.rename(backup_name)
            warn(f"配置文件损坏，已备份为 {backup_name}", ctx=None)
        except Exception as be:
            error(f"备份配置文件失败: {be}", ctx=None)
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        info("配置文件 config.json 已重置为默认配置，请编辑后重新运行。", ctx=None)
        sys.exit(0)

    # 自动补全缺失的配置项
    if deep_patch_defaults(config, DEFAULT_CONFIG):
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            info("已自动补全 config.json 中的缺省配置项并写回文件", ctx=None)
        except OSError as we:
            error(f"写回补全后的配置文件失败: {we}", ctx=None)

    return config


def get_bots() -> List[Dict]:
    return get_config().get("bots", [])


def get_bot_isolate_flag(app_id: str) -> bool:
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ISOLATE_GLOBAL_MEMORY", 0) == 1
    return False


def get_bot_system_prompt(app_id: str) -> Optional[str]:
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("SYSTEM_PROMPT")
    return None


def get_bot_enabled(app_id: str) -> bool:
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ENABLED", 1) == 1
    return True


def get_bot_auto_welcome(app_id: str) -> bool:
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("AUTO_WELCOME", 0) == 1
    return False


def get_bot_enable_tools(app_id: str) -> bool:
    """获取机器人是否开启工具调用（function calling）"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ENABLE_TOOLS", 1) == 1
    return True


def get_bot_max_tool_rounds(app_id: str) -> int:
    """获取机器人最大工具调用循环次数"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return int(bot.get("MAX_TOOL_ROUNDS", 6))
    return 6


def get_bot_allow_cross_push(app_id: str) -> bool:
    """获取机器人是否允许跨机器人推送（向其他机器人的用户/群发消息）"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_PUSH", 1) == 1
    return True


def get_bot_allow_cross_push_incoming(app_id: str) -> bool:
    """获取机器人是否允许其他机器人向本机器人的用户/群推送消息"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_PUSH_INCOMING", 1) == 1
    return True


def get_bot_allow_manage_all_push(app_id: str) -> bool:
    """获取机器人是否允许管理所有定时推送（包括其他机器人发起的）"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_MANAGE_ALL_PUSH", 1) == 1
    return True


def get_bot_allow_cross_wakeup(app_id: str) -> bool:
    """获取机器人是否允许跨机器人定时唤醒"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_WAKEUP", 1) == 1
    return True


def get_bot_allow_cross_wakeup_incoming(app_id: str) -> bool:
    """获取机器人是否允许其他机器人为本机器人的用户/群设置定时唤醒"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_WAKEUP_INCOMING", 1) == 1
    return True


def get_bot_allow_cross_get_list(app_id: str) -> bool:
    """获取机器人是否允许获取跨机器人用户和群列表"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_GET_LIST", 1) == 1
    return True


def get_bot_allow_cross_get_list_incoming(app_id: str) -> bool:
    """获取机器人是否允许被跨机器人获取用户和群列表"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_GET_LIST_INCOMING", 1) == 1
    return True


def get_bot_allow_cross_history(app_id: str) -> bool:
    """获取机器人是否允许跨机器人读写聊天记录（query_history/revoke/batch_revoke）"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_HISTORY", 1) == 1
    return True


def get_bot_allow_cross_history_incoming(app_id: str) -> bool:
    """获取机器人是否允许其他机器人读写本机器人的聊天记录"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("ALLOW_CROSS_BOT_HISTORY_INCOMING", 1) == 1
    return True


def get_bot_disable_ai_reply(app_id: str) -> bool:
    """
    获取机器人是否关闭 AI 回复（实时读取 config.json，无需重启）。

    0（默认）= 正常进行 AI 回复与工具调用
    1        = 关闭 AI 回复、工具调用、Judge 判定与媒体识别，仅记录消息、群、用户、
               群员信息，并保留历史摘要整理

    配置项名称为 DISABLE_AI_REPLY（自动补全生成大写），但读取时大小写不敏感，
    因此 disable_ai_reply / Disable_Ai_Reply 等写法同样生效。

    开关可随时修改 config.json 生效；本函数每次调用都会重新读取配置文件。
    """
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            # 大小写不敏感匹配（兼容 disable_ai_reply / Disable_Ai_Reply 等写法）
            actual_key = _ci_find_key(bot, "DISABLE_AI_REPLY")
            if actual_key is None:
                return False
            raw = bot.get(actual_key)
            if raw is None:
                return False
            try:
                return int(raw) == 1
            except (TypeError, ValueError):
                warn(f"机器人 {app_id} 的 DISABLE_AI_REPLY 配置无效（{raw!r}），按 0 处理", ctx=app_id)
                return False
    return False


# ==================== API 服务配置（实时读取） ====================
def _normalize_keys(raw) -> List[str]:
    """把密钥配置归一化为非空字符串列表（兼容字符串/数组写法）"""
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        return [s] if s else []
    if isinstance(raw, (list, tuple)):
        return [str(k).strip() for k in raw if str(k).strip()]
    return []


def get_bot_api_enabled(app_id: str) -> bool:
    """获取机器人自身的 API 服务是否开启（默认开=1，实时读取）"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            key = _ci_find_key(bot, "API_ENABLED")
            if key is None:
                return True
            try:
                return int(bot.get(key)) == 1
            except (TypeError, ValueError):
                warn(f"机器人 {app_id} 的 API_ENABLED 配置无效（{bot.get(key)!r}），按 1 处理", ctx=app_id)
                return True
    return True


def get_bot_api_keys(app_id: str) -> List[str]:
    """获取机器人自身的 API 服务密钥数组（默认空数组=不需要密钥，实时读取）"""
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            key = _ci_find_key(bot, "API_KEYS")
            if key is None:
                return []
            return _normalize_keys(bot.get(key))
    return []


def get_global_api_enabled() -> bool:
    """
    获取全局 API 管理通道开关（默认开=1，实时读取）。
    与各机器人的 API_ENABLED 互不干扰：关闭它只停用全局密钥通道。
    """
    cfg = get_config()
    key = _ci_find_key(cfg, "GLOBAL_API_ENABLED")
    if key is None:
        return True
    try:
        return int(cfg.get(key)) == 1
    except (TypeError, ValueError):
        warn(f"GLOBAL_API_ENABLED 配置无效（{cfg.get(key)!r}），按 1 处理", ctx=None)
        return True


def get_global_api_keys() -> List[str]:
    """
    获取全局 API 密钥数组（默认空数组=不需要密钥且拥有全部权限，实时读取）。
    全局密钥拥有全部权限，可以管理每一个机器人。
    """
    cfg = get_config()
    key = _ci_find_key(cfg, "GLOBAL_API_KEYS")
    if key is None:
        return []
    return _normalize_keys(cfg.get(key))


def get_api_bind_host() -> str:
    """API 服务监听地址"""
    cfg = get_config()
    key = _ci_find_key(cfg, "API_BIND_HOST")
    return str(cfg.get(key) or "0.0.0.0") if key else "0.0.0.0"


def get_api_bind_port() -> int:
    """API 服务监听端口"""
    cfg = get_config()
    key = _ci_find_key(cfg, "API_BIND_PORT")
    if key is None:
        return 8148
    try:
        return int(cfg.get(key))
    except (TypeError, ValueError):
        warn(f"API_BIND_PORT 配置无效（{cfg.get(key)!r}），按 8148 处理", ctx=None)
        return 8148


def get_ai_max_msg_len() -> int:
    return get_config().get("AI_MAX_MSG_LEN", 1500)


def get_context_limit() -> int:
    return get_config().get("CONTEXT_LIMIT", 15)


def get_judge_context_limit() -> int:
    return get_config().get("JUDGE_CONTEXT_LIMIT", 10)


def get_cooldown_seconds() -> int:
    return get_config().get("COOLDOWN_SECONDS", 2)


def get_compress_threshold() -> int:
    return get_config().get("COMPRESS_THRESHOLD", 25)


def get_max_workers() -> int:
    return get_config().get("MAX_WORKERS", 20)


def get_media_block() -> int:
    """媒体识别是否阻塞消息接收（实时读取 config.json，无需重启）。

    1 = 阻塞（旧逻辑）：识别期间接收循环被占用，后续消息排队等待识别结束。
    0 = 不阻塞（默认）：识别不占用接收循环；期间来新消息则本轮用占位、
        识别转后台跑完并写缓存。
    """
    try:
        return int(get_config().get("MEDIA_BLOCK", 0))
    except (TypeError, ValueError):
        return 0


def get_global_system_prompt() -> str:
    return get_config().get("SYSTEM_PROMPT", "you are a helpful ai")


def get_tool_choice() -> int:
    """获取全局工具调用策略：1=每次必须调用工具（required），0=不强制（auto）"""
    return int(get_config().get("TOOL_CHOICE", 1))


# ==================== 日志配置获取 ====================
def get_enable_log() -> int:
    """获取是否启用文件日志：0=禁用（仅控制台），1=启用"""
    return int(get_config().get("enable_log", 0))


def get_log_file() -> str:
    """获取日志文件路径（基于程序目录解析相对路径）。

    ★ 这里必须做目录包含校验，不能只做「相对路径就拼 BASE_DIR」：
      log_file 是**可经 /api/config/set 修改的配置项**，而 /api/logs
      直接 open() 它并返回内容 —— 只看 basename 脱敏是不够的，那只改了
      显示名，没有约束真正的读取目标。传入 "../../config.json" 或
      "x/../../config" 都能逃出项目目录，把任意文件读回来。
      所以解析后要求结果**必须位于 BASE_DIR 之内**，否则回退到默认值并告警。
    """
    raw = str(get_config().get("log_file", "log.txt"))
    p = Path(raw)
    if not p.is_absolute():
        p = BASE_DIR / p

    # 归一化（展开 .. 与 .），再做包含判定。
    # 用 resolve() 而非 absolute()：只有前者会真正消解 ".."。
    try:
        resolved = p.resolve()
        base = BASE_DIR.resolve()
    except OSError:
        return str(BASE_DIR / "log.txt")

    if resolved != base and base not in resolved.parents:
        warn(f"log_file 指向项目目录之外（{raw!r} -> {resolved}），已回退为 log.txt", ctx=None)
        return str(base / "log.txt")
    return str(resolved)


def get_max_log_length() -> int:
    """获取日志文件最大行数。

    返回 0（或负数）表示**不限制文件大小** —— log.py 的滚动 sink 只在
    max_lines > 0 时裁剪，因此 0 会一直追加、永不删旧行。
    """
    return int(get_config().get("max_log_length", 800))


def get_disable_print_in_console() -> int:
    """获取是否关闭控制台输出：0=输出，1=关闭"""
    return int(get_config().get("disable_print_in_console", 0))


def get_log_level() -> str:
    """获取文件日志记录等级"""
    return str(get_config().get("log_level", "DEBUG")).upper()


def get_log_show_msg_id() -> int:
    """获取日志前缀是否显示 msg_id：1=显示，0=隐藏"""
    return int(get_config().get("log_show_msg_id", 1))


def get_log_console_level() -> str:
    """获取控制台日志等级"""
    return str(get_config().get("log_console_level", "DEBUG")).upper()


def get_model_config(model_key: str) -> List[Dict[str, str]]:
    """
    获取指定模型的配置列表（多Key故障切换支持）。

    统一返回 list[dict]，每个元素形如：
        {"base_url": ..., "api_key": ..., "model_name": ...}

    兼容旧格式：若配置中仍是单个 dict，则自动包装为单元素列表。
    """
    config = get_config()
    models = config.get("models", {})
    if model_key not in models:
        raise ValueError(f"配置中未找到模型 '{model_key}' 的配置，请检查 models 字段。")

    cfg = models[model_key]

    # --- 兼容旧版 dict 格式（理论上 get_config 已迁移，此处为兜底） ---
    if isinstance(cfg, dict):
        return [cfg]

    if not isinstance(cfg, list):
        raise ValueError(f"模型 '{model_key}' 的配置格式错误，应为 list[dict]。")

    # 过滤出有效的 dict 项（必须含 base_url / api_key / model_name）
    valid: List[Dict[str, str]] = []
    for idx, item in enumerate(cfg):
        if not isinstance(item, dict):
            warn(f"模型 '{model_key}' 第 {idx} 项不是字典，已跳过。", ctx=None)
            continue
        if not item.get("base_url") or not item.get("api_key") or not item.get("model_name"):
            warn(f"模型 '{model_key}' 第 {idx} 项缺少 base_url/api_key/model_name，已跳过。", ctx=None)
            continue
        valid.append(item)

    if not valid:
        raise ValueError(f"模型 '{model_key}' 没有可用的配置项，请检查 models 字段。")

    return valid


def get_model_configs_count(model_key: str) -> int:
    """获取指定模型配置的 Key 数量（用于日志/状态展示）。"""
    try:
        return len(get_model_config(model_key))
    except Exception:
        return 0


# ==================== 机器人名称（运行时可变） ====================
def get_bot_name() -> str:
    return BOT_NAME


# ==================== 配置文件写入（崩溃安全） ====================
# 重要：绝不能直接 open(path,"w") 就地覆盖。
# 一旦写入过程中进程被中断（或内容非法），config.json 会变成半截 JSON，
# 而 get_config() 遇到解析失败会把文件改名为 [broken] 并重置为默认模板，
# 导致用户全部机器人配置丢失。因此这里采用「临时文件 + 原子替换」。
_CONFIG_WRITE_LOCK = None


def _get_write_lock():
    """懒加载写锁，避免多请求并发写入互相覆盖"""
    global _CONFIG_WRITE_LOCK
    if _CONFIG_WRITE_LOCK is None:
        import threading
        _CONFIG_WRITE_LOCK = threading.Lock()
    return _CONFIG_WRITE_LOCK


def write_config(config: Dict, keep_backup: bool = True) -> bool:
    """
    原子化写入 config.json。

    步骤：
      1) 先序列化为字符串并校验可被 json.loads 解析（防止写入非法内容）
      2) 写入同目录临时文件并 fsync 落盘
      3) 备份现有文件为 config.json.bak
      4) os.replace 原子替换目标文件

    任何一步失败都不会破坏现有 config.json，返回 False。
    """
    with _get_write_lock():
        try:
            text = json.dumps(config, ensure_ascii=False, indent=2)
            # 自校验：确保写出的内容一定能被解析回来
            json.loads(text)
        except (TypeError, ValueError) as e:
            error(f"[配置写入] 序列化失败，已放弃写入: {e}", ctx=None)
            return False

        tmp_path = CONFIG_FILE.with_name(CONFIG_FILE.name + ".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                import os as _os
                _os.fsync(f.fileno())
        except OSError as e:
            error(f"[配置写入] 临时文件写入失败: {e}", ctx=None)
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

        # 备份现有配置（便于人工回滚）
        if keep_backup and CONFIG_FILE.exists():
            try:
                import shutil
                shutil.copy2(CONFIG_FILE, CONFIG_FILE.with_name(CONFIG_FILE.name + ".bak"))
            except OSError as e:
                warn(f"[配置写入] 备份失败（不影响写入）: {e}", ctx=None)

        try:
            import os as _os
            _os.replace(tmp_path, CONFIG_FILE)   # 原子替换
            return True
        except OSError as e:
            error(f"[配置写入] 原子替换失败: {e}", ctx=None)
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False


# 绝不允许通过 API 读取明文或改写的字段（机器人凭证）
SENSITIVE_KEYS = {"APP_SECRET", "APP_TOKEN", "SECRET", "TOKEN", "PASSWORD"}

# API 密钥类字段：读取时脱敏，但允许写入（否则无法通过 API 自助设置密钥）
API_KEY_FIELDS = {"API_KEYS", "GLOBAL_API_KEYS", "KEYS"}


def is_sensitive_key(key: str) -> bool:
    """判断某配置键是否为敏感字段（凭证），用于读时脱敏、写时禁止"""
    k = str(key).strip().upper()
    if k in SENSITIVE_KEYS:
        return True
    # 模型配置里的 API Key 也属于凭证，必须一并脱敏，
    # 否则 /api/config/get 会把 models 的明文 key 整段吐出去
    return "API_KEY" in k or k.endswith("_KEY") or k == "KEY"


def is_api_key_field(key: str) -> bool:
    """判断是否为 API 密钥数组字段：读取脱敏，但允许写入"""
    return str(key).strip().upper() in API_KEY_FIELDS


def set_bot_name(name: str):
    global BOT_NAME
    BOT_NAME = name


# ==================== 初始化：机器人列表 & 线程池 ====================
BOTS = get_bots()
if not BOTS:
    error("配置文件中没有机器人信息，请添加 'bots' 数组。", ctx=None)
    sys.exit(1)

MAX_WORKERS = get_max_workers()
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def get_executor() -> ThreadPoolExecutor:
    return _executor


# ==================== 数据目录 ====================
MEMORY_FILE = BASE_DIR / "memory.json"
USER_MAP_FILE = BASE_DIR / "user_map.json"
HISTORY_DIR = BASE_DIR / "history"
MEDIA_CACHE_DIR = BASE_DIR / "media_cache"
QUN_MEMORY_DIR = BASE_DIR / "qun_memory"
C2C_MEMORY_DIR = BASE_DIR / "c2c_memory"
BOT_MEMORY_DIR = BASE_DIR / "bot_memory"

HISTORY_DIR.mkdir(exist_ok=True)
MEDIA_CACHE_DIR.mkdir(exist_ok=True)
QUN_MEMORY_DIR.mkdir(exist_ok=True)
C2C_MEMORY_DIR.mkdir(exist_ok=True)
BOT_MEMORY_DIR.mkdir(exist_ok=True)


# ==================== 旧数据迁移 ====================
def migrate_old_data():
    old_file = BASE_DIR / "threads.json"
    if old_file.exists() and not (MEMORY_FILE.exists() and any(HISTORY_DIR.glob("*.json"))):
        old = None
        try:
            with open(old_file, "r", encoding="utf-8") as f:
                old = json.load(f)
        except Exception as e:
            warn(f"旧文件 {old_file} 损坏，无法迁移，跳过。", ctx=None)
            backup_name = old_file.with_name(f"[broken]{old_file.name}")
            if backup_name.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = old_file.with_name(f"[broken]{timestamp}_{old_file.name}")
            try:
                old_file.rename(backup_name)
                warn(f"已备份损坏文件为 {backup_name}", ctx=None)
            except Exception as be:
                error(f"备份失败: {be}", ctx=None)
        if old is not None:
            info("检测到旧的 threads.json，正在迁移...", ctx=None)
            memory_data = {"global_memory": old.get("global_memory", []), "enabled": 1}
            with open(MEMORY_FILE, "w", encoding="utf-8") as f:
                json.dump(memory_data, f, ensure_ascii=False, indent=2)
            if "user_mapping" in old and old["user_mapping"]:
                with open(MIRROR_FILE, "w", encoding="utf-8") as f:
                    json.dump(old["user_mapping"], f, ensure_ascii=False, indent=2)
            for key, val in old.items():
                if key in ("global_memory", "user_mapping"):
                    continue
                if isinstance(val, dict) and "history" in val:
                    hist_file = HISTORY_DIR / f"{key}.json"
                    with open(hist_file, "w", encoding="utf-8") as f:
                        json.dump(val["history"], f, ensure_ascii=False, indent=2)
            backup = old_file.with_suffix(".json.bak")
            old_file.rename(backup)
            info(f"迁移完成，旧文件备份为 {backup}", ctx=None)
    else:
        if MEMORY_FILE.exists():
            try:
                with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                warn(f"记忆文件 {MEMORY_FILE} 损坏，跳过迁移处理。", ctx=None)
                data = None
            # 旧版 memory.json 中的 user_mapping 已废弃，由 memory.mirror_migrate_old() 处理
            if data is not None and "user_mapping" in data:
                del data["user_mapping"]
                with open(MEMORY_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                info("已清理 memory.json 中的旧 user_mapping 字段", ctx=None)


migrate_old_data()

# ==================== 初始化日志系统 ====================
setup_logger()
