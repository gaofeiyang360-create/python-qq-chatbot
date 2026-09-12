# -*- coding: utf-8 -*-
# config.py — 配置文件实时读取写入
import sys
import json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, List

from log import info, warn, error, setup_logger

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
    "max_log_length": 800,           # 日志文件最大行数，达到后自动轮转
    "disable_print_in_console": 0,   # 0=控制台输出，1=关闭控制台显示（文件日志仍可启用）
    "log_level": "DEBUG",            # 文件日志记录等级：DEBUG/INFO/WARNING/ERROR/CRITICAL

    "bots": [
        {
            "APP_ID": "YOUR_APP_ID",                    # 替换为实际的机器人 App ID
            "APP_SECRET": "YOUR_APP_SECRET",            # 替换为实际的机器人 App Secret
            "SYSTEM_PROMPT": "你是一个幽默风趣的AI助手",  # 可根据需要自定义
            "ISOLATE_GLOBAL_MEMORY": 0,                 # 0 表示共享全局记忆，1 表示隔离
            "ENABLED": 1,                               # 1 启用，0 禁用
            "AUTO_WELCOME": 1,                          # 1 自动发送欢迎语
            "GROUP_MANAGE_WHITELIST": [],               # 群管理白名单（群ID列表）
            "ENABLE_TOOLS": 1,                          # 是否启用工具调用
            "MAX_TOOL_ROUNDS": 6                        # 工具调用最大轮数
        }
    ],
    "AI_MAX_MSG_LEN": 1500,          # 单条消息最大长度（字符数）
    "CONTEXT_LIMIT": 15,             # 上下文保留对话轮数
    "JUDGE_CONTEXT_LIMIT": 10,       # 判断模型使用的上下文轮数
    "COOLDOWN_SECONDS": 2,           # 用户请求冷却时间（秒）
    "COMPRESS_THRESHOLD": 25,        # 触发上下文压缩的阈值（轮数）
    "MAX_WORKERS": 20,               # 并发工作线程数
    "SYSTEM_PROMPT": "you are a helpful ai",  # 全局系统提示词（可作为后备）

    "models": {
        "judge": {                   # 判断模型（用于意图识别/路由）
            "base_url": "YOUR_JUDGE_BASE_URL",      # 替换为实际的 API 地址
            "api_key": "YOUR_JUDGE_API_KEY",        # 替换为实际的 API Key
            "model_name": "your-judge-model"        # 替换为实际模型名称
        },
        "main": {                    # 主对话模型
            "base_url": "YOUR_MAIN_BASE_URL",
            "api_key": "YOUR_MAIN_API_KEY",
            "model_name": "your-main-model"
        },
        "vision": {                  # 视觉多模态模型
            "base_url": "YOUR_VISION_BASE_URL",
            "api_key": "YOUR_VISION_API_KEY",
            "model_name": "your-vision-model"
        }
    }
}

# 机器人名称（运行时可被 WebSocket Ready 事件更新）
BOT_NAME = "灵泽集AI"


# ==================== 安全 JSON 读取（自动修复损坏文件） ====================
def safe_load_json(file_path: Path, default_data):
    """
    尝试读取 JSON 文件，若文件不存在或格式损坏，则备份损坏文件并创建新文件写入 default_data，
    然后返回 default_data。
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
                warn(f"文件 {file_path} 损坏，已备份为 {backup_name}")
            except Exception as be:
                error(f"备份文件失败: {be}")
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(default_data, f, ensure_ascii=False, indent=2)
            info(f"已创建新的 {file_path} 使用默认数据")
        except Exception as we:
            error(f"创建新文件失败: {we}")
        return default_data


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
        if key not in loaded:
            loaded[key] = __import__('copy').deepcopy(default_val)
            info(f"配置补全: 缺少 '{full_key}'，已添加默认值")
            changed = True
        elif isinstance(default_val, dict) and isinstance(loaded[key], dict):
            # 递归合并嵌套 dict
            sub_changed = deep_patch_defaults(loaded[key], default_val, full_key)
            if sub_changed:
                changed = True
        elif isinstance(default_val, list) and isinstance(loaded[key], list):
            # list 类型：确保每个现有 item （dict）也补上默认模板中缺失的 key
            if default_val and isinstance(default_val[0], dict):
                template = default_val[0]
                for idx, item in enumerate(loaded[key]):
                    if isinstance(item, dict):
                        item_path = f"{full_key}[{idx}]"
                        for tk, tv in template.items():
                            if tk not in item:
                                item[tk] = __import__('copy').deepcopy(tv)
                                info(f"配置补全: 缺少 '{item_path}.{tk}'，已添加默认值")
                                changed = True
    return changed


# ==================== 配置文件加载（实时读取 + 自动补全） ====================
def get_config():
    if not CONFIG_FILE.exists():
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        info("配置文件 config.json 已生成，请编辑后重新运行。")
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
            warn(f"配置文件损坏，已备份为 {backup_name}")
        except Exception as be:
            error(f"备份配置文件失败: {be}")
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        info("配置文件 config.json 已重置为默认配置，请编辑后重新运行。")
        sys.exit(0)

    # 自动补全缺失的配置项
    if deep_patch_defaults(config, DEFAULT_CONFIG):
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            info("已自动补全 config.json 中的缺省配置项并写回文件")
        except OSError as we:
            error(f"写回补全后的配置文件失败: {we}")

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


def get_bot_group_manage_whitelist(app_id: str) -> List[str]:
    for bot in get_bots():
        if bot.get("APP_ID") == app_id:
            return bot.get("GROUP_MANAGE_WHITELIST", [])
    return []


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


def is_group_manage_enabled(app_id: str, group_id: str) -> bool:
    return group_id in get_bot_group_manage_whitelist(app_id)


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


def get_global_system_prompt() -> str:
    return get_config().get("SYSTEM_PROMPT", "you are a helpful ai")


# ==================== 日志配置获取 ====================
def get_enable_log() -> int:
    """获取是否启用文件日志：0=禁用（仅控制台），1=启用"""
    return int(get_config().get("enable_log", 0))


def get_log_file() -> str:
    """获取日志文件路径"""
    return str(get_config().get("log_file", "log.txt"))


def get_max_log_length() -> int:
    """获取日志文件最大行数"""
    return int(get_config().get("max_log_length", 800))


def get_disable_print_in_console() -> int:
    """获取是否关闭控制台输出：0=输出，1=关闭"""
    return int(get_config().get("disable_print_in_console", 0))


def get_log_level() -> str:
    """获取文件日志记录等级"""
    return str(get_config().get("log_level", "DEBUG")).upper()


def get_model_config(model_key: str) -> Dict[str, str]:
    config = get_config()
    models = config.get("models", {})
    if model_key not in models:
        raise ValueError(f"配置中未找到模型 '{model_key}' 的配置，请检查 models 字段。")
    return models[model_key]


# ==================== 机器人名称（运行时可变） ====================
def get_bot_name() -> str:
    return BOT_NAME


def set_bot_name(name: str):
    global BOT_NAME
    BOT_NAME = name


# ==================== 初始化：机器人列表 & 线程池 ====================
BOTS = get_bots()
if not BOTS:
    error("配置文件中没有机器人信息，请添加 'bots' 数组。")
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
            warn(f"旧文件 {old_file} 损坏，无法迁移，跳过。")
            backup_name = old_file.with_name(f"[broken]{old_file.name}")
            if backup_name.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_name = old_file.with_name(f"[broken]{timestamp}_{old_file.name}")
            try:
                old_file.rename(backup_name)
                warn(f"已备份损坏文件为 {backup_name}")
            except Exception as be:
                error(f"备份失败: {be}")
        if old is not None:
            info("检测到旧的 threads.json，正在迁移...")
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
            info(f"迁移完成，旧文件备份为 {backup}")
    else:
        if MEMORY_FILE.exists():
            try:
                with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                warn(f"记忆文件 {MEMORY_FILE} 损坏，跳过迁移处理。")
                data = None
            # 旧版 memory.json 中的 user_mapping 已废弃，由 memory.mirror_migrate_old() 处理
            if data is not None and "user_mapping" in data:
                del data["user_mapping"]
                with open(MEMORY_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                info("已清理 memory.json 中的旧 user_mapping 字段")


migrate_old_data()

# ==================== 初始化日志系统 ====================
setup_logger()
