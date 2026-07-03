# -*- coding: utf-8 -*-
"""
Module: config.init
Description: KHAOS 系统配置初始化模块。
             负责加载并验证所有 YAML 配置文件，注入环境变量，
             校验完整性，并提供全局唯一的配置访问入口。
             符合全球顶尖量化对冲基金对配置管理的安全与合规要求。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.6.0
"""

import copy
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# 自定义异常
# ---------------------------------------------------------------------------
class ConfigError(Exception):
    """配置相关的致命错误。"""

# ---------------------------------------------------------------------------
# 基础环境变量要求（可扩展）
# ---------------------------------------------------------------------------
_REQUIRED_ENV_VARS_BASE = ["KHAOS_MODE"]
def _get_required_env_vars() -> List[str]:
    extra = os.environ.get("EXTRA_REQUIRED_ENV_VARS", "")
    extra_vars = [v.strip() for v in extra.split(",") if v.strip()]
    return _REQUIRED_ENV_VARS_BASE + extra_vars

# ---------------------------------------------------------------------------
# 配置文件路径常量
# ---------------------------------------------------------------------------
DEFAULT_STRATEGY_CONFIG = "config/strategy.default.yaml"
DEFAULT_EVOLUTION_CONFIG = "config/evolution.default.yaml"
DEFAULT_LOGGING_CONFIG = "config/logging.yaml"

ENV_STRATEGY_PATH = "KHAOS_STRATEGY_CONFIG_PATH"
ENV_EVOLUTION_PATH = "KHAOS_EVOLUTION_CONFIG_PATH"
ENV_LOGGING_PATH   = "KHAOS_LOGGING_CONFIG_PATH"

ENV_PERMISSIVE_STRICT = "KHAOS_CONFIG_PERMISSION_STRICT"  # 严格权限检查开关

logger = logging.getLogger("khaos.config.init")

# ---- 线程安全延迟导入 ----
_settings = None
_settings_lock = threading.Lock()

def _get_settings_module():
    global _settings
    if _settings is None:
        with _settings_lock:
            if _settings is None:
                from config import settings as _mod
                _settings = _mod
    return _settings

# ---------------------------------------------------------------------------
# 环境变量与路径辅助
# ---------------------------------------------------------------------------
def _get_env_non_empty(var: str) -> Optional[str]:
    """获取环境变量，忽略空字符串。"""
    val = os.environ.get(var)
    return val if val else None

def _check_required_env() -> None:
    missing = []
    for var in _get_required_env_vars():
        if var not in os.environ or not os.environ[var]:
            missing.append(var)
    if missing:
        msg = f"Missing or empty required environment variables: {missing}"
        logger.critical(msg)
        raise ConfigError(msg)

def _validate_config_file(path: Path, max_size_mb: int = 5) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    size_bytes = path.stat().st_size
    if size_bytes > max_size_mb * 1024 * 1024:
        raise ValueError(f"Config file {path} is too large ({size_bytes} bytes). Max allowed is {max_size_mb}MB")
    # 权限检查
    mode = path.stat().st_mode
    if mode & 0o077:  # 其他用户有任何权限
        strict = os.environ.get(ENV_PERMISSIVE_STRICT, "true").lower() == "true"
        msg = f"Config file {path} has permissive permissions ({oct(mode)}). Expected 0600."
        if strict:
            raise PermissionError(msg)
        else:
            logger.warning(msg)

# ---- 路径解析（忽略空环境变量） ----
def _resolve_strategy_path(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    env = _get_env_non_empty(ENV_STRATEGY_PATH)
    if env:
        return Path(env).expanduser().resolve()
    return Path(DEFAULT_STRATEGY_CONFIG).expanduser().resolve()

def _resolve_evolution_path(explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    env = _get_env_non_empty(ENV_EVOLUTION_PATH)
    if env:
        return Path(env).expanduser().resolve()
    return Path(DEFAULT_EVOLUTION_CONFIG).expanduser().resolve()

def _resolve_logging_path(explicit: Optional[str] = None) -> Optional[Path]:
    if explicit:
        path = Path(explicit).expanduser().resolve()
    else:
        env = _get_env_non_empty(ENV_LOGGING_PATH)
        if env:
            path = Path(env).expanduser().resolve()
        else:
            path = Path(DEFAULT_LOGGING_CONFIG).expanduser().resolve()
    if not path.exists():
        logger.warning(f"Logging config file not found: {path}. Falling back to basic console logging.")
        return None
    return path

# ---------------------------------------------------------------------------
# 环境变量替换（支持深度嵌套与类型转换）
# ---------------------------------------------------------------------------
def _substitute_env_in_dict(d: Dict[str, Any], max_depth: int = 20) -> Dict[str, Any]:
    import re
    pattern = re.compile(r'\$\{([^}^{]+)\}')

    def replace_value(value: Any, depth: int = 0) -> Any:
        if depth > max_depth:
            raise RecursionError("Maximum recursion depth exceeded in environment variable substitution")
        if isinstance(value, str):
            matches = pattern.findall(value)
            if matches:
                if pattern.fullmatch(value) and len(matches) == 1:
                    var = matches[0]
                    if var not in os.environ or os.environ[var] == "":
                        raise KeyError(f"Environment variable '{var}' required but not set or empty")
                    raw = os.environ[var]
                    try:
                        import ast
                        return ast.literal_eval(raw)
                    except (ValueError, SyntaxError):
                        return raw
                else:
                    def replacer(m):
                        var = m.group(1)
                        if var not in os.environ or os.environ[var] == "":
                            raise KeyError(f"Environment variable '{var}' required but not set")
                        return os.environ[var]
                    return pattern.sub(replacer, value)
        elif isinstance(value, dict):
            return {k: replace_value(v, depth + 1) for k, v in value.items()}
        elif isinstance(value, list):
            return [replace_value(item, depth + 1) for item in value]
        return value

    return replace_value(d)

# ---------------------------------------------------------------------------
# 日志配置加载
# ---------------------------------------------------------------------------
def _load_logging_config(path: Optional[Path]) -> None:
    if path is None:
        _reset_logging()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to read logging config {path}: {e}")
        _reset_logging()
        logging.basicConfig(level=logging.INFO)
        return

    try:
        # 环境变量替换
        config_dict = _substitute_env_in_dict(config_dict)
        # 依赖检查（操作副本）
        _check_logging_dependencies(config_dict)
        _reset_logging()
        import logging.config
        logging.config.dictConfig(config_dict)
    except Exception as e:
        logger.error(f"Failed to apply logging configuration: {e}. Falling back to basic console logging.")
        _reset_logging()
        logging.basicConfig(level=logging.INFO)

def _reset_logging():
    """重置日志系统，确保 basicConfig 可重新生效。"""
    root = logging.root
    root.handlers = []
    root.filters = []
    # 移除所有已有的 logger 设置（保留名称，但清除 handlers）
    for logger_name in logging.root.manager.loggerDict:
        logger_obj = logging.getLogger(logger_name)
        logger_obj.handlers = []
        logger_obj.level = logging.NOTSET

def _check_logging_dependencies(config_dict: Dict[str, Any]) -> None:
    """检查日志配置依赖，如果缺失则降级（操作副本，不修改原始配置）。"""
    # 深拷贝防止修改原始
    formatters = copy.deepcopy(config_dict.get("formatters", {}))
    for fmt_name, fmt_cfg in formatters.items():
        cls_path = fmt_cfg.get("class")
        if cls_path and isinstance(cls_path, str):
            # 处理可能的 () 调用
            if cls_path.endswith("()"):
                cls_path = cls_path[:-2]
            parts = cls_path.rsplit(".", 1)
            if len(parts) == 2:
                module_name, _ = parts
                try:
                    __import__(module_name)
                except ImportError:
                    logger.warning(f"Formatter dependency {cls_path} not found. Replacing with basic formatter.")
                    fmt_cfg["class"] = "logging.Formatter"
                    fmt_cfg["format"] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                    # 写回原配置（因为我们要应用修改）
                    config_dict["formatters"][fmt_name] = fmt_cfg

# ---------------------------------------------------------------------------
# 进化配置验证
# ---------------------------------------------------------------------------
def _validate_evolution_config(path: Path) -> bool:
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        if not isinstance(cfg, dict) or "evolution" not in cfg:
            raise ValueError("Missing 'evolution' section")
        evol = cfg["evolution"]
        # 检查关键子字段（若存在）
        if "enabled" not in evol:
            logger.warning("Evolution config missing 'enabled' field, assuming True.")
        return True
    except Exception as e:
        logger.warning(f"Evolution config at {path} is invalid: {e}")
        return False

# ---------------------------------------------------------------------------
# 全局配置代理更新
# ---------------------------------------------------------------------------
def _update_proxy_instance(instance_name: str) -> None:
    settings = _get_settings_module()
    # 使用公共方法切换代理指向
    if hasattr(settings, 'switch_instance'):
        settings.switch_instance(instance_name)
    else:
        # 向后兼容：直接修改（此路径仅用于尚未升级的 settings）
        settings.config._instance_name = instance_name
        logger.warning("Using direct proxy update; please update config.settings to expose switch_instance().")

# ---------------------------------------------------------------------------
# 初始化入口
# ---------------------------------------------------------------------------
def init_config(
    strategy_path: Optional[str] = None,
    evolution_path: Optional[str] = None,
    logging_path: Optional[str] = None,
    instance_name: Optional[str] = None,
) -> Tuple[Any, Optional[Dict[str, Any]]]:
    """
    初始化整个 KHAOS 配置环境。

    Args:
        strategy_path: 策略配置文件路径，优先于环境变量。
        evolution_path: 进化配置文件路径。
        logging_path: 日志配置文件路径。
        instance_name: 配置实例名称，默认从 KHAOS_INSTANCE_ID 获取或 'main'。

    Returns:
        (策略配置对象, 进化配置字典或None)
    """
    # 1. 预检环境变量
    _check_required_env()

    # 2. 确定实例名称
    if instance_name is None:
        instance_name = os.environ.get("KHAOS_INSTANCE_ID", "").strip()
        if not instance_name:
            instance_name = "main"
            logger.info("No KHAOS_INSTANCE_ID set, using 'main' as default instance name.")

    # 3. 加载日志配置（尽早）
    log_path = _resolve_logging_path(logging_path)
    _load_logging_config(log_path)

    logger.info("Initializing KHAOS configuration...")
    logger.info(f"Strategy instance: {instance_name}")

    # 4. 加载策略配置
    strat_file = _resolve_strategy_path(strategy_path)
    _validate_config_file(strat_file)
    logger.info(f"Using strategy config: {strat_file}")

    raw_strat = yaml.safe_load(strat_file.read_text(encoding="utf-8"))
    if not isinstance(raw_strat, dict) or "strategy" not in raw_strat:
        raise ConfigError(f"Strategy config file {strat_file} does not contain a 'strategy' top-level key.")
    # 提取 strategy 部分并替换环境变量（用于验证）
    strategy_raw = raw_strat["strategy"]
    # 提前替换环境变量以便 pydantic 验证
    strategy_raw_substituted = _substitute_env_in_dict(strategy_raw)
    settings = _get_settings_module()
    # dry-run 验证
    try:
        settings.validate_config(strategy_raw_substituted, substitute_env=False)  # 已替换
    except Exception as e:
        logger.critical(f"Strategy configuration validation failed: {e}")
        raise ConfigError(f"Invalid strategy configuration in {strat_file}: {e}")

    # 5. 进化配置加载（可选）
    evol_file = _resolve_evolution_path(evolution_path)
    evolution_config = None
    if evol_file.exists():
        if _validate_evolution_config(evol_file):
            with open(evol_file, "r", encoding="utf-8") as f:
                raw_evol = yaml.safe_load(f)
            # 替换环境变量
            try:
                evolution_config = _substitute_env_in_dict(raw_evol)
            except KeyError as e:
                logger.error(f"Evolution config contains unresolved environment variable: {e}. Evolution disabled.")
                evolution_config = None
            if evolution_config:
                logger.info(f"Evolution config loaded and substituted from {evol_file}")
        else:
            logger.warning("Evolution config invalid, evolution features disabled.")
    else:
        logger.info(f"No evolution config found at {evol_file}, evolution features disabled.")

    # 6. 正式初始化策略配置实例
    try:
        manager = settings.get_manager() if hasattr(settings, 'get_manager') else settings._manager
        config_obj = manager.initialize(instance_name, str(strat_file))
    except Exception as e:
        logger.critical(f"Failed to initialize strategy config instance: {e}")
        raise ConfigError(f"Configuration initialization failed: {e}")

    # 7. 代理更新
    _update_proxy_instance(instance_name)

    logger.info(f"Configuration initialization complete. Mode: {config_obj.mode}, Version: {config_obj.version}")
    # 输出完整性信息
    prov = settings.get_provenance(instance_name)
    if prov.get("hash") and prov["hash"] != "none":
        logger.info(f"Config integrity verified (hash: {prov['hash'][:8]}...)")
    return config_obj, evolution_config


# ---------------------------------------------------------------------------
# 公开 API（从 settings 重新导出，方便外部引用）
# ---------------------------------------------------------------------------
from config.settings import (
    get_config,
    reload_config,
    register_config_observer,
    update_config_value,
    export_config,
    validate_config,
    get_provenance,
    StrategyConfig,
    config,
)

__all__ = [
    "init_config",
    "ConfigError",
    "get_config",
    "reload_config",
    "register_config_observer",
    "update_config_value",
    "export_config",
    "validate_config",
    "get_provenance",
    "StrategyConfig",
    "config",
    "DEFAULT_STRATEGY_CONFIG",
    "DEFAULT_EVOLUTION_CONFIG",
    "DEFAULT_LOGGING_CONFIG",
]

__version__ = "2.6.0"
