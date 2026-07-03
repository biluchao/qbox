# -*- coding: utf-8 -*-
"""
Module: config.settings.py
Description: 全局配置加载、验证、热更新、审计与血缘跟踪，
             符合全球顶级对冲基金（万亿美金账户）的生产级安全与合规标准。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.3.0
"""

import ast
import atexit
import hashlib
import hmac
import logging
import os
import re
import socket
import sys
import time
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import yaml
from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings
from packaging import version as pkg_version

logger = logging.getLogger("khaos.config")

# ---------------------------------------------------------------------------
# 依赖版本检查
# ---------------------------------------------------------------------------
_MIN_DEPS = {
    "pydantic": "2.0.0",
    "PyYAML": "6.0",
    "packaging": "21.0",
}
try:
    import pydantic as _pydantic
    _pydantic_ver = _pydantic.__version__
except ImportError:
    _pydantic_ver = None
try:
    import yaml as _yaml_mod
    _yaml_ver = getattr(_yaml_mod, "__version__", "unknown")
except ImportError:
    _yaml_ver = None
try:
    import packaging
    _packaging_ver = packaging.__version__
except ImportError:
    _packaging_ver = None

_versions = {
    "pydantic": _pydantic_ver,
    "PyYAML": _yaml_ver,
    "packaging": _packaging_ver,
}
for lib, min_ver in _MIN_DEPS.items():
    ver = _versions.get(lib)
    if ver is None:
        logger.critical(f"{lib} is not installed. Please install it.")
        sys.exit(1)
    if pkg_version.parse(ver) < pkg_version.parse(min_ver):
        logger.warning(f"{lib} version {ver} is below recommended {min_ver}")

# ---------------------------------------------------------------------------
# 环境变量机密设置
# ---------------------------------------------------------------------------
class EnvSecrets(BaseSettings):
    KHAOS_MODE: str = Field(..., description="系统模式: paper/live/hybrid")
    BINANCE_API_KEY: Optional[str] = None
    BINANCE_SECRET: Optional[str] = None
    PAGERDUTY_API_KEY: Optional[str] = None
    ALERT_WEBHOOK_URL: Optional[str] = None
    CONFIG_PATH: Optional[str] = None
    CONFIG_BACKUP_PATH: Optional[str] = None
    CONFIG_SIGNING_KEY: Optional[str] = None
    CONFIG_ENCRYPTION_KEY: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "forbid"


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------
def mask_secret(value: str, show_chars: int = 4) -> str:
    if len(value) <= show_chars:
        return "*" * len(value)
    return value[:show_chars] + "*" * (len(value) - show_chars)


def _safe_str(obj: Any) -> str:
    """将对象转换为字符串，对 SecretStr 等敏感类型进行脱敏。"""
    if isinstance(obj, SecretStr):
        return "***"
    return str(obj)


def _resolve_config_path(config_path: Optional[str] = None) -> Path:
    if config_path:
        path = Path(config_path)
    else:
        env_path = os.environ.get("CONFIG_PATH")
        if env_path and env_path.strip():  # 非空
            path = Path(env_path.strip())
        else:
            backup = os.environ.get("CONFIG_BACKUP_PATH")
            if backup and backup.strip():
                path = Path(backup.strip())
                logger.warning(f"Using backup config path: {path}")
            else:
                raise FileNotFoundError(
                    "No configuration path provided. Set CONFIG_PATH or CONFIG_BACKUP_PATH environment variable."
                )
    return path.expanduser().resolve()


def _check_file_size(path: Path, max_size_mb: int = 5) -> None:
    size_bytes = path.stat().st_size
    if size_bytes > max_size_mb * 1024 * 1024:
        raise ValueError(f"Config file {path} is too large ({size_bytes} bytes). Max allowed is {max_size_mb}MB.")


def _check_file_permissions(path: Path) -> None:
    mode = path.stat().st_mode
    if mode & 0o077:
        logger.warning(
            f"Config file {path} has too permissive permissions ({oct(mode)}). Expected 0600."
        )


def _safe_literal_eval(val: str) -> Any:
    """尝试安全地将环境变量值转换为 Python 原生类型，失败则返回原字符串。"""
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return val


def _substitute_env_vars(value: Any, depth: int = 0, max_depth: int = 20) -> Any:
    """递归替换字符串中的 ${VAR} 为环境变量值，若变量未设置则抛出 KeyError。
       对于纯占位符字符串，尝试转换为原生类型。"""
    if depth > max_depth:
        raise RecursionError("Maximum recursion depth exceeded in environment variable substitution")
    if isinstance(value, str):
        pattern = re.compile(r'\$\{([^}^{]+)\}')
        matches = pattern.findall(value)
        if matches:
            if pattern.fullmatch(value) and len(matches) == 1:
                env_var = matches[0]
                if env_var not in os.environ:
                    raise KeyError(f"Environment variable '{env_var}' required but not set")
                raw = os.environ[env_var]
                return _safe_literal_eval(raw)
            else:
                def replace_match(match):
                    var = match.group(1)
                    if var not in os.environ:
                        raise KeyError(f"Environment variable '{var}' required but not set")
                    return os.environ[var]
                return pattern.sub(replace_match, value)
    elif isinstance(value, dict):
        return {k: _substitute_env_vars(v, depth + 1, max_depth) for k, v in value.items()}
    elif isinstance(value, list):
        return [_substitute_env_vars(item, depth + 1, max_depth) for item in value]
    return value


def _remove_integrity_block(yaml_str: str) -> str:
    """从 YAML 字符串中移除顶级键 `_integrity` 及其所有子内容，基于缩进。"""
    lines = yaml_str.splitlines(keepends=True)
    result_lines = []
    skip = False
    base_indent = None
    for line in lines:
        if not line.strip() or line.strip().startswith("#"):
            if skip:
                continue
            result_lines.append(line)
            continue
        stripped = line.lstrip()
        current_indent = len(line) - len(stripped)
        if not skip:
            if stripped.startswith("_integrity:"):
                skip = True
                base_indent = current_indent
                continue
            result_lines.append(line)
        else:
            # 如果当前行缩进大于 base_indent，则仍在 _integrity 块内
            if current_indent > base_indent:
                continue
            else:
                # 遇到同级或更高级的键，结束跳过
                skip = False
                base_indent = None
                # 检查当前行是否又是 _integrity （极少数情况）
                if stripped.startswith("_integrity:"):
                    skip = True
                    base_indent = current_indent
                    continue
                result_lines.append(line)
    return "".join(result_lines)


def _get_nested(obj: Any, path: List[str]) -> Any:
    """递归访问嵌套对象/字典路径，兼容 BaseModel 与 dict 混合结构。"""
    current = obj
    for p in path:
        if isinstance(current, BaseModel):
            current = getattr(current, p, None)
        elif isinstance(current, dict):
            current = current.get(p)
        else:
            return None
        if current is None:
            return None
    return current


def _check_path_exists(model_cls: type, path: List[str]) -> bool:
    """递归检查字段路径在 pydantic 模型中是否存在。"""
    if not path:
        return True
    field = path[0]
    if field not in model_cls.model_fields:
        return False
    field_info = model_cls.model_fields[field]
    if len(path) == 1:
        return True
    # 如果字段是 BaseModel 子类，继续递归
    annotation = field_info.annotation
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _check_path_exists(annotation, path[1:])
    # 对于 dict 类型，我们无法进一步校验子键，直接返回 True
    return True


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """递归合并两个字典，override 中的值覆盖 base。"""
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


# ---------------------------------------------------------------------------
# Pydantic 模型（不可变）
# ---------------------------------------------------------------------------
class UniverseSymbolOverride(BaseModel):
    position_concentration_max: Optional[float] = None
    max_leverage: Optional[float] = None
    model_config = {"extra": "allow", "frozen": True}


class UniverseSymbol(BaseModel):
    symbol: str
    risk_overrides: Optional[UniverseSymbolOverride] = None
    model_config = {"extra": "allow", "frozen": True}


class Universe(BaseModel):
    symbols: List[UniverseSymbol]
    timeframe: str = "3m"
    quote_asset: str = "USDT"
    min_notional: float = 10.0
    model_config = {"extra": "allow", "frozen": True}


class RiskLimits(BaseModel):
    max_daily_loss_pct: float = Field(default=0.05, ge=0.0, le=0.5)
    max_consecutive_losses: int = Field(default=5, ge=1, le=100)
    max_drawdown_from_peak_pct: float = Field(default=0.15, ge=0.0, le=0.5)
    position_concentration_max: float = Field(default=0.25, ge=0.0, le=1.0)
    net_exposure_max_pct: float = Field(default=3.0, ge=0.0, le=10.0)
    volatility_breaker: Dict[str, Any] = Field(default_factory=dict)
    recovery: Dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "allow", "frozen": True}


class Execution(BaseModel):
    order_type: str = Field(default="limit", pattern=r"^(limit|market)$")
    allow_market_orders: bool = False
    limit_offset_bps: float = Field(default=2.0, ge=0.0, le=100.0)
    max_order_value: float = Field(default=1_000_000.0, ge=0.0)
    time_in_force: str = Field(default="GTC", pattern=r"^(GTC|IOC|FOK)$")
    retry_attempts: int = Field(default=2, ge=0, le=10)
    retry_delay_ms: int = Field(default=200, ge=0, le=10000)
    retry_backoff: str = Field(default="exponential", pattern=r"^(fixed|exponential)$")
    cancel_timeout_sec: float = Field(default=5.0, ge=0.0, le=60.0)
    unfilled_action: str = Field(default="cancel", pattern=r"^(cancel|switch_to_market|reprice_limit)$")
    twap: Optional[Dict[str, Any]] = None
    model_config = {"extra": "allow", "frozen": True}


class CostModel(BaseModel):
    maker_fee: float = Field(default=0.0002, ge=0.0, le=0.01)
    taker_fee: float = Field(default=0.0004, ge=0.0, le=0.01)
    base_slippage_pct: float = Field(default=0.0005, ge=0.0, le=0.05)
    twap_slippage_pct: float = Field(default=0.0003, ge=0.0, le=0.05)
    stress_slippage_pct: float = Field(default=0.005, ge=0.0, le=0.05)
    funding_rate: Dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "allow", "frozen": True}


class BrokerParams(BaseModel):
    api_key: SecretStr
    secret: SecretStr
    testnet: bool = False
    default_leverage: float = Field(default=1.0, gt=0.0, le=125.0)
    margin_type: str = Field(default="ISOLATED", pattern=r"^(ISOLATED|CROSSED)$")
    position_mode: str = Field(default="Hedge", pattern=r"^(Hedge|OneWay)$")
    model_config = {"extra": "allow", "frozen": True}

    def __repr__(self):
        return (
            f"BrokerParams(api_key=***, secret=***, testnet={self.testnet})"
        )


class Broker(BaseModel):
    type: str = Field(default="binance", pattern=r"^(binance|bybit|okx)$")
    params: BrokerParams
    model_config = {"extra": "allow", "frozen": True}


class StrategyConfig(BaseModel):
    name: str = "KHAOS"
    version: Optional[str] = None
    mode: str
    universe: Universe
    modules: Dict[str, Any] = Field(default_factory=dict)
    risk_limits: RiskLimits
    execution: Execution
    market_impact: Optional[Dict[str, Any]] = None
    liquidity_requirements: Optional[Dict[str, Any]] = None
    cost_model: CostModel
    paper_trading: Optional[Dict[str, Any]] = None
    circuit_breaker: Dict[str, Any] = Field(default_factory=dict)
    data_integrity: Dict[str, Any] = Field(default_factory=dict)
    time_sync: Optional[Dict[str, Any]] = None
    state_persistence: Optional[Dict[str, Any]] = None
    monitoring: Dict[str, Any] = Field(default_factory=dict)
    compliance: Dict[str, Any] = Field(default_factory=dict)
    stress_testing: Optional[Dict[str, Any]] = None
    rollback: Optional[Dict[str, Any]] = None
    capital_allocation: Optional[Dict[str, Any]] = None
    broker: Broker
    config_metadata: Optional[Dict[str, Any]] = None
    model_config = {"extra": "allow", "frozen": True}

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v not in ("paper", "live", "hybrid"):
            raise ValueError(f"Invalid mode '{v}'. Must be paper/live/hybrid")
        return v

    @model_validator(mode="after")
    def check_mode_broker_consistency(self):
        if self.mode == "live":
            if self.broker.params.api_key.get_secret_value() == "" or self.broker.params.secret.get_secret_value() == "":
                raise ValueError("Live mode requires valid broker API key and secret")
        return self

    def __repr__(self):
        return f"StrategyConfig(name={self.name}, mode={self.mode}, version={self.version})"


# ---------------------------------------------------------------------------
# 审计后端接口
# ---------------------------------------------------------------------------
class AuditBackend:
    def log(self, event: str, details: Dict[str, Any]):
        logger.info(f"AUDIT: {event} | {details}")


# ---------------------------------------------------------------------------
# 配置管理器（线程安全，多实例，原子热更）
# ---------------------------------------------------------------------------
class ConfigManager:
    def __init__(self, audit_backend: Optional[AuditBackend] = None):
        self._lock = threading.RLock()
        self._instances: Dict[str, StrategyConfig] = {}
        self._observers: Dict[str, List[Callable[[StrategyConfig], None]]] = {}
        self._provenance: Dict[str, Dict[str, Any]] = {}
        self._audit = audit_backend or AuditBackend()
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="cfg-observer")
        self._shutdown_registered = False
        atexit.register(self.shutdown)

    def load(self, config_path: Optional[str] = None) -> Tuple[StrategyConfig, Dict[str, Any]]:
        path = _resolve_config_path(config_path)
        _check_file_size(path)
        _check_file_permissions(path)

        try:
            with open(path, "r", encoding="utf-8") as f:
                raw_yaml_str = f.read()
        except (PermissionError, OSError) as e:
            raise RuntimeError(f"Cannot read config file {path}: {e}")

        try:
            raw_cfg = yaml.safe_load(raw_yaml_str)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file {path}: {e}")

        if not isinstance(raw_cfg, dict):
            raise ValueError("Configuration file must contain a YAML mapping at top level")

        # ---- 完整性校验 ----
        integrity_block = raw_cfg.get("_integrity", {})
        expected_hash = integrity_block.get("hash", "")
        algorithm = integrity_block.get("algorithm", "sha256").lower()
        signature = integrity_block.get("signature", "")
        signing_key = os.environ.get("CONFIG_SIGNING_KEY")

        if expected_hash or signature:
            # 从原始 YAML 字符串中移除 _integrity 块
            raw_content = _remove_integrity_block(raw_yaml_str)

            if expected_hash:
                if algorithm in ("sha256", "sha384", "sha512"):
                    computed = hashlib.new(algorithm, raw_content.encode("utf-8")).hexdigest()
                else:
                    raise ValueError(f"Unsupported hash algorithm: {algorithm}")
                if computed != expected_hash:
                    raise ValueError("Configuration integrity check FAILED (hash mismatch).")

            if signature:
                if not signing_key:
                    raise ValueError("Signature present but CONFIG_SIGNING_KEY environment variable not set")
                if algorithm == "hmac-sha256":
                    expected_sig = hmac.new(
                        signing_key.encode(), raw_content.encode(), hashlib.sha256
                    ).hexdigest()
                    if expected_sig != signature:
                        raise ValueError("Configuration signature verification FAILED (HMAC).")
                elif algorithm == "ed25519":
                    try:
                        from cryptography.hazmat.primitives import serialization
                        from cryptography.hazmat.primitives.asymmetric import ed25519
                    except ImportError:
                        raise ImportError("cryptography library is required for ed25519 signature verification")
                    try:
                        public_key = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(signing_key))
                        public_key.verify(bytes.fromhex(signature), raw_content.encode())
                    except Exception as e:
                        raise ValueError(f"ed25519 signature verification failed: {e}")
                else:
                    raise ValueError(f"Unsupported signature algorithm: {algorithm}")

            logger.info("Configuration integrity/signature verified.")

        # ---- 环境变量替换 ----
        try:
            processed_cfg = _substitute_env_vars(raw_cfg)
        except KeyError as e:
            raise RuntimeError(f"Missing environment variable referenced in config: {e}")

        strategy_cfg = processed_cfg.get("strategy")
        if not strategy_cfg:
            raise ValueError("Missing 'strategy' section in configuration")

        version = strategy_cfg.get("version", "0.0.0")
        if not _is_version_compatible(version, "2.0.0", "3.0.0"):
            raise ValueError(
                f"Config version {version} is not compatible with engine (>=2.0.0, <3.0.0)"
            )

        # 模块白名单
        allowed_modules = {"trendline", "regime", "micro_accel", "sizer", "stops", "add_rules"}
        modules = strategy_cfg.get("modules")
        if modules and isinstance(modules, dict):
            unknown = set(modules.keys()) - allowed_modules
            if unknown:
                logger.warning(f"Unknown modules in config: {unknown}")

        # Pydantic 验证
        try:
            config = StrategyConfig(**strategy_cfg)
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise

        provenance = {
            "loaded_at": datetime.now(timezone.utc).isoformat(),
            "path": str(path),
            "version": version,
            "hash": expected_hash or "none",
            "user": os.environ.get("USER", "unknown"),
            "hostname": socket.gethostname(),
        }
        return config, provenance

    def initialize(self, name: str = "main", config_path: Optional[str] = None) -> StrategyConfig:
        with self._lock:
            if name not in self._instances:
                config, prov = self.load(config_path)
                self._instances[name] = config
                self._provenance[name] = prov
                self._observers.setdefault(name, [])
            return self._instances[name]

    def get_config(self, name: str = "main") -> StrategyConfig:
        if name not in self._instances:
            self.initialize(name)
        return self._instances[name]

    def reload(self, name: str = "main", config_path: Optional[str] = None) -> StrategyConfig:
        old_config = self._instances.get(name)
        try:
            new_config, prov = self.load(config_path)
        except Exception as e:
            logger.error(f"Failed to reload config '{name}': {e}. Keeping old config.")
            raise

        with self._lock:
            self._instances[name] = new_config
            self._provenance[name] = prov
            observers = self._observers.get(name, [])
            for obs in observers:
                self._executor.submit(self._notify_observer, obs, new_config)
            self._audit.log("config_reloaded", {
                "instance": name,
                "old_version": old_config.version if old_config else "none",
                "new_version": new_config.version,
                "path": prov["path"]
            })
        return new_config

    def _notify_observer(self, observer: Callable[[StrategyConfig], None], config: StrategyConfig):
        try:
            observer(config)
        except Exception:
            logger.exception("Observer failed during config notification")

    def register_observer(self, name: str, callback: Callable[[StrategyConfig], None]):
        if name not in self._observers:
            self._observers[name] = []
        self._observers[name].append(callback)

    def unregister_observer(self, name: str, callback: Callable[[StrategyConfig], None]):
        if name in self._observers:
            self._observers[name].remove(callback)

    def update_value(self, name: str, field_path: str, new_value: Any, user: str = "system") -> StrategyConfig:
        config = self.get_config(name)
        metadata = config.config_metadata or {}
        mutable_fields = metadata.get("mutable_fields", {})

        parts = field_path.split(".")
        # 检查路径可变性
        for i in range(1, len(parts) + 1):
            prefix = ".".join(parts[:i])
            if prefix in mutable_fields and not mutable_fields[prefix]:
                raise PermissionError(f"Field '{prefix}' is not mutable. Modification denied.")

        # 检查路径是否有效
        if not _check_path_exists(StrategyConfig, parts):
            raise AttributeError(f"Configuration path '{field_path}' does not exist in StrategyConfig.")

        old_value = _get_nested(config, parts)

        # 构建深层更新字典
        update_dict = _build_update_dict(parts, new_value)
        config_dict = config.model_dump()
        # 深度合并
        new_dict = _deep_merge(config_dict, update_dict)
        try:
            new_config = StrategyConfig(**new_dict)
        except ValidationError as e:
            raise ValueError(f"Update results in invalid configuration: {e}")

        with self._lock:
            self._instances[name] = new_config
            self._audit.log("config_value_updated", {
                "instance": name,
                "field": field_path,
                "old_value": _safe_str(old_value),
                "new_value": _safe_str(new_value),
                "user": user,
                "hostname": socket.gethostname(),
            })
            for obs in self._observers.get(name, []):
                self._executor.submit(self._notify_observer, obs, new_config)
        return new_config

    def export_config(self, name: str = "main") -> str:
        config = self.get_config(name)
        config_dict = config.model_dump()
        # 隐藏 broker secret
        if "broker" in config_dict and "params" in config_dict["broker"]:
            config_dict["broker"]["params"]["secret"] = "***"
            config_dict["broker"]["params"]["api_key"] = "***"
        return yaml.dump(config_dict, sort_keys=False)

    def validate_config(self, config_dict: Dict[str, Any], substitute_env: bool = True) -> StrategyConfig:
        if substitute_env:
            config_dict = _substitute_env_vars(config_dict)
        return StrategyConfig(**config_dict)

    def get_provenance(self, name: str = "main") -> Dict[str, Any]:
        return self._provenance.get(name, {})

    def shutdown(self):
        self._executor.shutdown(wait=True)

    def __del__(self):
        self.shutdown()


def _is_version_compatible(version_str: str, min_ver: str, max_ver: str) -> bool:
    try:
        v = pkg_version.parse(version_str)
        return pkg_version.parse(min_ver) <= v < pkg_version.parse(max_ver)
    except Exception:
        return False


def _build_update_dict(path: List[str], value: Any) -> Dict[str, Any]:
    if not path:
        return {}
    result = value
    for part in reversed(path):
        result = {part: result}
    return result


# ---------------------------------------------------------------------------
# 全局管理器与便捷代理
# ---------------------------------------------------------------------------
_manager = ConfigManager()

def get_config(name: str = "main") -> StrategyConfig:
    return _manager.get_config(name)

def reload_config(name: str = "main", config_path: Optional[str] = None) -> StrategyConfig:
    return _manager.reload(name, config_path)

def register_config_observer(name: str, callback: Callable[[StrategyConfig], None]):
    _manager.register_observer(name, callback)

def update_config_value(name: str, field_path: str, new_value: Any, user: str = "system") -> StrategyConfig:
    return _manager.update_value(name, field_path, new_value, user)

def export_config(name: str = "main") -> str:
    return _manager.export_config(name)

def validate_config(config_dict: Dict[str, Any], substitute_env: bool = True) -> StrategyConfig:
    return _manager.validate_config(config_dict, substitute_env)

def get_provenance(name: str = "main") -> Dict[str, Any]:
    return _manager.get_provenance(name)

class _ConfigProxy:
    """只读配置代理，指向 'main' 实例，支持 IDE 自动补全。"""
    def __init__(self, instance_name: str = "main"):
        self._instance_name = instance_name

    def __getattr__(self, item):
        try:
            cfg = get_config(self._instance_name)
        except Exception as e:
            raise RuntimeError(f"Configuration for '{self._instance_name}' not available: {e}")
        return getattr(cfg, item)

    def __setattr__(self, item, value):
        if item == "_instance_name":
            super().__setattr__(item, value)
        else:
            raise RuntimeError("Direct modification of config is not allowed. Use update_config_value().")

    def __dir__(self):
        try:
            cfg = get_config(self._instance_name)
            return list(cfg.model_fields.keys())
        except Exception:
            return ["_instance_name"]

    def __repr__(self):
        try:
            cfg = get_config(self._instance_name)
            return repr(cfg)
        except Exception:
            return f"<ConfigProxy for '{self._instance_name}' (unloaded)>"

config = _ConfigProxy("main")
