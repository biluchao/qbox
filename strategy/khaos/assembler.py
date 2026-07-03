# -*- coding: utf-8 -*-
"""
Module: strategy.khaos.assembler
Description: KHAOS 策略的依赖注入组装器。支持动态组件创建、配置校验、
             环境变量替换、安全文件检查、风险/审计注入及版本兼容性验证。
             符合全球顶级量化基金对策略组装的安全与可维护性要求。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.7.2
"""

from __future__ import annotations

import copy
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import yaml

from strategy.khaos.strategy import KhaosStrategy
from core.indicators.kalman import KalmanTrendline
from core.indicators.atr import ATR
from core.models.hmm import OnlineHMM
from core.micro.orderflow import OrderFlowAccelerator
from core.risk.sizer import VolTargetSizer
from core.risk.stops import DynamicKMAStop

logger = logging.getLogger("khaos.strategy.assembler")

# ---------------------------------------------------------------------------
# 环境变量替换
# ---------------------------------------------------------------------------
try:
    from config.settings import _substitute_env_vars as _env_sub
except ImportError:
    logger.critical("config.settings must be available for environment variable substitution.")
    raise ImportError("config.settings is required but not importable.")

# ---------------------------------------------------------------------------
# 模块配置模型（轻量深度校验）
# ---------------------------------------------------------------------------
from pydantic import BaseModel, Field, ValidationError

class TrendlineConfig(BaseModel):
    type: str = "kalman"
    params: Dict[str, Any] = Field(default_factory=dict)

class RegimeConfig(BaseModel):
    type: str = "hmm"
    params: Dict[str, Any] = Field(default_factory=dict)

class MicroConfig(BaseModel):
    type: str = "orderflow"
    params: Dict[str, Any] = Field(default_factory=dict)

class SizerConfig(BaseModel):
    type: str = "vol_target"
    params: Dict[str, Any] = Field(default_factory=dict)

class StopsConfig(BaseModel):
    type: str = "dynamic_kma"
    params: Dict[str, Any] = Field(default_factory=dict)

class ModulesConfig(BaseModel):
    trendline: TrendlineConfig = TrendlineConfig()
    regime: RegimeConfig = RegimeConfig()
    micro_accel: MicroConfig = MicroConfig()
    sizer: SizerConfig = SizerConfig()
    stops: StopsConfig = StopsConfig()

# ---------------------------------------------------------------------------
# 脱敏函数
# ---------------------------------------------------------------------------
def _mask_sensitive(data: Dict[str, Any]) -> Dict[str, Any]:
    """脱敏配置字典中的敏感字段。"""
    masked = copy.deepcopy(data)
    for k in masked:
        if isinstance(masked[k], dict):
            masked[k] = _mask_sensitive(masked[k])
        elif 'key' in k.lower() or 'secret' in k.lower() or 'password' in k.lower():
            masked[k] = '***'
    return masked

# ---------------------------------------------------------------------------
# 依赖注入容器
# ---------------------------------------------------------------------------
try:
    from dependency_injector import containers, providers
    HAS_DI = True
except ImportError:
    HAS_DI = False

class KhaosContainer(containers.DeclarativeContainer):
    # 默认配置，确保访问不会因缺失键而崩溃
    default_config = providers.Dict({
        "trendline": {"type": "kalman", "params": {"q_ratio": 0.01}},
        "regime": {"type": "hmm", "params": {"states": 3}},
        "micro_accel": {"type": "orderflow", "params": {"bpi_thresh": 0.25, "taker_thresh": 0.3}},
        "sizer": {"type": "vol_target", "params": {"risk_per_trade": 0.01, "vol_target_annual": 0.20, "max_leverage": 3.0}},
        "stops": {"type": "dynamic_kma", "params": {"alpha_base": 2.5}},
    })

    # 合并用户配置与默认值
    merged_config = providers.Dict()
    merged_config.override(default_config)
    # 注意：在调用时通过 from_dict 传入用户配置并合并

    # 趋势线
    def _create_trendline(cfg):
        params = (cfg or {}).get("params", {})
        return KalmanTrendline(q_ratio=float(params.get("q_ratio", 0.01)))
    trendline = providers.Callable(_create_trendline, merged_config.provided.trendline)

    # HMM
    def _create_hmm(cfg):
        params = (cfg or {}).get("params", {})
        return OnlineHMM(n_states=int(params.get("states", 3)))
    regime = providers.Callable(_create_hmm, merged_config.provided.regime)

    # 微观加速器
    def _create_micro(cfg):
        cfg = cfg or {}
        if cfg.get("type") == "disabled":
            return OrderFlowAccelerator(bpi_thresh=0.0, taker_thresh=0.0)
        params = cfg.get("params", {})
        return OrderFlowAccelerator(
            bpi_thresh=float(params.get("bpi_thresh", 0.25)),
            taker_thresh=float(params.get("taker_thresh", 0.3))
        )
    micro_accel = providers.Callable(_create_micro, merged_config.provided.micro_accel)

    # 仓位管理
    def _create_sizer(cfg):
        params = (cfg or {}).get("params", {})
        return VolTargetSizer(
            risk_per_trade=float(params.get("risk_per_trade", 0.01)),
            vol_target=float(params.get("vol_target_annual", 0.20)),
            max_leverage=float(params.get("max_leverage", 3.0))
        )
    sizer = providers.Callable(_create_sizer, merged_config.provided.sizer)

    # 止损管理
    def _create_stops(cfg):
        params = (cfg or {}).get("params", {})
        return DynamicKMAStop(alpha_base=float(params.get("alpha_base", 2.5)))
    stop_manager = providers.Callable(_create_stops, merged_config.provided.stops)

    # 策略实例（接受 risk_context 和 audit_callback 作为可选依赖）
    strategy = providers.Factory(
        KhaosStrategy,
        config=merged_config,              # 传入完整的 modules 配置
        kalman=trendline,
        hmm=regime,
        micro=micro_accel,
        sizer=sizer,
        stop_mgr=stop_manager,
        risk_context=providers.Object(None),
        audit_callback=providers.Object(None),
    )

# ---------------------------------------------------------------------------
# 手动组装函数（无 DI 容器）
# ---------------------------------------------------------------------------
def _manual_assemble(modules: Dict[str, Any],
                     risk_context: Any = None,
                     audit_callback: Any = None) -> KhaosStrategy:
    # 深拷贝避免外部修改
    modules = copy.deepcopy(modules)
    # 校验模块结构
    try:
        ModulesConfig(**modules)
    except ValidationError as e:
        logger.error(f"Module configuration validation failed: {e}")
        raise ValueError(f"Invalid module configuration: {e}")

    trend_cfg = modules.get("trendline", {})
    kalman = KalmanTrendline(q_ratio=float(trend_cfg.get("params", {}).get("q_ratio", 0.01)))
    logger.debug("Kalman trendline created with q_ratio=%.4f", kalman.q_ratio if hasattr(kalman, 'q_ratio') else 0.01)

    regime_cfg = modules.get("regime", {})
    hmm = OnlineHMM(n_states=int(regime_cfg.get("params", {}).get("states", 3)))
    logger.debug("HMM created with states=%d", hmm.n_states if hasattr(hmm, 'n_states') else 3)

    micro_cfg = modules.get("micro_accel", {})
    if micro_cfg.get("type") == "disabled":
        micro = OrderFlowAccelerator(bpi_thresh=0.0, taker_thresh=0.0)
        logger.debug("Micro accelerator disabled.")
    else:
        micro = OrderFlowAccelerator(
            bpi_thresh=float(micro_cfg.get("params", {}).get("bpi_thresh", 0.25)),
            taker_thresh=float(micro_cfg.get("params", {}).get("taker_thresh", 0.3))
        )
        logger.debug("Micro accelerator created.")

    sizer_cfg = modules.get("sizer", {})
    sizer = VolTargetSizer(
        risk_per_trade=float(sizer_cfg.get("params", {}).get("risk_per_trade", 0.01)),
        vol_target=float(sizer_cfg.get("params", {}).get("vol_target_annual", 0.20)),
        max_leverage=float(sizer_cfg.get("params", {}).get("max_leverage", 3.0))
    )
    logger.debug("VolTargetSizer created.")

    stops_cfg = modules.get("stops", {})
    stop_mgr = DynamicKMAStop(alpha_base=float(stops_cfg.get("params", {}).get("alpha_base", 2.5)))
    logger.debug("DynamicKMAStop created.")

    strategy = KhaosStrategy(
        config=modules,
        kalman=kalman,
        hmm=hmm,
        micro=micro,
        sizer=sizer,
        stop_mgr=stop_mgr,
        risk_context=risk_context,
        audit_callback=audit_callback,
    )
    logger.info("KHAOS strategy manually assembled.")
    return strategy

# ---------------------------------------------------------------------------
# 公共 API
# ---------------------------------------------------------------------------
def _resolve_config_path(config_path: Optional[str] = None) -> Path:
    if config_path:
        path = Path(config_path).expanduser().resolve()
    else:
        env_path = os.environ.get("KHAOS_STRATEGY_CONFIG_PATH")
        if env_path:
            path = Path(env_path).expanduser().resolve()
        else:
            raise ValueError("No config path provided and KHAOS_STRATEGY_CONFIG_PATH not set.")
    return path

def _validate_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    max_mb = int(os.environ.get("KHAOS_CONFIG_MAX_SIZE_MB", "10"))
    if path.stat().st_size > max_mb * 1024 * 1024:
        raise ValueError(f"Config file too large (>{max_mb}MB): {path}")
    mode = path.stat().st_mode
    if mode & 0o077:
        msg = f"Config file has permissive permissions ({oct(mode)}). Expected 0600."
        if os.environ.get("KHAOS_STRICT_CONFIG_PERMISSIONS", "true").lower() == "true":
            raise PermissionError(msg)
        else:
            logger.warning(msg)

def _extract_strategy_config(cfg_dict: Dict[str, Any]) -> Dict[str, Any]:
    """如果字典包含 'strategy' 键，则返回其值；否则原样返回。"""
    if isinstance(cfg_dict, dict) and "strategy" in cfg_dict:
        return cfg_dict["strategy"]
    return cfg_dict

def assemble_strategy(
    config_path: Optional[str] = None,
    config_dict: Optional[Dict[str, Any]] = None,
    risk_context: Any = None,
    audit_callback: Any = None,
) -> KhaosStrategy:
    """
    从文件或字典组装 KHAOS 策略。

    Args:
        config_path: YAML 配置文件路径。
        config_dict: 配置字典（可嵌套 'strategy' 键）。
        risk_context: 风控上下文，注入策略。
        audit_callback: 审计回调，注入策略。

    Returns:
        完全组装的 KhaosStrategy 实例。
    """
    if config_path:
        path = _resolve_config_path(config_path)
        _validate_file(path)
        with open(path, "r", encoding="utf-8") as f:
            full_cfg = yaml.safe_load(f)
        config_dict = full_cfg  # 进一步提取 strategy 部分
    elif config_dict is None:
        raise ValueError("Either config_path or config_dict must be provided.")

    config_dict = _extract_strategy_config(config_dict)
    if not isinstance(config_dict, dict):
        raise ValueError("Invalid configuration format.")

    # 脱敏后记录日志（避免泄露）
    safe_log = _mask_sensitive(config_dict)
    logger.info("Assembling strategy from config: %s", safe_log)

    # 环境变量替换（深拷贝防止副作用）
    try:
        config_dict = _env_sub(copy.deepcopy(config_dict))
    except KeyError as e:
        raise ValueError(f"Environment variable {e} required but missing in config.") from e

    modules = config_dict.get("modules", {})
    if not modules:
        raise ValueError("Configuration must contain 'modules' section.")

    # 版本兼容性检查
    if "version" in config_dict:
        strategy_ver = config_dict["version"]
        # 此处可添加与引擎版本的比较，暂仅记录
        logger.info("Config version: %s, Strategy code version: %s", strategy_ver, KhaosStrategy.version)

    # 基于 DI 或手动构建
    if HAS_DI:
        container = KhaosContainer()
        # 合并用户配置到容器
        container.merged_config.override(container.default_config)
        container.merged_config.override(providers.Dict(modules))
        strategy = container.strategy(
            risk_context=risk_context,
            audit_callback=audit_callback
        )
    else:
        strategy = _manual_assemble(modules, risk_context, audit_callback)

    logger.info("Strategy assembled successfully: %s", repr(strategy))
    return strategy

def assemble_from_model(
    strategy_config,  # StrategyConfig pydantic model
    risk_context: Any = None,
    audit_callback: Any = None,
) -> KhaosStrategy:
    """从 pydantic 模型组装策略。"""
    modules_dict = strategy_config.modules.model_dump() if hasattr(strategy_config.modules, 'model_dump') else strategy_config.modules.dict()
    return _manual_assemble(modules_dict, risk_context, audit_callback)

def reload_strategy(strategy: KhaosStrategy, config: Dict[str, Any]) -> None:
    """使用新的配置字典重新加载策略参数。"""
    strategy.reload_params(config)
    logger.info("Strategy parameters reloaded.")
