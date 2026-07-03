# -*- coding: utf-8 -*-
"""
Module: engine
Description: KHAOS 事件驱动引擎包。
             提供回测、实盘、影子账户引擎，支持按模式自动创建。
             包含版本管理、环境检查、线程安全延迟导入及健康监测。
             符合全球顶级量化基金对引擎入口的安全与可运维性要求。

Examples:
    from engine import create_engine, EngineMode

    # 创建影子引擎
    engine = create_engine(EngineMode.SHADOW)
    await engine.run()

    # 或直接使用便捷函数
    from engine import create_engine_from_config
    engine = create_engine_from_config()
    await engine.run()

Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.5.0
License: Proprietary
Copyright: KHAOS Fund, 2026. All rights reserved.
"""

from __future__ import annotations

import logging
import os
import threading
from enum import Enum
from typing import Any, Dict, List, Optional, Type

logger = logging.getLogger("khaos.engine")

# ---------------------------------------------------------------------------
# 元信息
# ---------------------------------------------------------------------------
__version__ = "2.5.0"
__author__ = "KHAOS Quant Team"
__license__ = "Proprietary"

# ---------------------------------------------------------------------------
# 引擎模式枚举
# ---------------------------------------------------------------------------
class EngineMode(Enum):
    BACKTEST = "BACKTEST"
    LIVE = "LIVE"
    SHADOW = "SHADOW"

# ---------------------------------------------------------------------------
# 关键环境变量预检（缺失时仅警告）
# ---------------------------------------------------------------------------
_REQUIRED_ENV_VARS = {
    EngineMode.LIVE: ["BINANCE_API_KEY", "BINANCE_SECRET"],
    EngineMode.SHADOW: [],
    EngineMode.BACKTEST: [],
}
for mode, vars_ in _REQUIRED_ENV_VARS.items():
    missing = [v for v in vars_ if not os.environ.get(v)]
    if missing:
        logger.warning(f"Engine mode {mode.value} may not work properly: missing environment variables {missing}")

# ---------------------------------------------------------------------------
# 线程安全的延迟导入
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_loaded = False

# 存储导入的引擎类
BaseEngine = None
BacktestEngine = None
LiveEngine = None
ShadowEngine = None

# 活跃引擎实例注册表
_running_engines: Dict[str, Any] = {}

# 惰性属性名称
_LAZY_ATTRS = {
    "BaseEngine",
    "BacktestEngine",
    "LiveEngine",
    "ShadowEngine",
}

def _perform_imports():
    global _loaded, BaseEngine, BacktestEngine, LiveEngine, ShadowEngine
    if _loaded:
        return
    with _lock:
        if _loaded:
            return

        # 基类必须可用
        from engine.base import BaseEngine as _Base
        BaseEngine = _Base
        logger.debug("BaseEngine loaded.")

        # 回测引擎
        try:
            from engine.backtest import BacktestEngine as _Back
            BacktestEngine = _Back
            logger.debug("BacktestEngine loaded.")
        except ImportError as e:
            logger.error(f"BacktestEngine could not be loaded: {e}")

        # 实盘引擎
        try:
            from engine.live import LiveEngine as _Live
            LiveEngine = _Live
            logger.debug("LiveEngine loaded.")
        except ImportError as e:
            logger.error(f"LiveEngine could not be loaded: {e}")

        # 影子引擎
        try:
            from engine.shadow import ShadowEngine as _Shadow
            ShadowEngine = _Shadow
            logger.debug("ShadowEngine loaded.")
        except ImportError as e:
            logger.error(f"ShadowEngine could not be loaded: {e}")

        _loaded = True

def __getattr__(name: str):
    if name in _LAZY_ATTRS:
        _perform_imports()
        obj = globals().get(name)
        if obj is None:
            raise RuntimeError(f"Engine component '{name}' is unavailable.")
        return obj
    raise AttributeError(f"module 'engine' has no attribute '{name}'")

def __dir__():
    return list(__all__)  # __all__ 会在模块末尾定义

# ---------------------------------------------------------------------------
# 工厂函数
# ---------------------------------------------------------------------------
def create_engine(mode: EngineMode, **kwargs) -> Any:
    """
    根据模式创建引擎实例。

    Args:
        mode: 引擎模式枚举。
        **kwargs: 传递给引擎构造函数的额外参数。

    Returns:
        引擎实例。
    """
    _perform_imports()
    if mode == EngineMode.BACKTEST:
        if BacktestEngine is None:
            raise RuntimeError("BacktestEngine is not available.")
        engine = BacktestEngine(**kwargs)
    elif mode == EngineMode.LIVE:
        if LiveEngine is None:
            raise RuntimeError("LiveEngine is not available.")
        engine = LiveEngine(**kwargs)
    elif mode == EngineMode.SHADOW:
        if ShadowEngine is None:
            raise RuntimeError("ShadowEngine is not available.")
        engine = ShadowEngine(**kwargs)
    else:
        raise ValueError(f"Unknown engine mode: {mode}")

    _running_engines[id(engine)] = engine
    logger.info(f"Engine created: {engine}")
    return engine

def create_engine_from_config(**kwargs) -> Any:
    """从全局配置创建引擎（自动根据 mode 选择）。"""
    from config.settings import get_config
    cfg = get_config()
    mode_str = cfg.mode.upper()
    try:
        mode = EngineMode(mode_str)
    except ValueError:
        raise ValueError(f"Invalid engine mode in config: {mode_str}")
    return create_engine(mode, config=cfg, **kwargs)

# ---------------------------------------------------------------------------
# 运维接口
# ---------------------------------------------------------------------------
def health_check() -> Dict[str, Any]:
    """返回引擎包的可用性状态。"""
    _perform_imports()
    return {
        "version": __version__,
        "engines": {
            "base": BaseEngine is not None,
            "backtest": BacktestEngine is not None,
            "live": LiveEngine is not None,
            "shadow": ShadowEngine is not None,
        },
        "running_instances": len(_running_engines),
    }

def get_running_engines() -> List[Any]:
    """返回当前活跃的引擎实例列表。"""
    return list(_running_engines.values())

async def shutdown_all() -> None:
    """关闭所有运行中的引擎，释放资源。"""
    for engine in list(_running_engines.values()):
        try:
            await engine.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down engine {engine}: {e}")
    _running_engines.clear()
    logger.info("All engines shut down.")

# ---------------------------------------------------------------------------
# 脱敏工具
# ---------------------------------------------------------------------------
def _mask_sensitive(data: Dict[str, Any]) -> Dict[str, Any]:
    import copy
    masked = copy.deepcopy(data)
    for key in masked:
        if isinstance(masked[key], dict):
            masked[key] = _mask_sensitive(masked[key])
        elif "key" in key.lower() or "secret" in key.lower() or "password" in key.lower():
            masked[key] = "***"
    return masked

# ---------------------------------------------------------------------------
# 公共 API
# ---------------------------------------------------------------------------
__all__ = [
    "BaseEngine",
    "BacktestEngine",
    "LiveEngine",
    "ShadowEngine",
    "EngineMode",
    "create_engine",
    "create_engine_from_config",
    "health_check",
    "get_running_engines",
    "shutdown_all",
    "__version__",
    "__author__",
]

# 模块 repr
def __repr__():
    return f"<module 'engine' version={__version__}>"
