# -*- coding: utf-8 -*-
"""
Module: strategy
Description: KHAOS 策略层入口。
             提供策略基类、注册表、工厂函数与具体策略实现。
             所有策略必须实现 AbstractStrategy 接口，确保可插拔。
             支持通过配置文件动态组装，并内置版本兼容性检查。

Usage:
    from strategy import create_strategy, list_strategies

    # 通过名称创建策略实例
    strat = create_strategy("KHAOS", config_path="config/strategy.prod.yaml")

    # 列出所有已注册策略
    print(list_strategies())

Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.6.0 (module)
Compatible Engine: >=2.6.0, <3.0.0
License: Proprietary - All Rights Reserved
"""

import importlib
import logging
import threading
import warnings
from typing import Any, Dict, List, Optional, Type

# ---------------------------------------------------------------------------
# 版本与元信息
# ---------------------------------------------------------------------------
__version__ = "2.6.0"
__author__ = "KHAOS Quant Team"
__license__ = "Proprietary"
__engine_compatibility__ = ">=2.6.0, <3.0.0"

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logger = logging.getLogger("khaos.strategy")

# ---------------------------------------------------------------------------
# 延迟导入的核心基类
# ---------------------------------------------------------------------------
from strategy.base import AbstractStrategy

# ---------------------------------------------------------------------------
# 策略注册表（线程安全）
# ---------------------------------------------------------------------------
_lock = threading.RLock()
_STRATEGY_REGISTRY: Dict[str, Type[AbstractStrategy]] = {}
_STRATEGY_META: Dict[str, Dict[str, Any]] = {}

def _check_strategy_compliance(cls: Type[AbstractStrategy]) -> None:
    """验证策略类是否完全实现了抽象接口。"""
    if not issubclass(cls, AbstractStrategy):
        raise TypeError(f"{cls.__name__} must inherit from AbstractStrategy")
    # 尝试实例化检查抽象方法（需提供最小依赖，此处仅检查抽象方法列表）
    abstract_methods = cls.__abstractmethods__
    if abstract_methods:
        raise TypeError(f"{cls.__name__} does not implement abstract methods: {abstract_methods}")

def register_strategy(cls: Type[AbstractStrategy]) -> Type[AbstractStrategy]:
    """装饰器：将策略类注册到全局注册表。"""
    with _lock:
        name = cls.__name__  # 默认使用类名作为键，可通过类属性 `strategy_name` 覆盖
        strategy_name = getattr(cls, 'strategy_name', name)
        if strategy_name in _STRATEGY_REGISTRY:
            # 如果已注册，检查是否相同类，否则警告
            if _STRATEGY_REGISTRY[strategy_name] != cls:
                logger.warning(f"Strategy '{strategy_name}' already registered and will be overwritten.")
        _check_strategy_compliance(cls)
        _STRATEGY_REGISTRY[strategy_name] = cls
        _STRATEGY_META[strategy_name] = {
            'class': cls.__name__,
            'version': getattr(cls, 'version', 'unknown'),
            'author': getattr(cls, 'author', 'unknown'),
            'deprecated': getattr(cls, 'deprecated', False),
        }
        logger.info(f"Strategy registered: {strategy_name} (v{_STRATEGY_META[strategy_name]['version']})")
    return cls

def get_strategy_class(name: str) -> Type[AbstractStrategy]:
    """根据名称获取策略类对象。"""
    with _lock:
        if name not in _STRATEGY_REGISTRY:
            raise KeyError(f"Strategy '{name}' not found in registry. Available: {list(_STRATEGY_REGISTRY.keys())}")
        return _STRATEGY_REGISTRY[name]

def list_strategies() -> List[Dict[str, Any]]:
    """列出所有已注册策略及其元信息。"""
    with _lock:
        return [{'name': k, **v} for k, v in _STRATEGY_META.items()]

def reload_strategies(package: str = "strategy") -> None:
    """重新扫描并注册策略包中的所有模块，用于热更新。"""
    import pkgutil
    import importlib
    import inspect
    with _lock:
        _STRATEGY_REGISTRY.clear()
        _STRATEGY_META.clear()
    # 遍历包内所有模块
    for _, module_name, is_pkg in pkgutil.iter_modules(__import__(package).__path__, package + "."):
        try:
            mod = importlib.import_module(module_name)
            for name, obj in inspect.getmembers(mod, inspect.isclass):
                if issubclass(obj, AbstractStrategy) and obj is not AbstractStrategy:
                    # 将未使用装饰器但符合接口的类注册（如果它们没有手动注册）
                    if name not in _STRATEGY_REGISTRY:
                        register_strategy(obj)
        except Exception as e:
            logger.warning(f"Failed to import strategy module {module_name}: {e}")

# ---------------------------------------------------------------------------
# 工厂函数（延迟导入具体策略，避免循环依赖）
# ---------------------------------------------------------------------------
def create_strategy(
    name: str,
    config_path: Optional[str] = None,
    **kwargs
) -> AbstractStrategy:
    """
    根据名称创建策略实例。
    该函数会从注册表中查找策略类，并调用其构造函数。
    如果具体策略模块未被导入，则自动导入。

    Args:
        name: 策略名称（与注册表中的键匹配）
        config_path: 策略配置文件路径，传递给策略构造函数。
        **kwargs: 其他传递给策略构造函数的参数。

    Returns:
        策略实例。
    """
    cls = get_strategy_class(name)
    # 实例化（假设构造函数接受 config_path 和 **kwargs）
    try:
        return cls(config_path=config_path, **kwargs)
    except TypeError as e:
        raise TypeError(f"Failed to instantiate strategy '{name}': {e}") from e

# ---------------------------------------------------------------------------
# 预注册内置策略（延迟导入，失败时只记录警告）
# ---------------------------------------------------------------------------
def _register_builtin_strategies() -> None:
    """尝试导入并注册内置的 KHAOS 策略。"""
    try:
        from strategy.khaos.strategy import KhaosStrategy
        register_strategy(KhaosStrategy)
    except ImportError as e:
        logger.warning(f"Built-in KHAOS strategy could not be imported: {e}")
    except Exception as e:
        logger.error(f"Unexpected error registering KHAOS strategy: {e}")

# 在模块加载时自动注册内置策略（但不强制要求必须成功）
_register_builtin_strategies()

# ---------------------------------------------------------------------------
# 版本兼容性检查
# ---------------------------------------------------------------------------
def _check_engine_compatibility() -> None:
    """检查策略模块版本与预期引擎版本是否兼容。"""
    try:
        from packaging import version
        # 此处应从某个全局配置获取引擎版本，为简化示例，假设环境变量 KHAOS_ENGINE_VERSION 存在
        engine_ver = os.environ.get("KHAOS_ENGINE_VERSION", "2.6.0")
        module_ver = __version__
        # 简单比较：模块主次版本应与引擎匹配
        if version.parse(module_ver).major != version.parse(engine_ver).major or \
           version.parse(module_ver).minor != version.parse(engine_ver).minor:
            logger.critical(
                f"Strategy module version {module_ver} is incompatible with engine version {engine_ver}."
                f" Expected {__engine_compatibility__}."
            )
            # 不阻止加载，但发出严重警告，某些场景可抛出异常
            raise RuntimeError(f"Strategy module version mismatch: {module_ver} vs engine {engine_ver}")
    except ImportError:
        logger.warning("Could not check engine compatibility due to missing 'packaging' library.")
    except KeyError as e:
        logger.debug(f"Engine compatibility check skipped: {e}")

# 执行兼容性检查（若环境配置了引擎版本）
import os
_check_engine_compatibility()

# ---------------------------------------------------------------------------
# 公共 API
# ---------------------------------------------------------------------------
__all__ = [
    "AbstractStrategy",
    "register_strategy",
    "get_strategy_class",
    "list_strategies",
    "create_strategy",
    "reload_strategies",
    "__version__",
    "__author__",
    "__license__",
      ]
