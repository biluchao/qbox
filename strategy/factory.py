# -*- coding: utf-8 -*-
"""
Module: strategy.factory
Description: KHAOS 策略工厂。
             提供策略类的发现、实例化、初始化与元数据查询。
             支持自动导入、配置校验、版本兼容性检查与审计记录。
             所有操作线程安全，异常友好。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.6.0
"""

from __future__ import annotations

import copy
import importlib
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Type

from strategy.base import AbstractStrategy, StrategyState

logger = logging.getLogger("khaos.strategy.factory")

# 版本标识
__version__ = "2.6.0"

# ---------------------------------------------------------------------------
# 策略上下文：统一传递引擎服务
# ---------------------------------------------------------------------------
class StrategyContext:
    """封装策略所需的引擎服务，避免传递散乱的 kwargs。"""
    def __init__(self,
                 risk_context: Any = None,
                 audit_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
                 extra: Optional[Dict[str, Any]] = None):
        self.risk_context = risk_context
        self.audit_callback = audit_callback
        self.extra = extra or {}

# ---------------------------------------------------------------------------
# 自定义异常
# ---------------------------------------------------------------------------
class StrategyInstantiationError(Exception):
    """策略实例化失败时抛出。"""

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------
def _resolve_module_path(strategy_name: str) -> str:
    """根据策略名称解析模块路径。
       优先使用环境变量 STRATEGY_MODULE_MAP (JSON: {"KHAOS":"strategy.khaos.strategy"})。
    """
    import json
    map_str = __import__('os').environ.get("STRATEGY_MODULE_MAP", "")
    if map_str:
        try:
            mapping = json.loads(map_str)
            if strategy_name in mapping:
                return mapping[strategy_name]
        except json.JSONDecodeError:
            logger.warning("STRATEGY_MODULE_MAP is not valid JSON.")
    # 默认约定
    return f"strategy.{strategy_name.lower()}.strategy"

# 线程安全缓存（与注册表共享锁）
_import_lock = threading.RLock()
_MODULE_CACHE: Dict[str, Type[AbstractStrategy]] = {}

def _load_strategy_class(name: str) -> Type[AbstractStrategy]:
    """动态加载并返回策略类，优先从注册表获取，否则自动导入。"""
    from strategy import get_strategy_class, register_strategy  # 延迟导入避免循环
    try:
        return get_strategy_class(name)
    except KeyError:
        pass

    module_path = _resolve_module_path(name)
    try:
        with _import_lock:
            module = importlib.import_module(module_path)
    except ImportError as e:
        raise StrategyInstantiationError(
            f"Could not import module '{module_path}' for strategy '{name}'. "
            f"Make sure the module exists and all dependencies are installed. Original error: {e}"
        )

    # 查找目标类
    target_cls = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if not isinstance(attr, type) or not issubclass(attr, AbstractStrategy) or attr is AbstractStrategy:
            continue
        # 优先匹配显式声明的 strategy_name 类属性
        strategy_name_attr = getattr(attr, 'strategy_name', None)
        if strategy_name_attr == name:
            target_cls = attr
            break
        # 其次匹配类名
        if attr.__name__ == name and target_cls is None:
            target_cls = attr
    if target_cls is None:
        raise StrategyInstantiationError(
            f"No AbstractStrategy subclass found in '{module_path}' that matches strategy name '{name}'."
        )

    # 注册
    register_strategy(target_cls)
    with _import_lock:
        _MODULE_CACHE[name] = target_cls
    return target_cls

# ---------------------------------------------------------------------------
# 配置校验
# ---------------------------------------------------------------------------
def _validate_config(cls: Type[AbstractStrategy], config: Dict[str, Any]) -> Dict[str, Any]:
    """使用策略定义的 pydantic 模型（若有）校验并返回配置字典。"""
    if hasattr(cls, 'get_config_model'):
        model_cls = cls.get_config_model()
        if model_cls is not None:
            try:
                # 兼容 pydantic v1/v2
                if hasattr(model_cls, 'model_validate'):
                    validated = model_cls.model_validate(config)
                    return validated.model_dump()
                else:
                    validated = model_cls(**config)
                    return validated.dict()
            except Exception as e:
                raise StrategyInstantiationError(
                    f"Configuration validation failed for strategy '{cls.__name__}': {e}"
                ) from e
    return config

# ---------------------------------------------------------------------------
# 公共工厂方法
# ---------------------------------------------------------------------------
def create_strategy(
    name: str,
    config: Optional[Dict[str, Any]] = None,
    context: Optional[StrategyContext] = None,
    **kwargs
) -> AbstractStrategy:
    """
    根据名称创建策略实例（未初始化）。
    Args:
        name: 策略名称。
        config: 策略参数字典，会自动进行环境变量替换与模型校验。
        context: 策略上下文，包含 risk_context, audit_callback 等。
        **kwargs: 其他传递给策略构造函数的参数，会与 context 合并。
    Returns:
        策略实例。
    Raises:
        StrategyInstantiationError: 任何创建失败情况。
    """
    cls = _load_strategy_class(name)

    # 环境变量替换（若 config 中存在 ${...}）
    if config:
        from config.settings import _substitute_env_vars  # 避免循环导入
        try:
            config = _substitute_env_vars(copy.deepcopy(config))
        except Exception as e:
            raise StrategyInstantiationError(f"Environment variable substitution failed in config: {e}")

    # 配置校验
    config = _validate_config(cls, config)

    # 版本兼容性检查
    try:
        strategy_version = cls.version
    except AttributeError:
        raise StrategyInstantiationError(f"Strategy class '{cls.__name__}' must define a 'version' class attribute.")
    # 此处可调用引擎版本检查（示例使用环境变量）
    import os
    engine_version = os.environ.get("KHAOS_ENGINE_VERSION", "0.0.0")
    if not _version_compatible(strategy_version, engine_version):
        logger.warning(
            f"Strategy '{name}' version {strategy_version} may be incompatible with engine version {engine_version}."
        )

    # 构造参数
    init_kwargs = {}
    if context:
        init_kwargs["risk_context"] = context.risk_context
        init_kwargs["audit_callback"] = context.audit_callback
        if context.extra:
            init_kwargs.update(context.extra)
    init_kwargs.update(kwargs)

    try:
        instance = cls(config=config, **init_kwargs)
    except Exception as e:
        logger.exception(f"Failed to instantiate strategy '{name}'")
        raise StrategyInstantiationError(f"Instantiation of '{name}' failed: {e}") from e

    logger.info(f"Strategy instance created: {instance}")
    return instance


async def create_and_initialize(
    name: str,
    config: Optional[Dict[str, Any]] = None,
    context: Optional[StrategyContext] = None,
    **kwargs
) -> AbstractStrategy:
    """创建策略并自动调用 initialize()。如果初始化失败，抛出异常。"""
    instance = create_strategy(name, config, context, **kwargs)
    try:
        success = await instance.initialize()
    except Exception as e:
        raise StrategyInstantiationError(f"Strategy '{name}' initialize() raised an exception: {e}") from e
    if not success:
        raise StrategyInstantiationError(f"Strategy '{name}' initialize() returned False.")
    return instance


def create_strategy_from_config(
    config_dict: Dict[str, Any],
    context: Optional[StrategyContext] = None
) -> AbstractStrategy:
    """
    从配置字典创建策略。支持两种结构：
    1. {"type": "KHAOS", "config": {...}}  （传统方式）
    2. {"name": "KHAOS", "params": {...}}   （兼容方式）
    """
    name = config_dict.get("type") or config_dict.get("name")
    if not name:
        raise StrategyInstantiationError("Configuration must contain 'type' or 'name' field.")
    params = config_dict.get("config") or config_dict.get("params", {})
    return create_strategy(name, config=params, context=context)


def list_strategies() -> List[Dict[str, Any]]:
    """列出所有已注册策略的元信息。"""
    from strategy import list_strategies as _list
    return _list()


def get_strategy_meta(name: str) -> Dict[str, Any]:
    """获取指定策略的元信息。"""
    from strategy import list_strategies as _list
    for info in _list():
        if info["name"] == name:
            return info
    raise KeyError(f"Strategy '{name}' not found")


def dry_run_instantiate(name: str, config: Optional[Dict[str, Any]] = None) -> AbstractStrategy:
    """试实例化策略但不初始化，用于测试配置有效性。"""
    return create_strategy(name, config=config, context=None)


# ---------------------------------------------------------------------------
# 版本兼容性工具
# ---------------------------------------------------------------------------
def _version_compatible(strategy_ver: str, engine_ver: str) -> bool:
    from packaging import version
    try:
        strat = version.parse(strategy_ver)
        eng = version.parse(engine_ver)
        # 主版本必须相同，次版本策略 <= 引擎
        return strat.major == eng.major and strat.minor <= eng.minor
    except Exception:
        return True  # 不阻断

# ---------------------------------------------------------------------------
# 环境变量替换导入
# ---------------------------------------------------------------------------
try:
    from config.settings import _substitute_env_vars
except ImportError:
    _substitute_env_vars = lambda x: x  # 降级不处理

# ---------------------------------------------------------------------------
__all__ = [
    "create_strategy",
    "create_and_initialize",
    "create_strategy_from_config",
    "list_strategies",
    "get_strategy_meta",
    "dry_run_instantiate",
    "StrategyContext",
    "StrategyInstantiationError",
  ]
