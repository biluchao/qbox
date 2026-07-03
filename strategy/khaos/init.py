# -*- coding: utf-8 -*-
"""
Module: strategy.khaos
Description: KHAOS 策略子包。
             提供 KHAOS 策略实现、组装器及配置校验模型。
             通过惰性加载自动注册到全局策略注册表，支持版本兼容性检查。
             所有符号均可通过 `from strategy.khaos import ...` 按需加载，
             避免不必要的重依赖初始化。

Examples:
    from strategy.khaos import KhaosStrategy, assemble_strategy

    # 使用默认配置创建策略
    strategy = assemble_strategy(config_path='config/strategy.prod.yaml')

    # 查看已注册策略
    from strategy import list_strategies
    print(list_strategies())

Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.7.3
License: Proprietary
Copyright: KHAOS Fund, 2026. All rights reserved.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("khaos.strategy.khaos")

# ---------------------------------------------------------------------------
# 模块元信息
# ---------------------------------------------------------------------------
__version__ = "2.7.3"
__author__ = "KHAOS Quant Team"
__license__ = "Proprietary"
__copyright__ = "Copyright 2026 KHAOS Fund. All rights reserved."

# ---------------------------------------------------------------------------
# 动态路径
# ---------------------------------------------------------------------------
_BASE_DIR = Path(__file__).resolve().parent.parent.parent  # 项目根目录
DEFAULT_CONFIG_PATH = os.environ.get(
    "KHAOS_STRATEGY_CONFIG_PATH",
    str(_BASE_DIR / "config" / "strategy.default.yaml")
)

# ---------------------------------------------------------------------------
# 线程安全与惰性加载基础设施
# ---------------------------------------------------------------------------
_import_lock = threading.Lock()
_loaded = False

# 存储所有惰性加载的属性名
_LAZY_ATTRS = {
    "KhaosStrategy",
    "assemble_strategy",
    "assemble_from_model",
    "reload_strategy",
    "KhaosContainer",
    "ModulesConfig",
    "AbstractStrategy",
}

# 用于动态调整的符号集合，部分属性依赖可选的 assembler
_OPTIONAL_ASSEMBLER_ATTRS = {
    "assemble_strategy",
    "assemble_from_model",
    "reload_strategy",
    "KhaosContainer",
    "ModulesConfig",
}

def _perform_imports():
    """一次性导入所有核心类与函数（线程安全）。"""
    global _loaded
    if _loaded:
        return
    with _import_lock:
        if _loaded:
            return

        # 强制要求 numpy 可用
        try:
            import numpy as np  # noqa: F401
        except ImportError as e:
            raise RuntimeError("numpy is required by KHAOS strategy but not installed.") from e

        # 导入策略核心
        from strategy.khaos.strategy import KhaosStrategy as _KS
        from strategy.base import AbstractStrategy as _AS

        # 尝试导入组装器（可能失败）
        try:
            from strategy.khaos.assembler import (
                assemble_strategy as _assemble,
                assemble_from_model as _assemble_model,
                reload_strategy as _reload,
                KhaosContainer as _Container,
                ModulesConfig as _ModCfg,
            )
            _has_assembler = True
        except ImportError:
            logger.warning("Assembler module not available; some functions will be absent.")
            _assemble = None
            _assemble_model = None
            _reload = None
            _Container = None
            _ModCfg = None
            _has_assembler = False

        # 将所有符号注入模块全局命名空间
        module_globals = globals()
        module_globals["KhaosStrategy"] = _KS
        module_globals["AbstractStrategy"] = _AS
        if _has_assembler:
            module_globals["assemble_strategy"] = _assemble
            module_globals["assemble_from_model"] = _assemble_model
            module_globals["reload_strategy"] = _reload
            module_globals["KhaosContainer"] = _Container
            module_globals["ModulesConfig"] = _ModCfg

        # 注册到全局策略表
        try:
            from strategy import register_strategy
            if issubclass(_KS, _AS):
                register_strategy(_KS)
                logger.info("KHAOS strategy registered in global registry.")
        except Exception as e:
            logger.error("Failed to register KHAOS strategy in global registry: %s", e)

        _loaded = True

# ---------------------------------------------------------------------------
# 惰性属性访问
# ---------------------------------------------------------------------------
def __getattr__(name: str):
    if name in _LAZY_ATTRS:
        _perform_imports()
        # 现在属性应该已经在模块全局中
        try:
            return globals()[name]
        except KeyError:
            if name in _OPTIONAL_ASSEMBLER_ATTRS and not globals().get("assemble_strategy"):
                raise RuntimeError(
                    f"'{name}' is unavailable because the assembler module could not be loaded."
                )
            raise AttributeError(f"module 'strategy.khaos' has no attribute '{name}'")
    raise AttributeError(f"module 'strategy.khaos' has no attribute '{name}'")

def __dir__():
    base = list(__all__) if "__all__" in globals() else []
    return base + list(_LAZY_ATTRS)

# ---------------------------------------------------------------------------
# 动态 __all__ 构建（根据可用性）
# ---------------------------------------------------------------------------
_dynamic_all = ["__version__", "__author__", "__license__", "DEFAULT_CONFIG_PATH",
                "create_from_config_file", "health_check", "get_active_config"]

# 始终可用的符号
_dynamic_all.extend(["KhaosStrategy", "AbstractStrategy"])

# 仅当组装器可用时才导出
# 我们在这里无法预知，但在 _perform_imports 后可以更新，但模块级 __all__ 应保持静态，
# 因此将所有可能符号列出，但通过 __getattr__ 保护访问。
__all__ = [
    "KhaosStrategy",
    "AbstractStrategy",
    "assemble_strategy",
    "assemble_from_model",
    "reload_strategy",
    "KhaosContainer",
    "ModulesConfig",
    "create_from_config_file",
    "health_check",
    "get_active_config",
    "DEFAULT_CONFIG_PATH",
    "__version__",
    "__author__",
    "__license__",
]

# ---------------------------------------------------------------------------
# 便捷函数
# ---------------------------------------------------------------------------
def create_from_config_file(
    config_path: Optional[str] = None,
    risk_context: Any = None,
    audit_callback: Any = None,
):
    """
    从 YAML 配置文件创建策略实例。

    Args:
        config_path: 配置文件路径，默认使用 DEFAULT_CONFIG_PATH。
        risk_context: 风控上下文。
        audit_callback: 审计回调。

    Returns:
        KhaosStrategy 实例。
    """
    path = Path(config_path or DEFAULT_CONFIG_PATH).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    # 触发惰性导入
    _perform_imports()
    assembler = globals().get("assemble_strategy")
    if assembler is None:
        raise RuntimeError("Assembler is not available. Cannot create strategy.")
    return assembler(config_path=str(path), risk_context=risk_context, audit_callback=audit_callback)

def health_check() -> Dict[str, Any]:
    """执行快速健康检查，验证核心组件是否可用。"""
    result = {"strategy_class": False, "assembler": False, "numpy": False}
    try:
        _perform_imports()
        result["strategy_class"] = globals().get("KhaosStrategy") is not None
        result["assembler"] = globals().get("assemble_strategy") is not None
    except Exception as e:
        result["error"] = str(e)
    try:
        import numpy as np
        result["numpy"] = True
    except ImportError:
        pass
    return result

def get_active_config() -> Optional[Dict[str, Any]]:
    """尝试从全局配置管理器获取当前活动配置。"""
    try:
        from config.settings import get_config
        cfg = get_config()
        return cfg.model_dump() if hasattr(cfg, "model_dump") else cfg.dict()
    except Exception:
        return None

# ---------------------------------------------------------------------------
# 模块repr
# ---------------------------------------------------------------------------
def __repr__():
    return f"<module 'strategy.khaos' version={__version__}>"
