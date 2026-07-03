# -*- coding: utf-8 -*-
"""
Module: strategy.base
Description: KHAOS 策略抽象基类。
             所有具体策略必须继承此类并实现其抽象方法。
             定义了与引擎交互的完整协议，包括行情回调、信号生成、
             风控校验、状态管理、审计钩子和生命周期管理。
             符合全球顶尖量化基金对策略接口的合规与安全要求。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.6.0
Compatible Engine: >=2.6.0, <3.0.0
"""

from __future__ import annotations

import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

# ---------------------------------------------------------------------------
# 核心事件对象（生产环境应从 core.events 导入，此处为自包含示例）
# ---------------------------------------------------------------------------
@dataclass
class Bar:
    """标准K线数据，兼容币安等交易所格式。"""
    symbol: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float = 0.0
    trades: int = 0
    timeframe: str = "3m"

@dataclass
class Signal:
    """交易信号，包含唯一标识、方向、数量、有效期等信息。"""
    symbol: str
    direction: str                      # LONG / SHORT / CLOSE
    size: float
    order_type: str = "LIMIT"
    limit_price: Optional[float] = None
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: int = 60               # 超过此时间未成交则自动取消

@dataclass
class Position:
    """持仓信息（引擎提供，策略只读）。"""
    symbol: str
    side: str
    quantity: float
    avg_entry_price: float
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    market_value: float = 0.0

class StrategyState(Enum):
    """策略生命周期状态。"""
    CREATED = "CREATED"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

# ---------------------------------------------------------------------------
# 抽象策略基类
# ---------------------------------------------------------------------------
class AbstractStrategy(ABC):
    """
    抽象策略基类，定义了与 KHAOS 引擎交互的标准协议。

    子类必须实现：
        - version (类属性)
        - on_bar

    子类可选覆盖：
        - initialize
        - shutdown
        - on_tick
        - on_position_update
        - on_order_filled
        - on_order_cancelled
        - validate_signal
        - reset

    生命周期：
        1. 引擎加载配置，调用 __init__ 创建实例。
        2. 引擎注入风险上下文（RiskContext）和审计回调（audit_callback）。
        3. 引擎调用 await strategy.initialize() -> bool。
        4. 若初始化成功，引擎调用 on_bar 处理K线并获取信号。
        5. 信号返回前自动通过 validate_signal 过滤。
        6. 引擎关闭前调用 await strategy.shutdown()。
    """

    version: str  # 子类必须覆盖

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Args:
            config: 策略参数字典，从 YAML 配置文件的 modules 段加载。
        """
        self.config = config or {}
        self.name: str = self.__class__.__name__
        self.state: StrategyState = StrategyState.CREATED
        self.logger = logging.getLogger(f"khaos.strategy.{self.name}")

        # 由引擎注入的外部依赖
        self.risk_context: Optional[Any] = None   # RiskContext 实例
        self.audit_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None

    # ---- 抽象方法 ----

    @abstractmethod
    async def on_bar(self, bar: Bar, equity: float) -> Optional[Signal]:
        """
        接收一根K线，计算并返回交易信号。

        Args:
            bar: 当前K线数据对象。
            equity: 当前账户权益（用于仓位计算）。

        Returns:
            交易信号对象，若无信号则返回 None。

        Raises:
            StrategyError: 当策略内部出现不可恢复的错误时。
        """
        ...

    # ---- 可选回调 ----

    async def on_tick(self, tick: Any) -> Optional[Signal]:
        """接收逐笔成交（可选实现）。"""
        return None

    async def on_position_update(self, positions: Dict[str, Position]) -> None:
        """持仓更新通知。"""
        pass

    async def on_order_filled(self, order: Any) -> None:
        """订单完全成交回调。"""
        pass

    async def on_order_cancelled(self, order: Any) -> None:
        """订单取消回调。"""
        pass

    # ---- 生命周期管理 ----

    async def initialize(self, **kwargs) -> bool:
        """
        策略初始化，例如加载历史数据、预热指标等。

        Returns:
            True 表示初始化成功，False 表示失败（引擎将标记策略为 ERROR 状态）。
        """
        self.state = StrategyState.INITIALIZING
        # 子类覆盖时需先调用 super()
        self.state = StrategyState.RUNNING
        self.logger.info(f"Strategy {self.name} initialized.")
        return True

    async def shutdown(self) -> None:
        """策略关闭清理。"""
        self.state = StrategyState.STOPPING
        self.logger.info(f"Strategy {self.name} shutting down.")
        self.state = StrategyState.STOPPED

    # ---- 风控与信号校验 ----

    async def validate_signal(self, signal: Signal) -> bool:
        """
        在信号发出前执行风控校验。
        如果注入的 risk_context 可用，则委托其检查；子类也可覆盖添加自定义逻辑。

        Returns:
            True 表示信号通过，可发送；False 则丢弃。
        """
        if self.risk_context and hasattr(self.risk_context, 'check_signal'):
            return await self.risk_context.check_signal(signal, self)
        return True

    # ---- 审计 ----

    async def _audit(self, event: str, details: Dict[str, Any]) -> None:
        """发送审计事件（由引擎注入的 audit_callback 处理）。"""
        if self.audit_callback:
            try:
                await self.audit_callback(event, {**details, "strategy": self.name})
            except Exception as e:
                self.logger.error(f"Audit callback failed: {e}")

    # ---- 工具方法 ----

    @classmethod
    def get_config_model(cls) -> Optional[type]:
        """
        返回用于验证配置的 pydantic 模型类（可选）。
        子类可覆盖以启用配置自动校验。
        """
        return None

    async def reset(self) -> None:
        """重置策略到初始状态（用于回测/优化）。"""
        self.logger.info("Strategy reset.")

    def __repr__(self) -> str:
        return f"{self.name}(version={self.version}, state={self.state.value})"
