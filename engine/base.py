# -*- coding: utf-8 -*-
"""
Module: engine.base
Description: KHAOS 事件驱动引擎抽象基类（生产级，修复版）。
             提供严格的状态机、安全审计、异步资源管理和完善的错误处理。
             适用于万亿美金账户级别的极致安全要求。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.5.1
Compatible Engine Versions: >=2.5.0, <3.0.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, Union

from pydantic import BaseModel, Field
from strategy.base import AbstractStrategy, Bar, Signal

logger = logging.getLogger("khaos.engine.base")

# ---------------------------------------------------------------------------
# 辅助数据类型
# ---------------------------------------------------------------------------
@dataclass
class OrderResult:
    """订单执行结果，支持部分成交、拒绝等状态。"""
    success: bool
    filled_size: float = 0.0
    message: str = ""

# ---------------------------------------------------------------------------
# 引擎配置模型（强类型验证）
# ---------------------------------------------------------------------------
class EngineConfig(BaseModel):
    mode: str = "paper"
    strategy_class: Optional[Union[Type[AbstractStrategy], Callable[..., AbstractStrategy]]] = None
    strategy_config: Dict[str, Any] = Field(default_factory=dict)
    risk_context: Any = None
    audit_callback: Optional[Callable[[str, Dict[str, Any]], Any]] = None
    heartbeat_interval: float = Field(default=1.0, ge=0.1, le=60.0)
    max_latency_ms: float = Field(default=100.0, ge=0.0)
    max_bars_per_run: Optional[int] = None
    shutdown_timeout_sec: float = 10.0

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"

# ---------------------------------------------------------------------------
# 引擎状态
# ---------------------------------------------------------------------------
class EngineState(Enum):
    CREATED = "CREATED"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

# ---------------------------------------------------------------------------
# 抽象基类
# ---------------------------------------------------------------------------
class BaseEngine(ABC):
    """
    事件驱动交易引擎抽象基类。

    子类必须实现所有抽象方法，确保：
    - 信号执行不应抛出异常，而是返回 OrderResult。
    - _connect / _disconnect 保证连接正确建立和释放。
    - _get_equity 必须返回最新权益。

    生命周期：
        CREATED -> INITIALIZING -> RUNNING -> PAUSED/RUNNING -> STOPPING -> STOPPED
        任何状态都可能进入 ERROR，可通过 reset_error() 恢复至 CREATED。
    """

    def __init__(self, config: Optional[EngineConfig] = None):
        self.config = config or EngineConfig()
        self.strategy: Optional[AbstractStrategy] = None
        self.state = EngineState.CREATED
        self._bar_count: int = 0
        self._last_bar_time: Optional[float] = None
        self._total_latency_ms: float = 0.0
        self._shutdown_event = asyncio.Event()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._run_task: Optional[asyncio.Task] = None
        self._observers: List[Callable[[str, Any], None]] = []  # 实例级观察者列表
        self._last_valid_timestamp: Optional[int] = None

    # ---- 抽象方法 ----
    @abstractmethod
    async def _on_bar(self, bar: Bar) -> Optional[Signal]:
        """由子类实现的K线回调，返回信号。"""
        ...

    @abstractmethod
    async def _execute_signal(self, signal: Signal) -> OrderResult:
        """
        执行信号，返回订单结果。
        实现时必须捕获所有异常，不应抛出。
        """
        ...

    @abstractmethod
    async def _get_equity(self) -> float:
        """返回当前账户权益。"""
        ...

    @abstractmethod
    async def _fetch_positions(self) -> Dict[str, Any]:
        """获取当前持仓。"""
        ...

    @abstractmethod
    async def _connect(self) -> None:
        """建立数据/交易连接。失败应抛出异常。"""
        ...

    @abstractmethod
    async def _disconnect(self) -> None:
        """断开连接并清理。不应抛出异常，所有错误内部捕获。"""
        ...

    # ---- 生命周期管理 ----
    async def initialize(self) -> None:
        if self.state not in (EngineState.CREATED, EngineState.ERROR):
            raise RuntimeError(f"Cannot initialize in state {self.state}")
        self.state = EngineState.INITIALIZING
        logger.info("Initializing engine...")

        # 创建策略实例
        if self.strategy is None and self.config.strategy_class is not None:
            factory = self.config.strategy_class
            kwargs = dict(
                config=self.config.strategy_config,
                risk_context=self.config.risk_context,
                audit_callback=self.config.audit_callback,
            )
            try:
                if isinstance(factory, type) and issubclass(factory, AbstractStrategy):
                    self.strategy = factory(**kwargs)
                elif callable(factory):
                    self.strategy = factory(**kwargs)
                else:
                    raise ValueError("Invalid strategy_class.")
            except Exception as e:
                logger.exception("Failed to instantiate strategy.")
                raise RuntimeError("Strategy instantiation failed.") from e
        if self.strategy is None:
            raise RuntimeError("No strategy instance available.")

        success = await self.strategy.initialize()
        if not success:
            self.state = EngineState.ERROR
            raise RuntimeError("Strategy initialization returned False.")

        try:
            await self._connect()
        except Exception:
            self.state = EngineState.ERROR
            logger.exception("Connection failed during initialization.")
            raise
        self.state = EngineState.RUNNING
        logger.info("Engine initialized successfully.")

    async def run(self) -> None:
        if self.state != EngineState.RUNNING:
            raise RuntimeError("Engine must be in RUNNING state to start.")
        if self.strategy is None:
            raise RuntimeError("Strategy not set.")

        # 保存任务引用，确保 shutdown 能等待
        self._run_task = asyncio.current_task()
        self._heartbeat_task = asyncio.create_task(self._heartbeat())
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("Run task cancelled.")
        finally:
            await self._cleanup()

    async def pause(self) -> None:
        if self.state == EngineState.RUNNING:
            self.state = EngineState.PAUSED
            logger.info("Engine paused.")

    async def resume(self) -> None:
        if self.state == EngineState.PAUSED:
            self.state = EngineState.RUNNING
            logger.info("Engine resumed.")

    async def shutdown(self) -> None:
        if self.state in (EngineState.STOPPED, EngineState.STOPPING):
            return
        self.state = EngineState.STOPPING
        logger.info("Shutting down engine...")
        self._shutdown_event.set()

        if self._run_task and not self._run_task.done():
            try:
                await asyncio.wait_for(self._run_task, timeout=self.config.shutdown_timeout_sec)
            except asyncio.TimeoutError:
                logger.warning(f"Run task did not finish within {self.config.shutdown_timeout_sec}s, cancelling.")
                self._run_task.cancel()
        self.state = EngineState.STOPPED
        logger.info("Engine stopped.")

    def reset_error(self) -> None:
        if self.state == EngineState.ERROR:
            logger.warning("Resetting engine from ERROR state.")
            self._total_latency_ms = 0.0
            self._last_bar_time = None
            self._bar_count = 0
            self._shutdown_event.clear()
            self.state = EngineState.CREATED

    async def _cleanup(self) -> None:
        """内部清理，确保所有资源释放（即使部分步骤失败）。"""
        # 停止心跳
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # 关闭策略
        if self.strategy:
            try:
                await self.strategy.shutdown()
            except Exception as e:
                logger.error(f"Strategy shutdown error: {e}")

        # 断开连接
        try:
            await self._disconnect()
        except Exception as e:
            logger.error(f"Disconnect error: {e}")

        # 取消运行任务（如果仍在）
        if self._run_task and not self._run_task.done():
            self._run_task.cancel()

    # ---- 核心处理流程 ----
    async def _process_bar(self, bar: Bar) -> None:
        """处理单根K线的标准流程，包含校验、风控、审计和延迟监控。"""
        start = time.monotonic()
        try:
            # PAUSED 状态不处理行情
            if self.state not in (EngineState.RUNNING,):
                return
            if not self._validate_bar(bar):
                logger.warning(f"Invalid bar received: {bar}")
                return
            self._bar_count += 1
            self._last_bar_time = start

            signal = await self.strategy.on_bar(bar, await self._get_equity())
            if signal is None:
                return

            # 基本信号校验
            if signal.size <= 0:
                logger.warning(f"Signal with non-positive size ignored: {signal}")
                return

            # 策略验证
            validated = True
            if hasattr(self.strategy, 'validate_signal') and callable(self.strategy.validate_signal):
                validated = await self.strategy.validate_signal(signal)
            if not validated:
                logger.info(f"Signal rejected by strategy validation: {signal}")
                await self._safe_audit("signal_rejected", {
                    "signal_id": signal.signal_id,
                    "symbol": signal.symbol,
                    "direction": signal.direction,
                    "size": signal.size,
                    "reason": "strategy_validation",
                    "bar_time": bar.open_time
                })
                return

            # 执行
            result = await self._execute_signal(signal)
            if not result.success:
                logger.warning(f"Order execution failed: {result.message}")
            await self._safe_audit("signal_executed", {
                "signal_id": signal.signal_id,
                "symbol": signal.symbol,
                "direction": signal.direction,
                "size": signal.size,
                "filled": result.filled_size,
                "success": result.success,
                "bar_time": bar.open_time
            })
        except Exception as e:
            logger.exception(f"Unhandled error processing bar: {e}")
            self.state = EngineState.ERROR
            self._notify("error", {"exception": str(e), "bar_time": getattr(bar, 'open_time', None)})
        finally:
            elapsed = (time.monotonic() - start) * 1000
            self._total_latency_ms += elapsed
            if elapsed > self.config.max_latency_ms:
                logger.warning(f"Bar processing latency {elapsed:.2f} ms exceeded limit {self.config.max_latency_ms} ms")

    def _validate_bar(self, bar: Bar) -> bool:
        if bar is None:
            return False
        if bar.high < bar.low or bar.close <= 0:
            return False
        # 时间单调性检查（可选）
        if self._last_valid_timestamp is not None and bar.open_time < self._last_valid_timestamp:
            logger.warning(f"Bar timestamp decreased: {bar.open_time} < {self._last_valid_timestamp}")
            return False
        self._last_valid_timestamp = bar.open_time
        return True

    async def _safe_audit(self, event: str, details: Dict[str, Any]) -> None:
        if self.config.audit_callback is None:
            return
        try:
            # 审计回调可能是同步或异步
            result = self.config.audit_callback(event, details)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"Audit callback error for event {event}: {e}")

    # ---- 心跳与监控 ----
    async def _heartbeat(self) -> None:
        while self.state in (EngineState.RUNNING, EngineState.PAUSED):
            if self._shutdown_event.is_set():
                break
            await asyncio.sleep(self.config.heartbeat_interval)
            avg_lat = self._total_latency_ms / max(self._bar_count, 1)
            logger.debug(f"Heartbeat: bars={self._bar_count}, avg_lat={avg_lat:.2f}ms, state={self.state.value}")

    # ---- 事件总线 ----
    def register_observer(self, callback: Callable[[str, Any], None]) -> None:
        self._observers.append(callback)

    def _notify(self, event_type: str, data: Any) -> None:
        for obs in self._observers:
            try:
                obs(event_type, data)
            except Exception:
                logger.exception("Observer notification failed")

    # ---- 公开属性 ----
    @property
    def bar_count(self) -> int:
        return self._bar_count

    @property
    def average_latency_ms(self) -> float:
        return self._total_latency_ms / max(self._bar_count, 1)

    def __repr__(self) -> str:
        state_val = self.state.value if self.state else 'UNKNOWN'
        return f"<{self.__class__.__name__} state={state_val} bars={self._bar_count}>"
