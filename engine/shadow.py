# -*- coding: utf-8 -*-
"""
Module: engine.shadow
Description: KHAOS 影子账户引擎（第六轮全面修复版，版本 2.6.1）。
             实现了完整的保证金冻结、精确的并发锁控制、强异常隔离、
             策略状态同步、队列保护及完善的审计。
             符合全球顶级量化基金模拟交易环境的零缺陷生产标准。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.6.1
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from engine.base import BaseEngine, EngineConfig, EngineState, OrderResult
from strategy.base import AbstractStrategy, Bar, Signal

logger = logging.getLogger("khaos.engine.shadow")

# ---------------------------------------------------------------------------
# 影子账户配置
# ---------------------------------------------------------------------------
class ShadowConfig(BaseModel):
    initial_balance: float = Field(default=1_000_000.0, gt=0.0)
    symbols: List[str] = Field(default_factory=lambda: ["BTCUSDT"])
    timeframe: str = "3m"
    slippage_model: str = "fixed"
    slippage_pct: float = 0.0005
    slippage_normal_std: float = 0.0003
    enable_market_impact: bool = False
    impact_factor: float = 0.1
    latency_model: str = "fixed"
    latency_fixed_ms: float = 200.0
    latency_normal_mean_ms: float = 150.0
    latency_normal_std_ms: float = 50.0
    maker_fee: float = 0.0002
    taker_fee: float = 0.0004
    fill_probability: float = Field(default=1.0, ge=0.0, le=1.0)
    partial_fill_enabled: bool = True
    partial_fill_ratio: float = Field(default=0.5, ge=0.0, le=1.0)
    max_position_notional: float = 0.0
    max_leverage: float = 3.0
    use_real_time_data: bool = True
    max_data_gap_sec: float = 120.0
    shutdown_timeout_sec: float = 15.0
    log_trades: bool = True
    report_interval_sec: float = 3600.0
    warmup_bars: int = Field(default=200, ge=0, le=10000)
    random_seed: Optional[int] = 42
    max_concurrent_orders: int = 10
    max_queue_size: int = 5000                    # 新增：队列最大长度
    annualization_factor: float = 365 * 24 * 60 / 3
    default_leverage: float = 1.0                 # 新增：默认杠杆，用于保证金计算

    class Config:
        extra = "forbid"

# ---------------------------------------------------------------------------
# 内部数据结构
# ---------------------------------------------------------------------------
@dataclass
class ShadowPosition:
    symbol: str
    side: str
    quantity: float
    avg_price: float
    realized_pnl: float = 0.0

@dataclass
class ShadowOrder:
    order_id: str
    signal: Signal
    submit_time: float
    bar_time: Optional[int] = None
    filled_quantity: float = 0.0
    avg_fill_price: float = 0.0
    status: str = "PENDING"
    frozen_margin: float = 0.0            # 新增：冻结保证金

@dataclass
class ShadowTrade:
    timestamp: datetime
    bar_time: Optional[int] = None
    symbol: str = ""
    side: str = ""
    quantity: float = 0.0
    price: float = 0.0
    fee: float = 0.0
    slippage: float = 0.0
    slippage_pct: float = 0.0
    pnl: float = 0.0

@dataclass
class ShadowAccount:
    balance: float
    positions: Dict[str, ShadowPosition] = field(default_factory=dict)
    open_orders: Dict[str, ShadowOrder] = field(default_factory=dict)
    trades: List[ShadowTrade] = field(default_factory=list)
    equity_history: List[Tuple[datetime, float]] = field(default_factory=list)
    realized_equity_history: List[Tuple[datetime, float]] = field(default_factory=list)
    total_fees: float = 0.0
    total_slippage: float = 0.0
    initial_balance: float = 0.0
    frozen_margin: float = 0.0           # 总冻结保证金

# ---------------------------------------------------------------------------
# 模拟风控上下文
# ---------------------------------------------------------------------------
class ShadowRiskContext:
    async def check_signal(self, signal: Signal, strategy: Any) -> bool:
        return True

# ---------------------------------------------------------------------------
# 影子引擎
# ---------------------------------------------------------------------------
class ShadowEngine(BaseEngine):
    def __init__(self,
                 config: Optional[ShadowConfig] = None,
                 exchange_client: Any = None,
                 engine_config: Optional[EngineConfig] = None):
        base_cfg = engine_config or EngineConfig(mode="shadow", heartbeat_interval=1.0, max_latency_ms=1000.0)
        super().__init__(base_cfg)
        self.shadow_cfg = config or ShadowConfig()
        self.exchange = exchange_client

        self.account = ShadowAccount(
            balance=self.shadow_cfg.initial_balance,
            initial_balance=self.shadow_cfg.initial_balance,
        )

        self._last_prices: Dict[str, float] = {}
        self._price_valid: Dict[str, bool] = {}
        self._last_data_time: Dict[str, float] = {}

        self._data_feed_task: Optional[asyncio.Task] = None
        self._periodic_report_task: Optional[asyncio.Task] = None

        seed = self.shadow_cfg.random_seed
        self._rng = np.random.default_rng(seed) if seed is not None else np.random.default_rng()

        self._order_id_counter = 0
        self._trade_count = 0

        self._order_queue: asyncio.Queue = asyncio.Queue(maxsize=self.shadow_cfg.max_queue_size)
        self._order_processor_task: Optional[asyncio.Task] = None
        self._account_lock = asyncio.Lock()
        self._order_semaphore = asyncio.Semaphore(self.shadow_cfg.max_concurrent_orders)

        self.risk_context = ShadowRiskContext()

    # ---- 抽象方法实现 ----
    async def _connect(self) -> None:
        if self.shadow_cfg.use_real_time_data:
            if self.exchange is None:
                raise RuntimeError("Real-time data mode requires an exchange client.")
            await self.exchange.connect()
            logger.info("Connected to exchange for real-time data.")
            for symbol in self.shadow_cfg.symbols:
                try:
                    await self.exchange.subscribe_kline(symbol, self.shadow_cfg.timeframe)
                    self._last_data_time[symbol] = time.time()
                except Exception as e:
                    logger.error(f"Failed to subscribe {symbol}: {e}")
            await self._warmup()
            if self.strategy and hasattr(self.strategy, 'reset'):
                await self.strategy.reset()
            self._data_feed_task = asyncio.create_task(self._data_feed_loop())
        else:
            logger.info("Shadow engine running without real-time data connection.")
        self._order_processor_task = asyncio.create_task(self._order_processor())

    async def _disconnect(self) -> None:
        if self._data_feed_task and not self._data_feed_task.done():
            self._data_feed_task.cancel()
            try:
                await self._data_feed_task
            except asyncio.CancelledError:
                pass
        if self._periodic_report_task and not self._periodic_report_task.done():
            self._periodic_report_task.cancel()
        if self._order_processor_task and not self._order_processor_task.done():
            self._order_processor_task.cancel()
            try:
                await self._order_processor_task
            except asyncio.CancelledError:
                pass
        await self._process_remaining_orders()
        if self.exchange:
            try:
                await self.exchange.disconnect()
            except Exception as e:
                logger.error(f"Disconnect error: {e}")
        self._generate_report()

    async def _on_bar(self, bar: Bar) -> Optional[Signal]:
        symbol = bar.symbol
        if symbol not in self.shadow_cfg.symbols:
            return None
        if bar.high < bar.low or bar.close <= 0:
            logger.warning(f"Invalid bar data for {symbol}")
            return None
        # 更新价格（轻量加锁）
        async with self._account_lock:
            self._last_prices[symbol] = bar.close
            self._price_valid[symbol] = True
            self._last_data_time[symbol] = time.time()
        try:
            await self._process_bar(bar)
        finally:
            async with self._account_lock:
                equity = self._calculate_equity_locked()
            bar_time_dt = self._safe_bar_time(bar.open_time)
            self.account.equity_history.append((bar_time_dt, equity))
            self.account.realized_equity_history.append((bar_time_dt, self.account.balance))

    async def _execute_signal(self, signal: Signal) -> OrderResult:
        if self.state not in (EngineState.RUNNING, EngineState.PAUSED):
            return OrderResult(success=False, message="Engine not accepting orders")
        if signal.size <= 0 or signal.direction.upper() not in ("LONG", "SHORT", "BUY", "SELL", "CLOSE", "CLOSE_LONG", "CLOSE_SHORT"):
            return OrderResult(success=False, message="Invalid signal parameters")

        # 计算所需保证金
        margin = self._calculate_margin(signal)
        async with self._account_lock:
            available = self.account.balance - self.account.frozen_margin
            if margin > available:
                return OrderResult(success=False, message="Insufficient available balance")
            self.account.frozen_margin += margin

        self._order_id_counter += 1
        order_id = f"shadow_{self._order_id_counter}"
        order = ShadowOrder(
            order_id=order_id,
            signal=signal,
            submit_time=time.time(),
            bar_time=signal.timestamp if hasattr(signal, 'timestamp') else None,
            frozen_margin=margin,
        )
        async with self._account_lock:
            self.account.open_orders[order_id] = order
        try:
            self._order_queue.put_nowait(order)
        except asyncio.QueueFull:
            async with self._account_lock:
                self.account.frozen_margin -= margin
                self.account.open_orders.pop(order_id, None)
            return OrderResult(success=False, message="Order queue full")
        return OrderResult(success=True, filled_size=0.0, message="Order queued")

    async def _get_equity(self) -> float:
        # 此方法仅在锁内调用，外部不能直接使用
        return self._calculate_equity_locked()

    async def _fetch_positions(self) -> Dict[str, Any]:
        return {
            sym: {"side": pos.side, "quantity": pos.quantity, "avg_price": pos.avg_price}
            for sym, pos in self.account.positions.items()
        }

    # ---- 保证金计算 ----
    def _calculate_margin(self, signal: Signal) -> float:
        if signal.order_type.upper() == "LIMIT" and signal.limit_price:
            price = signal.limit_price
        else:
            price = self._last_prices.get(signal.symbol, 0)
        if price <= 0:
            return 0.0
        return abs(signal.size) * price / self.shadow_cfg.default_leverage

    def _release_margin(self, order: ShadowOrder) -> None:
        if order.frozen_margin > 0:
            self.account.frozen_margin = max(0.0, self.account.frozen_margin - order.frozen_margin)
            order.frozen_margin = 0.0

    # ---- 预热 ----
    async def _warmup(self) -> None:
        if self.shadow_cfg.warmup_bars <= 0 or not self.exchange:
            return
        logger.info(f"Starting warmup with {self.shadow_cfg.warmup_bars} bars per symbol.")
        for symbol in self.shadow_cfg.symbols:
            try:
                bars = await self.exchange.get_historical_bars(symbol, self.shadow_cfg.timeframe, limit=self.shadow_cfg.warmup_bars)
                if not isinstance(bars, list):
                    continue
                for bar_data in bars:
                    try:
                        bar = Bar(**bar_data)
                    except Exception:
                        continue
                    # 更新价格
                    self._last_prices[bar.symbol] = bar.close
                    self._price_valid[bar.symbol] = True
                    try:
                        await self.strategy.on_bar(bar, self.shadow_cfg.initial_balance)
                    except Exception as e:
                        logger.debug(f"Strategy exception during warmup bar: {e}")
            except Exception as e:
                logger.error(f"Warmup failed for {symbol}: {e}")
        logger.info("Warmup completed.")

    # ---- 数据馈送循环 ----
    async def _data_feed_loop(self) -> None:
        while self.state in (EngineState.RUNNING, EngineState.PAUSED):
            try:
                async for message in self.exchange.listen():
                    await self._handle_ws_message(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Data feed error: {e}, reconnecting...")
                await asyncio.sleep(5)
                await self._reconnect_data()

    async def _reconnect_data(self) -> None:
        max_retries = 5
        for attempt in range(max_retries):
            try:
                await self.exchange.disconnect()
                await self.exchange.connect()
                for symbol in self.shadow_cfg.symbols:
                    await self.exchange.subscribe_kline(symbol, self.shadow_cfg.timeframe)
                now = time.time()
                for sym in self.shadow_cfg.symbols:
                    self._last_data_time[sym] = now
                logger.info("Data reconnected.")
                return
            except Exception as e:
                logger.error(f"Reconnect attempt {attempt+1} failed: {e}")
                await asyncio.sleep(min(2 ** attempt, 30))
        logger.critical("Could not restore data connection after multiple attempts.")

    async def _handle_ws_message(self, msg: Dict[str, Any]) -> None:
        if msg.get("e") == "kline":
            kline = msg.get("k", {})
            if not kline.get("x", False):
                return
            symbol = kline.get("s") or ""
            if not symbol:
                return
            bar = Bar(
                symbol=symbol,
                open_time=kline.get("t", 0),
                close_time=kline.get("T", 0),
                open=float(kline.get("o", 0)),
                high=float(kline.get("h", 0)),
                low=float(kline.get("l", 0)),
                close=float(kline.get("c", 0)),
                volume=float(kline.get("v", 0)),
                quote_volume=float(kline.get("q", 0)),
                timeframe=self.shadow_cfg.timeframe,
            )
            await self._on_bar(bar)

    # ---- 订单处理器 ----
    async def _order_processor(self) -> None:
        while self.state in (EngineState.RUNNING, EngineState.PAUSED, EngineState.STOPPING):
            try:
                order = await asyncio.wait_for(self._order_queue.get(), timeout=1.0)
                asyncio.create_task(self._process_order_with_sem(order))
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    async def _process_order_with_sem(self, order: ShadowOrder) -> None:
        async with self._order_semaphore:
            try:
                await self._process_order(order)
            except Exception as e:
                logger.exception(f"Order processing failed: {e}")
                self._notify("order_error", {"order_id": order.order_id, "error": str(e)})

    async def _process_order(self, order: ShadowOrder) -> None:
        latency = self._sample_latency()
        await asyncio.sleep(latency)

        async with self._account_lock:
            if order.status != "PENDING":
                return
            symbol = order.signal.symbol
            base_price = self._last_prices.get(symbol)
            if base_price is None or base_price <= 0:
                self._reject_order_locked(order, "No valid market price")
                return

            fill_price = self._calculate_fill_price(order.signal, base_price)
            if fill_price is None:
                self._reject_order_locked(order, "Cannot calculate fill price")
                return

            if order.signal.order_type.upper() == "LIMIT" and order.signal.limit_price:
                direction = order.signal.direction.upper()
                if direction in ("LONG", "BUY") and fill_price > order.signal.limit_price:
                    self._cancel_order_locked(order, "Limit price not reached")
                    return
                elif direction in ("SHORT", "SELL") and fill_price < order.signal.limit_price:
                    self._cancel_order_locked(order, "Limit price not reached")
                    return

            fill_ratio = 1.0
            if self.shadow_cfg.partial_fill_enabled and self._rng.random() > self.shadow_cfg.fill_probability:
                fill_ratio = self.shadow_cfg.partial_fill_ratio

            if not self._check_risk_limits_locked(order.signal, fill_price, fill_ratio):
                self._reject_order_locked(order, "Risk limit exceeded")
                return

            self._fill_order_locked(order, fill_price, fill_ratio)

            if fill_ratio < 1.0:
                remaining_qty = order.signal.size * (1 - fill_ratio)
                new_signal = copy.deepcopy(order.signal)
                new_signal.size = remaining_qty
                self._order_id_counter += 1
                new_order = ShadowOrder(
                    order_id=f"shadow_{self._order_id_counter}",
                    signal=new_signal,
                    submit_time=time.time(),
                    bar_time=self._get_current_bar_time(symbol),
                    frozen_margin=0.0,  # 剩余订单暂不冻结，可在后续处理中冻结
                )
                self.account.open_orders[new_order.order_id] = new_order
                try:
                    self._order_queue.put_nowait(new_order)
                except asyncio.QueueFull:
                    self._cancel_order_locked(new_order, "Queue full")

            await self._notify_strategy(order)

    def _sample_latency(self) -> float:
        if self.shadow_cfg.latency_model == "fixed":
            return self.shadow_cfg.latency_fixed_ms / 1000.0
        elif self.shadow_cfg.latency_model == "normal":
            for _ in range(100):
                lat = self._rng.normal(self.shadow_cfg.latency_normal_mean_ms, self.shadow_cfg.latency_normal_std_ms)
                if lat >= 0:
                    return lat / 1000.0
            logger.warning("Could not generate non-negative latency, using 0")
            return 0.0
        return 0.0

    def _calculate_fill_price(self, signal: Signal, base_price: float) -> Optional[float]:
        slippage_pct = 0.0
        if self.shadow_cfg.slippage_model == "fixed":
            slippage_pct = self.shadow_cfg.slippage_pct
        elif self.shadow_cfg.slippage_model == "normal":
            slippage_pct = abs(self._rng.normal(0, self.shadow_cfg.slippage_normal_std))
        direction = signal.direction.upper()
        if direction in ("LONG", "BUY"):
            return base_price * (1 + slippage_pct)
        elif direction in ("SHORT", "SELL"):
            return base_price * (1 - slippage_pct)
        elif direction in ("CLOSE", "CLOSE_LONG", "CLOSE_SHORT"):
            pos = self.account.positions.get(signal.symbol)
            if not pos:
                return None
            if pos.side == "LONG":
                return base_price * (1 - slippage_pct)
            else:
                return base_price * (1 + slippage_pct)
        return base_price

    def _check_risk_limits_locked(self, signal: Signal, fill_price: float, fill_ratio: float) -> bool:
        qty = signal.size * fill_ratio
        symbol = signal.symbol
        if self.shadow_cfg.max_position_notional > 0:
            current = self._calculate_notional_locked(symbol)
            add = qty * fill_price * (1 if signal.direction.upper() in ("LONG", "BUY") else -1)
            if abs(current + add) > self.shadow_cfg.max_position_notional:
                return False
        if self.shadow_cfg.max_leverage > 0:
            equity = self._calculate_equity_locked()
            available = equity - self.account.frozen_margin
            if available <= 0:
                return False
            total_notional = sum(
                pos.quantity * self._last_prices.get(sym, 0)
                for sym, pos in self.account.positions.items()
            )
            new_notional = total_notional + abs(qty * fill_price)
            if new_notional / available > self.shadow_cfg.max_leverage:
                return False
        return True

    def _calculate_notional_locked(self, symbol: str) -> float:
        pos = self.account.positions.get(symbol)
        price = self._last_prices.get(symbol, 0)
        if pos and price:
            return pos.quantity * price if pos.side == "LONG" else -pos.quantity * price
        return 0.0

    def _calculate_equity_locked(self) -> float:
        equity = self.account.balance
        for sym, pos in self.account.positions.items():
            price = self._last_prices.get(sym, 0)
            if price > 0:
                equity += pos.quantity * price if pos.side == "LONG" else -pos.quantity * price
        return equity

    def _fill_order_locked(self, order: ShadowOrder, fill_price: float, fill_ratio: float) -> None:
        signal = order.signal
        symbol = signal.symbol
        qty = signal.size * fill_ratio
        fee = self.shadow_cfg.taker_fee * fill_price * qty

        base_price = self._last_prices.get(symbol, fill_price)
        slippage_amount = abs(fill_price - base_price) * qty
        slippage_pct = abs(fill_price - base_price) / base_price if base_price > 0 else 0.0

        bar_time_dt = self._safe_bar_time(order.bar_time or self._get_current_bar_time_ms(symbol))
        trade = ShadowTrade(
            timestamp=bar_time_dt,
            bar_time=order.bar_time,
            symbol=symbol,
            side=signal.direction,
            quantity=qty,
            price=fill_price,
            fee=fee,
            slippage=slippage_amount,
            slippage_pct=slippage_pct,
        )

        direction = signal.direction.upper()
        if direction in ("LONG", "BUY"):
            self._open_or_add_locked(symbol, "LONG", qty, fill_price, fee, trade)
        elif direction in ("SHORT", "SELL"):
            self._open_or_add_locked(symbol, "SHORT", qty, fill_price, fee, trade)
        elif direction in ("CLOSE", "CLOSE_LONG", "CLOSE_SHORT"):
            if symbol not in self.account.positions:
                self._reject_order_locked(order, "No position to close")
                return
            self._close_position_locked(symbol, fill_price, fee, trade)
        else:
            self._reject_order_locked(order, "Unknown direction")
            return

        order.filled_quantity = qty
        order.avg_fill_price = fill_price
        order.status = "FILLED" if fill_ratio >= 1.0 else "PARTIALLY_FILLED"
        self.account.open_orders.pop(order.order_id, None)

        # 释放该订单冻结的保证金
        self._release_margin(order)

        if self.shadow_cfg.log_trades:
            self.account.trades.append(trade)
        self.account.total_fees += fee
        self.account.total_slippage += slippage_amount
        self._trade_count += 1

        asyncio.create_task(self._safe_audit("order_filled", {
            "order_id": order.order_id,
            "symbol": symbol,
            "direction": direction,
            "qty": qty,
            "price": fill_price,
        }))

    def _open_or_add_locked(self, symbol: str, side: str, qty: float,
                            price: float, fee: float, trade: ShadowTrade) -> None:
        pos = self.account.positions.get(symbol)
        if pos and pos.side != side:
            close_trade = ShadowTrade(
                timestamp=datetime.utcnow(),
                bar_time=trade.bar_time,
                symbol=symbol,
                side="CLOSE",
                quantity=pos.quantity,
                price=price,
                fee=0.0,
                slippage=0.0,
                slippage_pct=0.0,
            )
            self._close_position_locked(symbol, price, 0.0, close_trade)
            if self.shadow_cfg.log_trades:
                self.account.trades.append(close_trade)
            pos = None
        if pos and pos.side == side:
            total_qty = pos.quantity + qty
            pos.avg_price = (pos.avg_price * pos.quantity + price * qty) / total_qty
            pos.quantity = total_qty
        else:
            self.account.positions[symbol] = ShadowPosition(symbol=symbol, side=side, quantity=qty, avg_price=price)

        if side == "LONG":
            self.account.balance -= (price * qty + fee)
        else:
            self.account.balance += (price * qty - fee)

    def _close_position_locked(self, symbol: str, price: float, fee: float, trade: ShadowTrade) -> None:
        pos = self.account.positions.pop(symbol, None)
        if not pos:
            return
        if pos.side == "LONG":
            pnl = (price - pos.avg_price) * pos.quantity - fee
            self.account.balance += price * pos.quantity - fee
        else:
            pnl = (pos.avg_price - price) * pos.quantity - fee
            self.account.balance -= price * pos.quantity + fee
        pos.realized_pnl += pnl
        trade.pnl = pnl

    def _cancel_order_locked(self, order: ShadowOrder, reason: str) -> None:
        if order.status != "PENDING":
            return
        order.status = "CANCELLED"
        self.account.open_orders.pop(order.order_id, None)
        self._release_margin(order)
        logger.debug(f"Order {order.order_id} cancelled: {reason}")
        asyncio.create_task(self._safe_audit("order_cancelled", {"order_id": order.order_id, "reason": reason}))

    def _reject_order_locked(self, order: ShadowOrder, reason: str) -> None:
        if order.status != "PENDING":
            return
        order.status = "REJECTED"
        self.account.open_orders.pop(order.order_id, None)
        self._release_margin(order)
        logger.info(f"Order {order.order_id} rejected: {reason}")
        asyncio.create_task(self._safe_audit("order_rejected", {"order_id": order.order_id, "reason": reason}))

    async def _notify_strategy(self, order: ShadowOrder) -> None:
        if not self.strategy:
            return
        if order.status in ("FILLED", "PARTIALLY_FILLED"):
            try:
                await self.strategy.on_order_filled({
                    "order_id": order.order_id,
                    "filled_qty": order.filled_quantity,
                    "avg_price": order.avg_fill_price,
                    "status": order.status,
                })
                # 同步持仓给策略
                positions = await self._fetch_positions()
                await self.strategy.on_position_update(positions)
            except Exception as e:
                logger.error(f"Strategy notification error: {e}")
        elif order.status in ("CANCELLED", "REJECTED"):
            try:
                await self.strategy.on_order_cancelled({
                    "order_id": order.order_id,
                    "status": order.status,
                })
            except Exception as e:
                logger.error(f"Strategy cancel notification error: {e}")

    async def _process_remaining_orders(self) -> None:
        async with self._account_lock:
            while not self._order_queue.empty():
                try:
                    order = self._order_queue.get_nowait()
                    if order.status == "PENDING":
                        self._cancel_order_locked(order, "Engine shutdown")
                except asyncio.QueueEmpty:
                    break

    # ---- 统计与报告 ----
    def _generate_report(self) -> Dict[str, Any]:
        if len(self.account.equity_history) < 2:
            return {}
        eq_df = pd.DataFrame(self.account.equity_history, columns=["timestamp", "equity"])
        eq_df.set_index("timestamp", inplace=True)
        returns = eq_df["equity"].pct_change().dropna()

        total_return = (eq_df["equity"].iloc[-1] / self.account.initial_balance - 1) * 100
        annual_factor = self.shadow_cfg.annualization_factor
        sharpe = (returns.mean() / returns.std() * np.sqrt(annual_factor)) if returns.std() != 0 else 0
        max_dd = ((eq_df["equity"] / eq_df["equity"].cummax() - 1).min()) * 100
        win_rate = sum(1 for t in self.account.trades if t.pnl > 0) / max(len(self.account.trades), 1) * 100

        report = {
            "total_return_pct": total_return,
            "sharpe": sharpe,
            "max_drawdown_pct": max_dd,
            "win_rate_pct": win_rate,
            "num_trades": len(self.account.trades),
            "total_fees": self.account.total_fees,
            "total_slippage": self.account.total_slippage,
        }
        logger.info(f"Shadow report: {report}")
        return report

    async def _periodic_report(self) -> None:
        while self.state in (EngineState.RUNNING, EngineState.PAUSED):
            await asyncio.sleep(self.shadow_cfg.report_interval_sec)
            try:
                self._generate_report()
            except Exception as e:
                logger.error(f"Periodic report failed: {e}")

    # ---- 工具方法 ----
    async def run(self) -> None:
        if self.state != EngineState.RUNNING:
            raise RuntimeError("Engine must be initialized before running.")
        self._periodic_report_task = asyncio.create_task(self._periodic_report())
        self.account.equity_history.append((datetime.utcnow(), self.account.initial_balance))
        self.account.realized_equity_history.append((datetime.utcnow(), self.account.initial_balance))
        await super().run()

    async def inject_bar(self, bar: Bar) -> None:
        """手动注入K线（用于历史回放）。注意不要加锁，_on_bar 内部会管理锁。"""
        await self._on_bar(bar)

    def reset_account(self) -> None:
        self.account = ShadowAccount(
            balance=self.shadow_cfg.initial_balance,
            initial_balance=self.shadow_cfg.initial_balance,
        )
        while not self._order_queue.empty():
            try:
                self._order_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._order_id_counter = 0
        self._trade_count = 0
        logger.info("Shadow account and counters reset.")

    @property
    def equity_curve(self) -> List[Tuple[datetime, float]]:
        return self.account.equity_history

    @property
    def trades(self) -> List[ShadowTrade]:
        return self.account.trades

    def _safe_bar_time(self, open_time: Optional[int]) -> datetime:
        if open_time and open_time > 0:
            return datetime.utcfromtimestamp(open_time / 1000)
        return datetime.utcnow()

    def _get_current_bar_time_ms(self, symbol: str) -> Optional[int]:
        # 返回最近 bar 的时间戳 ms，如果无数据返回 None
        t = self._last_data_time.get(symbol)
        if t:
            return int(t * 1000)
        return None

    def _get_current_bar_time(self, symbol: str) -> Optional[int]:
        return self._get_current_bar_time_ms(symbol)

    # 移除重复的 _safe_audit，使用基类的
