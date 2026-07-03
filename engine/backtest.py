# -*- coding: utf-8 -*-
"""
Module: engine.backtest
Description: KHAOS 事件驱动回测引擎（生产级修复版）。
             基于历史数据准确模拟订单执行、手续费、滑点与权益曲线。
             严格遵循资金流向规则，杜绝未来数据泄露，输出精确绩效统计。
             符合全球顶尖量化基金对回测引擎的精度与审计要求。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.5.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from engine.base import BaseEngine, EngineConfig, EngineState, OrderResult
from strategy.base import AbstractStrategy, Bar, Signal

logger = logging.getLogger("khaos.engine.backtest")

# ---------------------------------------------------------------------------
# 回测特定配置
# ---------------------------------------------------------------------------
class BacktestConfig(BaseModel):
    """回测引擎专用配置。"""
    initial_capital: float = Field(default=1_000_000.0, gt=0.0)
    maker_fee: float = Field(default=0.0002, ge=0.0, le=0.01)
    taker_fee: float = Field(default=0.0004, ge=0.0, le=0.01)
    slippage_model: str = "fixed"          # fixed, normal
    slippage_pct: float = Field(default=0.0005, ge=0.0, le=0.05)
    slippage_normal_std: float = 0.0003
    enable_market_impact: bool = False
    impact_factor: float = 0.1
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    symbols: List[str] = Field(default_factory=lambda: ["BTCUSDT"])
    timeframe: str = "3m"
    warmup_bars: int = 200

    class Config:
        extra = "forbid"

# ---------------------------------------------------------------------------
# 内部数据结构
# ---------------------------------------------------------------------------
@dataclass
class Position:
    symbol: str
    side: str  # LONG / SHORT
    quantity: float
    avg_price: float
    realized_pnl: float = 0.0

@dataclass
class Trade:
    timestamp: datetime
    symbol: str
    side: str
    quantity: float
    price: float
    fee: float
    slippage: float
    pnl: float = 0.0

@dataclass
class Account:
    balance: float
    positions: Dict[str, Position] = field(default_factory=dict)
    equity_history: List[Tuple[datetime, float]] = field(default_factory=list)
    trades: List[Trade] = field(default_factory=list)
    total_fees: float = 0.0
    total_slippage: float = 0.0

# ---------------------------------------------------------------------------
# 回测引擎
# ---------------------------------------------------------------------------
class BacktestEngine(BaseEngine):
    """
    事件驱动回测引擎。
    严格管理资金流向，杜绝未来数据，输出精确回测报告。
    """

    def __init__(self,
                 config: Optional[BacktestConfig] = None,
                 data: Optional[Dict[str, pd.DataFrame]] = None):
        self.bt_config = config or BacktestConfig()
        engine_cfg = EngineConfig(
            mode="backtest",
            heartbeat_interval=999,  # 回测不使用心跳
        )
        super().__init__(engine_cfg)
        self.data = data or {}
        self.account = Account(balance=self.bt_config.initial_capital)
        self._rng = np.random.default_rng(42)

        # 用于记录每个 symbol 的当前价格，避免未来数据
        self._current_prices: Dict[str, float] = {}
        self._last_bar_price: float = 0.0
        self._signal_history: List[Dict[str, Any]] = []

    # ---- 实现抽象方法 ----
    async def _on_bar(self, bar: Bar) -> Optional[Signal]:
        """回测中不通过此方法驱动，但为了接口完整性提供一个空实现。"""
        pass

    async def _connect(self) -> None:
        logger.info("Backtest engine connected (no-op).")

    async def _disconnect(self) -> None:
        logger.info("Backtest engine disconnected (no-op).")

    async def _get_equity(self) -> float:
        return self._calculate_equity()

    async def _fetch_positions(self) -> Dict[str, Any]:
        return {sym: {"side": pos.side, "quantity": pos.quantity,
                      "avg_price": pos.avg_price}
                for sym, pos in self.account.positions.items()}

    async def _execute_signal(self, signal: Signal) -> OrderResult:
        return self._simulate_fill(signal)

    # ---- 数据加载 ----
    def load_data_from_csv(self, symbol: str, filepath: str) -> None:
        df = pd.read_csv(filepath, parse_dates=["open_time"])
        self.data[symbol] = df.set_index("open_time").sort_index()
        logger.info(f"Loaded {len(df)} bars for {symbol}")

    # ---- 主回测循环 ----
    async def run(self) -> None:
        if self.state != EngineState.RUNNING:
            raise RuntimeError("Engine must be initialized before running.")
        if not self.data:
            raise ValueError("No data loaded.")

        # 合并多品种数据
        all_bars = []
        for symbol, df in self.data.items():
            for idx, row in df.iterrows():
                bar = Bar(
                    symbol=symbol,
                    open_time=int(idx.timestamp() * 1000),
                    close_time=0,  # 回测不需要精确 close_time
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row.get("volume", 0.0),
                    timeframe=self.bt_config.timeframe,
                )
                all_bars.append(bar)
        all_bars.sort(key=lambda x: x.open_time)

        # 预热阶段：只更新指标，不交易
        if self.bt_config.warmup_bars > 0 and len(all_bars) > self.bt_config.warmup_bars:
            warmup_bars = all_bars[:self.bt_config.warmup_bars]
            for bar in warmup_bars:
                self._current_prices[bar.symbol] = bar.close  # 更新当前价格
                await self.strategy.on_bar(bar, self._calculate_equity())
            all_bars = all_bars[self.bt_config.warmup_bars:]
            logger.info(f"Warmup completed with {len(warmup_bars)} bars.")

        # 主循环
        for bar in all_bars:
            self._current_prices[bar.symbol] = bar.close
            self._last_bar_price = bar.close  # 用于订单模拟
            await self._process_bar(bar)

            # 记录权益
            self.account.equity_history.append(
                (datetime.utcfromtimestamp(bar.open_time / 1000), self._calculate_equity())
            )

        self._generate_report()
        self.state = EngineState.STOPPED
        logger.info("Backtest finished.")

    # ---- 订单模拟 ----
    def _simulate_fill(self, signal: Signal) -> OrderResult:
        """模拟订单执行，更新账户并返回结果。"""
        symbol = signal.symbol
        direction = signal.direction.upper()
        size = abs(signal.size)
        if size <= 0:
            return OrderResult(success=False, message="Invalid size")

        base_price = self._last_bar_price
        if base_price <= 0:
            return OrderResult(success=False, message="No recent price available")

        # 计算滑点
        if self.bt_config.slippage_model == "fixed":
            slippage_pct = self.bt_config.slippage_pct
        elif self.bt_config.slippage_model == "normal":
            slippage_pct = abs(self._rng.normal(0, self.bt_config.slippage_normal_std))
        else:
            slippage_pct = 0.0

        # 根据方向确定成交价格（买入不利方向价格更高，卖出不利方向价格更低）
        if direction in ("LONG", "BUY"):
            fill_price = base_price * (1 + slippage_pct)
        elif direction in ("SHORT", "SELL"):
            fill_price = base_price * (1 - slippage_pct)
        elif direction in ("CLOSE", "CLOSE_LONG", "CLOSE_SHORT"):
            # 根据持仓方向反向平仓
            pos = self.account.positions.get(symbol)
            if not pos:
                return OrderResult(success=False, message="No position to close")
            if pos.side == "LONG":
                fill_price = base_price * (1 - slippage_pct)  # 卖出平仓，价格压低
            else:
                fill_price = base_price * (1 + slippage_pct)  # 买入平仓，价格抬高
        else:
            return OrderResult(success=False, message=f"Unknown direction: {direction}")

        fee = self.bt_config.taker_fee * fill_price * size

        # 记录交易基本信息
        trade = Trade(
            timestamp=datetime.utcnow(),
            symbol=symbol,
            side=direction,
            quantity=size,
            price=fill_price,
            fee=fee,
            slippage=slippage_pct * base_price,
        )

        # 执行订单
        if direction in ("LONG", "BUY"):
            # 先平反向持仓
            if symbol in self.account.positions and self.account.positions[symbol].side == "SHORT":
                self._close_position(symbol, fill_price, fee, trade)
            self._open_or_add_position(symbol, "LONG", size, fill_price, fee)
        elif direction in ("SHORT", "SELL"):
            if symbol in self.account.positions and self.account.positions[symbol].side == "LONG":
                self._close_position(symbol, fill_price, fee, trade)
            self._open_or_add_position(symbol, "SHORT", size, fill_price, fee)
        else:  # CLOSE 族
            self._close_position(symbol, fill_price, fee, trade)

        self.account.total_fees += fee
        self.account.total_slippage += slippage_pct * base_price * size
        self.account.trades.append(trade)
        self._signal_history.append({"signal": signal, "result": "success", "trade": trade})

        return OrderResult(success=True, filled_size=size)

    def _open_or_add_position(self, symbol: str, side: str,
                               quantity: float, price: float, fee: float) -> None:
        """开仓或加仓，正确处理多空现金流。"""
        if side == "LONG":
            cash_flow = - (price * quantity + fee)  # 支出
        else:  # SHORT
            cash_flow = + (price * quantity - fee)  # 卖出收入

        self.account.balance += cash_flow

        if symbol in self.account.positions:
            pos = self.account.positions[symbol]
            if pos.side == side:
                total_qty = pos.quantity + quantity
                pos.avg_price = (pos.avg_price * pos.quantity + price * quantity) / total_qty
                pos.quantity = total_qty
            else:
                # 不应该出现，因为调用前已平仓
                logger.error(f"Position mismatch: expected {side}, got {pos.side}. Closing existing.")
                self._close_position(symbol, price, fee)
                self._open_or_add_position(symbol, side, quantity, price, fee)
        else:
            self.account.positions[symbol] = Position(
                symbol=symbol, side=side, quantity=quantity, avg_price=price
            )

    def _close_position(self, symbol: str, price: float, fee: float,
                        trade: Optional[Trade] = None) -> None:
        """平仓，正确处理多空现金流并返回盈亏。"""
        pos = self.account.positions.pop(symbol, None)
        if pos is None:
            return

        # 计算平仓现金流和盈亏
        if pos.side == "LONG":
            cash_flow = price * pos.quantity - fee  # 卖出收入
            pnl = (price - pos.avg_price) * pos.quantity - fee
        else:  # SHORT
            cash_flow = - (price * pos.quantity + fee)  # 买回归还支出
            pnl = (pos.avg_price - price) * pos.quantity - fee

        self.account.balance += cash_flow
        pos.realized_pnl += pnl

        if trade:
            trade.pnl = pnl  # 记录盈亏到交易

    def _calculate_equity(self) -> float:
        """基于当前价格计算总权益 = 现金 + 多头市值 - 空头市值。"""
        equity = self.account.balance
        for sym, pos in self.account.positions.items():
            price = self._current_prices.get(sym)
            if price is None or price <= 0:
                continue
            if pos.side == "LONG":
                equity += pos.quantity * price
            else:  # SHORT
                equity -= pos.quantity * price
        return equity

    # ---- 统计报告 ----
    def _generate_report(self) -> None:
        if not self.account.equity_history:
            return
        eq_df = pd.DataFrame(self.account.equity_history, columns=["timestamp", "equity"])
        eq_df.set_index("timestamp", inplace=True)
        returns = eq_df["equity"].pct_change().dropna()

        total_return = (eq_df["equity"].iloc[-1] / self.bt_config.initial_capital - 1) * 100
        # 年化夏普（3分钟数据，一年约 175200 个3分钟）
        periods_per_year = 365 * 24 * 60 / 3
        sharpe = (returns.mean() / returns.std() * np.sqrt(periods_per_year)) if returns.std() != 0 else 0
        max_dd = ((eq_df["equity"] / eq_df["equity"].cummax() - 1).min()) * 100
        win_rate = sum(1 for t in self.account.trades if t.pnl > 0) / max(len(self.account.trades), 1) * 100

        report = f"""
        ========== Backtest Report ==========
        Total Return:   {total_return:.2f}%
        Sharpe Ratio:   {sharpe:.2f}
        Max Drawdown:   {max_dd:.2f}%
        Win Rate:       {win_rate:.2f}%
        Total Trades:   {len(self.account.trades)}
        Total Fees:     {self.account.total_fees:.4f}
        Total Slippage: {self.account.total_slippage:.4f}
        =====================================
        """
        logger.info(report)
        self.report = {
            "total_return_pct": total_return,
            "sharpe": sharpe,
            "max_drawdown_pct": max_dd,
            "win_rate_pct": win_rate,
            "num_trades": len(self.account.trades),
            "total_fees": self.account.total_fees,
            "total_slippage": self.account.total_slippage,
        }

    # ---- 辅助方法 ----
    async def initialize(self, strategy: Optional[AbstractStrategy] = None) -> None:
        """允许直接传入策略实例。"""
        if strategy:
            self.strategy = strategy
        await super().initialize()
