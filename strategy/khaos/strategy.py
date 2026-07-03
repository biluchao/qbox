# -*- coding: utf-8 -*-
"""
Module: strategy.khaos.strategy
Description: KHAOS 策略核心实现，支持多空双向、动态止损、部分平仓及参数热更新。
             达到全球顶级量化对冲基金生产级策略实现标准。
Author: KHAOS Quant Team
Created: 2026-07-03
Version: 2.7.0
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from strategy.base import AbstractStrategy, Bar, Signal, Position, StrategyState
from core.indicators.kalman import KalmanTrendline
from core.indicators.atr import ATR
from core.models.hmm import OnlineHMM
from core.micro.orderflow import OrderFlowAccelerator
from core.risk.sizer import VolTargetSizer
from core.risk.stops import DynamicKMAStop

logger = logging.getLogger("khaos.strategy.Khaos")

class Direction:
    LONG = "LONG"
    SHORT = "SHORT"

class KhaosStrategy(AbstractStrategy):
    version = "2.7.0"
    strategy_name = "KHAOS"

    def __init__(self, config: Optional[Dict[str, Any]] = None,
                 kalman: Optional[KalmanTrendline] = None,
                 hmm: Optional[OnlineHMM] = None,
                 micro: Optional[OrderFlowAccelerator] = None,
                 sizer: Optional[VolTargetSizer] = None,
                 stop_mgr: Optional[DynamicKMAStop] = None,
                 risk_context: Any = None,
                 audit_callback: Any = None):
        super().__init__(config)
        self.kma = kalman or KalmanTrendline(
            q_ratio=float(self.config.get("trendline", {}).get("params", {}).get("q_ratio", 0.01))
        )
        self.atr = ATR(14)
        self.hmm = hmm or OnlineHMM(
            n_states=int(self.config.get("regime", {}).get("params", {}).get("states", 3))
        )
        self.micro = micro or OrderFlowAccelerator(
            bpi_thresh=float(self.config.get("micro_accel", {}).get("params", {}).get("bpi_thresh", 0.25)),
            taker_thresh=float(self.config.get("micro_accel", {}).get("params", {}).get("taker_thresh", 0.3))
        )
        self.sizer = sizer or VolTargetSizer(
            risk_per_trade=float(self.config.get("sizer", {}).get("params", {}).get("risk_per_trade", 0.01)),
            vol_target=float(self.config.get("sizer", {}).get("params", {}).get("vol_target_annual", 0.20)),
            max_leverage=float(self.config.get("sizer", {}).get("params", {}).get("max_leverage", 3.0))
        )
        self.stop_mgr = stop_mgr or DynamicKMAStop(
            alpha_base=float(self.config.get("stops", {}).get("params", {}).get("alpha_base", 2.5))
        )

        self.risk_context = risk_context
        self.audit_callback = audit_callback

        self.state: str = "NEUTRAL"
        self.d_max: float = 0.0
        self.neutral_counter: int = 0
        self.neutral_bars_limit: int = 15
        self.embryo_counter: int = 0
        self.embryo_bars_limit: int = 5

        # 按 symbol 的持仓信息
        self.positions: Dict[str, Position] = {}
        self.last_add_price: Dict[str, float] = {}
        self.add_count: Dict[str, int] = {}
        self.add_step_atr = float(self.config.get("add_rules", {}).get("params", {}).get("step_atr", 0.5))
        self.max_additions = int(self.config.get("add_rules", {}).get("params", {}).get("max_additions", 5))

        self.signal_ttl = int(self.config.get("signal_ttl", 60))

        self._last_signal_bar_time: Optional[int] = None
        self._prev_slope: float = 0.0

    # ---- 策略核心回调 ----
    async def on_bar(self, bar: Bar, equity: float) -> Optional[Signal]:
        try:
            await self._update_indicators(bar)
            self._update_state_machine(bar)

            # PAUSED 状态强制平仓
            if self.state == "PAUSED" and self.positions:
                return self._force_close_all(bar.symbol)

            signal = await self._generate_signal(bar, equity)
            if signal:
                if not await self.validate_signal(signal):
                    logger.info(f"Signal rejected by risk: {signal}")
                    return None
                await self._safe_audit("signal_generated", {
                    "symbol": signal.symbol,
                    "direction": signal.direction,
                    "size": signal.size,
                    "state": self.state
                })
                self._last_signal_bar_time = bar.open_time
            return signal
        except Exception:
            logger.exception(f"Unhandled exception in on_bar at {bar.open_time}")
            self.state = StrategyState.ERROR.value
            return None

    async def initialize(self, **kwargs) -> bool:
        await super().initialize(**kwargs)
        logger.info(f"KHAOS strategy v{self.version} initialized.")
        return True

    async def on_position_update(self, positions: Dict[str, Position]) -> None:
        await super().on_position_update(positions)
        self.positions = positions
        # 可在此重新计算统一止损等

    # ---- 内部更新 ----
    async def _update_indicators(self, bar: Bar) -> None:
        obs_var = (bar.high - bar.low) ** 2
        self.ma_level, self.ma_slope = self.kma.update(bar.close, obs_var)
        self.kma_obs_std = np.sqrt(obs_var)

        self.current_atr = max(self.atr.update(bar.high, bar.low, bar.close), 1e-8)

        ret = np.log(bar.close / bar.open) if bar.open > 0 else 0.0
        vol_ratio = bar.volume / self.current_atr
        slope_norm = self.ma_slope / self.current_atr
        d_norm = (bar.close - self.ma_level) / self.current_atr
        features = np.array([[ret, vol_ratio, d_norm, slope_norm]])
        self.hmm.predict_proba(features)

        # 保存前一斜率用于衰减检测
        self._prev_slope = self.ma_slope

    # ---- 状态机 ----
    def _update_state_machine(self, bar: Bar) -> None:
        close = bar.close
        atr = self.current_atr
        d_val = close - self.ma_level
        slope = self.ma_slope

        if self.state == "NEUTRAL":
            self.neutral_counter += 1
            if self.neutral_counter > self.neutral_bars_limit:
                self.state = "PAUSED"
                logger.info("Entered PAUSED due to prolonged neutral market")
                return
            if close > self.ma_level and slope > 0.0:
                self.state = "LONG_EMBRYO"
                self.embryo_counter = 0
            elif close < self.ma_level and slope < 0.0:
                self.state = "SHORT_EMBRYO"
                self.embryo_counter = 0
            return

        if self.state == "PAUSED":
            self.neutral_counter += 1
            # 连续 N 根后尝试恢复
            if self.neutral_counter > 10:
                self.state = "NEUTRAL"
                self.neutral_counter = 0
                return
            # 或者强势突破恢复
            if close > self.ma_level and slope > 0.02 and d_val > 0.3 * atr:
                self.state = "LONG_TREND"
                self.d_max = d_val
                self.neutral_counter = 0
            elif close < self.ma_level and slope < -0.02 and -d_val > 0.3 * atr:
                self.state = "SHORT_TREND"
                self.d_max = -d_val
                self.neutral_counter = 0
            return

        # 胚胎超时
        if self.state in ("LONG_EMBRYO", "SHORT_EMBRYO"):
            self.embryo_counter += 1
            if self.embryo_counter > self.embryo_bars_limit:
                self.state = "NEUTRAL"
                self.neutral_counter = 0
                return

        # LONG side
        if self.state == "LONG_EMBRYO":
            if close > self.ma_level and slope > 0.05 and d_val > 0.3 * atr:
                self.state = "LONG_TREND"
                self.d_max = d_val
            elif close < self.ma_level:
                self.state = "NEUTRAL"
            return

        if self.state == "LONG_TREND":
            if d_val > self.d_max:
                self.d_max = d_val
            if self.d_max > 0 and d_val < 0.5 * self.d_max and close > self.ma_level:
                self.state = "LONG_RETRACE"
            elif close < self.ma_level:
                self.state = "SHORT_EMBRYO" if slope < 0.0 else "NEUTRAL"
            return

        if self.state == "LONG_RETRACE":
            if close < self.ma_level:
                self.state = "NEUTRAL"
            elif self.d_max > 0 and d_val > 0.8 * self.d_max and slope > 0.0:
                self.state = "LONG_TREND"
            return

        # SHORT side
        if self.state == "SHORT_EMBRYO":
            if close < self.ma_level and slope < -0.05 and -d_val > 0.3 * atr:
                self.state = "SHORT_TREND"
                self.d_max = -d_val
            elif close > self.ma_level:
                self.state = "NEUTRAL"
            return

        if self.state == "SHORT_TREND":
            if -d_val > self.d_max:
                self.d_max = -d_val
            if self.d_max > 0 and -d_val < 0.5 * self.d_max and close < self.ma_level:
                self.state = "SHORT_RETRACE"
            elif close > self.ma_level:
                self.state = "LONG_EMBRYO" if slope > 0.0 else "NEUTRAL"
            return

        if self.state == "SHORT_RETRACE":
            if close > self.ma_level:
                self.state = "NEUTRAL"
            elif self.d_max > 0 and -d_val > 0.8 * self.d_max and slope < 0.0:
                self.state = "SHORT_TREND"
            return

    # ---- 信号生成 ----
    async def _generate_signal(self, bar: Bar, equity: float) -> Optional[Signal]:
        if self._last_signal_bar_time == bar.open_time:
            return None

        close_signal = self._check_stop(bar.close)
        if close_signal:
            return close_signal

        partial_signal = self._check_partial_close(bar)
        if partial_signal:
            return partial_signal

        if self.state in ("LONG_TREND", "LONG_RETRACE"):
            return self._handle_direction(bar, equity, Direction.LONG)
        elif self.state in ("SHORT_TREND", "SHORT_RETRACE"):
            return self._handle_direction(bar, equity, Direction.SHORT)
        return None

    def _check_stop(self, current_price: float) -> Optional[Signal]:
        for symbol, pos in self.positions.items():
            if pos.side == Direction.LONG:
                stop_price = self.stop_mgr.calc_stop(
                    self.ma_level, self.kma_obs_std,
                    self.hmm.latest_proba[1], self.ma_slope,
                    direction=Direction.LONG
                )
                if current_price <= stop_price:
                    return Signal(symbol=symbol, direction="CLOSE", size=pos.quantity,
                                  signal_id="stop_long")
            elif pos.side == Direction.SHORT:
                stop_price = self.stop_mgr.calc_stop(
                    self.ma_level, self.kma_obs_std,
                    self.hmm.latest_proba[2], abs(self.ma_slope),
                    direction=Direction.SHORT
                )
                if current_price >= stop_price:
                    return Signal(symbol=symbol, direction="CLOSE", size=pos.quantity,
                                  signal_id="stop_short")
        return None

    def _check_partial_close(self, bar: Bar) -> Optional[Signal]:
        if not self.positions:
            return None
        # 检测斜率从陡峭快速衰减（绝对值下降超过阈值）
        if abs(self._prev_slope) > 2.0 and abs(self.ma_slope) < 1.0:
            for symbol, pos in self.positions.items():
                close_size = pos.quantity * 0.5
                if pos.side == Direction.LONG:
                    return Signal(symbol=symbol, direction="CLOSE_LONG", size=close_size,
                                  signal_id="partial_long")
                else:
                    return Signal(symbol=symbol, direction="CLOSE_SHORT", size=close_size,
                                  signal_id="partial_short")
        return None

    def _handle_direction(self, bar: Bar, equity: float, direction: str) -> Optional[Signal]:
        symbol = bar.symbol
        pos = self.positions.get(symbol)
        # 开仓
        if not pos or pos.quantity == 0:
            units = self.sizer.calc_units(equity, self.current_atr, bar.close)
            if units <= 0:
                return None
            units = self._round_lot(units)
            self.last_add_price[symbol] = bar.close
            self.add_count[symbol] = 0
            return Signal(symbol=symbol, direction=direction, size=units,
                          order_type="LIMIT", limit_price=bar.close,
                          ttl_seconds=self.signal_ttl)

        # 加仓
        if self.add_count.get(symbol, 0) < self.max_additions:
            last = self.last_add_price.get(symbol, bar.close)
            if (direction == Direction.LONG and bar.close - last > self.add_step_atr * self.current_atr) or \
               (direction == Direction.SHORT and last - bar.close > self.add_step_atr * self.current_atr):
                units = self.sizer.calc_units(equity, self.current_atr, bar.close)
                units = self._round_lot(units)
                if units > 0:
                    self.last_add_price[symbol] = bar.close
                    self.add_count[symbol] = self.add_count.get(symbol, 0) + 1
                    return Signal(symbol=symbol, direction=direction, size=units,
                                  ttl_seconds=self.signal_ttl)
        return None

    def _round_lot(self, units: float) -> int:
        """根据交易所最小交易量及步长取整，这里假设最小 1 张。"""
        return max(1, int(units))

    def _force_close_all(self, symbol: str) -> Optional[Signal]:
        """生成平掉所有仓位的信号。"""
        for sym, pos in self.positions.items():
            if sym == symbol:
                return Signal(symbol=sym, direction="CLOSE", size=pos.quantity,
                              signal_id="force_close")
        return None

    # ---- 参数热更新 ----
    def reload_params(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.kma = KalmanTrendline(
            q_ratio=float(self.config.get("trendline", {}).get("params", {}).get("q_ratio", 0.01))
        )
        self.hmm = OnlineHMM(
            n_states=int(self.config.get("regime", {}).get("params", {}).get("states", 3))
        )
        self.micro = OrderFlowAccelerator(
            bpi_thresh=float(self.config.get("micro_accel", {}).get("params", {}).get("bpi_thresh", 0.25)),
            taker_thresh=float(self.config.get("micro_accel", {}).get("params", {}).get("taker_thresh", 0.3))
        )
        self.sizer = VolTargetSizer(
            risk_per_trade=float(self.config.get("sizer", {}).get("params", {}).get("risk_per_trade", 0.01)),
            vol_target=float(self.config.get("sizer", {}).get("params", {}).get("vol_target_annual", 0.20)),
            max_leverage=float(self.config.get("sizer", {}).get("params", {}).get("max_leverage", 3.0))
        )
        self.stop_mgr = DynamicKMAStop(
            alpha_base=float(self.config.get("stops", {}).get("params", {}).get("alpha_base", 2.5))
        )
        self.add_step_atr = float(self.config.get("add_rules", {}).get("params", {}).get("step_atr", 0.5))
        self.max_additions = int(self.config.get("add_rules", {}).get("params", {}).get("max_additions", 5))
        self.signal_ttl = int(self.config.get("signal_ttl", 60))
        logger.info("All strategy parameters reloaded.")

    async def _safe_audit(self, event: str, details: Dict[str, Any]) -> None:
        if self.audit_callback:
            try:
                if asyncio.iscoroutinefunction(self.audit_callback):
                    await self.audit_callback(event, details)
                else:
                    self.audit_callback(event, details)
            except Exception as e:
                logger.error(f"Audit callback error: {e}")

    @property
    def is_initialized(self) -> bool:
        return self.state != StrategyState.CREATED.value
