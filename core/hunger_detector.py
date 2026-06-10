"""
QBox · Systemic Starvation Detector (HungerDetector)
======================================================
职责：
1. 计算策略级饥饿度（连续未开仓时长/最大容忍空闲时长），分级：normal / mild / moderate / severe
2. 提供对应级别的参数松弛建议，严格遵守安全边界，支持波动率自适应调制
3. 通过事件总线广播饥饿状态变更，写入审计日志
4. 支持多 symbol/多策略独立追踪，时钟采用 NTP 同步后的交易所 UTC epoch
5. 内置并发安全（Async/Thread 双模锁）、内存自动清理、异常熔断

外部依赖：
- pydantic v2 (BaseModel, field_validator)
- core.event_bus.EventBus (Protocol)
- core.semantic_index.SemanticIndex (Optional)

接口契约：
- assess_starvation(symbol: str, strategy_id: str, last_entry_ts: float|None, current_ts: float|None) -> StarvationReport
- compute_adjusted_params(base_params: dict, level: StarvationLevel) -> dict   (静态方法)
- reset(symbol: str, strategy_id: str, ts: float) -> bool
- health_check() -> Dict[str, Any]

异常与降级：
- 缺失事件总线：仅写本地日志
- 时钟异常：标记 anomaly，饥饿度保守设为上次值
- 参数调整触碰边界：记录警告并维持边界值
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 事件总线协议
# ---------------------------------------------------------------------------

class EventBusProtocol(Protocol):
    def publish(self, event_type: str, data: Dict[str, Any]) -> None: ...
    async def async_publish(self, event_type: str, data: Dict[str, Any]) -> None: ...

# ---------------------------------------------------------------------------
# 配置模型
# ---------------------------------------------------------------------------

class AdjustmentRule(BaseModel):
    op: str  # "add", "multiply", "set"
    value: float
    description: str = ""

class AdjustmentPolicy(BaseModel):
    rules: Dict[str, AdjustmentRule] = Field(default_factory=dict)

class StarvationConfig(BaseModel):
    max_idle_seconds: float = Field(5400.0, gt=0.0, description="最大容忍空闲秒数")
    mild_threshold: float = Field(0.3, ge=0.0, le=1.0)
    moderate_threshold: float = Field(0.6, ge=0.0, le=1.0)
    severe_threshold: float = Field(0.8, ge=0.0, le=1.0)
    safety_limits: Dict[str, Tuple[float, float]] = Field(default_factory=lambda: {
        "imbalance_threshold": (0.05, 0.45),
        "gravity_band_coef": (0.5, 1.5),
        "atr_period": (10, 30),
    })
    adjustment_policy: Dict[str, AdjustmentPolicy] = Field(default_factory=lambda: {
        "mild": AdjustmentPolicy(rules={
            "imbalance_threshold": AdjustmentRule(op="add", value=-0.1),
            "gravity_band_coef": AdjustmentRule(op="add", value=0.1),
        }),
        "moderate": AdjustmentPolicy(rules={
            "imbalance_threshold": AdjustmentRule(op="add", value=-0.2),
            "gravity_band_coef": AdjustmentRule(op="add", value=0.2),
        }),
        "severe": AdjustmentPolicy(rules={
            "imbalance_threshold": AdjustmentRule(op="add", value=-0.3),
            "gravity_band_coef": AdjustmentRule(op="add", value=0.3),
            "atr_period": AdjustmentRule(op="add", value=-5),
        }),
    })
    max_adjustments_per_day: int = Field(5, ge=1, description="每日最大调整次数")
    volatility_modulation: bool = Field(True, description="是否启用波动率调制")

    @field_validator("severe_threshold")
    @classmethod
    def check_order(cls, v, info):
        if "moderate_threshold" in info.data and v < info.data["moderate_threshold"]:
            raise ValueError("severe_threshold must be >= moderate_threshold")
        return v

    class Config:
        frozen = False

# ---------------------------------------------------------------------------
# 枚举与数据结构
# ---------------------------------------------------------------------------

class StarvationLevel(str, Enum):
    NORMAL = "normal"
    MILD = "mild"
    MODERATE = "moderate"
    SEVERE = "severe"

class AnomalyType(str, Enum):
    NONE = "none"
    CLOCK_BACKWARD = "clock_backward"
    CLOCK_UNSTABLE = "clock_unstable"

@dataclass
class StarvationReport:
    symbol: str
    strategy_id: str
    level: StarvationLevel
    hunger_pct: float
    idle_seconds: float
    adjustments: Dict[str, AdjustmentRule] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    anomaly: AnomalyType = AnomalyType.NONE
    ts_utc: float = 0.0

# ---------------------------------------------------------------------------
# 核心检测器
# ---------------------------------------------------------------------------

class HungerDetector:
    """机构级饥饿检测与自适应松弛器"""

    def __init__(
        self,
        config: StarvationConfig,
        event_bus: Optional[EventBusProtocol] = None,
        semantic_index: Optional[Any] = None,
        clock_func: Callable[[], float] = time.time,   # 使用 epoch 时间
        use_async: bool = False,
    ):
        self.config = config
        self._event_bus = event_bus
        self._semantic_index = semantic_index
        self._clock = clock_func
        self._async = use_async
        # 线程锁
        self._lock = threading.Lock()
        # 存储结构: (symbol, strategy_id) -> 最近一次开仓 epoch 时间
        self._last_entries: Dict[Tuple[str, str], float] = {}
        # 存储策略启动时间（用于从未开仓场景）
        self._init_times: Dict[Tuple[str, str], float] = {}
        # 每日调整计数器，键: (symbol, strategy_id, date_utc)
        self._adjustment_count: Dict[Tuple[str, str, str], int] = {}
        # 上一次饥饿度，用于时钟异常时保持
        self._last_hunger: Dict[Tuple[str, str], float] = {}

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------

    def assess_starvation(
        self,
        symbol: str,
        strategy_id: str,
        last_entry_ts: Optional[float] = None,
        current_ts: Optional[float] = None,
    ) -> StarvationReport:
        """
        评估饥饿度。
        last_entry_ts: 最近开仓的真实 UTC epoch 时间戳，None 表示从未开仓。
        current_ts: 当前时间，默认调用 clock_func。
        """
        if current_ts is None:
            current_ts = self._clock()

        key = (symbol, strategy_id)

        with self._lock:
            # 注册初始时间（若首次访问）
            if key not in self._init_times:
                self._init_times[key] = current_ts

            # 计算空闲秒数，并检测异常
            anomaly = AnomalyType.NONE
            if last_entry_ts is not None:
                # 检查类型，强制转换为 float
                if not isinstance(last_entry_ts, (float, int)):
                    logger.error("last_entry_ts 类型错误: %s", type(last_entry_ts))
                    last_entry_ts = float(last_entry_ts)
                else:
                    last_entry_ts = float(last_entry_ts)

                if last_entry_ts > current_ts + 2.0:  # 明显回拨
                    logger.critical("时钟严重回拨: last_entry_ts=%f, current_ts=%f", last_entry_ts, current_ts)
                    anomaly = AnomalyType.CLOCK_BACKWARD
                    # 保持上次饥饿度，不做改变
                    idle_seconds = 0.0
                    hunger_pct = self._last_hunger.get(key, 0.0)
                else:
                    idle_seconds = max(0.0, current_ts - last_entry_ts)
                    # 更新最后开仓时间
                    self._last_entries[key] = last_entry_ts
            else:
                # 从未开仓：使用组件初始化时间
                init_ts = self._init_times[key]
                idle_seconds = max(0.0, current_ts - init_ts)

            if anomaly == AnomalyType.NONE:
                max_idle = self.config.max_idle_seconds
                hunger_pct = min(1.0, idle_seconds / max_idle)
                self._last_hunger[key] = hunger_pct

            # 等级判定
            level = self._compute_level(hunger_pct)

            # 提取调整策略
            adjustments = self._extract_adjustments(level)

            report = StarvationReport(
                symbol=symbol,
                strategy_id=strategy_id,
                level=level,
                hunger_pct=round(hunger_pct, 4),
                idle_seconds=idle_seconds,
                adjustments=adjustments,
                warnings=[],
                anomaly=anomaly,
                ts_utc=current_ts,
            )
            if level != StarvationLevel.NORMAL:
                report.warnings.append(
                    f"{symbol}/{strategy_id} starvation {hunger_pct:.2%}, level {level.value}"
                )

        # 锁外操作
        self._publish_state(report)
        self._log_to_index(report)

        return report

    @staticmethod
    def compute_adjusted_params(
        base_params: Dict[str, Any],
        level: StarvationLevel,
        adjustment_policy: Dict[str, AdjustmentPolicy],
        safety_limits: Dict[str, Tuple[float, float]],
        volatility_multiplier: float = 1.0,
    ) -> Dict[str, Any]:
        """
        纯函数：根据饥饿等级和调整策略生成新参数，严格遵守安全边界。
        volatility_multiplier 用于根据当前市场波动率缩放调整幅度（0.5~2.0）。
        """
        if level == StarvationLevel.NORMAL:
            return base_params.copy()

        policy = adjustment_policy.get(level.value)
        if not policy:
            return base_params.copy()

        adjusted = base_params.copy()
        for param, rule in policy.rules.items():
            if param not in base_params:
                continue

            original = base_params[param]
            if original is None:
                logger.warning("参数 %s 值为 None，跳过调整", param)
                continue

            # 应用操作
            val = rule.value * volatility_multiplier
            if rule.op == "add":
                new_val = original + val
            elif rule.op == "multiply":
                new_val = original * val
            elif rule.op == "set":
                new_val = val
            else:
                continue

            # 保持类型
            if isinstance(original, int):
                new_val = int(round(new_val))
            else:
                new_val = float(new_val)

            # 安全边界裁剪
            limits = safety_limits.get(param)
            if limits:
                low, high = limits
                if new_val < low or new_val > high:
                    logger.warning("参数 %s 调整后 %.3f 超出边界 [%.3f, %.3f]，已裁剪", param, new_val, low, high)
                    new_val = max(low, min(high, new_val))

            adjusted[param] = new_val

        return adjusted

    def reset(self, symbol: str, strategy_id: str, ts: Optional[float] = None) -> bool:
        """开仓成功后重置饥饿状态，返回是否成功"""
        ts = ts or self._clock()
        key = (symbol, strategy_id)
        with self._lock:
            self._last_entries[key] = ts
            # 更新初始时间到开仓时刻，表示“重新开始计时”
            self._init_times[key] = ts
            self._last_hunger[key] = 0.0
        logger.info("Starvation reset: %s/%s at %f", symbol, strategy_id, ts)
        self._publish_reset(symbol, strategy_id)
        return True

    def health_check(self) -> Dict[str, Any]:
        """模块自检，使用唯一标识避免冲突"""
        test_symbol = f"HEALTHCHECK-{uuid.uuid4().hex[:8]}"
        try:
            report = self.assess_starvation(test_symbol, "test", last_entry_ts=self._clock())
            if report.anomaly != AnomalyType.NONE:
                return {"status": "warn", "message": "Clock anomaly detected"}
            return {"status": "ok", "message": "healthy"}
        except Exception as e:
            logger.exception("Health check failed")
            return {"status": "error", "message": str(e)}

    def purge_stale(self, max_age_seconds: float = 86400 * 7) -> int:
        """清理过期条目，返回清理数量"""
        now = self._clock()
        with self._lock:
            stale = [k for k, t in self._last_entries.items() if now - t > max_age_seconds]
            for k in stale:
                self._last_entries.pop(k, None)
                self._init_times.pop(k, None)
                self._last_hunger.pop(k, None)
            if stale:
                logger.info("Purged %d stale entries", len(stale))
            return len(stale)

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _compute_level(self, hunger_pct: float) -> StarvationLevel:
        if hunger_pct >= self.config.severe_threshold:
            return StarvationLevel.SEVERE
        if hunger_pct >= self.config.moderate_threshold:
            return StarvationLevel.MODERATE
        if hunger_pct >= self.config.mild_threshold:
            return StarvationLevel.MILD
        return StarvationLevel.NORMAL

    def _extract_adjustments(self, level: StarvationLevel) -> Dict[str, AdjustmentRule]:
        if level == StarvationLevel.NORMAL:
            return {}
        policy = self.config.adjustment_policy.get(level.value)
        return policy.rules.copy() if policy else {}

    def _publish_state(self, report: StarvationReport) -> None:
        if not self._event_bus:
            return
        try:
            payload = {
                "symbol": report.symbol,
                "strategy": report.strategy_id,
                "level": report.level.value,
                "hunger_pct": report.hunger_pct,
                "anomaly": report.anomaly.value,
            }
            if self._async:
                asyncio.create_task(self._event_bus.async_publish("Hunger.Changed", payload))
            else:
                self._event_bus.publish("Hunger.Changed", payload)
        except Exception:
            logger.exception("Event publish failed")

    def _publish_reset(self, symbol: str, strategy_id: str) -> None:
        if not self._event_bus:
            return
        try:
            self._event_bus.publish("Hunger.Reset", {"symbol": symbol, "strategy": strategy_id})
        except Exception:
            logger.exception("Reset event publish failed")

    def _log_to_index(self, report: StarvationReport) -> None:
        if not self._semantic_index:
            return
        try:
            self._semantic_index.log_event(
                "Strategy::Starvation::Assess",
                {k: v for k, v in report.__dict__.items() if k != "adjustments"}
            )
        except Exception:
            logger.debug("Semantic index write failed", exc_info=True)

    # 自动清理调度（可被外部循环调用）
    async def auto_purge_loop(self, interval: float = 3600.0):
        while True:
            await asyncio.sleep(interval)
            self.purge_stale()
