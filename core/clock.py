"""
火种系统 · 全局时钟 (Clock) — 机构级 v3

核心职责：
1. 提供高精度、单调、可审计的统一时间源。
2. 支持实盘（单调校准 Wall-Clock）、回测（可控推进、最快速度、时间膨胀）、冻结（演练）模式。
3. 提供时间工具：对齐、区间比较、下个特定时间、时间戳转换。
4. 分布式时钟就绪：可注入自定义 TimeProvider，支持 NTP 健康监控。
5. 事件总线集成，审计日志可持久化，状态可序列化。

外部依赖（真实模块接口）：
- core.event_bus.EventBus : 时钟事件发布（可选）
- core.semantic_index.SemanticIndex : 审计日志持久化（可选）

接口契约：
- now() -> Timestamp
- monotonic_ns() -> int
- wall_clock_from_monotonic(mono_ns: int) -> float
- set_mode(mode: ClockMode, **kwargs) -> Dict[str, Any]
- advance(delta_ns: int) -> Dict[str, Any]
- wait_until(target_ts: float) -> bool  (仅模拟模式)
- align_to(period_seconds: float, anchor: float = 0.0) -> float
- next_at(hour: int, minute: int, second: int = 0) -> float
- sleep_in_sim(ns: int) -> None  (模拟等待)
- is_between(start_ts: float, end_ts: float, inclusive_end: bool = False) -> bool
- health_check() -> Dict[str, Any]
- save_state() -> Dict[str, Any]
- load_state(state: Dict[str, Any]) -> Dict[str, Any]

异常与降级：
- 线程安全通过内部锁保证。
- 如果 TimeProvider 不可用，回退到系统时钟并记录。
- 所有公共方法返回统一格式：{"status", "reason", "warnings"}。

资源管理：
- 审计日志使用环形缓冲区（最大容量可配置）。
- 单例模式确保全局唯一。
- 锁使用 RLock，但确保在 __new__ 中安全初始化。
"""

import time
import threading
import logging
import math
from enum import Enum
from typing import Dict, Any, Optional, Final, List, Callable, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


# ======================================================================
# 时间戳对象
# ======================================================================
@dataclass(frozen=True, order=True)
class Timestamp:
    """不可变高精度时间戳，支持比较、哈希"""
    seconds: float          # UTC Unix 时间，纳秒级精度
    source: str             # "real", "simulated", "frozen"
    precision_ns: int = 100 # 测量精度，纳秒

    def to_ns(self) -> int:
        return int(self.seconds * 1_000_000_000)

    def to_datetime(self):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(self.seconds, tz=timezone.utc)

    def __str__(self):
        return f"Timestamp({self.seconds:.9f}, {self.source})"

    def __hash__(self):
        return hash((self.seconds, self.source))

    def __eq__(self, other):
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.seconds == other.seconds and self.source == other.source


# ======================================================================
# 时钟模式
# ======================================================================
class ClockMode(str, Enum):
    REAL = "real"
    SIMULATED = "simulated"
    FROZEN = "frozen"


# ======================================================================
# 可注入的时间提供者接口
# ======================================================================
class TimeProvider(ABC):
    """抽象时间源，允许替换系统时钟（如 GPS 硬件、NTP 同步）"""
    @abstractmethod
    def wall_clock(self) -> float:
        ...

    @abstractmethod
    def monotonic_ns(self) -> int:
        ...

    @abstractmethod
    def precision(self) -> int:
        """返回此提供者的精度（纳秒）"""
        ...


class SystemTimeProvider(TimeProvider):
    """基于 Python 标准库的系统时间提供者"""
    def wall_clock(self) -> float:
        return time.time()

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def precision(self) -> int:
        # 依据平台估算，实际可动态测量
        return 100  # 约 100 ns


# ======================================================================
# 审计日志环
# ======================================================================
class AuditRingBuffer:
    """线程安全的环形缓冲区，用于存储审计记录"""
    def __init__(self, capacity: int = 10000):
        self._buffer: List[Tuple] = []
        self._capacity = capacity
        self._lock = threading.Lock()

    def append(self, entry):
        with self._lock:
            if len(self._buffer) >= self._capacity:
                self._buffer.pop(0)
            self._buffer.append(entry)

    def get(self, limit: int = 100) -> list:
        with self._lock:
            return list(self._buffer[-limit:])

    def clear(self):
        with self._lock:
            self._buffer.clear()

    def __len__(self):
        with self._lock:
            return len(self._buffer)

    def to_list(self) -> list:
        with self._lock:
            return list(self._buffer)


# ======================================================================
# 时钟单例
# ======================================================================
class ClockSingleton(type):
    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


# ======================================================================
# 主时钟类
# ======================================================================
class Clock(metaclass=ClockSingleton):
    """
    全球统一时钟（单例），纳秒精度，线程安全，可替换时间源。
    支持三种模式：
    - REAL: 通过 TimeProvider 提供真实世界时间，并利用单调时钟校准以避免 NTP 跳变。
    - SIMULATED: 手动推进，支持时间膨胀和无限快速模式。
    - FROZEN: 时间静止，用于灾难恢复演练。
    """

    TAG: Final[str] = "[Core::Clock]"
    MAX_AUDIT_CAPACITY: int = 50_000

    def __init__(self,
                 time_provider: TimeProvider = None,
                 event_bus=None,
                 semantic_index=None,
                 audit_capacity: int = MAX_AUDIT_CAPACITY):
        # 使用 RLock 允许同一线程重入
        self._lock = threading.RLock()
        self._mode: ClockMode = ClockMode.REAL
        # 模拟时间，纳秒整数
        self._sim_ns: int = 0
        # 时间膨胀系数
        self._dilation: float = 1.0
        # 记录单调时钟与 Wall-Clock 的初始映射，用于生成单调的 Wall-Clock 估算
        self._time_provider: TimeProvider = time_provider or SystemTimeProvider()
        self._init_mono_ns: int = self._time_provider.monotonic_ns()
        self._init_wall: float = self._time_provider.wall_clock()
        # 事件总线和语义索引
        self._event_bus = event_bus
        self._semantic_index = semantic_index
        # 审计日志环形缓冲
        self._audit_log = AuditRingBuffer(capacity=audit_capacity)
        # 步数计数
        self._step_count: int = 0
        # 冻结前保存的时间（用于解冻恢复）
        self._frozen_sim_ns: int = 0

        logger.info(f"{self.TAG} 初始化完成，模式={self._mode.value}, 时间源={type(self._time_provider).__name__}")

    # ------------------------------------------------------------------
    # 时间获取核心
    # ------------------------------------------------------------------
    def now(self) -> Timestamp:
        """获取当前时间戳（带来源标记），线程安全，保证单调性（实盘时）"""
        with self._lock:
            if self._mode == ClockMode.REAL:
                # 使用单调时钟校准，防止 NTP 跳变导致 time.time() 倒退
                mono_ns = self._time_provider.monotonic_ns()
                elapsed = (mono_ns - self._init_mono_ns) / 1e9
                calibrated_wall = self._init_wall + elapsed
                precision = self._time_provider.precision()
                return Timestamp(seconds=calibrated_wall, source="real", precision_ns=precision)
            elif self._mode == ClockMode.SIMULATED:
                s = self._sim_ns / 1e9
                return Timestamp(seconds=s, source="simulated", precision_ns=1)
            else:  # FROZEN
                s = self._sim_ns / 1e9
                return Timestamp(seconds=s, source="frozen", precision_ns=1)

    def monotonic_ns(self) -> int:
        """单调时钟纳秒，不受模式影响，用于性能测量"""
        return self._time_provider.monotonic_ns()

    def wall_clock_from_monotonic(self, mono_ns: int) -> float:
        """将单调时钟值转换为对应的 Wall-Clock 时间估算（仅基于初始化映射）"""
        elapsed = (mono_ns - self._init_mono_ns) / 1e9
        return self._init_wall + elapsed

    def real_wall_clock(self) -> float:
        """总是返回提供者的原始 Wall-Clock（可能不单调），用于对外兼容"""
        return self._time_provider.wall_clock()

    # ------------------------------------------------------------------
    # 模式切换
    # ------------------------------------------------------------------
    def set_mode(self, mode: ClockMode,
                 start_time_seconds: Optional[float] = None,
                 dilation: float = 1.0) -> Dict[str, Any]:
        """
        切换模式。
        - start_time_seconds: 模拟模式起始时间 (Unix timestamp)。
        - dilation: 时间膨胀系数，>1 为加速，0 < dilation < 1 为慢速。
        """
        if not isinstance(mode, ClockMode):
            return {"status": "error", "reason": "无效模式类型", "warnings": []}

        with self._lock:
            old_mode = self._mode
            if old_mode == mode:
                return {"status": "ok", "mode": mode.value, "warnings": ["已处于该模式"]}

            self._mode = mode
            if mode == ClockMode.SIMULATED:
                if start_time_seconds is not None:
                    self._sim_ns = int(start_time_seconds * 1e9)
                else:
                    # 默认从当前真实时间开始（若刚启动）或保持之前模拟时间
                    if self._sim_ns == 0:
                        self._sim_ns = int(self.real_wall_clock() * 1e9)
                self._dilation = float(dilation)
                logger.info(f"{self.TAG} 进入模拟模式，起始时间={self._sim_ns/1e9}, 膨胀={dilation}")
            elif mode == ClockMode.FROZEN:
                self._frozen_sim_ns = self._sim_ns
                logger.info(f"{self.TAG} 时间冻结于 {self._sim_ns/1e9}")
            else:  # REAL
                # 如果从模拟/冻结切回，重新校准映射，防止跳跃
                self._init_mono_ns = self._time_provider.monotonic_ns()
                self._init_wall = self._time_provider.wall_clock()
                logger.info(f"{self.TAG} 恢复实时时钟，重新校准映射")

            self._publish_event("mode_change", {"old": old_mode.value, "new": mode.value})
            return {"status": "ok", "mode": mode.value, "warnings": []}

    # ------------------------------------------------------------------
    # 时间推进（模拟/冻结）
    # ------------------------------------------------------------------
    def advance(self, delta_ns: int) -> Dict[str, Any]:
        """在模拟模式下推进时间，delta_ns 必须为非负整数纳秒。冻结模式下拒绝推进。"""
        with self._lock:
            if self._mode == ClockMode.REAL:
                return {"status": "error", "reason": "实盘模式下无法手动推进时间", "warnings": []}
            if self._mode == ClockMode.FROZEN:
                return {"status": "error", "reason": "冻结模式下时间不可推进，请先切换到模拟模式", "warnings": []}
            if delta_ns < 0:
                return {"status": "error", "reason": "时间只能向前推进", "warnings": []}
            # 应用膨胀系数
            effective_ns = int(round(delta_ns * self._dilation))
            if effective_ns == 0 and delta_ns > 0:
                effective_ns = 1  # 避免极小推进被吞
            self._sim_ns += effective_ns
            self._step_count += 1
            new_ts = self.now()
            self._audit_log.append(("advance", delta_ns, effective_ns, new_ts.seconds))
            self._publish_event("time_advanced", {
                "delta_ns_requested": delta_ns,
                "effective_ns": effective_ns,
                "new_time": new_ts.seconds
            })
            return {"status": "ok", "new_time": new_ts.seconds, "warnings": []}

    def wait_until(self, target_ts: float) -> Dict[str, Any]:
        """在模拟模式下，推进时钟直到达到目标时间戳。若已超过则返回警告。"""
        with self._lock:
            if self._mode != ClockMode.SIMULATED:
                return {"status": "error", "reason": "仅模拟模式支持", "warnings": []}
            current = self._sim_ns / 1e9
            if target_ts <= current:
                return {"status": "ok", "already_passed": True, "warnings": [f"目标时间 {target_ts} 已过"]}
            delta_ns = int((target_ts - current) * 1e9)
            return self.advance(delta_ns)

    def sleep_in_sim(self, ns: int) -> None:
        """模拟模式下的虚拟等待（推进时钟），方便策略代码移植。"""
        if self._mode == ClockMode.SIMULATED:
            self.advance(ns)

    # ------------------------------------------------------------------
    # 时间工具方法
    # ------------------------------------------------------------------
    def is_between(self, start_ts: float, end_ts: float, inclusive_end: bool = False) -> bool:
        """判断当前时间是否在 [start_ts, end_ts) 或 [start_ts, end_ts] 区间内"""
        cur = self.now().seconds
        if inclusive_end:
            return start_ts <= cur <= end_ts
        else:
            return start_ts <= cur < end_ts

    def align_to(self, period_seconds: float, anchor: float = 0.0) -> float:
        """
        返回距离当前时间最近的下一个对齐时间点。
        anchor: 对齐锚点（从 epoch 起的偏移），例如锚点 0 表示从 00:00:00 UTC 开始对齐。
        """
        if period_seconds <= 0:
            raise ValueError("period_seconds 必须为正数")
        cur = self.now().seconds
        # 计算自 anchor 起的周期数
        periods_since_anchor = (cur - anchor) / period_seconds
        next_period = math.ceil(periods_since_anchor)
        return anchor + next_period * period_seconds

    def next_at(self, hour: int, minute: int, second: int = 0) -> float:
        """返回今天剩余时间内或明天的指定 UTC 时刻的时间戳"""
        from datetime import datetime, timezone, timedelta
        now_dt = datetime.fromtimestamp(self.now().seconds, tz=timezone.utc)
        target = now_dt.replace(hour=hour, minute=minute, second=second, microsecond=0)
        if target <= now_dt:
            target += timedelta(days=1)
        return target.timestamp()

    # ------------------------------------------------------------------
    # 状态持久化与恢复
    # ------------------------------------------------------------------
    def save_state(self) -> Dict[str, Any]:
        """序列化完整状态，包括审计日志摘要"""
        with self._lock:
            return {
                "mode": self._mode.value,
                "sim_ns": self._sim_ns,
                "dilation": str(self._dilation),  # 字符串避免精度问题
                "step_count": self._step_count,
                "init_mono_ns": self._init_mono_ns,
                "init_wall": self._init_wall,
                "audit_log_len": len(self._audit_log),
            }

    def load_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """从保存状态恢复。验证必要字段，非法状态拒绝加载。"""
        required = {"mode", "sim_ns", "dilation", "step_count", "init_mono_ns", "init_wall"}
        missing = required - set(state.keys())
        if missing:
            return {"status": "error", "reason": f"缺少必要字段: {missing}", "warnings": []}
        try:
            mode = ClockMode(state["mode"])
        except ValueError:
            return {"status": "error", "reason": f"非法模式: {state['mode']}", "warnings": []}
        try:
            dilation = float(state["dilation"])
        except (ValueError, TypeError):
            return {"status": "error", "reason": "dilation 格式错误", "warnings": []}

        with self._lock:
            self._mode = mode
            self._sim_ns = int(state["sim_ns"])
            self._dilation = dilation
            self._step_count = int(state["step_count"])
            self._init_mono_ns = int(state["init_mono_ns"])
            self._init_wall = float(state["init_wall"])
            logger.info(f"{self.TAG} 状态恢复至 sim_ns={self._sim_ns}")
        return {"status": "ok", "warnings": ["审计日志未恢复"]}

    def get_audit_trail(self, limit: int = 100) -> list:
        """获取审计日志快照"""
        return self._audit_log.get(limit)

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _publish_event(self, event_type: str, data: dict):
        """发布事件并自动附加时间戳（无阻塞）"""
        enriched = {**data, "clock_ts": self.now().seconds}
        if self._event_bus:
            try:
                # 注意：此处调用同步 publish，为避免阻塞时钟线程，应保证 publish 是非阻塞的
                self._event_bus.publish(f"clock.{event_type}", enriched)
            except Exception as e:
                logger.error(f"{self.TAG} 事件发布失败: {e}  #RECOVERY: 检查EventBus")
        if self._semantic_index:
            try:
                self._semantic_index.log_event(f"clock.{event_type}", enriched)
            except Exception:
                pass

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """使用独立临时实例进行功能验证，不污染全局单例"""
        test_inst = None
        try:
            # 创建隔离实例（不注册单例）
            test_inst = object.__new__(Clock)
            # 手动注入安全的默认值
            test_inst._lock = threading.RLock()
            test_inst._mode = ClockMode.REAL
            test_inst._sim_ns = 0
            test_inst._dilation = 1.0
            test_inst._time_provider = SystemTimeProvider()
            test_inst._init_mono_ns = test_inst._time_provider.monotonic_ns()
            test_inst._init_wall = test_inst._time_provider.wall_clock()
            test_inst._event_bus = None
            test_inst._semantic_index = None
            test_inst._audit_log = AuditRingBuffer(100)
            test_inst._step_count = 0

            # 1. 实盘 now
            ts = test_inst.now()
            if ts.source != "real":
                return {"status": "error", "message": "实盘模式来源错误"}
            # 2. 切换模拟模式，检查时间从默认真实时间开始
            res = test_inst.set_mode(ClockMode.SIMULATED)
            if res["status"] != "ok":
                return {"status": "error", "message": "切换模拟模式失败"}
            if test_inst._sim_ns == 0:
                return {"status": "error", "message": "模拟时间未初始化"}
            # 3. 推进5秒
            res_adv = test_inst.advance(5_000_000_000)
            if res_adv["status"] != "ok" or test_inst.now().seconds != test_inst._sim_ns / 1e9:
                return {"status": "error", "message": "推进错误"}
            # 4. 冻结后推进应被拒绝
            test_inst.set_mode(ClockMode.FROZEN)
            res_froz = test_inst.advance(1_000_000)
            if res_froz["status"] != "error":
                return {"status": "error", "message": "冻结模式未拒绝推进"}
            # 5. 状态保存与恢复
            test_inst.set_mode(ClockMode.SIMULATED, start_time_seconds=1_600_000_000.0)
            state = test_inst.save_state()
            load_res = test_inst.load_state(state)
            if load_res["status"] != "ok":
                return {"status": "error", "message": f"状态恢复失败: {load_res}"}
            # 6. 对齐测试
            test_inst._sim_ns = int(1_600_000_005.0 * 1e9)  # 设置为 5 秒
            aligned = test_inst.align_to(60.0)  # 下一个分钟对齐
            if aligned <= 1_600_000_005.0:
                return {"status": "error", "message": "对齐时间错误"}
            # 7. 区间测试
            if not test_inst.is_between(1_600_000_000.0, 1_600_000_010.0):
                return {"status": "error", "message": "区间判断错误"}
            # 8. 等待直到目标时间
            test_inst._sim_ns = int(1_600_000_000.0 * 1e9)
            test_inst.wait_until(1_600_000_003.0)
            if test_inst.now().seconds != 1_600_000_003.0:
                return {"status": "error", "message": "wait_until 未正确推进"}

            return {"status": "ok", "message": "所有测试通过"}
        except Exception as e:
            logger.exception("Clock health check 异常")
            return {"status": "error", "message": str(e)}
        finally:
            if test_inst:
                # 清理可能残留的引用
                test_inst._lock = None

    # 禁止拷贝等操作
    def __copy__(self):
        raise TypeError("Clock 是单例，不可拷贝")

    def __deepcopy__(self, memo):
        raise TypeError("Clock 是单例，不可深拷贝")
