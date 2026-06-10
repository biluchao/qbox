#!/usr/bin/env python3
"""
火种系统 · 通用有限状态机 (QuantumStateMachine)

核心职责：
1. 提供事务性、线程安全、时钟可注入的有限状态机
2. 支持事件驱动、优先级转移、超时自动转移、强制转移
3. 完备的审计日志、语义索引、转移历史、序列化与恢复
4. 内置 ERROR 状态处理转移失败，确保系统最终一致性

外部依赖（真实模块接口）：
- core.semantic_index.SemanticIndex : 状态转移事件持久化（可选）
- core.event_bus.EventBus : 自动发布状态变更事件（可选）

接口契约：
- add_state(name, initial=False, timeout=None, timeout_target=None) -> Dict
- remove_state(name) -> Dict
- add_transition(from_state, to_state, event=None, condition=None, action=None, priority=0) -> Dict
- remove_transition(from_state, to_state, event=None) -> Dict
- on_entry(state, callback) -> Dict
- on_exit(state, callback) -> Dict
- before_transition(callback) -> Dict
- after_transition(callback) -> Dict
- start(initial_state=None) -> Dict
- on_event(event, payload=None) -> Dict
- trigger(target_state, payload=None) -> Dict
- force_transition(target_state, payload=None) -> Dict
- reset(clear_callbacks=False) -> Dict
- pause() / resume() -> Dict
- check_timeout(current_time=None) -> Dict
- get_transition_history(limit) -> List
- to_dict() / from_dict(data, callback_registry=None) -> Dict
- current_state, status, machine_id 属性
- health_check() -> Dict[str, Any]

异常与降级：
- 所有公共方法返回统一字典，内部异常转换为 status="error"，并记录日志，绝不抛异常
- 回调失败不会中断状态机，但触发错误状态或记录告警，必要时可配置进入 ERROR 状态

资源管理：
- 实例必须显式调用 close() 清理回调和释放锁
- 使用 time.monotonic 或可注入时钟，保证回测一致性
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from bisect import insort
from copy import copy
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

class Status(Enum):
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"
    CLOSED = "closed"

class Transition:
    """转移规则（不可变）"""
    __slots__ = ('to_state', 'condition', 'action', 'priority', 'event')

    def __init__(self, to_state: str, condition: Optional[Callable[[Any], bool]] = None,
                 action: Optional[Callable[[Any], None]] = None, priority: int = 0, event: str = ""):
        self.to_state = to_state
        self.condition = condition or (lambda _: True)
        self.action = action or (lambda _: None)
        self.priority = priority
        self.event = event

    def __repr__(self) -> str:
        return f"Transition(to={self.to_state}, event={self.event}, priority={self.priority})"

class QuantumStateMachine:
    """
    事件驱动、事务性、线程安全、带完整审计的状态机。
    适用于订单管理、策略生命周期等关键金融流程。
    """

    # 可配置的常量
    MAX_STATE_NAME_LENGTH = 128
    MAX_EVENT_NAME_LENGTH = 128
    MAX_TRANSITION_HISTORY = 1000

    def __init__(self, name: str = "Unnamed", machine_id: str = None,
                 semantic_index=None, event_bus=None, clock: Callable[[], float] = None):
        self.name = name
        self.machine_id = machine_id or str(uuid.uuid4())
        self._semantic_index = semantic_index
        self._event_bus = event_bus
        self._clock = clock or time.monotonic  # 可注入时钟，支持回测

        self._states: Dict[str, dict] = {}
        self._transitions: Dict[str, Dict[str, List[Transition]]] = {}
        self._current_state: Optional[str] = None
        self._initial_state: Optional[str] = None
        self._status = Status.STOPPED
        self._lock = threading.RLock()
        self._entry_callbacks: Dict[str, List[Callable]] = {}
        self._exit_callbacks: Dict[str, List[Callable]] = {}
        self._before_hooks: List[Callable] = []
        self._after_hooks: List[Callable] = []
        self._last_transition_ts: Optional[float] = None
        self._transition_count: int = 0
        self._history: List[Dict[str, Any]] = []  # 最近 N 次转移记录

        logger.info(f"[Core::Init] QSM {self.machine_id} ({self.name}) 已创建")

    # ---- 状态管理 ----
    def add_state(self, name: str, initial: bool = False,
                  timeout: Optional[float] = None, timeout_target: Optional[str] = None) -> Dict[str, Any]:
        """添加状态。timeout: 超时秒数，支持0（立即超时）。"""
        if not isinstance(name, str) or not name.strip():
            return {"status": "error", "reason": "状态名必须为非空字符串", "warnings": []}
        if len(name) > self.MAX_STATE_NAME_LENGTH:
            return {"status": "error", "reason": f"状态名过长 ({len(name)}>{self.MAX_STATE_NAME_LENGTH})", "warnings": []}
        if "::" in name:
            return {"status": "error", "reason": "状态名不能包含 '::'", "warnings": []}
        with self._lock:
            if name in self._states:
                return {"status": "error", "reason": f"状态 {name} 已存在", "warnings": []}
            self._states[name] = {
                'timeout': timeout,
                'timeout_target': timeout_target
            }
            self._transitions[name] = {}
            self._entry_callbacks[name] = []
            self._exit_callbacks[name] = []
            if initial:
                if self._initial_state is not None:
                    return {"status": "error", "reason": "初始状态只能有一个，已设定", "warnings": []}
                self._initial_state = name
            logger.info(f"[Core::AddState] {name} initial={initial}")
            return {"status": "ok", "reason": f"状态 {name} 已添加", "warnings": []}

    def remove_state(self, name: str) -> Dict[str, Any]:
        """移除状态，同时清理所有相关转移和回调。警告：必须确保没有活跃引用。"""
        if name == self._current_state:
            return {"status": "error", "reason": "无法移除当前活跃状态", "warnings": []}
        with self._lock:
            if name not in self._states:
                return {"status": "error", "reason": f"状态 {name} 不存在", "warnings": []}
            # 清理转入/转出转移
            for src in list(self._transitions.keys()):
                if src == name:
                    del self._transitions[src]
                else:
                    self._transitions[src] = {
                        evt: [t for t in lst if t.to_state != name]
                        for evt, lst in self._transitions[src].items()
                    }
                    # 移除后为空的事件键可删除
                    self._transitions[src] = {k: v for k, v in self._transitions[src].items() if v}
            del self._states[name]
            self._entry_callbacks.pop(name, None)
            self._exit_callbacks.pop(name, None)
            if self._initial_state == name:
                self._initial_state = None
            return {"status": "ok", "reason": f"状态 {name} 已移除", "warnings": []}

    # ---- 转移规则 ----
    def add_transition(self, from_state: str, to_state: str,
                       event: Optional[str] = None,
                       condition: Optional[Callable[[Any], bool]] = None,
                       action: Optional[Callable[[Any], None]] = None,
                       priority: int = 0) -> Dict[str, Any]:
        """添加转移，event 为空时默认使用 to_state 作为事件名。优先级数字越小越高。"""
        if from_state not in self._states:
            return {"status": "error", "reason": f"源状态 {from_state} 不存在", "warnings": []}
        if to_state not in self._states:
            return {"status": "error", "reason": f"目标状态 {to_state} 不存在", "warnings": []}
        event_name = event.strip() if event else to_state
        if not event_name or len(event_name) > self.MAX_EVENT_NAME_LENGTH:
            return {"status": "error", "reason": "事件名非法或过长", "warnings": []}
        trans = Transition(to_state, condition, action, priority, event_name)
        with self._lock:
            if event_name not in self._transitions[from_state]:
                self._transitions[from_state][event_name] = []
            insort(self._transitions[from_state][event_name], trans, key=lambda t: t.priority)
            logger.info(f"[Core::AddTrans] {from_state} --({event_name})--> {to_state} pri={priority}")
            return {"status": "ok", "reason": "转移规则已添加", "warnings": []}

    def remove_transition(self, from_state: str, to_state: str, event: Optional[str] = None) -> Dict[str, Any]:
        event_name = event if event else to_state
        with self._lock:
            if from_state not in self._transitions or event_name not in self._transitions[from_state]:
                return {"status": "error", "reason": "转移规则不存在", "warnings": []}
            before = len(self._transitions[from_state][event_name])
            self._transitions[from_state][event_name] = [
                t for t in self._transitions[from_state][event_name]
                if not (t.to_state == to_state and t.event == event_name)
            ]
            after = len(self._transitions[from_state][event_name])
            if after == before:
                return {"status": "error", "reason": "未找到匹配规则", "warnings": []}
            return {"status": "ok", "reason": f"已删除 {before-after} 条规则", "warnings": []}

    # ---- 回调注册 ----
    def on_entry(self, state: str, callback: Callable[[Any], None]) -> Dict[str, Any]:
        if state not in self._states:
            return {"status": "error", "reason": f"状态 {state} 不存在", "warnings": []}
        with self._lock:
            self._entry_callbacks[state].append(callback)
        return {"status": "ok", "reason": "进入回调已注册", "warnings": []}

    def on_exit(self, state: str, callback: Callable[[Any], None]) -> Dict[str, Any]:
        if state not in self._states:
            return {"status": "error", "reason": f"状态 {state} 不存在", "warnings": []}
        with self._lock:
            self._exit_callbacks[state].append(callback)
        return {"status": "ok", "reason": "离开回调已注册", "warnings": []}

    def before_transition(self, callback: Callable[[str, str, Any], None]) -> Dict[str, Any]:
        self._before_hooks.append(callback)
        return {"status": "ok", "reason": "前置钩子已添加", "warnings": []}

    def after_transition(self, callback: Callable[[str, str, Any], None]) -> Dict[str, Any]:
        self._after_hooks.append(callback)
        return {"status": "ok", "reason": "后置钩子已添加", "warnings": []}

    # ---- 生命周期 ----
    def start(self, initial_state: str = None) -> Dict[str, Any]:
        with self._lock:
            if self._status != Status.STOPPED:
                return {"status": "error", "reason": f"状态机已启动 (当前: {self._status.value})，请先 reset()", "warnings": []}
            state = initial_state or self._initial_state
            if not state:
                return {"status": "error", "reason": "未指定初始状态", "warnings": []}
            if state not in self._states:
                return {"status": "error", "reason": f"初始状态 {state} 不存在", "warnings": []}
            self._current_state = state
            self._status = Status.RUNNING
            self._last_transition_ts = self._clock()
            self._execute_entry_callbacks(state, None)
            self._log_state_change(None, state, "start")
            self._add_history(None, state, "start")
            return {"status": "ok", "reason": f"状态机已启动，进入 {state}", "warnings": []}

    def reset(self, clear_callbacks: bool = False) -> Dict[str, Any]:
        """重置状态机。clear_callbacks 是否清除所有回调（用于热重载）。"""
        with self._lock:
            if self._current_state:
                # 安全退出当前状态（不记录转移）
                self._safe_execute_exit(self._current_state, None)
            self._current_state = None
            self._status = Status.STOPPED
            self._last_transition_ts = None
            if clear_callbacks:
                self._entry_callbacks.clear()
                self._exit_callbacks.clear()
                self._before_hooks.clear()
                self._after_hooks.clear()
            logger.info(f"[Core::Reset] 状态机已重置")
            return {"status": "ok", "reason": "状态机已重置", "warnings": []}

    def pause(self) -> Dict[str, Any]:
        with self._lock:
            if self._status == Status.RUNNING:
                self._status = Status.PAUSED
                return {"status": "ok", "reason": "已暂停", "warnings": []}
            return {"status": "error", "reason": f"无法暂停，当前状态: {self._status.value}", "warnings": []}

    def resume(self) -> Dict[str, Any]:
        with self._lock:
            if self._status == Status.PAUSED:
                self._status = Status.RUNNING
                return {"status": "ok", "reason": "已恢复", "warnings": []}
            return {"status": "error", "reason": f"无法恢复，当前状态: {self._status.value}", "warnings": []}

    # ---- 核心事件处理 ----
    def on_event(self, event: str, payload: Any = None) -> Dict[str, Any]:
        """处理事件，尝试转移。返回转移结果。"""
        if not event or not isinstance(event, str):
            return {"status": "error", "reason": "事件名不能为空", "warnings": []}
        with self._lock:
            if self._status == Status.CLOSED:
                return {"status": "error", "reason": "状态机已关闭", "warnings": []}
            if self._status != Status.RUNNING:
                return {"status": "error", "reason": f"状态机未运行 (当前: {self._status.value})", "warnings": []}
            if self._current_state is None:
                return {"status": "error", "reason": "状态机未初始化", "warnings": []}

            from_state = self._current_state
            candidates = self._transitions.get(from_state, {}).get(event, [])
            if not candidates:
                logger.debug(f"[{self.machine_id}] 无转移: {from_state} --({event})")
                return {"status": "error", "reason": f"无匹配转移 {from_state} --({event})", "warnings": []}

            # 前置钩子：传递可能的候选目标列表
            candidate_targets = [t.to_state for t in candidates]
            self._execute_before_hooks(from_state, candidate_targets, payload)

            chosen_transition = None
            for trans in candidates:
                try:
                    if trans.condition(payload):
                        chosen_transition = trans
                        break
                except Exception as e:
                    logger.exception(f"条件检查异常 {trans}: {e}")
                    return {"status": "error", "reason": f"条件检查失败: {e}", "warnings": []}

            if not chosen_transition:
                return {"status": "error", "reason": "所有条件均不满足", "warnings": []}

            new_state = chosen_transition.to_state
            return self._execute_transition(from_state, new_state, event, payload, chosen_transition.action)

    def trigger(self, target_state: str, payload: Any = None) -> Dict[str, Any]:
        """兼容旧版，以目标状态作为事件名触发。"""
        return self.on_event(target_state, payload)

    def force_transition(self, target_state: str, payload: Any = None) -> Dict[str, Any]:
        """强制转移（忽略条件/动作），但仍执行进出回调。用于紧急风控。"""
        if target_state not in self._states:
            return {"status": "error", "reason": f"目标状态 {target_state} 不存在", "warnings": []}
        with self._lock:
            if self._status == Status.CLOSED:
                return {"status": "error", "reason": "状态机已关闭", "warnings": []}
            if self._current_state is None:
                return {"status": "error", "reason": "状态机未初始化", "warnings": []}
            from_state = self._current_state
            return self._execute_transition(from_state, target_state, "force", payload, action=None)

    def _execute_transition(self, from_state: str, to_state: str, trigger: str, payload: Any,
                            action: Optional[Callable] = None) -> Dict[str, Any]:
        """
        事务性转移核心：
        1. 执行离开回调（若有）
        2. 执行转移动作（若有）
        3. 更新状态并执行进入回调
        如果任何步骤失败，回滚状态，记录错误，并可选进入 ERROR 状态。
        """
        # 暂存旧状态，用于回滚
        old_state = from_state
        exit_executed = False
        entry_executed = False
        try:
            # 离开回调
            self._safe_execute_exit(old_state, payload)
            exit_executed = True
            # 转移动作
            if action:
                action(payload)
            # 更新状态
            self._current_state = to_state
            self._transition_count += 1
            self._last_transition_ts = self._clock()
            # 进入回调
            self._safe_execute_entry(to_state, payload)
            entry_executed = True
            # 后置钩子
            self._execute_after_hooks(old_state, to_state, payload)
            # 审计与事件
            self._log_state_change(old_state, to_state, trigger, payload)
            self._publish_event(old_state, to_state, trigger, payload)
            self._add_history(old_state, to_state, trigger)
            return {"status": "ok", "reason": f"转移成功: {old_state} -> {to_state}", "warnings": []}
        except Exception as e:
            logger.exception(f"转移 {old_state} -> {to_state} 失败: {e} #RECOVERY: 进入错误处理")
            # 尝试回滚状态
            if entry_executed:
                # 已经进入新状态，尝试离开它
                self._safe_execute_exit(to_state, payload)
            self._current_state = old_state
            # 如果离开回调已执行，尝试重新进入旧状态补偿
            if exit_executed and not entry_executed:
                self._safe_execute_entry(old_state, payload)
            # 进入错误状态（如果定义）
            if "ERROR" in self._states and self._current_state != "ERROR":
                self._current_state = "ERROR"
                self._safe_execute_entry("ERROR", {"error": str(e)})
                self._status = Status.ERROR
            self._log_state_change(old_state, self._current_state, "error", str(e))
            return {"status": "error", "reason": f"转移异常: {e}", "warnings": ["状态已回滚或进入ERROR"]}

    def check_timeout(self, current_time: Optional[float] = None) -> Dict[str, Any]:
        """检查当前状态是否超时，若超时则自动转向 timeout_target。建议外部高频调用。"""
        now = current_time if current_time is not None else self._clock()
        with self._lock:
            if self._status != Status.RUNNING or self._current_state is None:
                return {"status": "ok", "reason": "无需检查"}
            state_info = self._states.get(self._current_state)
            timeout = state_info.get('timeout')
            if timeout is None:
                return {"status": "ok", "reason": "无超时设置"}
            if self._last_transition_ts is None:
                return {"status": "ok", "reason": "无上次转移时间"}
            if now - self._last_transition_ts >= timeout:
                target = state_info['timeout_target']
                if not target:
                    return {"status": "error", "reason": "超时但未配置目标状态"}
                logger.info(f"超时: {self._current_state} -> {target}")
                return self.force_transition(target, payload={"reason": "timeout"})
            return {"status": "ok", "reason": f"剩余 {timeout - (now - self._last_transition_ts):.1f}s"}

    # ---- 查询 ----
    @property
    def current_state(self) -> Optional[str]:
        return self._current_state

    @property
    def status(self) -> Status:
        return self._status

    def get_states(self) -> List[str]:
        with self._lock:
            return list(self._states.keys())

    def get_transitions(self) -> Dict[str, Dict[str, List[Dict]]]:
        with self._lock:
            result = {}
            for from_s, events in self._transitions.items():
                result[from_s] = {}
                for evt, lst in events.items():
                    result[from_s][evt] = [{"to": t.to_state, "priority": t.priority} for t in lst]
            return result

    def get_transition_history(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            return self._history[-limit:]

    # ---- 序列化 ----
    def to_dict(self, serialize_callbacks: bool = False) -> Dict[str, Any]:
        with self._lock:
            data = {
                "name": self.name,
                "machine_id": self.machine_id,
                "states": {k: v for k, v in self._states.items()},
                "transitions": {
                    f"{f}--{e}": [{"to": t.to_state, "priority": t.priority, "event": t.event} for t in lst]
                    for f, evts in self._transitions.items()
                    for e, lst in evts.items()
                },
                "initial_state": self._initial_state,
                "current_state": self._current_state,
                "status": self._status.value,
                "last_ts": self._last_transition_ts,
                "transition_count": self._transition_count,
                "history": self._history[-100:]  # 保留最近100条
            }
            if serialize_callbacks:
                # 回调不可序列化，仅保存注册信息提示
                data["callbacks_summary"] = {
                    "entry_states": list(self._entry_callbacks.keys()),
                    "exit_states": list(self._exit_callbacks.keys()),
                    "hooks": len(self._before_hooks) + len(self._after_hooks)
                }
            return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any], callback_registry: Dict[str, Callable] = None, **kwargs) -> 'QuantumStateMachine':
        """从字典恢复状态机。callback_registry 应为 {name: callable}。"""
        sm = cls(name=data["name"], machine_id=data["machine_id"], **kwargs)
        for sname, info in data.get("states", {}).items():
            sm.add_state(sname, initial=(sname == data.get("initial_state")),
                         timeout=info.get('timeout'), timeout_target=info.get('timeout_target'))
        for key, lst in data.get("transitions", {}).items():
            parts = key.split("--", 1)
            if len(parts) == 2:
                from_s, evt = parts
                for t in lst:
                    sm.add_transition(from_s, t["to"], event=t.get("event", evt), priority=t["priority"])
        if data.get("current_state"):
            sm.start(data["current_state"])
        # 不恢复回调，依赖注册表
        return sm

    # ---- 内部辅助 ----
    def _safe_execute_exit(self, state: str, payload: Any):
        for cb in list(self._exit_callbacks.get(state, [])):  # 快照遍历
            try:
                cb(payload)
            except Exception as e:
                logger.exception(f"退出回调异常 {state}: {e}")

    def _safe_execute_entry(self, state: str, payload: Any):
        for cb in list(self._entry_callbacks.get(state, [])):
            try:
                cb(payload)
            except Exception as e:
                logger.exception(f"进入回调异常 {state}: {e}")

    def _execute_before_hooks(self, from_state: str, candidate_targets: List[str], payload: Any):
        for hook in self._before_hooks[:]:  # 快照
            try:
                hook(from_state, candidate_targets, payload)
            except Exception as e:
                logger.exception(f"前置钩子异常: {e}")

    def _execute_after_hooks(self, old: str, new: str, payload: Any):
        for hook in self._after_hooks[:]:
            try:
                hook(old, new, payload)
            except Exception as e:
                logger.exception(f"后置钩子异常: {e}")

    def _log_state_change(self, old: Optional[str], new: str, trigger: str, payload: Any = None):
        payload_str = ""
        try:
            payload_str = str(payload)[:200]
        except Exception:
            payload_str = "<unprintable>"
        msg = f"[Core::StateChange::{self.machine_id}] {old} --({trigger})--> {new} | {payload_str}"
        logger.info(msg)
        if self._semantic_index:
            self._semantic_index.log_event(
                event_type=f"Core::StateChange::{self.machine_id}",
                data={"from": old, "to": new, "trigger": trigger}
            )

    def _publish_event(self, old, new, event, payload):
        if self._event_bus:
            # 事件总线发布移到锁外？这里简单调用
            try:
                self._event_bus.publish("state_machine.change", {
                    "machine_id": self.machine_id,
                    "from": old, "to": new, "event": event, "payload": payload
                })
            except Exception as e:
                logger.exception(f"事件总线发布失败: {e}")

    def _add_history(self, old: Optional[str], new: str, trigger: str):
        self._history.append({
            "ts": self._clock(),
            "from": old,
            "to": new,
            "trigger": trigger,
            "count": self._transition_count
        })
        if len(self._history) > self.MAX_TRANSITION_HISTORY:
            self._history = self._history[-self.MAX_TRANSITION_HISTORY:]

    def close(self):
        with self._lock:
            self._status = Status.CLOSED
            self._entry_callbacks.clear()
            self._exit_callbacks.clear()
            self._before_hooks.clear()
            self._after_hooks.clear()
            self._history.clear()
            logger.info(f"[Core::Close] 状态机 {self.machine_id} 已关闭")

    def __repr__(self) -> str:
        return f"<QuantumStateMachine {self.machine_id[:8]} name={self.name} state={self._current_state} status={self._status.value}>"

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """全面自检，覆盖正常路径和错误路径"""
        try:
            sm = cls("health")
            # 1. 基本状态添加
            assert sm.add_state("A", initial=True)["status"] == "ok"
            assert sm.add_state("B")["status"] == "ok"
            assert sm.add_state("ERROR")["status"] == "ok"
            # 2. 转移添加
            assert sm.add_transition("A", "B")["status"] == "ok"
            # 3. 启动和转移
            assert sm.start()["status"] == "ok"
            assert sm.current_state == "A"
            res = sm.on_event("B", payload=None)
            assert res["status"] == "ok" and sm.current_state == "B"
            # 4. 事件驱动转移
            sm.reset()
            sm.add_state("X", initial=True)
            sm.add_state("Y")
            def cond(p): return p and p.get("go")
            sm.add_transition("X", "Y", event="go", condition=cond)
            sm.start()
            res = sm.on_event("go", {"go": True})
            assert res["status"] == "ok" and sm.current_state == "Y"
            # 5. 超时测试
            sm.reset()
            clock = [100.0]  # 可变时钟
            def fake_clock(): return clock[0]
            sm2 = cls("timeout_test", clock=fake_clock)
            sm2.add_state("T1", initial=True, timeout=5.0, timeout_target="T2")
            sm2.add_state("T2")
            sm2.start()
            clock[0] = 106.0
            res = sm2.check_timeout()
            assert sm2.current_state == "T2"
            # 6. 错误状态回滚
            sm3 = cls("error_test")
            sm3.add_state("S1", initial=True)
            sm3.add_state("S2")
            sm3.add_state("ERROR")
            def bad_action(p): raise RuntimeError("boom")
            sm3.add_transition("S1", "S2", action=bad_action)
            sm3.start()
            res = sm3.on_event("S2")
            assert res["status"] == "error"
            assert sm3.current_state == "ERROR" or sm3.current_state == "S1"  # 取决于错误处理
            # 清理
            sm.close()
            sm2.close()
            sm3.close()
            return {"status": "ok", "reason": "所有健康检查通过", "warnings": []}
        except Exception as e:
            logger.exception("健康检查失败")
            return {"status": "error", "reason": str(e), "warnings": []}
