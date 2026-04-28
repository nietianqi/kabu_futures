from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .config import StrategyConfig
from .models import MultiTimeframeScore, PositionState, Signal, StrategyIntent
from .risk import OrderThrottle, RiskManager
from .sessions import SessionState, classify_jst_session, time_in_jst_window


MINUTE_SIGNAL_ENGINES = frozenset(("minute_orb", "minute_vwap", "directional_intraday"))


@dataclass(frozen=True)
class DecisionTrace:
    stage: str
    action: str
    reason: str
    blocked_by: str | None = None
    checks: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "stage": self.stage,
            "action": self.action,
            "reason": self.reason,
            "checks": self.checks,
        }
        if self.blocked_by is not None:
            payload["blocked_by"] = self.blocked_by
        return payload

    def as_metadata(self) -> dict[str, object]:
        metadata: dict[str, object] = {
            "decision_trace": self.as_dict(),
            "decision_stage": self.stage,
            "decision_action": self.action,
            "policy_reason": self.reason,
        }
        if self.blocked_by is not None:
            metadata["blocked_by"] = self.blocked_by
        return metadata


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    trace: DecisionTrace
    metadata: dict[str, object] = field(default_factory=dict)
    reject_stage: str | None = None

    @property
    def merged_metadata(self) -> dict[str, object]:
        metadata = dict(self.metadata)
        metadata.update(self.trace.as_metadata())
        if self.reject_stage is not None:
            metadata["reject_stage"] = self.reject_stage
        return metadata


class StrategyEntryPolicy:
    """Centralizes strategy entry authorization and its explanation metadata."""

    def __init__(self, config: StrategyConfig, risk: RiskManager, throttle: OrderThrottle) -> None:
        self.config = config
        self.risk = risk
        self.throttle = throttle

    def evaluate_minute(
        self,
        signal: Signal,
        timestamp: datetime,
        mtf_score: MultiTimeframeScore,
        intent: StrategyIntent,
        position: PositionState,
    ) -> PolicyDecision:
        block_reason, session_state, block_window = self.new_entry_block(timestamp)
        observe_reason = self.minute_observe_only_reason(signal)
        mtf_ok, mtf_reason = self.validate_mtf_score(mtf_score, require_execution=False)
        alpha_ok = intent.allowed
        alpha_reason = intent.veto_reason or intent.reason
        risk_ok, risk_reason = self.risk.validate_signal(signal, position)
        checks = {
            "session_ok": block_reason is None,
            "observe_only": observe_reason is not None,
            "mtf_ok": mtf_ok,
            "mtf_reason": mtf_reason,
            "alpha_ok": alpha_ok,
            "alpha_reason": alpha_reason,
            "risk_ok": risk_ok,
            "risk_reason": risk_reason,
        }
        metadata = session_metadata(session_state, block_window)
        if block_reason is not None:
            return self._reject("trade_intent", block_reason, "session_gate", "session_filter", checks, metadata)
        if observe_reason is not None:
            metadata["observe_only_signal"] = True
            return self._reject("trade_intent", observe_reason, "observe_only", "observe_only", checks, metadata)
        if not mtf_ok:
            return self._reject("trade_intent", mtf_reason, "multi_timeframe", "multi_timeframe", checks, metadata)
        if not alpha_ok:
            return self._reject("trade_intent", alpha_reason, "alpha_stack", "alpha_stack", checks, metadata)
        if not risk_ok:
            return self._reject("trade_intent", risk_reason, "risk", "risk", checks, metadata)
        return self._allow("trade_intent", signal.reason or "minute_signal_allowed", checks, metadata)

    def evaluate_micro(
        self,
        signal: Signal,
        timestamp: datetime,
        mtf_score: MultiTimeframeScore,
        intent: StrategyIntent,
        position: PositionState,
    ) -> PolicyDecision:
        throttle_ok, throttle_reason = self.throttle.allow(timestamp)
        mtf_ok, mtf_reason = self.validate_mtf_score(mtf_score, require_execution=True)
        risk_ok, risk_reason = self.risk.validate_signal(signal, position)
        alpha_ok = intent.allowed
        alpha_reason = intent.veto_reason or intent.reason
        block_reason, session_state, block_window = self.new_entry_block(timestamp)
        checks = {
            "session_ok": block_reason is None,
            "session_reason": block_reason or "ok",
            "throttle_ok": throttle_ok,
            "throttle_reason": throttle_reason,
            "mtf_ok": mtf_ok,
            "mtf_reason": mtf_reason,
            "risk_ok": risk_ok,
            "risk_reason": risk_reason,
            "alpha_ok": alpha_ok,
            "alpha_reason": alpha_reason,
        }
        metadata = session_metadata(session_state, block_window)
        if block_reason is not None:
            return self._reject("trade_intent", block_reason, "session_gate", "session_filter", checks, metadata)
        if not throttle_ok:
            return self._reject("trade_intent", throttle_reason, "throttle", "throttle", checks, metadata)
        if not mtf_ok:
            return self._reject("trade_intent", mtf_reason, "multi_timeframe", "multi_timeframe", checks, metadata)
        if not risk_ok:
            return self._reject("trade_intent", risk_reason, "risk", "risk", checks, metadata)
        if not alpha_ok:
            return self._reject("trade_intent", alpha_reason, "alpha_stack", "alpha_stack", checks, metadata)
        return self._allow("trade_intent", signal.reason or "micro_signal_allowed", checks, metadata)

    def new_entry_block(self, timestamp: datetime) -> tuple[str | None, SessionState, str | None]:
        session_state = classify_jst_session(timestamp, self.config.session_schedule)
        if not session_state.new_entries_allowed:
            return "session_not_tradeable", session_state, None
        for window in self.config.micro_engine.no_new_entry_windows_jst:
            if time_in_jst_window(timestamp, window):
                return "session_not_tradeable", session_state, window
        return None, session_state, None

    def minute_observe_only_reason(self, signal: Signal) -> str | None:
        if (
            self.config.minute_engine.trend_pullback_long_observe_only
            and signal.engine == "minute_vwap"
            and signal.reason == "trend_pullback_long"
        ):
            return "trend_pullback_long_observe_only"
        if (
            self.config.minute_engine.trend_pullback_short_observe_only
            and signal.engine == "minute_vwap"
            and signal.reason == "trend_pullback_short"
        ):
            return "trend_pullback_short_observe_only"
        if (
            self.config.minute_engine.directional_intraday_long_observe_only
            and signal.engine == "directional_intraday"
            and signal.direction == "long"
        ):
            return "directional_intraday_long_observe_only"
        return None

    def validate_mtf_score(self, score: MultiTimeframeScore, require_execution: bool) -> tuple[bool, str]:
        if score.veto_reason is not None:
            return False, score.veto_reason
        if score.total_score < self.config.multi_timeframe.min_total_score_to_trade:
            return False, "multi_timeframe_score_below_threshold"
        if require_execution and score.execution_score < self.config.multi_timeframe.min_execution_score_to_chase:
            return False, "execution_score_below_threshold"
        if score.position_scale == "none":
            return False, "position_scale_none"
        return True, "ok"

    def _allow(
        self,
        stage: str,
        reason: str,
        checks: dict[str, object],
        metadata: dict[str, object],
    ) -> PolicyDecision:
        trace = DecisionTrace(stage, "allow", reason, checks=checks)
        return PolicyDecision(True, reason, trace, metadata)

    def _reject(
        self,
        stage: str,
        reason: str,
        blocked_by: str,
        reject_stage: str,
        checks: dict[str, object],
        metadata: dict[str, object],
    ) -> PolicyDecision:
        trace = DecisionTrace(stage, "reject", reason, blocked_by=blocked_by, checks=checks)
        return PolicyDecision(False, reason, trace, metadata, reject_stage=reject_stage)


class LiveEntryPolicy:
    """Live-only order authorization. Paper/observe replay deliberately bypasses this."""

    def __init__(self, config: StrategyConfig) -> None:
        self.config = config

    def evaluate_signal(self, signal: Signal) -> PolicyDecision:
        checks = {
            "engine": signal.engine,
            "supported_engines": self.config.live_execution.supported_engines,
            "is_tradeable": signal.is_tradeable,
            "symbol": signal.symbol,
            "primary_symbol": self.config.symbols.primary,
        }
        if signal.engine not in self.config.live_execution.supported_engines:
            return _live_reject("live_unsupported_signal_engine", "live_supported_engines", checks)
        if not signal.is_tradeable:
            return _live_reject("non_tradeable_signal", "signal_tradeability", checks)
        if signal.price is None:
            return _live_reject("missing_signal_price", "signal_price", checks)
        if signal.symbol != self.config.symbols.primary:
            return _live_reject("unsupported_symbol", "symbol", checks)
        if signal.engine in MINUTE_SIGNAL_ENGINES:
            minute_decision = self._evaluate_minute_signal(signal, checks)
            if not minute_decision.allowed:
                return minute_decision
        trace = DecisionTrace("execution_order", "allow", "live_entry_signal_allowed", checks=checks)
        return PolicyDecision(True, "live_entry_signal_allowed", trace)

    def _evaluate_minute_signal(self, signal: Signal, checks: dict[str, object]) -> PolicyDecision:
        atr = signal.metadata.get("atr")
        execution_score = signal.metadata.get("execution_score")
        min_execution_score = self.config.multi_timeframe.min_execution_score_to_chase
        checks.update(
            {
                "atr": atr,
                "execution_score": execution_score,
                "min_execution_score_to_chase": min_execution_score,
            }
        )
        if not isinstance(atr, (int, float)) or float(atr) <= 0:
            return _live_reject("live_minute_atr_missing", "live_minute_atr", checks)
        if not isinstance(execution_score, (int, float)) or float(execution_score) < min_execution_score:
            return _live_reject(
                "live_minute_execution_score_below_threshold",
                "live_minute_execution_score",
                checks,
            )
        return PolicyDecision(True, "live_minute_signal_allowed", DecisionTrace("execution_order", "allow", "live_minute_signal_allowed", checks=checks))


def event_trace_metadata(
    stage: str,
    action: str,
    reason: str,
    blocked_by: str | None = None,
    checks: dict[str, object] | None = None,
) -> dict[str, object]:
    return DecisionTrace(stage, action, reason, blocked_by=blocked_by, checks=checks or {}).as_metadata()


def session_metadata(session_state: SessionState, extra_window: str | None = None) -> dict[str, object]:
    metadata: dict[str, object] = {
        "session_phase": session_state.phase,
        "session_window_jst": session_state.window_jst,
        "new_entries_allowed": session_state.new_entries_allowed and extra_window is None,
        "api_window_status": session_state.api_window_status,
    }
    if extra_window is not None:
        metadata["no_new_entry_window_jst"] = extra_window
    return metadata


def _live_reject(reason: str, blocked_by: str, checks: dict[str, object]) -> PolicyDecision:
    trace = DecisionTrace("execution_order", "reject", reason, blocked_by=blocked_by, checks=checks)
    return PolicyDecision(False, reason, trace, reject_stage=blocked_by)
