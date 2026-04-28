from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .config import StrategyConfig
from .models import Signal
from .policy import event_trace_metadata


MINUTE_VWAP_PULLBACK_REASONS = frozenset(("trend_pullback_long", "trend_pullback_short"))
MICRO_SMALL_LOSS_EXIT_REASONS = frozenset(("imbalance_neutral", "microprice_neutral_or_reverse"))
MINUTE_STOP_LOSS_PAUSE_THRESHOLD = 2
MINUTE_TREND_PULLBACK_MIN_SCORE = 10.0


@dataclass(frozen=True)
class LiveSafetyDecision:
    allowed: bool
    reason: str
    metadata: dict[str, object] = field(default_factory=dict)


class LiveSafetyState:
    """Live-only safety gates that need memory across entries and exits."""

    def __init__(self, config: StrategyConfig) -> None:
        self.config = config
        self.minute_cooldown_until: dict[str, datetime] = {}
        self.minute_pause_until: dict[str, datetime] = {}
        self.minute_consecutive_stop_losses: dict[str, int] = {}
        self.micro_small_loss_count = 0
        self.micro_pause_until: datetime | None = None

    def evaluate_entry(self, signal: Signal, event_time: datetime) -> LiveSafetyDecision:
        if _is_minute_vwap_pullback(signal):
            decision = self._evaluate_minute_vwap_pullback(signal, event_time)
            if not decision.allowed:
                return decision
        if signal.engine == "micro_book":
            decision = self._evaluate_micro_entry(event_time)
            if not decision.allowed:
                return decision
        return LiveSafetyDecision(True, "live_safety_allowed")

    def record_entry(self, signal: Signal, event_time: datetime) -> None:
        if not _is_minute_vwap_pullback(signal):
            return
        cooldown_seconds = max(0, int(self.config.live_execution.minute_cooldown_seconds))
        if cooldown_seconds <= 0:
            return
        self.minute_cooldown_until[_minute_key(signal.engine, signal.reason, signal.direction)] = event_time + timedelta(
            seconds=cooldown_seconds
        )

    def record_exit(
        self,
        engine: str | None,
        direction: str | None,
        signal_reason: str | None,
        exit_reason: str,
        pnl_ticks: float,
        event_time: datetime,
    ) -> None:
        engine_name = str(engine or "")
        direction_name = str(direction or "")
        signal_reason_name = str(signal_reason or "")
        if engine_name == "minute_vwap" and signal_reason_name in MINUTE_VWAP_PULLBACK_REASONS:
            self._record_minute_exit(engine_name, direction_name, signal_reason_name, exit_reason, pnl_ticks, event_time)
        if engine_name == "micro_book":
            self._record_micro_exit(exit_reason, pnl_ticks, event_time)

    def summary(self, event_time: datetime | None = None) -> dict[str, object]:
        return {
            "minute_cooldown_until": _format_time_map(self.minute_cooldown_until),
            "minute_pause_until": _format_time_map(self.minute_pause_until),
            "minute_consecutive_stop_losses": dict(self.minute_consecutive_stop_losses),
            "micro_small_loss_count": self.micro_small_loss_count,
            "micro_pause_until": self.micro_pause_until.isoformat() if self.micro_pause_until is not None else None,
            "micro_pause_active": bool(event_time is not None and self.micro_pause_until is not None and event_time < self.micro_pause_until),
        }

    def _evaluate_minute_vwap_pullback(self, signal: Signal, event_time: datetime) -> LiveSafetyDecision:
        checks = _minute_checks(signal, self._minute_execution_score_threshold())
        atr = signal.metadata.get("atr")
        execution_score = signal.metadata.get("execution_score")
        if not isinstance(atr, (int, float)) or float(atr) <= 0:
            return _reject("live_minute_atr_missing", "live_minute_atr", checks)
        if not isinstance(execution_score, (int, float)) or float(execution_score) < self._minute_execution_score_threshold():
            return _reject("live_minute_execution_score_below_threshold", "live_minute_execution_score", checks)

        key = _minute_key(signal.engine, signal.reason, signal.direction)
        cooldown_until = self.minute_cooldown_until.get(key)
        if cooldown_until is not None and event_time < cooldown_until:
            checks["cooldown_until"] = cooldown_until.isoformat()
            return _reject("live_minute_cooldown_active", "live_minute_cooldown", checks)
        if cooldown_until is not None and event_time >= cooldown_until:
            self.minute_cooldown_until.pop(key, None)

        pause_until = self.minute_pause_until.get(key)
        if pause_until is not None and event_time < pause_until:
            checks["pause_until"] = pause_until.isoformat()
            checks["consecutive_stop_losses"] = self.minute_consecutive_stop_losses.get(key, 0)
            return _reject("live_minute_stop_loss_pause", "live_minute_stop_loss_pause", checks)
        if pause_until is not None and event_time >= pause_until:
            self.minute_pause_until.pop(key, None)
            self.minute_consecutive_stop_losses[key] = 0
        return LiveSafetyDecision(True, "live_minute_safety_allowed")

    def _evaluate_micro_entry(self, event_time: datetime) -> LiveSafetyDecision:
        if self.micro_pause_until is None:
            return LiveSafetyDecision(True, "live_micro_safety_allowed")
        if event_time >= self.micro_pause_until:
            self.micro_pause_until = None
            self.micro_small_loss_count = 0
            return LiveSafetyDecision(True, "live_micro_safety_allowed")
        return _reject(
            "live_micro_small_loss_pause",
            "live_micro_small_loss_pause",
            {
                "micro_pause_until": self.micro_pause_until.isoformat(),
                "micro_small_loss_count": self.micro_small_loss_count,
                "max_consecutive_micro_small_losses": self.config.live_execution.max_consecutive_micro_small_losses,
            },
        )

    def _record_minute_exit(
        self,
        engine: str,
        direction: str,
        signal_reason: str,
        exit_reason: str,
        pnl_ticks: float,
        event_time: datetime,
    ) -> None:
        key = _minute_key(engine, signal_reason, direction)
        if exit_reason == "stop_loss" and pnl_ticks < 0:
            count = self.minute_consecutive_stop_losses.get(key, 0) + 1
            self.minute_consecutive_stop_losses[key] = count
            if count >= MINUTE_STOP_LOSS_PAUSE_THRESHOLD:
                self.minute_pause_until[key] = _next_quarter_hour(event_time)
            return
        self.minute_consecutive_stop_losses[key] = 0

    def _record_micro_exit(self, exit_reason: str, pnl_ticks: float, event_time: datetime) -> None:
        if exit_reason in MICRO_SMALL_LOSS_EXIT_REASONS and pnl_ticks < 0:
            self.micro_small_loss_count += 1
            if self.micro_small_loss_count >= self.config.live_execution.max_consecutive_micro_small_losses:
                pause_seconds = max(0, int(self.config.live_execution.micro_loss_pause_seconds))
                self.micro_pause_until = event_time + timedelta(seconds=pause_seconds)
            return
        if pnl_ticks >= 0 or exit_reason not in MICRO_SMALL_LOSS_EXIT_REASONS:
            self.micro_small_loss_count = 0

    def _minute_execution_score_threshold(self) -> float:
        return max(MINUTE_TREND_PULLBACK_MIN_SCORE, float(self.config.multi_timeframe.min_execution_score_to_chase))


def _is_minute_vwap_pullback(signal: Signal) -> bool:
    return signal.engine == "minute_vwap" and signal.reason in MINUTE_VWAP_PULLBACK_REASONS


def _minute_key(engine: str, reason: str, direction: str) -> str:
    return f"{engine}:{reason}:{direction}"


def _minute_checks(signal: Signal, min_execution_score: float) -> dict[str, object]:
    return {
        "engine": signal.engine,
        "reason": signal.reason,
        "direction": signal.direction,
        "atr": signal.metadata.get("atr"),
        "execution_score": signal.metadata.get("execution_score"),
        "min_execution_score_to_chase": min_execution_score,
    }


def _reject(reason: str, blocked_by: str, checks: dict[str, object]) -> LiveSafetyDecision:
    return LiveSafetyDecision(
        False,
        reason,
        event_trace_metadata("execution_order", "reject", reason, blocked_by=blocked_by, checks=checks),
    )


def _next_quarter_hour(value: datetime) -> datetime:
    minute = ((value.minute // 15) + 1) * 15
    if minute >= 60:
        return value.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return value.replace(minute=minute, second=0, microsecond=0)


def _format_time_map(values: dict[str, datetime]) -> dict[str, str]:
    return {key: value.isoformat() for key, value in values.items()}
