from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
import time
from typing import Callable, TypeVar

from .api import KabuApiError, classify_kabu_api_error
from .config import StrategyConfig


T = TypeVar("T")


class LiveApiHealth:
    def __init__(self, config: StrategyConfig) -> None:
        self.config = config
        self.error_counts: Counter[str] = Counter()
        self.last_error: dict[str, object] | None = None
        self.auth_failed = False
        self.api_backoff_until: datetime | None = None
        self.latency_samples: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=100))
        self.last_latency_ms: dict[str, float] = {}
        self.wrong_instance_cooldown_until: datetime | None = None
        self.wrong_instance_errors = 0

    def record_error(self, exc: KabuApiError, event_time: datetime, operation: str) -> dict[str, object]:
        category = classify_kabu_api_error(exc)
        self.error_counts[category] += 1
        if category in {"auth_error", "auth_recovery_failed"}:
            self.auth_failed = True
        if category in {"rate_limit", "service_unavailable"}:
            backoff_seconds = self._api_backoff_seconds(category)
            self.api_backoff_until = event_time + timedelta(seconds=backoff_seconds)
        if category == "kabu_station_wrong_instance":
            self.wrong_instance_errors += 1
            cooldown_seconds = max(30.0, float(self.config.live_execution.entry_failure_cooldown_seconds))
            self.wrong_instance_cooldown_until = event_time + timedelta(seconds=cooldown_seconds)
        self.last_error = {
            "category": category,
            "operation": operation,
            "error": str(exc),
            "timestamp": event_time.isoformat(),
            "status_code": getattr(exc, "status_code", None),
        }
        return {
            "api_error_category": category,
            "category": category,
            "api_operation": operation,
            "api_status_code": getattr(exc, "status_code", None),
        }

    def record_success(self) -> None:
        self.auth_failed = False

    def call(self, operation: str, func: Callable[..., T], *args: object, **kwargs: object) -> T:
        started = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            latency_ms = (time.perf_counter() - started) * 1000.0
            self.last_latency_ms[operation] = round(latency_ms, 4)
            self.latency_samples[operation].append(latency_ms)

    def api_backoff_active(self, event_time: datetime) -> bool:
        if self.api_backoff_until is None:
            return False
        if event_time >= self.api_backoff_until:
            self.api_backoff_until = None
            return False
        return True

    def wrong_instance_cooldown_active(self, event_time: datetime) -> bool:
        if self.wrong_instance_cooldown_until is None:
            return False
        if event_time >= self.wrong_instance_cooldown_until:
            self.wrong_instance_cooldown_until = None
            return False
        return True

    def summary(
        self,
        event_time: datetime | None = None,
        *,
        position_sync_blocked: bool = False,
        last_position_poll_at: datetime | None = None,
    ) -> dict[str, object]:
        now = event_time or datetime.now(timezone.utc)
        return {
            "last_error": self.last_error,
            "error_counts": dict(self.error_counts),
            "latency_ms": _latency_summary(self.latency_samples),
            "last_latency_ms": dict(self.last_latency_ms),
            "auth_failed": self.auth_failed,
            "api_backoff_until": self.api_backoff_until.isoformat() if self.api_backoff_until is not None else None,
            "api_backoff_active": self.api_backoff_until is not None and now < self.api_backoff_until,
            "position_sync_blocked": position_sync_blocked,
            "wrong_instance_errors": self.wrong_instance_errors,
            "wrong_instance_cooldown_until": self.wrong_instance_cooldown_until.isoformat()
            if self.wrong_instance_cooldown_until is not None
            else None,
            "wrong_instance_cooldown_active": self.wrong_instance_cooldown_until is not None
            and now < self.wrong_instance_cooldown_until,
            "next_position_retry_at": (
                last_position_poll_at + timedelta(seconds=self.config.live_execution.position_poll_interval_seconds)
            ).isoformat()
            if position_sync_blocked and last_position_poll_at is not None
            else None,
        }

    def _api_backoff_seconds(self, category: str) -> float:
        base = max(1.0, float(self.config.live_execution.api_error_cooldown_seconds))
        count = max(1, self.error_counts.get(category, 1))
        return min(120.0, base * (2 ** min(count - 1, 3)))


def _latency_summary(samples: dict[str, deque[float]]) -> dict[str, dict[str, float]]:
    return {operation: _latency_bucket(values) for operation, values in samples.items() if values}


def _latency_bucket(values: deque[float]) -> dict[str, float]:
    ordered = sorted(values)
    if not ordered:
        return {"count": 0.0, "last": 0.0, "p50": 0.0, "p95": 0.0}
    return {
        "count": float(len(ordered)),
        "last": round(values[-1], 4),
        "p50": round(_percentile_sorted(ordered, 0.50), 4),
        "p95": round(_percentile_sorted(ordered, 0.95), 4),
    }


def _percentile_sorted(values: list[float], pct: float) -> float:
    idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * pct))))
    return values[idx]
