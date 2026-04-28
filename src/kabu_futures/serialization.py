from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import OrderBook, Signal


def event_time(book: OrderBook) -> datetime:
    return book.received_at or book.timestamp


def signal_snapshot(signal: Signal) -> dict[str, object]:
    return {
        "engine": signal.engine,
        "symbol": signal.symbol,
        "direction": signal.direction,
        "confidence": signal.confidence,
        "price": signal.price,
        "reason": signal.reason,
        "metadata": signal.metadata,
        "score": signal.score,
        "signal_horizon": signal.signal_horizon,
        "expected_hold_seconds": signal.expected_hold_seconds,
        "risk_budget_pct": signal.risk_budget_pct,
        "veto_reason": signal.veto_reason,
        "position_scale": signal.position_scale,
    }


def signal_to_dict(signal: Signal) -> dict[str, Any]:
    return signal_snapshot(signal)
