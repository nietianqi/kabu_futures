from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import Direction, Signal


@dataclass(frozen=True)
class PendingLiveOrder:
    order_id: str
    symbol: str
    symbol_code: str
    exchange: int
    direction: Direction
    qty: int
    submitted_at: datetime
    reason: str
    signal: Signal | None = None
    position_key: str | None = None


@dataclass(frozen=True)
class LivePositionState:
    symbol: str
    symbol_code: str
    exchange: int
    direction: Direction
    qty: int
    entry_price: float
    entry_time: datetime
    hold_id: str | None


@dataclass
class LivePositionSlot:
    position: LivePositionState
    trade: Any
    entry_signal: Signal
    entry_order_id: str | None = None
    entry_execution_price: float | None = None
    entry_execution_id: str | None = None
    entry_execution_day: str | None = None
    entry_price_mismatch: bool = False


def position_key(position: LivePositionState) -> str:
    if position.hold_id:
        return f"hold:{position.hold_id}"
    return f"single_position_without_hold_id:{position.symbol_code}"


def trade_engine(trade: Any | None) -> str | None:
    if trade is None:
        return None
    return str(getattr(trade, "engine", "micro_book"))


def validated_symbol_code(position: LivePositionState) -> str | None:
    symbol_code = str(position.symbol_code or "")
    if symbol_code and symbol_code != position.symbol and symbol_code.isdigit():
        return symbol_code
    return None


def position_summary(position: LivePositionState | None) -> dict[str, object] | None:
    if position is None:
        return None
    return {
        "symbol": position.symbol,
        "symbol_code": position.symbol_code,
        "exchange": position.exchange,
        "direction": position.direction,
        "qty": position.qty,
        "entry_price": position.entry_price,
        "hold_id": position.hold_id,
    }


def pending_summary(order: PendingLiveOrder | None) -> dict[str, object] | None:
    if order is None:
        return None
    return {
        "order_id": order.order_id,
        "symbol": order.symbol,
        "symbol_code": order.symbol_code,
        "exchange": order.exchange,
        "direction": order.direction,
        "qty": order.qty,
        "submitted_at": order.submitted_at.isoformat(),
        "reason": order.reason,
        "position_key": order.position_key,
    }
