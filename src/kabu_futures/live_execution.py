from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .api import KabuApiError, KabuStationClient
from .config import StrategyConfig, default_config
from .execution import MicroTradeManager
from .models import BookFeatures, Direction, OrderBook, Signal
from .orders import KabuConstants, KabuFutureOrderBuilder
from .paper_execution import ExecutionEvent


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


class LiveExecutionController:
    """Small live-order executor for kabu futures.

    V1 intentionally supports only micro_book entries. It submits FAK limit
    entry orders, syncs the real kabu position, and sends close orders from the
    same micro exit logic used by paper mode.
    """

    def __init__(
        self,
        client: KabuStationClient,
        config: StrategyConfig | None = None,
        symbol_codes: dict[str, str] | None = None,
    ) -> None:
        self.client = client
        self.config = config or default_config()
        self.symbol_codes = symbol_codes or {}
        self.orders = KabuFutureOrderBuilder()
        self.trade_manager = MicroTradeManager(self.config.micro_engine, self.config.tick_size)
        self.pending_entry: PendingLiveOrder | None = None
        self.pending_exit: PendingLiveOrder | None = None
        self.live_position: LivePositionState | None = None
        self.entry_signal: Signal | None = None
        self.last_position_poll_at: datetime | None = None
        self.reported_order_statuses: set[tuple[str, str]] = set()
        self.last_order_snapshot_by_id: dict[str, dict[str, Any]] = {}
        self.orders_submitted = 0
        self.order_errors = 0

    def on_signal(self, signal: Signal, book: OrderBook, exchange: int) -> list[ExecutionEvent]:
        event_time = _event_time(book)
        if signal.engine not in self.config.live_execution.supported_engines:
            return [_event("execution_reject", signal, book, "live_unsupported_signal_engine")]
        if signal.engine != "micro_book":
            return [_event("execution_reject", signal, book, "live_unsupported_signal_engine")]
        if not signal.is_tradeable:
            return [_event("execution_reject", signal, book, "non_tradeable_signal")]
        if signal.price is None:
            return [_event("execution_reject", signal, book, "missing_signal_price")]
        if signal.symbol != self.config.symbols.primary:
            return [_event("execution_reject", signal, book, "unsupported_symbol")]
        qty = min(self.config.micro_engine.qty, self.config.live_execution.max_order_qty)
        if qty <= 0:
            return [_event("execution_reject", signal, book, "live_qty_not_positive")]
        if self.live_position is not None or self.trade_manager.trade is not None:
            return [_event("execution_reject", signal, book, "already_has_live_position")]
        if self.pending_entry is not None or self.pending_exit is not None:
            return [_event("execution_reject", signal, book, "already_has_live_order")]
        symbol_code = self._symbol_code(signal.symbol)
        if symbol_code is None:
            return [_event("execution_reject", signal, book, "missing_symbol_code")]

        intent = self.orders.new_limit(
            symbol_code,
            exchange,
            signal.direction,
            qty,
            signal.price,
            tif=self.config.live_execution.entry_time_in_force,
        )
        payload = intent.to_payload()
        try:
            response = self.client.sendorder_future(payload)
        except KabuApiError as exc:
            self.order_errors += 1
            return [_event("live_order_error", signal, book, "entry_order_api_error", {"error": str(exc), "order_payload": payload})]
        order_id = _order_id(response)
        if _order_result(response) != 0 or order_id is None:
            self.order_errors += 1
            return [_event("live_order_error", signal, book, "entry_order_rejected", {"response": response, "order_payload": payload})]

        self.orders_submitted += 1
        self.pending_entry = PendingLiveOrder(order_id, signal.symbol, symbol_code, exchange, signal.direction, qty, event_time, "entry", signal)
        self.entry_signal = signal
        return [
            _event(
                "live_order_submitted",
                signal,
                book,
                "entry_limit_fak_submitted",
                {"order_id": order_id, "order_payload": payload, "response": response},
                qty=qty,
            )
        ]

    def on_book(self, book: OrderBook, features: BookFeatures | None, exchange: int) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        if book.symbol != self.config.symbols.primary:
            return events
        event_time = _event_time(book)
        if self._should_poll_positions(event_time):
            events.extend(self._sync_order_status(book))
            events.extend(self._sync_position(book, exchange))
        if self.pending_entry is not None and event_time - self.pending_entry.submitted_at > timedelta(
            seconds=self.config.live_execution.max_pending_entry_seconds
        ):
            pending = self.pending_entry
            self.pending_entry = None
            self.entry_signal = None
            events.append(
                ExecutionEvent(
                    "live_sync_error",
                    pending.symbol,
                    pending.direction,
                    qty=pending.qty,
                    reason="pending_entry_not_confirmed",
                    timestamp=event_time,
                    metadata={
                        "order_id": pending.order_id,
                        "symbol_code": pending.symbol_code,
                        "last_order_status": self.last_order_snapshot_by_id.get(pending.order_id),
                    },
                )
            )

        trade = self.trade_manager.trade
        if trade is None or self.live_position is None or self.pending_exit is not None:
            return events
        decision = self.trade_manager.evaluate_exit(book, features)
        if not decision.should_exit:
            return events
        intent = self.trade_manager.build_exit_order(decision, book, exchange, hold_id=self.live_position.hold_id)
        if intent is None:
            return events
        intent = _with_tif(intent, self.config.live_execution.exit_time_in_force)
        payload = intent.to_payload()
        try:
            response = self.client.sendorder_future(payload)
        except KabuApiError as exc:
            self.order_errors += 1
            events.append(
                ExecutionEvent(
                    "live_order_error",
                    trade.symbol,
                    trade.direction,
                    qty=trade.qty,
                    entry_price=trade.entry_price,
                    reason="exit_order_api_error",
                    timestamp=event_time,
                    metadata={"error": str(exc), "exit_reason": decision.reason, "order_payload": payload},
                )
            )
            return events
        order_id = _order_id(response)
        if _order_result(response) != 0 or order_id is None:
            self.order_errors += 1
            events.append(
                ExecutionEvent(
                    "live_order_error",
                    trade.symbol,
                    trade.direction,
                    qty=trade.qty,
                    entry_price=trade.entry_price,
                    reason="exit_order_rejected",
                    timestamp=event_time,
                    metadata={"response": response, "exit_reason": decision.reason, "order_payload": payload},
                )
            )
            return events
        self.orders_submitted += 1
        self.pending_exit = PendingLiveOrder(
            order_id,
            trade.symbol,
            self.live_position.symbol_code,
            exchange,
            trade.direction,
            trade.qty,
            event_time,
            decision.reason,
            self.entry_signal,
        )
        events.append(
            ExecutionEvent(
                "live_order_submitted",
                trade.symbol,
                trade.direction,
                qty=trade.qty,
                entry_price=trade.entry_price,
                exit_price=decision.price,
                reason="exit_order_submitted",
                timestamp=event_time,
                metadata={"order_id": order_id, "exit_reason": decision.reason, "order_payload": payload, "response": response},
            )
        )
        return events

    def heartbeat_metadata(self) -> dict[str, object]:
        return {
            "paper_position": None,
            "paper_pending_order": None,
            "paper_trades": 0,
            "paper_pnl_ticks": 0.0,
            "paper_pnl_yen": 0.0,
            "paper_pending_orders": 0,
            "paper_micro_position": None,
            "paper_minute_position": None,
            "paper_micro_trades": 0,
            "paper_minute_trades": 0,
            "paper_micro_pnl_ticks": 0.0,
            "paper_minute_pnl_ticks": 0.0,
            "live_position": _position_summary(self.live_position),
            "live_pending_entry": _pending_summary(self.pending_entry),
            "live_pending_exit": _pending_summary(self.pending_exit),
            "live_last_order_statuses": dict(self.last_order_snapshot_by_id),
            "live_orders_submitted": self.orders_submitted,
            "live_order_errors": self.order_errors,
        }

    def _sync_order_status(self, book: OrderBook) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        for pending in tuple(order for order in (self.pending_entry, self.pending_exit) if order is not None):
            try:
                orders = _orders_list(self.client.orders(product=3, id=pending.order_id, details="true"))
            except KabuApiError as exc:
                self.order_errors += 1
                events.append(
                    ExecutionEvent(
                        "live_sync_error",
                        pending.symbol,
                        pending.direction,
                        qty=pending.qty,
                        reason="orders_api_error",
                        timestamp=_event_time(book),
                        metadata={"error": str(exc), "order_id": pending.order_id, "symbol_code": pending.symbol_code},
                    )
                )
                continue
            order = _find_order(orders, pending.order_id)
            if order is None:
                continue
            snapshot = _order_status_snapshot(order)
            self.last_order_snapshot_by_id[pending.order_id] = snapshot
            status_key = (pending.order_id, _status_signature(snapshot))
            if status_key not in self.reported_order_statuses:
                self.reported_order_statuses.add(status_key)
                events.append(
                    ExecutionEvent(
                        "live_order_status",
                        pending.symbol,
                        pending.direction,
                        qty=pending.qty,
                        reason="order_status_update",
                        timestamp=_event_time(book),
                        metadata={"order_id": pending.order_id, "symbol_code": pending.symbol_code, "order_status": snapshot},
                    )
                )
            if not _order_is_unfilled_terminal(order):
                continue
            if self.pending_entry is not None and pending.order_id == self.pending_entry.order_id:
                self.pending_entry = None
                self.entry_signal = None
                reason = "entry_order_expired_or_unfilled"
            elif self.pending_exit is not None and pending.order_id == self.pending_exit.order_id:
                self.pending_exit = None
                reason = "exit_order_expired_or_unfilled"
            else:
                continue
            events.append(
                ExecutionEvent(
                    "live_order_expired",
                    pending.symbol,
                    pending.direction,
                    qty=pending.qty,
                    reason=reason,
                    timestamp=_event_time(book),
                    metadata={"order_id": pending.order_id, "symbol_code": pending.symbol_code, "order_status": snapshot},
                )
            )
        return events

    def _sync_position(self, book: OrderBook, exchange: int) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        event_time = _event_time(book)
        self.last_position_poll_at = event_time
        symbol_code = self._symbol_code(self.config.symbols.primary)
        if symbol_code is None:
            return events
        try:
            positions = _positions_list(self.client.positions(product=3, symbol=symbol_code, addinfo="true"))
        except KabuApiError as exc:
            self.order_errors += 1
            return [
                ExecutionEvent(
                    "live_sync_error",
                    self.config.symbols.primary,
                    "flat",
                    reason="positions_api_error",
                    timestamp=event_time,
                    metadata={"error": str(exc), "symbol_code": symbol_code},
                )
            ]
        active = [_normalize_position(item) for item in positions if _position_qty(item) > 0]
        active = [item for item in active if item is not None and item.symbol_code == symbol_code]
        if len(active) > 1:
            return [
                ExecutionEvent(
                    "live_sync_error",
                    self.config.symbols.primary,
                    "flat",
                    reason="multiple_live_positions",
                    timestamp=event_time,
                    metadata={"positions": [item.__dict__ for item in active]},
                )
            ]
        if not active:
            if self.live_position is not None or self.pending_exit is not None:
                direction = self.live_position.direction if self.live_position is not None else self.pending_exit.direction
                qty = self.live_position.qty if self.live_position is not None else self.pending_exit.qty
                events.append(
                    ExecutionEvent(
                        "live_position_flat",
                        self.config.symbols.primary,
                        direction,
                        qty=qty,
                        reason="position_flat_confirmed",
                        timestamp=event_time,
                    )
                )
            self.live_position = None
            self.pending_exit = None
            self.trade_manager.mark_closed()
            return events

        position = active[0]
        if self.live_position is None:
            self.live_position = position
            self.pending_entry = None
            entry_signal = Signal("micro_book", self.config.symbols.primary, position.direction, 1.0, position.entry_price)
            self.trade_manager.open_from_signal(entry_signal, event_time, qty=position.qty)
            events.append(
                ExecutionEvent(
                    "live_position_detected",
                    position.symbol,
                    position.direction,
                    qty=position.qty,
                    entry_price=position.entry_price,
                    reason="position_sync",
                    timestamp=event_time,
                    metadata={"symbol_code": position.symbol_code, "exchange": exchange, "hold_id": position.hold_id},
                )
            )
        return events

    def _should_poll_positions(self, event_time: datetime) -> bool:
        if self.pending_entry is None and self.pending_exit is None and self.live_position is None:
            return False
        if self.last_position_poll_at is None:
            return True
        return (event_time - self.last_position_poll_at).total_seconds() >= self.config.live_execution.position_poll_interval_seconds

    def _symbol_code(self, symbol: str) -> str | None:
        return self.symbol_codes.get(symbol)


def _event_time(book: OrderBook) -> datetime:
    return book.received_at or book.timestamp


def _event(
    event_type: str,
    signal: Signal,
    book: OrderBook,
    reason: str,
    metadata: dict[str, object] | None = None,
    qty: int = 0,
) -> ExecutionEvent:
    details = {"signal": _signal_snapshot(signal)}
    if metadata:
        details.update(metadata)
    return ExecutionEvent(
        event_type,  # type: ignore[arg-type]
        signal.symbol,
        signal.direction,
        qty=qty,
        entry_price=signal.price,
        reason=reason,
        timestamp=_event_time(book),
        metadata=details,
    )


def _order_result(response: dict[str, Any]) -> int | None:
    result = response.get("Result")
    return int(result) if isinstance(result, (int, float, str)) and str(result).isdigit() else None


def _order_id(response: dict[str, Any]) -> str | None:
    order_id = response.get("OrderId") or response.get("OrderID")
    return str(order_id) if order_id else None


def _positions_list(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    return []


def _orders_list(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [item for item in response if isinstance(item, dict)]
    if isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    return []


def _find_order(orders: list[dict[str, Any]], order_id: str) -> dict[str, Any] | None:
    for item in orders:
        candidate = item.get("ID") or item.get("OrderId") or item.get("OrderID")
        if str(candidate) == order_id:
            return item
    return orders[0] if len(orders) == 1 else None


def _order_status_snapshot(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": order.get("ID") or order.get("OrderId") or order.get("OrderID"),
        "state": order.get("State"),
        "order_state": order.get("OrderState"),
        "order_qty": order.get("OrderQty"),
        "cum_qty": order.get("CumQty"),
        "side": order.get("Side"),
        "price": order.get("Price"),
        "details": [
            {
                "rec_type": detail.get("RecType"),
                "state": detail.get("State"),
                "qty": detail.get("Qty"),
                "price": detail.get("Price"),
                "execution_id": detail.get("ExecutionID"),
                "execution_day": detail.get("ExecutionDay"),
            }
            for detail in (order.get("Details") or [])
            if isinstance(detail, dict)
        ],
    }


def _status_signature(snapshot: dict[str, Any]) -> str:
    details = snapshot.get("details")
    return f"{snapshot.get('state')}:{snapshot.get('order_state')}:{snapshot.get('cum_qty')}:{details}"


def _order_is_unfilled_terminal(order: dict[str, Any]) -> bool:
    state = _optional_int(order.get("State"))
    order_state = _optional_int(order.get("OrderState"))
    cum_qty = _float_value(order.get("CumQty"))
    details = [item for item in (order.get("Details") or []) if isinstance(item, dict)]
    rec_types = {_optional_int(item.get("RecType")) for item in details}
    if cum_qty > 0:
        return False
    if rec_types.intersection({3, 7}):
        return True
    return state == 5 or order_state == 5


def _position_qty(item: dict[str, Any]) -> int:
    value = item.get("LeavesQty") or item.get("HoldQty") or item.get("Qty") or 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _normalize_position(item: dict[str, Any]) -> LivePositionState | None:
    symbol_code = item.get("Symbol")
    side = item.get("Side")
    price = item.get("Price")
    qty = _position_qty(item)
    if not symbol_code or side not in ("1", "2") or qty <= 0:
        return None
    try:
        entry_price = float(price)
    except (TypeError, ValueError):
        return None
    direction: Direction = "long" if side == KabuConstants.SIDE_BUY else "short"
    hold_id = item.get("ExecutionID") or item.get("HoldID")
    return LivePositionState(
        symbol="NK225micro",
        symbol_code=str(symbol_code),
        exchange=int(item.get("Exchange") or 0),
        direction=direction,
        qty=qty,
        entry_price=entry_price,
        entry_time=datetime.now(timezone.utc),
        hold_id=str(hold_id) if hold_id else None,
    )


def _optional_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _with_tif(intent: Any, tif: int) -> Any:
    if getattr(intent, "time_in_force", None) == tif:
        return intent
    from dataclasses import replace

    return replace(intent, time_in_force=tif)


def _position_summary(position: LivePositionState | None) -> dict[str, object] | None:
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


def _pending_summary(order: PendingLiveOrder | None) -> dict[str, object] | None:
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
    }


def _signal_snapshot(signal: Signal) -> dict[str, object]:
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
