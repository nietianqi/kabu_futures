from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .api import KabuApiError, KabuStationClient
from .config import StrategyConfig, default_config
from .execution import ExitDecision, MicroTradeManager, MinuteTradeManager
from .models import BookFeatures, Direction, OrderBook, Signal
from .orders import KabuConstants, KabuFutureOrderBuilder
from .paper_execution import ExecutionEvent
from .policy import LiveEntryPolicy, event_trace_metadata
from .serialization import event_time as book_event_time, signal_snapshot


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


class LiveExecutionController:
    """Small live-order executor for kabu futures.

    Submits FAK limit entry orders for configured signal engines, syncs the
    real kabu position, and sends close orders from the matching exit manager.
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
        self.entry_policy = LiveEntryPolicy(self.config)
        self.trade_manager = MicroTradeManager(
            self.config.micro_engine,
            self.config.tick_size_for(self.config.symbols.primary),
        )
        self.minute_trade_manager = MinuteTradeManager(
            self.config.minute_engine,
            self.config.micro_engine.qty,
            self.config.tick_size_for(self.config.symbols.primary),
        )
        self.pending_entry: PendingLiveOrder | None = None
        self.pending_exits: dict[str, PendingLiveOrder] = {}
        self.live_positions: dict[str, LivePositionSlot] = {}
        self.pending_exit: PendingLiveOrder | None = None
        self.live_position: LivePositionState | None = None
        self.entry_signal: Signal | None = None
        self.position_sync_blocked = False
        self.exit_retry_after: dict[str, datetime] = {}
        self.exit_failure_counts: dict[str, int] = {}
        self.exit_blocked: set[str] = set()
        self.consecutive_entry_failures = 0
        self.entry_cooldown_until: datetime | None = None
        self.orphan_entry_signal: Signal | None = None
        self.orphan_entry_order_id: str | None = None
        self.orphan_entry_expires_at: datetime | None = None
        self.pending_entry_grace_reported: set[str] = set()
        self.last_position_poll_at: datetime | None = None
        self.reported_order_statuses: set[tuple[str, str]] = set()
        self.last_order_snapshot_by_id: dict[str, dict[str, Any]] = {}
        self.orders_submitted = 0
        self.order_errors = 0
        self.entry_orders_submitted = 0
        self.exit_orders_submitted = 0
        self.entry_orders_expired = 0
        self.exit_orders_expired = 0
        self.positions_detected = 0
        self.own_entry_fills_detected = 0
        self.positions_flat = 0

    def on_signal(self, signal: Signal, book: OrderBook, exchange: int) -> list[ExecutionEvent]:
        event_time = book_event_time(book)
        events: list[ExecutionEvent] = []
        decision = self.entry_policy.evaluate_signal(signal)
        if not decision.allowed:
            return [_event("execution_reject", signal, book, decision.reason, decision.merged_metadata)]
        qty = min(self.config.micro_engine.qty, self.config.live_execution.max_order_qty)
        if qty <= 0:
            return [_event("execution_reject", signal, book, "live_qty_not_positive", _live_reject_metadata("live_qty_not_positive", "qty", {"qty": qty}))]
        if self.last_position_poll_at is None:
            events.extend(self._sync_position(book, exchange))
            events.extend(self._submit_take_profit_orders(book, exchange, event_time))
        if self.position_sync_blocked:
            return events + [_event("execution_reject", signal, book, "position_sync_blocked", _live_reject_metadata("position_sync_blocked", "position_state"))]
        if self.exit_blocked:
            return events + [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "exit_order_blocked_after_retries",
                    _live_reject_metadata(
                        "exit_order_blocked_after_retries",
                        "exit_order_state",
                        {
                            "live_exit_blocked": sorted(self.exit_blocked),
                            "live_exit_failure_counts": dict(self.exit_failure_counts),
                        },
                    ),
                )
            ]
        if self._entry_cooldown_active(event_time):
            return events + [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "entry_failure_cooldown",
                    _live_reject_metadata(
                        "entry_failure_cooldown",
                        "entry_order_state",
                        {
                            "live_entry_failure_count": self.consecutive_entry_failures,
                            "live_entry_cooldown_until": self.entry_cooldown_until.isoformat()
                            if self.entry_cooldown_until is not None
                            else None,
                        },
                    ),
                )
            ]
        if self._position_capacity_used(signal.symbol) >= self.config.risk.max_positions_per_symbol:
            return events + [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "max_positions_per_symbol",
                    _live_reject_metadata(
                        "max_positions_per_symbol",
                        "position_limit",
                        {
                            "open_positions": sum(
                                1 for slot in self.live_positions.values() if slot.position.symbol == signal.symbol
                            ),
                            "pending_entry": self.pending_entry is not None and self.pending_entry.symbol == signal.symbol,
                            "max_positions_per_symbol": self.config.risk.max_positions_per_symbol,
                            "symbol": signal.symbol,
                        },
                    ),
                )
            ]
        if self.pending_entry is not None:
            return events + [_event("execution_reject", signal, book, "already_has_live_order", _live_reject_metadata("already_has_live_order", "pending_order_state"))]
        symbol_code = self._symbol_code(signal.symbol)
        if symbol_code is None:
            return events + [_event("execution_reject", signal, book, "missing_symbol_code", _live_reject_metadata("missing_symbol_code", "symbol_mapping", {"symbol": signal.symbol}))]

        assert signal.price is not None
        entry_price = _entry_limit_price(
            signal.direction,
            signal.price,
            self.config.live_execution.entry_slippage_ticks,
            self.config.tick_size_for(signal.symbol),
        )
        intent = self.orders.new_limit(
            symbol_code,
            exchange,
            signal.direction,
            qty,
            entry_price,
            tif=self.config.live_execution.entry_time_in_force,
        )
        payload = intent.to_payload()
        try:
            response = self.client.sendorder_future(payload)
        except KabuApiError as exc:
            self.order_errors += 1
            failure_metadata = self._record_entry_failure(event_time, "entry_order_api_error")
            return [
                _event(
                    "live_order_error",
                    signal,
                    book,
                    "entry_order_api_error",
                    {
                        **_live_reject_metadata("entry_order_api_error", "kabu_api"),
                        "error": str(exc),
                        "order_payload": payload,
                        **failure_metadata,
                    },
                )
            ]
        order_id = _order_id(response)
        if _order_result(response) != 0 or order_id is None:
            self.order_errors += 1
            failure_metadata = self._record_entry_failure(event_time, "entry_order_rejected")
            return [
                _event(
                    "live_order_error",
                    signal,
                    book,
                    "entry_order_rejected",
                    {
                        **_live_reject_metadata("entry_order_rejected", "kabu_order_response"),
                        "response": response,
                        "order_payload": payload,
                        **failure_metadata,
                    },
                )
            ]

        self.orders_submitted += 1
        self.entry_orders_submitted += 1
        self.pending_entry = PendingLiveOrder(order_id, signal.symbol, symbol_code, exchange, signal.direction, qty, event_time, "entry", signal)
        self.entry_signal = signal
        events.append(
            _event(
                "live_order_submitted",
                signal,
                book,
                "entry_limit_fak_submitted",
                {
                    **decision.merged_metadata,
                    "order_id": order_id,
                    "entry_signal_price": signal.price,
                    "entry_order_price": entry_price,
                    "entry_slippage_ticks": self.config.live_execution.entry_slippage_ticks,
                    "order_payload": payload,
                    "response": response,
                },
                qty=qty,
            )
        )
        return events

    def on_book(self, book: OrderBook, features: BookFeatures | None, exchange: int) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        if not self.config.is_trade_symbol(book.symbol):
            return events
        event_time = book_event_time(book)
        if self._should_poll_positions(event_time):
            events.extend(self._sync_order_status(book))
            events.extend(self._sync_position(book, exchange))
        events.extend(self._handle_pending_entry_timeout(book, event_time))

        events.extend(self._submit_take_profit_orders(book, exchange, event_time))
        self._refresh_legacy_state()
        return events

    def heartbeat_metadata(self) -> dict[str, object]:
        positions = [_position_summary(slot.position) for slot in self.live_positions.values()]
        pending_exits = [_pending_summary(order) for order in self.pending_exits.values()]
        active_engines = [_trade_engine(slot.trade) for slot in self.live_positions.values()]
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
            "live_positions": positions,
            "live_position_count": len(self.live_positions),
            "live_pending_entry": _pending_summary(self.pending_entry),
            "live_pending_exit": _pending_summary(self.pending_exit),
            "live_pending_exits": pending_exits,
            "live_active_engine": _trade_engine(self._active_trade()),
            "live_active_engines": active_engines,
            "live_last_order_statuses": dict(self.last_order_snapshot_by_id),
            "live_orders_submitted": self.orders_submitted,
            "live_order_errors": self.order_errors,
            "live_entry_orders_submitted": self.entry_orders_submitted,
            "live_exit_orders_submitted": self.exit_orders_submitted,
            "live_entry_orders_expired": self.entry_orders_expired,
            "live_exit_orders_expired": self.exit_orders_expired,
            "live_positions_detected": self.positions_detected,
            "live_positions_flat": self.positions_flat,
            "live_entry_fill_rate": _ratio(self.own_entry_fills_detected, self.entry_orders_submitted),
            "live_own_entry_fills_detected": self.own_entry_fills_detected,
            "live_exit_blocked": sorted(self.exit_blocked),
            "live_exit_blocked_count": len(self.exit_blocked),
            "live_exit_failure_counts": dict(self.exit_failure_counts),
            "live_entry_failure_count": self.consecutive_entry_failures,
            "live_entry_cooldown_until": self.entry_cooldown_until.isoformat()
            if self.entry_cooldown_until is not None
            else None,
            "live_position_sync_blocked": self.position_sync_blocked,
            "live_exit_retry_after": {key: value.isoformat() for key, value in self.exit_retry_after.items()},
        }

    def _sync_order_status(self, book: OrderBook) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        pending_orders = [order for order in (self.pending_entry,) if order is not None]
        pending_orders.extend(self.pending_exits.values())
        for pending in tuple(pending_orders):
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
                        timestamp=book_event_time(book),
                        metadata={
                            **_live_reject_metadata("orders_api_error", "kabu_api"),
                            "error": str(exc),
                            "order_id": pending.order_id,
                            "symbol_code": pending.symbol_code,
                            "position_key": pending.position_key,
                        },
                    )
                )
                continue
            order = _find_order(orders, pending.order_id)
            if order is None:
                missing_key = (pending.order_id, "missing")
                if missing_key not in self.reported_order_statuses:
                    self.reported_order_statuses.add(missing_key)
                    events.append(
                        ExecutionEvent(
                            "live_sync_error",
                            pending.symbol,
                            pending.direction,
                            qty=pending.qty,
                            reason="order_status_missing",
                            timestamp=book_event_time(book),
                            metadata={
                                **_live_reject_metadata("order_status_missing", "order_status_lookup"),
                                "order_id": pending.order_id,
                                "symbol_code": pending.symbol_code,
                                "position_key": pending.position_key,
                                "orders_count": len(orders),
                            },
                        )
                    )
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
                        timestamp=book_event_time(book),
                        metadata={
                            **event_trace_metadata("execution_order", "status", "order_status_update"),
                            "order_id": pending.order_id,
                            "symbol_code": pending.symbol_code,
                            "position_key": pending.position_key,
                            "order_status": snapshot,
                        },
                    )
                )
            terminal_unfilled = _order_is_unfilled_terminal(order)
            terminal_partial_exit = _order_is_partially_filled_terminal(order, pending.qty)
            filled_qty = _float_value(order.get("CumQty"))
            if terminal_unfilled:
                if self.pending_entry is not None and pending.order_id == self.pending_entry.order_id:
                    self._clear_pending_entry()
                    self.entry_orders_expired += 1
                    reason = "entry_order_expired_or_unfilled"
                    blocked_by = "order_terminal_unfilled"
                    failure_metadata = self._record_entry_failure(book_event_time(book), reason)
                elif pending.position_key is not None and pending.position_key in self.pending_exits:
                    self.pending_exits.pop(pending.position_key, None)
                    self._refresh_legacy_state()
                    self.exit_orders_expired += 1
                    reason = "exit_order_expired_or_unfilled"
                    blocked_by = "order_terminal_unfilled"
                    failure_metadata = self._record_exit_failure(pending.position_key, book_event_time(book), reason)
                else:
                    continue
            elif terminal_partial_exit and pending.position_key is not None and pending.position_key in self.pending_exits:
                self.pending_exits.pop(pending.position_key, None)
                self._refresh_legacy_state()
                self.exit_orders_expired += 1
                reason = "exit_order_partially_filled"
                blocked_by = "order_terminal_partial_fill"
                failure_metadata = self._record_exit_failure(pending.position_key, book_event_time(book), reason)
            else:
                continue
            events.append(
                ExecutionEvent(
                    "live_order_expired",
                    pending.symbol,
                    pending.direction,
                    qty=pending.qty,
                    reason=reason,
                    timestamp=book_event_time(book),
                    metadata={
                        **event_trace_metadata("execution_order", "reject", reason, blocked_by),
                        "order_id": pending.order_id,
                        "symbol_code": pending.symbol_code,
                        "position_key": pending.position_key,
                        "order_status": snapshot,
                        "filled_qty": filled_qty,
                        "remaining_qty": max(0.0, float(pending.qty) - filled_qty),
                        **failure_metadata,
                    },
                )
            )
            if pending.position_key is not None and failure_metadata.get("exit_blocked"):
                events.append(
                    self._exit_blocked_event(
                        pending,
                        book_event_time(book),
                        snapshot,
                        str(failure_metadata.get("exit_failure_reason") or reason),
                        filled_qty,
                    )
                )
        return events

    def _sync_position(self, book: OrderBook, exchange: int) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        event_time = book_event_time(book)
        self.last_position_poll_at = event_time
        active: list[LivePositionState] = []
        symbol_by_code = {code: symbol for symbol, code in self.symbol_codes.items()}
        sync_failed = False
        for symbol in self.config.trade_symbols():
            symbol_code = self._symbol_code(symbol)
            if symbol_code is None:
                continue
            try:
                positions = _positions_list(self.client.positions(product=3, symbol=symbol_code, addinfo="true"))
            except KabuApiError as exc:
                self.order_errors += 1
                self.position_sync_blocked = True
                sync_failed = True
                events.append(
                    ExecutionEvent(
                        "live_sync_error",
                        symbol,
                        "flat",
                        reason="positions_api_error",
                        timestamp=event_time,
                        metadata={
                            **_live_reject_metadata("positions_api_error", "kabu_api"),
                            "error": str(exc),
                            "symbol_code": symbol_code,
                        },
                    )
                )
                continue
            active.extend(
                position
                for item in positions
                if _position_qty(item) > 0
                for position in (_normalize_position(item, symbol_by_code),)
                if position is not None and position.symbol_code == symbol_code
            )
        if sync_failed:
            return events
        self.position_sync_blocked = False
        positions_by_symbol: defaultdict[str, list[LivePositionState]] = defaultdict(list)
        for position in active:
            positions_by_symbol[position.symbol].append(position)
        missing_hold_id_symbols = [
            symbol
            for symbol, positions in positions_by_symbol.items()
            if len(positions) > 1 and any(position.hold_id is None for position in positions)
        ]
        if missing_hold_id_symbols:
            self.position_sync_blocked = True
            events.append(
                ExecutionEvent(
                    "live_sync_error",
                    ",".join(missing_hold_id_symbols),
                    "flat",
                    reason="missing_hold_id_for_multiple_positions",
                    timestamp=event_time,
                    metadata={
                        **_live_reject_metadata("missing_hold_id_for_multiple_positions", "position_state"),
                        "symbols": missing_hold_id_symbols,
                        "positions": [item.__dict__ for item in active if item.symbol in missing_hold_id_symbols],
                    },
                )
            )
            active = [position for position in active if position.symbol not in missing_hold_id_symbols or position.hold_id is not None]

        current_positions = {_position_key(position): position for position in active}
        for position_key, slot in tuple(self.live_positions.items()):
            if position_key in current_positions:
                slot.position = current_positions[position_key]
                continue
            self.positions_flat += 1
            self.live_positions.pop(position_key, None)
            self.pending_exits.pop(position_key, None)
            self.exit_retry_after.pop(position_key, None)
            self.exit_failure_counts.pop(position_key, None)
            self.exit_blocked.discard(position_key)
            events.append(
                ExecutionEvent(
                    "live_position_flat",
                    slot.position.symbol,
                    slot.position.direction,
                    qty=slot.position.qty,
                    reason="position_flat_confirmed",
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata("position_lifecycle", "exit", "position_flat_confirmed"),
                        "position_key": position_key,
                        "hold_id": slot.position.hold_id,
                    },
                )
            )

        pending_entry_consumed = False
        orphan_entry_consumed = False
        for position_key, position in current_positions.items():
            if position_key in self.live_positions:
                continue
            source_signal = None
            own_entry_detected = False
            if self.entry_signal is not None and self.entry_signal.symbol == position.symbol and not pending_entry_consumed:
                source_signal = self.entry_signal
                pending_entry_consumed = True
                own_entry_detected = True
            elif (
                self._orphan_entry_signal_active(event_time)
                and self.orphan_entry_signal is not None
                and self.orphan_entry_signal.symbol == position.symbol
                and not orphan_entry_consumed
            ):
                source_signal = self.orphan_entry_signal
                orphan_entry_consumed = True
                own_entry_detected = True
            entry_signal = _position_entry_signal(source_signal, position, position.symbol)
            trade = self._trade_from_position(entry_signal, event_time, position.qty)
            self.live_positions[position_key] = LivePositionSlot(position, trade, entry_signal)
            self.positions_detected += 1
            if own_entry_detected:
                self.own_entry_fills_detected += 1
                self._reset_entry_failures()
            events.append(
                ExecutionEvent(
                    "live_position_detected",
                    position.symbol,
                    position.direction,
                    qty=position.qty,
                    entry_price=position.entry_price,
                    reason="position_sync",
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata("position_lifecycle", "entry", "position_sync", checks={"engine": entry_signal.engine}),
                        "symbol_code": position.symbol_code,
                        "exchange": exchange,
                        "hold_id": position.hold_id,
                        "position_key": position_key,
                        "engine": entry_signal.engine,
                        "own_entry_detected": own_entry_detected,
                        "signal_reason": entry_signal.reason,
                    },
                )
            )
        if pending_entry_consumed:
            self._clear_pending_entry()
        if orphan_entry_consumed:
            self._clear_orphan_entry_signal()
        self._refresh_legacy_state()
        return events

    def _submit_exit_order(
        self,
        position_key: str,
        slot: LivePositionSlot,
        decision: ExitDecision,
        book: OrderBook,
        exchange: int,
        event_time: datetime,
    ) -> ExecutionEvent | None:
        trade = slot.trade
        position = slot.position
        intent = self._build_exit_order(slot, decision, exchange)
        if intent is None:
            return None
        intent = _with_tif(intent, self.config.live_execution.exit_time_in_force)
        payload = intent.to_payload()
        try:
            response = self.client.sendorder_future(payload)
        except KabuApiError as exc:
            self.order_errors += 1
            failure_metadata = self._record_exit_failure(position_key, event_time, "exit_order_api_error")
            return ExecutionEvent(
                "live_order_error",
                trade.symbol,
                trade.direction,
                qty=position.qty,
                entry_price=trade.entry_price,
                reason="exit_order_blocked_after_retries" if failure_metadata["exit_blocked"] else "exit_order_api_error",
                timestamp=event_time,
                metadata={
                    **_live_reject_metadata(
                        "exit_order_blocked_after_retries" if failure_metadata["exit_blocked"] else "exit_order_api_error",
                        "exit_order_state" if failure_metadata["exit_blocked"] else "kabu_api",
                    ),
                    "error": str(exc),
                    "exit_reason": decision.reason,
                    "position_key": position_key,
                    "hold_id": position.hold_id,
                    "order_payload": payload,
                    **failure_metadata,
                },
            )
        order_id = _order_id(response)
        if _order_result(response) != 0 or order_id is None:
            self.order_errors += 1
            failure_metadata = self._record_exit_failure(position_key, event_time, "exit_order_rejected")
            return ExecutionEvent(
                "live_order_error",
                trade.symbol,
                trade.direction,
                qty=position.qty,
                entry_price=trade.entry_price,
                reason="exit_order_blocked_after_retries" if failure_metadata["exit_blocked"] else "exit_order_rejected",
                timestamp=event_time,
                metadata={
                    **_live_reject_metadata(
                        "exit_order_blocked_after_retries" if failure_metadata["exit_blocked"] else "exit_order_rejected",
                        "exit_order_state" if failure_metadata["exit_blocked"] else "kabu_order_response",
                    ),
                    "response": response,
                    "exit_reason": decision.reason,
                    "position_key": position_key,
                    "hold_id": position.hold_id,
                    "order_payload": payload,
                    **failure_metadata,
                },
            )
        self.orders_submitted += 1
        self.exit_orders_submitted += 1
        self.exit_retry_after.pop(position_key, None)
        self.pending_exits[position_key] = PendingLiveOrder(
            order_id,
            trade.symbol,
            position.symbol_code,
            exchange,
            trade.direction,
            position.qty,
            event_time,
            decision.reason,
            slot.entry_signal,
            position_key,
        )
        self._refresh_legacy_state()
        return ExecutionEvent(
            "live_order_submitted",
            trade.symbol,
            trade.direction,
            qty=position.qty,
            entry_price=trade.entry_price,
            exit_price=decision.price,
            reason="exit_order_submitted",
            timestamp=event_time,
            metadata={
                **event_trace_metadata(
                    "position_lifecycle",
                    "exit",
                    decision.reason,
                    checks={"engine": getattr(trade, "engine", "micro_book")},
                ),
                "order_id": order_id,
                "engine": getattr(trade, "engine", "micro_book"),
                "exit_reason": decision.reason,
                "position_key": position_key,
                "hold_id": position.hold_id,
                "take_profit_price": getattr(trade, "take_profit_price", decision.price),
                "take_profit_ticks": getattr(trade, "take_profit_ticks", None),
                "order_payload": payload,
                "response": response,
            },
        )

    def _active_trade(self) -> Any | None:
        first = next(iter(self.live_positions.values()), None)
        return first.trade if first is not None else None

    def _evaluate_exit(self, book: OrderBook, features: BookFeatures | None) -> Any:
        trade = self._active_trade()
        if trade is None:
            return ExitDecision(False, "no_trade")
        return ExitDecision(True, "take_profit", getattr(trade, "take_profit_price", None))

    def _build_exit_order(self, slot: LivePositionSlot, decision: ExitDecision, exchange: int) -> Any:
        if decision.price is None:
            return None
        trade = slot.trade
        return self.orders.close_aggressive_limit(
            slot.position.symbol_code,
            exchange,
            trade.direction,
            slot.position.qty,
            decision.price,
            hold_id=slot.position.hold_id,
        )

    def _submit_take_profit_orders(
        self,
        book: OrderBook,
        exchange: int,
        event_time: datetime,
    ) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        for position_key, slot in tuple(self.live_positions.items()):
            if position_key in self.exit_blocked:
                continue
            if position_key in self.pending_exits:
                continue
            retry_after = self.exit_retry_after.get(position_key)
            if retry_after is not None and event_time < retry_after:
                continue
            decision = ExitDecision(True, "take_profit", getattr(slot.trade, "take_profit_price", None))
            exit_event = self._submit_exit_order(
                position_key,
                slot,
                decision,
                book,
                slot.position.exchange or exchange,
                event_time,
            )
            if exit_event is not None:
                events.append(exit_event)
        return events

    def _trade_from_position(self, signal: Signal, event_time: datetime, qty: int) -> Any:
        if signal.engine == "micro_book":
            manager = MicroTradeManager(self.config.micro_engine, self.config.tick_size_for(signal.symbol))
            return manager.open_from_signal(signal, event_time, qty=qty)
        manager = MinuteTradeManager(
            self.config.minute_engine,
            self.config.micro_engine.qty,
            self.config.tick_size_for(signal.symbol),
        )
        return manager.open_from_signal(signal, event_time, qty=qty)

    def _should_poll_positions(self, event_time: datetime) -> bool:
        if self.last_position_poll_at is None:
            return True
        return (event_time - self.last_position_poll_at).total_seconds() >= self.config.live_execution.position_poll_interval_seconds

    def _symbol_code(self, symbol: str) -> str | None:
        return self.symbol_codes.get(symbol)

    def _position_capacity_used(self, symbol: str | None = None) -> int:
        if symbol is None:
            return len(self.live_positions) + (1 if self.pending_entry is not None else 0)
        positions = sum(1 for slot in self.live_positions.values() if slot.position.symbol == symbol)
        pending = 1 if self.pending_entry is not None and self.pending_entry.symbol == symbol else 0
        return positions + pending

    def _handle_pending_entry_timeout(self, book: OrderBook, event_time: datetime) -> list[ExecutionEvent]:
        if self.pending_entry is None:
            return []
        pending = self.pending_entry
        elapsed = (event_time - pending.submitted_at).total_seconds()
        max_seconds = float(self.config.live_execution.max_pending_entry_seconds)
        grace_seconds = float(self.config.live_execution.pending_entry_grace_seconds)
        if elapsed <= max_seconds:
            return []
        last_status = self.last_order_snapshot_by_id.get(pending.order_id)
        if elapsed <= grace_seconds:
            if pending.order_id in self.pending_entry_grace_reported:
                return []
            self.pending_entry_grace_reported.add(pending.order_id)
            return [
                ExecutionEvent(
                    "live_sync_error",
                    pending.symbol,
                    pending.direction,
                    qty=pending.qty,
                    reason="pending_entry_grace_active",
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata("position_lifecycle", "status", "pending_entry_grace_active", "position_sync"),
                        "order_id": pending.order_id,
                        "symbol_code": pending.symbol_code,
                        "elapsed_seconds": elapsed,
                        "max_pending_entry_seconds": self.config.live_execution.max_pending_entry_seconds,
                        "pending_entry_grace_seconds": self.config.live_execution.pending_entry_grace_seconds,
                        "last_order_status": last_status,
                    },
                )
            ]
        self._clear_pending_entry(preserve_orphan=True, event_time=event_time)
        self.entry_orders_expired += 1
        failure_metadata = self._record_entry_failure(event_time, "pending_entry_timeout_after_grace")
        return [
            ExecutionEvent(
                "live_sync_error",
                pending.symbol,
                pending.direction,
                qty=pending.qty,
                reason="pending_entry_timeout_after_grace",
                timestamp=event_time,
                metadata={
                    **event_trace_metadata(
                        "position_lifecycle",
                        "reject",
                        "pending_entry_timeout_after_grace",
                        "position_sync",
                    ),
                    "order_id": pending.order_id,
                    "symbol_code": pending.symbol_code,
                    "elapsed_seconds": elapsed,
                    "max_pending_entry_seconds": self.config.live_execution.max_pending_entry_seconds,
                    "pending_entry_grace_seconds": self.config.live_execution.pending_entry_grace_seconds,
                    "last_order_status": last_status,
                    **failure_metadata,
                },
            )
        ]

    def _mark_exit_retry(self, position_key: str, event_time: datetime) -> datetime:
        retry_after = event_time + timedelta(seconds=max(1.0, self.config.live_execution.position_poll_interval_seconds))
        self.exit_retry_after[position_key] = retry_after
        return retry_after

    def _record_exit_failure(self, position_key: str, event_time: datetime, reason: str) -> dict[str, object]:
        count = self.exit_failure_counts.get(position_key, 0) + 1
        self.exit_failure_counts[position_key] = count
        blocked = count >= self.config.live_execution.max_consecutive_exit_failures
        retry_after: datetime | None = None
        if blocked:
            self.exit_blocked.add(position_key)
            self.exit_retry_after.pop(position_key, None)
        else:
            retry_after = self._mark_exit_retry(position_key, event_time)
        return {
            "exit_failure_reason": reason,
            "exit_failure_count": count,
            "max_consecutive_exit_failures": self.config.live_execution.max_consecutive_exit_failures,
            "exit_blocked": blocked,
            "retry_after": retry_after.isoformat() if retry_after is not None else None,
        }

    def _record_entry_failure(self, event_time: datetime, reason: str) -> dict[str, object]:
        self.consecutive_entry_failures += 1
        cooldown_until: datetime | None = None
        cooldown_active = self.consecutive_entry_failures >= self.config.live_execution.max_consecutive_entry_failures
        if cooldown_active:
            cooldown_until = event_time + timedelta(seconds=self.config.live_execution.entry_failure_cooldown_seconds)
            self.entry_cooldown_until = cooldown_until
        return {
            "entry_failure_reason": reason,
            "entry_failure_count": self.consecutive_entry_failures,
            "max_consecutive_entry_failures": self.config.live_execution.max_consecutive_entry_failures,
            "entry_cooldown_active": cooldown_active,
            "entry_cooldown_until": cooldown_until.isoformat() if cooldown_until is not None else None,
        }

    def _reset_entry_failures(self) -> None:
        self.consecutive_entry_failures = 0
        self.entry_cooldown_until = None

    def _entry_cooldown_active(self, event_time: datetime) -> bool:
        if self.entry_cooldown_until is None:
            return False
        if event_time >= self.entry_cooldown_until:
            self.entry_cooldown_until = None
            return False
        return True

    def _clear_pending_entry(self, preserve_orphan: bool = False, event_time: datetime | None = None) -> PendingLiveOrder | None:
        pending = self.pending_entry
        if pending is not None:
            self.pending_entry_grace_reported.discard(pending.order_id)
            if preserve_orphan and pending.signal is not None and event_time is not None:
                self.orphan_entry_signal = pending.signal
                self.orphan_entry_order_id = pending.order_id
                ttl_seconds = max(60.0, float(self.config.live_execution.pending_entry_grace_seconds))
                self.orphan_entry_expires_at = event_time + timedelta(seconds=ttl_seconds)
        self.pending_entry = None
        self.entry_signal = None
        return pending

    def _clear_orphan_entry_signal(self) -> None:
        self.orphan_entry_signal = None
        self.orphan_entry_order_id = None
        self.orphan_entry_expires_at = None

    def _orphan_entry_signal_active(self, event_time: datetime) -> bool:
        if self.orphan_entry_signal is None:
            return False
        if self.orphan_entry_expires_at is not None and event_time > self.orphan_entry_expires_at:
            self._clear_orphan_entry_signal()
            return False
        return True

    def _exit_blocked_event(
        self,
        pending: PendingLiveOrder,
        event_time: datetime,
        snapshot: dict[str, Any],
        failure_reason: str,
        filled_qty: float,
    ) -> ExecutionEvent:
        return ExecutionEvent(
            "live_order_error",
            pending.symbol,
            pending.direction,
            qty=pending.qty,
            reason="exit_order_blocked_after_retries",
            timestamp=event_time,
            metadata={
                **_live_reject_metadata("exit_order_blocked_after_retries", "exit_order_state"),
                "order_id": pending.order_id,
                "symbol_code": pending.symbol_code,
                "position_key": pending.position_key,
                "order_status": snapshot,
                "filled_qty": filled_qty,
                "remaining_qty": max(0.0, float(pending.qty) - filled_qty),
                "exit_failure_reason": failure_reason,
                "exit_failure_count": self.exit_failure_counts.get(pending.position_key or "", 0),
                "max_consecutive_exit_failures": self.config.live_execution.max_consecutive_exit_failures,
                "exit_blocked": True,
            },
        )

    def _refresh_legacy_state(self) -> None:
        first_slot = next(iter(self.live_positions.values()), None)
        self.live_position = first_slot.position if first_slot is not None else None
        self.pending_exit = next(iter(self.pending_exits.values()), None)


def _live_reject_metadata(
    reason: str,
    blocked_by: str,
    checks: dict[str, object] | None = None,
) -> dict[str, object]:
    return event_trace_metadata("execution_order", "reject", reason, blocked_by, checks or {})


def _position_entry_signal(source_signal: Signal | None, position: LivePositionState, fallback_symbol: str) -> Signal:
    if source_signal is None or source_signal.engine not in {"micro_book", *MinuteTradeManager.SUPPORTED_ENGINES}:
        return Signal("micro_book", fallback_symbol, position.direction, 1.0, position.entry_price, "position_sync")
    metadata = dict(source_signal.metadata)
    metadata["live_entry_signal_price"] = source_signal.price
    return Signal(
        source_signal.engine,
        source_signal.symbol,
        position.direction,
        source_signal.confidence,
        position.entry_price,
        source_signal.reason,
        metadata,
        source_signal.score,
        source_signal.signal_horizon,
        source_signal.expected_hold_seconds,
        source_signal.risk_budget_pct,
        source_signal.veto_reason,
        source_signal.position_scale,
    )


def _position_key(position: LivePositionState) -> str:
    if position.hold_id:
        return f"hold:{position.hold_id}"
    return f"single_position_without_hold_id:{position.symbol_code}"


def _trade_engine(trade: Any | None) -> str | None:
    if trade is None:
        return None
    return str(getattr(trade, "engine", "micro_book"))


def _event(
    event_type: str,
    signal: Signal,
    book: OrderBook,
    reason: str,
    metadata: dict[str, object] | None = None,
    qty: int = 0,
) -> ExecutionEvent:
    details = {"signal": signal_snapshot(signal)}
    if metadata:
        details.update(metadata)
    return ExecutionEvent(
        event_type,  # type: ignore[arg-type]
        signal.symbol,
        signal.direction,
        qty=qty,
        entry_price=signal.price,
        reason=reason,
        timestamp=book_event_time(book),
        metadata=details,
    )


def _order_result(response: dict[str, Any]) -> int | None:
    result = response.get("Result")
    return int(result) if isinstance(result, (int, float, str)) and str(result).isdigit() else None


def _order_id(response: dict[str, Any]) -> str | None:
    order_id = response.get("OrderId") or response.get("OrderID")
    return str(order_id) if order_id else None


def _entry_limit_price(direction: Direction, signal_price: float, slippage_ticks: int, tick_size: float) -> float:
    if slippage_ticks <= 0:
        return signal_price
    offset = slippage_ticks * tick_size
    if direction == "long":
        return signal_price + offset
    return signal_price - offset


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


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
    return None


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


def _order_is_partially_filled_terminal(order: dict[str, Any], order_qty: int) -> bool:
    cum_qty = _float_value(order.get("CumQty"))
    return 0 < cum_qty < float(order_qty) and _order_is_terminal(order)


def _order_is_terminal(order: dict[str, Any]) -> bool:
    state = _optional_int(order.get("State"))
    order_state = _optional_int(order.get("OrderState"))
    details = [item for item in (order.get("Details") or []) if isinstance(item, dict)]
    rec_types = {_optional_int(item.get("RecType")) for item in details}
    return state == 5 or order_state == 5 or bool(rec_types.intersection({3, 7}))


def _position_qty(item: dict[str, Any]) -> int:
    value = item.get("LeavesQty") or item.get("HoldQty") or item.get("Qty") or 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _normalize_position(item: dict[str, Any], symbol_by_code: dict[str, str] | None = None) -> LivePositionState | None:
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
    symbol = (symbol_by_code or {}).get(str(symbol_code), "NK225micro")
    return LivePositionState(
        symbol=symbol,
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
        "position_key": order.position_key,
    }
