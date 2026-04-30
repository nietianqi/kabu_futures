from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, TypeVar

from .api import KabuApiError, KabuStationClient
from .config import StrategyConfig, default_config
from .execution import ExitDecision, MicroTradeManager, MinuteTradeManager, pnl_ticks as calculate_pnl_ticks
from .models import BookFeatures, Direction, OrderBook, Signal
from .orders import KabuConstants, KabuFutureOrderBuilder
from .paper_execution import ExecutionEvent
from .policy import LiveEntryPolicy, event_trace_metadata
from .serialization import event_time as book_event_time, signal_snapshot
from .live_api_health import LiveApiHealth
from .live_safety import LiveSafetyState
from .live_state import (
    LivePositionSlot,
    LivePositionState,
    PendingLiveOrder,
    pending_summary,
    position_key as live_position_key,
    position_summary,
    trade_engine,
    validated_symbol_code,
)


T = TypeVar("T")


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
        self.live_safety = LiveSafetyState(self.config)
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
        self.entry_execution_by_order_id: dict[str, dict[str, Any]] = {}
        self.closed_exit_order_ids: set[str] = set()
        self.closed_trade_by_position_key: dict[str, dict[str, object]] = {}
        self.orders_submitted = 0
        self.order_errors = 0
        self.entry_orders_submitted = 0
        self.exit_orders_submitted = 0
        self.entry_orders_expired = 0
        self.exit_orders_expired = 0
        self.exit_orders_cancelled = 0
        self.positions_detected = 0
        self.own_entry_fills_detected = 0
        self.positions_flat = 0
        self.live_trades_count = 0
        self.live_wins = 0
        self.live_losses = 0
        self.live_pnl_ticks = 0.0
        self.live_pnl_yen = 0.0
        self.api_health = LiveApiHealth(self.config)
        self.loss_hold_guard_active = False
        self.loss_hold_guard_since: datetime | None = None
        self.loss_hold_guard_reason: str | None = None
        self.loss_hold_guard_snapshot: dict[str, object] = {}
        self.loss_hold_guard_reported = False
        self.entry_orders_cancelled = 0

    def on_signal(self, signal: Signal, book: OrderBook, exchange: int) -> list[ExecutionEvent]:
        event_time = book_event_time(book)
        events: list[ExecutionEvent] = []
        decision = self.entry_policy.evaluate_signal(signal)
        if not decision.allowed:
            return [_event("execution_reject", signal, book, decision.reason, decision.merged_metadata)]
        safety_decision = self.live_safety.evaluate_entry(signal, event_time)
        if not safety_decision.allowed:
            return [_event("execution_reject", signal, book, safety_decision.reason, safety_decision.metadata)]
        qty = min(self.config.micro_engine.qty, self.config.live_execution.max_order_qty)
        if qty <= 0:
            return [_event("execution_reject", signal, book, "live_qty_not_positive", _live_reject_metadata("live_qty_not_positive", "qty", {"qty": qty}))]
        rollover_metadata = _contract_rollover_block(event_time, self.config)
        if rollover_metadata is not None:
            return [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "contract_rollover_window",
                    _live_reject_metadata("contract_rollover_window", "contract_rollover", rollover_metadata),
                )
            ]
        if self.config.live_execution.kill_switch_enabled:
            return [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "kill_switch_active",
                    _live_reject_metadata("kill_switch_active", "kill_switch", self._live_guard_state(event_time)),
                )
            ]
        if self.loss_hold_guard_active:
            return [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "loss_hold_guard_active",
                    _live_reject_metadata("loss_hold_guard_active", "loss_hold_guard", self._live_guard_state(event_time)),
                )
            ]
        if self._api_backoff_active(event_time):
            return [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "live_api_backoff_active",
                    _live_reject_metadata(
                        "live_api_backoff_active",
                        "kabu_api_backoff",
                        {"live_api_health": self._live_api_health(event_time)},
                    ),
                )
            ]
        if self.last_position_poll_at is None or (self.position_sync_blocked and self._should_poll_positions(event_time)):
            events.extend(self._sync_position(book, exchange))
            events.extend(self._evaluate_live_guards(book, None, event_time))
            events.extend(self._submit_take_profit_orders(book, exchange, event_time))
        if self.loss_hold_guard_active:
            return events + [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "loss_hold_guard_active",
                    _live_reject_metadata("loss_hold_guard_active", "loss_hold_guard", self._live_guard_state(event_time)),
                )
            ]
        if self.position_sync_blocked:
            return events + [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "position_sync_blocked",
                    _live_reject_metadata(
                        "position_sync_blocked",
                        "position_state",
                        {"live_api_health": self._live_api_health(event_time)},
                    ),
                )
            ]
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
        if self._wrong_instance_cooldown_active(event_time):
            return events + [
                _event(
                    "execution_reject",
                    signal,
                    book,
                    "kabu_station_wrong_instance_cooldown",
                    _live_reject_metadata(
                        "kabu_station_wrong_instance_cooldown",
                        "kabu_station_instance",
                        {"live_api_health": self._live_api_health(event_time)},
                    ),
                )
            ]
        max_positions = self.config.live_execution.max_positions_per_symbol
        if self._position_capacity_used(signal.symbol) >= max_positions:
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
                            "max_positions_per_symbol": max_positions,
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
        order_send_time = datetime.now(timezone.utc)
        try:
            response = self._api_call("sendorder_future_entry", self.client.sendorder_future, payload)
        except KabuApiError as exc:
            self.order_errors += 1
            api_metadata = self._record_api_error(exc, event_time, "sendorder_future_entry")
            reason = _api_error_reason(api_metadata["category"], "entry_order_api_error")
            failure_metadata = self._record_entry_failure(event_time, str(reason))
            return [
                _event(
                    "live_order_error",
                    signal,
                    book,
                    str(reason),
                    {
                        **_live_reject_metadata(str(reason), _api_error_blocked_by(api_metadata["category"], "kabu_api")),
                        "error": str(exc),
                        "order_payload": payload,
                        "live_api_health": self._live_api_health(event_time),
                        **api_metadata,
                        **failure_metadata,
                    },
                )
            ]
        order_response_time = datetime.now(timezone.utc)
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
        self.live_safety.record_entry(signal, event_time)
        events.append(
            _event(
                "live_order_submitted",
                signal,
                book,
                "entry_limit_fak_submitted",
                {
                    **decision.merged_metadata,
                    "order_id": order_id,
                    "signal_time": event_time.isoformat(),
                    "order_send_time": order_send_time.isoformat(),
                    "api_response_time": order_response_time.isoformat(),
                    "signal_to_order_send_ms": _elapsed_ms(event_time, order_send_time),
                    "order_send_to_api_response_ms": _elapsed_ms(order_send_time, order_response_time),
                    "entry_signal_price": signal.price,
                    "entry_order_price": entry_price,
                    "entry_slippage_ticks": self.config.live_execution.entry_slippage_ticks,
                    "order_payload": payload,
                    "response": response,
                    "live_safety_state": self.live_safety.summary(event_time),
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
        events.extend(self._evaluate_live_guards(book, features, event_time))
        if self.config.live_execution.kill_switch_enabled:
            events.extend(self._cancel_all_pending_orders(book, event_time, "kill_switch_cancel_pending_order"))
        elif self.loss_hold_guard_active:
            events.extend(self._cancel_pending_entry(book, event_time, "loss_hold_guard_cancel_pending_entry"))
        events.extend(self._handle_pending_entry_timeout(book, event_time))

        events.extend(self._submit_take_profit_orders(book, exchange, event_time))
        self._refresh_legacy_state()
        return events

    def heartbeat_metadata(self) -> dict[str, object]:
        positions = [position_summary(slot.position) for slot in self.live_positions.values()]
        pending_exits = [pending_summary(order) for order in self.pending_exits.values()]
        active_engines = [trade_engine(slot.trade) for slot in self.live_positions.values()]
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
            "live_position": position_summary(self.live_position),
            "live_positions": positions,
            "live_position_count": len(self.live_positions),
            "live_pending_entry": pending_summary(self.pending_entry),
            "live_pending_exit": pending_summary(self.pending_exit),
            "live_pending_exits": pending_exits,
            "live_active_engine": trade_engine(self._active_trade()),
            "live_active_engines": active_engines,
            "live_last_order_statuses": dict(self.last_order_snapshot_by_id),
            "live_orders_submitted": self.orders_submitted,
            "live_order_errors": self.order_errors,
            "live_entry_orders_submitted": self.entry_orders_submitted,
            "live_entry_orders_cancelled": self.entry_orders_cancelled,
            "live_exit_orders_submitted": self.exit_orders_submitted,
            "live_exit_orders_cancelled": self.exit_orders_cancelled,
            "live_entry_orders_expired": self.entry_orders_expired,
            "live_exit_orders_expired": self.exit_orders_expired,
            "live_positions_detected": self.positions_detected,
            "live_positions_flat": self.positions_flat,
            "live_entry_fill_rate": _ratio(self.own_entry_fills_detected, self.entry_orders_submitted),
            "live_own_entry_fills_detected": self.own_entry_fills_detected,
            "live_trades_count": self.live_trades_count,
            "live_wins": self.live_wins,
            "live_losses": self.live_losses,
            "live_win_rate": _ratio(self.live_wins, self.live_trades_count),
            "live_pnl_ticks": round(self.live_pnl_ticks, 4),
            "live_pnl_yen": round(self.live_pnl_yen, 2),
            "live_avg_pnl_ticks": round(self.live_pnl_ticks / self.live_trades_count, 4)
            if self.live_trades_count
            else 0.0,
            "live_exit_blocked": sorted(self.exit_blocked),
            "live_exit_blocked_count": len(self.exit_blocked),
            "live_exit_failure_counts": dict(self.exit_failure_counts),
            "live_entry_failure_count": self.consecutive_entry_failures,
            "live_entry_cooldown_until": self.entry_cooldown_until.isoformat()
            if self.entry_cooldown_until is not None
            else None,
            "live_position_sync_blocked": self.position_sync_blocked,
            "live_exit_retry_after": {key: value.isoformat() for key, value in self.exit_retry_after.items()},
            "live_safety_state": self.live_safety.summary(),
            "live_api_health": self._live_api_health(),
            "live_guard_state": self._live_guard_state(),
        }

    def _sync_order_status(self, book: OrderBook) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        pending_orders = [order for order in (self.pending_entry,) if order is not None]
        pending_orders.extend(self.pending_exits.values())
        for pending in tuple(pending_orders):
            status_receive_time = book_event_time(book)
            try:
                orders = _orders_list(self._api_call("orders", self.client.orders, product=3, id=pending.order_id, details="true"))
            except KabuApiError as exc:
                self.order_errors += 1
                event_time = status_receive_time
                api_metadata = self._record_api_error(exc, event_time, "orders")
                events.append(
                    ExecutionEvent(
                        "live_sync_error",
                        pending.symbol,
                        pending.direction,
                        qty=pending.qty,
                        reason=str(_api_error_reason(api_metadata["category"], "orders_api_error")),
                        timestamp=event_time,
                        metadata={
                            **_live_reject_metadata(
                                str(_api_error_reason(api_metadata["category"], "orders_api_error")),
                                _api_error_blocked_by(api_metadata["category"], "kabu_api"),
                            ),
                            "error": str(exc),
                            "order_id": pending.order_id,
                            "symbol_code": pending.symbol_code,
                            "position_key": pending.position_key,
                            "live_api_health": self._live_api_health(event_time),
                            **api_metadata,
                        },
                    )
                )
                continue
            self._record_api_success()
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
                            "order_status_receive_time": status_receive_time.isoformat(),
                            "order_send_to_status_ms": _elapsed_ms(pending.submitted_at, status_receive_time),
                        },
                    )
                )
            terminal_unfilled = _order_is_unfilled_terminal(order)
            terminal_partial_exit = _order_is_partially_filled_terminal(order, pending.qty)
            terminal_filled = _order_is_filled_terminal(order, pending.qty)
            filled_qty = _float_value(order.get("CumQty"))
            if terminal_filled:
                if self.pending_entry is not None and pending.order_id == self.pending_entry.order_id:
                    self.entry_execution_by_order_id[pending.order_id] = _order_execution_snapshot(order)
                elif (
                    pending.position_key is not None
                    and pending.position_key in self.pending_exits
                    and pending.order_id not in self.closed_exit_order_ids
                ):
                    closed_event = self._record_live_trade_closed(pending, snapshot, book_event_time(book))
                    if closed_event is not None:
                        events.append(closed_event)
                    self.closed_exit_order_ids.add(pending.order_id)
                continue
            elif terminal_unfilled:
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
                positions = _positions_list(self._api_call("positions", self.client.positions, product=3, symbol=symbol_code, addinfo="true"))
            except KabuApiError as exc:
                self.order_errors += 1
                self.position_sync_blocked = True
                sync_failed = True
                api_metadata = self._record_api_error(exc, event_time, "positions")
                events.append(
                    ExecutionEvent(
                        "live_sync_error",
                        symbol,
                        "flat",
                        reason=str(_api_error_reason(api_metadata["category"], "positions_api_error")),
                        timestamp=event_time,
                        metadata={
                            **_live_reject_metadata(
                                str(_api_error_reason(api_metadata["category"], "positions_api_error")),
                                _api_error_blocked_by(api_metadata["category"], "kabu_api"),
                            ),
                            "error": str(exc),
                            "symbol_code": symbol_code,
                            "live_api_health": self._live_api_health(event_time),
                            **api_metadata,
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
        self._record_api_success()
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

        current_positions = {live_position_key(position): position for position in active}
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
            closed_trade = self.closed_trade_by_position_key.pop(position_key, None)
            guard_clear_metadata: dict[str, object] = {}
            if not self.live_positions:
                guard_clear_metadata = self._clear_loss_hold_guard(event_time, "positions_flat_confirmed")
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
                        "live_trade": closed_trade,
                        **guard_clear_metadata,
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
            entry_order_id = None
            if self.entry_signal is not None and self.entry_signal.symbol == position.symbol and not pending_entry_consumed:
                source_signal = self.entry_signal
                entry_order_id = self.pending_entry.order_id if self.pending_entry is not None else None
                pending_entry_consumed = True
                own_entry_detected = True
            elif (
                self._orphan_entry_signal_active(event_time)
                and self.orphan_entry_signal is not None
                and self.orphan_entry_signal.symbol == position.symbol
                and not orphan_entry_consumed
            ):
                source_signal = self.orphan_entry_signal
                entry_order_id = self.orphan_entry_order_id
                orphan_entry_consumed = True
                own_entry_detected = True
            entry_signal = _position_entry_signal(source_signal, position, position.symbol)
            trade = self._trade_from_position(entry_signal, event_time, position.qty)
            entry_execution = self.entry_execution_by_order_id.get(entry_order_id or "", {})
            entry_execution_price = _optional_float(entry_execution.get("execution_price"))
            entry_price_mismatch = (
                entry_execution_price is not None and abs(entry_execution_price - position.entry_price) > 1e-9
            )
            self.live_positions[position_key] = LivePositionSlot(
                position,
                trade,
                entry_signal,
                entry_order_id=entry_order_id,
                entry_execution_price=entry_execution_price,
                entry_execution_id=_optional_str(entry_execution.get("execution_id")),
                entry_execution_day=_optional_str(entry_execution.get("execution_day")),
                entry_price_mismatch=entry_price_mismatch,
            )
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
                        "entry_order_id": entry_order_id,
                        "position_entry_price": position.entry_price,
                        "entry_execution_price": entry_execution_price,
                        "entry_execution_id": entry_execution.get("execution_id"),
                        "entry_execution_day": entry_execution.get("execution_day"),
                        "entry_execution_qty": entry_execution.get("execution_qty"),
                        "fill_to_position_visible_ms": _elapsed_ms(_parse_optional_datetime(entry_execution.get("execution_day")), event_time),
                        "order_send_to_position_visible_ms": _elapsed_ms(self.pending_entry.submitted_at, event_time)
                        if self.pending_entry is not None and entry_order_id == self.pending_entry.order_id
                        else None,
                        "entry_price_mismatch": entry_price_mismatch,
                    },
                )
            )
        if pending_entry_consumed:
            consumed = self._clear_pending_entry()
            if consumed is not None:
                self.entry_execution_by_order_id.pop(consumed.order_id, None)
        if orphan_entry_consumed:
            if self.orphan_entry_order_id is not None:
                self.entry_execution_by_order_id.pop(self.orphan_entry_order_id, None)
            self._clear_orphan_entry_signal()
        self._refresh_legacy_state()
        return events

    def _record_live_trade_closed(
        self,
        pending: PendingLiveOrder,
        order_status: dict[str, Any],
        event_time: datetime,
    ) -> ExecutionEvent | None:
        position_key = pending.position_key
        if position_key is None:
            return None
        slot = self.live_positions.get(position_key)
        if slot is None:
            return None
        exit_price = _optional_float(order_status.get("execution_price"))
        if exit_price is None:
            exit_price = _optional_float(order_status.get("price"))
        if exit_price is None:
            return None

        symbol = slot.position.symbol
        tick_size = self.config.tick_size_for(symbol)
        tick_value = self.config.tick_value_yen_for(symbol)
        execution_qty = _optional_float(order_status.get("execution_qty"))
        qty = int(execution_qty) if execution_qty is not None and execution_qty > 0 else pending.qty
        pnl_ticks = calculate_pnl_ticks(slot.position.direction, slot.position.entry_price, exit_price, tick_size)
        pnl_yen = pnl_ticks * qty * tick_value

        self.live_trades_count += 1
        if pnl_ticks > 0:
            self.live_wins += 1
        elif pnl_ticks < 0:
            self.live_losses += 1
        self.live_pnl_ticks += pnl_ticks
        self.live_pnl_yen += pnl_yen
        engine = trade_engine(slot.trade) or slot.entry_signal.engine
        self.live_safety.record_exit(engine, slot.position.direction, slot.entry_signal.reason, pending.reason, pnl_ticks, event_time)

        trade_record: dict[str, object] = {
            "symbol": symbol,
            "direction": slot.position.direction,
            "qty": qty,
            "entry_price": slot.position.entry_price,
            "exit_price": exit_price,
            "pnl_ticks": round(pnl_ticks, 4),
            "pnl_yen": round(pnl_yen, 2),
            "exit_reason": pending.reason,
            "position_key": position_key,
            "hold_id": slot.position.hold_id,
            "entry_order_id": slot.entry_order_id,
            "entry_execution_price": slot.entry_execution_price,
            "entry_execution_id": slot.entry_execution_id,
            "entry_execution_day": slot.entry_execution_day,
            "entry_price_mismatch": slot.entry_price_mismatch,
            "exit_order_id": pending.order_id,
            "exit_execution_price": exit_price,
            "exit_execution_id": order_status.get("execution_id"),
            "exit_execution_ids": order_status.get("execution_ids"),
            "exit_execution_day": order_status.get("execution_day"),
            "exit_execution_qty": order_status.get("execution_qty"),
            "tick_size": tick_size,
            "tick_value_yen": tick_value,
            "engine": engine,
            "signal_reason": slot.entry_signal.reason,
            "live_safety_state": self.live_safety.summary(event_time),
        }
        self.closed_trade_by_position_key[position_key] = trade_record
        return ExecutionEvent(
            "live_trade_closed",
            symbol,
            slot.position.direction,
            qty=qty,
            entry_price=slot.position.entry_price,
            exit_price=exit_price,
            reason=pending.reason,
            pnl_ticks=round(pnl_ticks, 4),
            pnl_yen=round(pnl_yen, 2),
            timestamp=event_time,
            metadata={
                **event_trace_metadata("position_lifecycle", "exit", pending.reason, checks={"engine": trade_engine(slot.trade)}),
                **trade_record,
                "order_status": order_status,
            },
        )

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
        order_send_time = datetime.now(timezone.utc)
        try:
            response = self._api_call("sendorder_future_exit", self.client.sendorder_future, payload)
        except KabuApiError as exc:
            self.order_errors += 1
            api_metadata = self._record_api_error(exc, event_time, "sendorder_future_exit")
            api_reason = str(_api_error_reason(api_metadata["category"], "exit_order_api_error"))
            failure_metadata = self._record_exit_failure(position_key, event_time, api_reason)
            reason = "exit_order_blocked_after_retries" if failure_metadata["exit_blocked"] else api_reason
            return ExecutionEvent(
                "live_order_error",
                trade.symbol,
                trade.direction,
                qty=position.qty,
                entry_price=trade.entry_price,
                reason=reason,
                timestamp=event_time,
                metadata={
                    **_live_reject_metadata(
                        reason,
                        "exit_order_state" if failure_metadata["exit_blocked"] else _api_error_blocked_by(api_metadata["category"], "kabu_api"),
                    ),
                    "error": str(exc),
                    "exit_reason": decision.reason,
                    "position_key": position_key,
                    "hold_id": position.hold_id,
                    "order_payload": payload,
                    "live_api_health": self._live_api_health(event_time),
                    **api_metadata,
                    **failure_metadata,
                },
            )
        order_response_time = datetime.now(timezone.utc)
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
                "order_send_time": order_send_time.isoformat(),
                "api_response_time": order_response_time.isoformat(),
                "order_send_to_api_response_ms": _elapsed_ms(order_send_time, order_response_time),
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
        symbol_code = validated_symbol_code(slot.position)
        if symbol_code is None:
            return None
        return self.orders.close_aggressive_limit(
            symbol_code,
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
        if (
            self.config.live_execution.kill_switch_enabled
            or self.loss_hold_guard_active
            or self._wrong_instance_cooldown_active(event_time)
            or self._api_backoff_active(event_time)
        ):
            return events
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
        cancel_events: list[ExecutionEvent] = []
        if self.config.live_execution.cancel_pending_entry_on_timeout and _order_snapshot_active(last_status):
            cancel_events = self._cancel_pending_entry(book, event_time, "pending_entry_timeout_cancel")
        if self.pending_entry is not None:
            self._clear_pending_entry(preserve_orphan=True, event_time=event_time)
        self.entry_orders_expired += 1
        failure_metadata = self._record_entry_failure(event_time, "pending_entry_timeout_after_grace")
        return cancel_events + [
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

    def _cancel_all_pending_orders(self, book: OrderBook, event_time: datetime, reason: str) -> list[ExecutionEvent]:
        events = self._cancel_pending_entry(book, event_time, reason)
        for position_key, pending in tuple(self.pending_exits.items()):
            events.extend(self._cancel_pending_order(pending, event_time, reason, is_entry=False))
            self.pending_exits.pop(position_key, None)
        self._refresh_legacy_state()
        return events

    def _cancel_pending_entry(self, book: OrderBook, event_time: datetime, reason: str) -> list[ExecutionEvent]:
        if self.pending_entry is None:
            return []
        pending = self.pending_entry
        events = self._cancel_pending_order(pending, event_time, reason, is_entry=True)
        self._clear_pending_entry(preserve_orphan=True, event_time=event_time)
        return events

    def _cancel_pending_order(
        self,
        pending: PendingLiveOrder,
        event_time: datetime,
        reason: str,
        is_entry: bool,
    ) -> list[ExecutionEvent]:
        if not hasattr(self.client, "cancelorder"):
            return []
        try:
            response = self._api_call("cancelorder", self.client.cancelorder, pending.order_id)
        except KabuApiError as exc:
            self.order_errors += 1
            api_metadata = self._record_api_error(exc, event_time, "cancelorder")
            return [
                ExecutionEvent(
                    "live_order_error",
                    pending.symbol,
                    pending.direction,
                    qty=pending.qty,
                    reason=str(_api_error_reason(api_metadata["category"], "cancel_order_api_error")),
                    timestamp=event_time,
                    metadata={
                        **_live_reject_metadata(
                            str(_api_error_reason(api_metadata["category"], "cancel_order_api_error")),
                            _api_error_blocked_by(api_metadata["category"], "kabu_api"),
                        ),
                        "error": str(exc),
                        "order_id": pending.order_id,
                        "symbol_code": pending.symbol_code,
                        "position_key": pending.position_key,
                        "cancel_reason": reason,
                        "live_api_health": self._live_api_health(event_time),
                        **api_metadata,
                    },
                )
            ]
        if is_entry:
            self.entry_orders_cancelled += 1
        else:
            self.exit_orders_cancelled += 1
        return [
            ExecutionEvent(
                "live_order_cancelled",
                pending.symbol,
                pending.direction,
                qty=pending.qty,
                reason=reason,
                timestamp=event_time,
                metadata={
                    **event_trace_metadata("execution_order", "cancel", reason, checks={"order_id": pending.order_id}),
                    "order_id": pending.order_id,
                    "symbol_code": pending.symbol_code,
                    "position_key": pending.position_key,
                    "response": response,
                    "is_entry": is_entry,
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

    def _wrong_instance_cooldown_active(self, event_time: datetime) -> bool:
        return self.api_health.wrong_instance_cooldown_active(event_time)

    def _record_api_error(self, exc: KabuApiError, event_time: datetime, operation: str) -> dict[str, object]:
        metadata = self.api_health.record_error(exc, event_time, operation)
        if metadata.get("category") == "auth_recovery_failed":
            self.position_sync_blocked = True
        return metadata

    def _record_api_success(self) -> None:
        self.api_health.record_success()

    def _api_call(self, operation: str, func: Callable[..., T], *args: object, **kwargs: object) -> T:
        return self.api_health.call(operation, func, *args, **kwargs)

    def _api_backoff_active(self, event_time: datetime) -> bool:
        return self.api_health.api_backoff_active(event_time)

    def _live_api_health(self, event_time: datetime | None = None) -> dict[str, object]:
        return self.api_health.summary(
            event_time,
            position_sync_blocked=self.position_sync_blocked,
            last_position_poll_at=self.last_position_poll_at,
        )

    def _evaluate_live_guards(self, book: OrderBook, features: BookFeatures | None, event_time: datetime) -> list[ExecutionEvent]:
        if self.loss_hold_guard_active:
            if not self.live_positions:
                self._clear_loss_hold_guard(event_time, "no_live_positions")
            return []
        snapshot = self._loss_hold_snapshot(book)
        reason = _loss_hold_reason(snapshot, self.config.live_execution.loss_hold_guard_ticks, self.config.live_execution.daily_loss_limit_yen)
        if reason is None:
            return []
        suspect_reason = self._loss_hold_mark_suspect_reason(book, features, snapshot)
        if suspect_reason is not None:
            return [
                ExecutionEvent(
                    "live_guard_status",
                    book.symbol,
                    "flat",
                    reason="loss_hold_guard_mark_price_suspect",
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata(
                            "position_lifecycle",
                            "status",
                            "loss_hold_guard_mark_price_suspect",
                            "mark_price_quality",
                            {
                                "loss_hold_guard_reason": reason,
                                "loss_hold_guard_snapshot": snapshot,
                                "mark_price_suspect_reason": suspect_reason,
                                "spread_ticks": _features_spread_ticks(book, features, self.config.tick_size_for(book.symbol)),
                                "jump_detected": bool(features.jump_detected) if features is not None else False,
                                "jump_reason": features.jump_reason if features is not None else None,
                            },
                        ),
                        "manual_review_required": False,
                        "auto_loss_close_disabled": True,
                    },
                )
            ]
        self.loss_hold_guard_active = True
        self.loss_hold_guard_since = event_time
        self.loss_hold_guard_reason = reason
        self.loss_hold_guard_snapshot = snapshot
        self.loss_hold_guard_reported = True
        return [
            ExecutionEvent(
                "live_sync_error",
                book.symbol,
                "flat",
                reason="loss_hold_guard_active",
                timestamp=event_time,
                metadata={
                    **event_trace_metadata(
                        "position_lifecycle",
                        "status",
                        "loss_hold_guard_active",
                        "loss_hold_guard",
                        self._live_guard_state(event_time),
                    ),
                    "manual_review_required": True,
                    "auto_loss_close_disabled": True,
                },
            )
        ]

    def _loss_hold_mark_suspect_reason(
        self,
        book: OrderBook,
        features: BookFeatures | None,
        snapshot: dict[str, object],
    ) -> str | None:
        if not snapshot.get("positions"):
            return None
        tick_size = self.config.tick_size_for(book.symbol)
        spread_ticks = _features_spread_ticks(book, features, tick_size)
        if features is not None and features.symbol == book.symbol and features.jump_detected:
            return str(features.jump_reason or "jump_detected")
        if spread_ticks > 2.0:
            return "spread_wide"
        worst_ticks = abs(_optional_float(snapshot.get("worst_unrealized_ticks")) or 0.0)
        max_loss_ticks = float(self.config.live_execution.loss_hold_guard_ticks)
        if max_loss_ticks > 0 and worst_ticks > max_loss_ticks * 5.0:
            return "unrealized_ticks_outlier"
        return None

    def _clear_loss_hold_guard(self, event_time: datetime, reason: str) -> dict[str, object]:
        if not self.loss_hold_guard_active:
            return {}
        previous_since = self.loss_hold_guard_since
        previous_reason = self.loss_hold_guard_reason
        previous_snapshot = dict(self.loss_hold_guard_snapshot)
        self.loss_hold_guard_active = False
        self.loss_hold_guard_since = None
        self.loss_hold_guard_reason = None
        self.loss_hold_guard_snapshot = {}
        self.loss_hold_guard_reported = False
        return {
            "loss_hold_guard_cleared": True,
            "loss_hold_guard_clear_reason": reason,
            "previous_loss_hold_guard_since": previous_since.isoformat() if previous_since else None,
            "previous_loss_hold_guard_reason": previous_reason,
            "previous_loss_hold_guard_snapshot": previous_snapshot,
            "loss_hold_guard_cleared_at": event_time.isoformat(),
        }

    def _loss_hold_snapshot(self, book: OrderBook) -> dict[str, object]:
        positions: list[dict[str, object]] = []
        total_unrealized_yen = 0.0
        worst_unrealized_ticks = 0.0
        for position_key, slot in self.live_positions.items():
            if slot.position.symbol != book.symbol:
                continue
            exit_price = book.best_bid_price if slot.position.direction == "long" else book.best_ask_price
            tick_size = self.config.tick_size_for(slot.position.symbol)
            pnl_ticks = calculate_pnl_ticks(slot.position.direction, slot.position.entry_price, exit_price, tick_size)
            pnl_yen = pnl_ticks * slot.position.qty * self.config.tick_value_yen_for(slot.position.symbol)
            worst_unrealized_ticks = min(worst_unrealized_ticks, pnl_ticks)
            total_unrealized_yen += pnl_yen
            positions.append(
                {
                    "position_key": position_key,
                    "symbol": slot.position.symbol,
                    "direction": slot.position.direction,
                    "qty": slot.position.qty,
                    "entry_price": slot.position.entry_price,
                    "mark_price": exit_price,
                    "unrealized_ticks": round(pnl_ticks, 4),
                    "unrealized_yen": round(pnl_yen, 2),
                }
            )
        total_pnl_yen = self.live_pnl_yen + total_unrealized_yen
        return {
            "positions": positions,
            "realized_pnl_yen": round(self.live_pnl_yen, 2),
            "unrealized_pnl_yen": round(total_unrealized_yen, 2),
            "total_pnl_yen": round(total_pnl_yen, 2),
            "worst_unrealized_ticks": round(worst_unrealized_ticks, 4),
            "loss_hold_guard_ticks": self.config.live_execution.loss_hold_guard_ticks,
            "daily_loss_limit_yen": self.config.live_execution.daily_loss_limit_yen,
        }

    def _live_guard_state(self, event_time: datetime | None = None) -> dict[str, object]:
        return {
            "kill_switch_active": self.config.live_execution.kill_switch_enabled,
            "loss_hold_guard_active": self.loss_hold_guard_active,
            "loss_hold_guard_since": self.loss_hold_guard_since.isoformat() if self.loss_hold_guard_since else None,
            "loss_hold_guard_reason": self.loss_hold_guard_reason,
            "loss_hold_guard_snapshot": dict(self.loss_hold_guard_snapshot),
            "manual_review_required": self.loss_hold_guard_active or self.config.live_execution.kill_switch_enabled,
            "auto_loss_close_disabled": True,
            "event_time": event_time.isoformat() if event_time is not None else None,
        }

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


def _elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() * 1000.0, 4)


def _loss_hold_reason(snapshot: dict[str, object], max_loss_ticks: float, daily_loss_yen: float) -> str | None:
    worst_ticks = _optional_float(snapshot.get("worst_unrealized_ticks")) or 0.0
    total_pnl_yen = _optional_float(snapshot.get("total_pnl_yen")) or 0.0
    if max_loss_ticks > 0 and worst_ticks <= -float(max_loss_ticks):
        return "unrealized_loss_ticks_limit"
    if daily_loss_yen > 0 and total_pnl_yen <= -float(daily_loss_yen):
        return "daily_realized_or_unrealized_loss_limit"
    return None


def _features_spread_ticks(book: OrderBook, features: BookFeatures | None, tick_size: float) -> float:
    if features is not None and features.symbol == book.symbol:
        return float(features.spread_ticks)
    if tick_size <= 0:
        return 0.0
    return float(book.spread / tick_size)


def _order_snapshot_active(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return True
    state = _optional_int(snapshot.get("state"))
    order_state = _optional_int(snapshot.get("order_state"))
    if state == 5 or order_state == 5:
        return False
    return True


def _api_error_reason(category: object, fallback: str) -> str:
    if category == "kabu_station_wrong_instance":
        return "kabu_station_wrong_instance"
    if category == "auth_recovery_failed":
        return "auth_recovery_failed"
    if category == "rate_limit":
        return "kabu_api_rate_limit"
    if category == "service_unavailable":
        return "kabu_api_service_unavailable"
    if category == "server_error":
        return "kabu_api_server_error"
    if category == "bad_request":
        return "kabu_api_bad_request"
    if category == "forbidden":
        return "kabu_api_forbidden"
    return fallback


def _api_error_blocked_by(category: object, fallback: str) -> str:
    if category == "kabu_station_wrong_instance":
        return "kabu_station_instance"
    if category in {"auth_error", "auth_recovery_failed"}:
        return "kabu_auth"
    if category in {"rate_limit", "service_unavailable"}:
        return "kabu_api_backoff"
    if category in {"server_error", "bad_request", "forbidden"}:
        return "kabu_api"
    return fallback


def _contract_rollover_block(event_time: datetime, config: StrategyConfig) -> dict[str, object] | None:
    deriv_month = int(config.symbols.deriv_month or 0)
    if deriv_month <= 0:
        return None
    year = deriv_month // 100
    month = deriv_month % 100
    if year <= 0 or month < 1 or month > 12:
        return None
    sq_date = _second_friday(year, month)
    last_trade_date = _previous_business_day(sq_date)
    block_start = _subtract_business_days(last_trade_date, config.symbols.rollover_business_days_before_last_trade)
    today = event_time.astimezone(timezone(timedelta(hours=9))).date() if event_time.tzinfo else event_time.date()
    if today < block_start:
        return None
    return {
        "deriv_month": deriv_month,
        "sq_date": sq_date.isoformat(),
        "estimated_last_trade_date": last_trade_date.isoformat(),
        "block_start_date": block_start.isoformat(),
        "rollover_business_days_before_last_trade": config.symbols.rollover_business_days_before_last_trade,
    }


def _second_friday(year: int, month: int) -> date:
    current = date(year, month, 1)
    fridays = 0
    while True:
        if current.weekday() == 4:
            fridays += 1
            if fridays == 2:
                return current
        current = current + timedelta(days=1)


def _previous_business_day(value: date) -> date:
    current = value - timedelta(days=1)
    while current.weekday() >= 5:
        current = current - timedelta(days=1)
    return current


def _subtract_business_days(value: date, days: int) -> date:
    current = value
    remaining = max(0, int(days))
    while remaining > 0:
        current = current - timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


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
    execution = _order_execution_snapshot(order)
    return {
        "id": order.get("ID") or order.get("OrderId") or order.get("OrderID"),
        "state": order.get("State"),
        "order_state": order.get("OrderState"),
        "order_qty": order.get("OrderQty"),
        "cum_qty": order.get("CumQty"),
        "side": order.get("Side"),
        "price": order.get("Price"),
        **execution,
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


def _order_execution_snapshot(order: dict[str, Any]) -> dict[str, Any]:
    details = [detail for detail in (order.get("Details") or []) if isinstance(detail, dict)]
    executions: list[tuple[float, float, str | None, str | None]] = []
    for detail in details:
        rec_type = _optional_int(detail.get("RecType"))
        execution_id = _optional_str(detail.get("ExecutionID"))
        price = _optional_float(detail.get("Price"))
        if price is None or (execution_id is None and rec_type != 8):
            continue
        qty = _optional_float(detail.get("Qty")) or 1.0
        executions.append((price, qty, execution_id, _optional_str(detail.get("ExecutionDay"))))
    if executions:
        total_qty = sum(qty for _, qty, _, _ in executions)
        avg_price = sum(price * qty for price, qty, _, _ in executions) / total_qty if total_qty > 0 else executions[-1][0]
        execution_ids = [execution_id for _, _, execution_id, _ in executions if execution_id]
        execution_days = [execution_day for _, _, _, execution_day in executions if execution_day]
        return {
            "execution_price": avg_price,
            "execution_qty": total_qty,
            "execution_id": execution_ids[-1] if execution_ids else None,
            "execution_ids": execution_ids,
            "execution_day": execution_days[-1] if execution_days else None,
            "execution_days": execution_days,
            "execution_source": "details",
        }
    if _float_value(order.get("CumQty")) > 0:
        price = _optional_float(order.get("Price"))
        if price is not None:
            return {
                "execution_price": price,
                "execution_qty": _float_value(order.get("CumQty")),
                "execution_id": None,
                "execution_ids": [],
                "execution_day": None,
                "execution_days": [],
                "execution_source": "order_price",
            }
    return {
        "execution_price": None,
        "execution_qty": 0.0,
        "execution_id": None,
        "execution_ids": [],
        "execution_day": None,
        "execution_days": [],
        "execution_source": None,
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


def _order_is_filled_terminal(order: dict[str, Any], order_qty: int) -> bool:
    return _float_value(order.get("CumQty")) >= float(order_qty) and _order_is_terminal(order)


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


def _optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _parse_optional_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
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
