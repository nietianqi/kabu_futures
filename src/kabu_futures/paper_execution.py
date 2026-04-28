from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from .config import StrategyConfig, default_config
from .execution import MicroTradeManager, MinuteTradeManager, pnl_ticks as calculate_pnl_ticks
from .models import BookFeatures, Direction, OrderBook, Signal
from .policy import event_trace_metadata
from .serialization import event_time as book_event_time, signal_snapshot


TradeMode = Literal["observe", "paper", "live"]
PaperFillModel = Literal["immediate", "touch"]
ExecutionEventType = Literal[
    "paper_entry",
    "paper_exit",
    "paper_pending",
    "paper_cancel",
    "execution_reject",
    "execution_skip",
    "live_order_submitted",
    "live_order_cancelled",
    "live_order_status",
    "live_order_expired",
    "live_order_error",
    "live_position_detected",
    "live_position_flat",
    "live_trade_closed",
    "live_sync_error",
]


@dataclass(frozen=True)
class ExecutionEvent:
    event_type: ExecutionEventType
    symbol: str
    direction: Direction
    qty: int = 0
    entry_price: float | None = None
    exit_price: float | None = None
    reason: str = ""
    pnl_ticks: float | None = None
    pnl_yen: float | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "event": self.event_type,
            "event_type": self.event_type,
            "symbol": self.symbol,
            "direction": self.direction,
            "qty": self.qty,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "reason": self.reason,
            "pnl_ticks": self.pnl_ticks,
            "pnl_yen": self.pnl_yen,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class PaperExecutionStats:
    trades: int = 0
    pnl_ticks: float = 0.0
    pnl_yen: float = 0.0
    pending_orders: int = 0


@dataclass(frozen=True)
class PendingPaperOrder:
    signal: Signal
    qty: int
    limit_price: float
    created_at: datetime
    expires_at: datetime


@dataclass
class _PaperMicroSlot:
    trade_manager: MicroTradeManager
    entry_signal: Signal | None


@dataclass
class _PaperMinuteSlot:
    trade_manager: MinuteTradeManager
    entry_signal: Signal | None


class PaperMicroExecutor:
    """Paper-only lifecycle manager for microstructure scalp signals."""

    def __init__(self, config: StrategyConfig | None = None, fill_model: PaperFillModel = "immediate") -> None:
        self.config = config or default_config()
        self.fill_model = fill_model
        self.trade_slots: list[_PaperMicroSlot] = []
        self.pending_order: PendingPaperOrder | None = None
        self.trades = 0
        self.pnl_ticks = 0.0
        self.pnl_yen = 0.0

    def on_signal(self, signal: Signal, book: OrderBook) -> list[ExecutionEvent]:
        if signal.engine != "micro_book":
            return [self._reject(signal, book, "non_micro_signal")]
        if not signal.is_tradeable:
            return [self._reject(signal, book, "non_tradeable_signal")]
        if signal.price is None:
            return [self._reject(signal, book, "missing_signal_price")]
        if not self.config.is_trade_symbol(signal.symbol):
            return [self._reject(signal, book, "unsupported_symbol")]
        if self.position_count(signal.symbol) >= self.config.risk.max_positions_per_symbol:
            return [self._reject(signal, book, "max_positions_per_symbol")]
        if self.pending_order is not None:
            return [self._reject(signal, book, "already_has_pending_order")]

        event_time = book_event_time(book)
        if self.fill_model == "touch":
            self.pending_order = PendingPaperOrder(
                signal=signal,
                qty=self.config.micro_engine.qty,
                limit_price=signal.price,
                created_at=event_time,
                expires_at=event_time + _pending_timeout(self.config.micro_engine.max_hold_seconds),
            )
            return [
                ExecutionEvent(
                    "paper_pending",
                    signal.symbol,
                    signal.direction,
                    qty=self.config.micro_engine.qty,
                    entry_price=signal.price,
                    reason="waiting_for_touch",
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata("execution_order", "pending", "waiting_for_touch", checks={"engine": signal.engine}),
                        "fill_model": self.fill_model,
                        "expires_at": self.pending_order.expires_at.isoformat(),
                        "signal": signal_snapshot(signal),
                    },
                )
            ]
        return [self._open_position(signal, book, event_time, "immediate_fill")]

    def on_book(self, book: OrderBook, features: BookFeatures | None = None) -> list[ExecutionEvent]:
        slots_before_pending = list(self.trade_slots)
        pending_events = self._evaluate_pending_order(book)
        new_slots = self.trade_slots[len(slots_before_pending) :]
        events: list[ExecutionEvent] = list(pending_events)
        active: list[_PaperMicroSlot] = []
        for slot in slots_before_pending:
            trade = slot.trade_manager.trade
            if trade is None or book.symbol != trade.symbol:
                active.append(slot)
                continue
            decision = slot.trade_manager.evaluate_exit(book, features)
            if not decision.should_exit or decision.price is None:
                active.append(slot)
                continue
            closed = slot.trade_manager.mark_closed()
            if closed is None:
                continue
            events.append(self._exit_event(closed, decision, book, slot.entry_signal))
        active.extend(new_slots)
        self.trade_slots = active
        return events

    def position_summary(self) -> dict[str, object] | None:
        summaries = self.position_summaries()
        return summaries[0] if summaries else None

    def position_summaries(self) -> list[dict[str, object]]:
        summaries: list[dict[str, object]] = []
        for idx, slot in enumerate(self.trade_slots):
            trade = slot.trade_manager.trade
            if trade is None:
                continue
            summaries.append(
                {
                    "position_id": f"micro-{idx + 1}",
                    "symbol": trade.symbol,
                    "direction": trade.direction,
                    "qty": trade.qty,
                    "entry_price": trade.entry_price,
                    "entry_time": trade.entry_time.isoformat(),
                    "take_profit_price": trade.take_profit_price,
                    "stop_loss_price": trade.stop_loss_price,
                    "take_profit_ticks": trade.take_profit_ticks,
                }
            )
        return summaries

    def position_count(self, symbol: str | None = None) -> int:
        summaries = self.position_summaries()
        if symbol is None:
            return len(summaries)
        return sum(1 for summary in summaries if summary.get("symbol") == symbol)

    def pending_count(self) -> int:
        return 1 if self.pending_order is not None else 0

    def _exit_event(
        self,
        closed: object,
        decision: object,
        book: OrderBook,
        entry_signal: Signal | None,
    ) -> ExecutionEvent:
        tick_size = self.config.tick_size_for(closed.symbol)
        pnl_ticks = calculate_pnl_ticks(closed.direction, closed.entry_price, decision.price, tick_size)
        pnl_yen = pnl_ticks * closed.qty * self.config.tick_value_yen_for(closed.symbol)
        self.trades += 1
        self.pnl_ticks += pnl_ticks
        self.pnl_yen += pnl_yen
        return ExecutionEvent(
            "paper_exit",
            closed.symbol,
            closed.direction,
            qty=closed.qty,
            entry_price=closed.entry_price,
            exit_price=decision.price,
            reason=decision.reason,
            pnl_ticks=round(pnl_ticks, 4),
            pnl_yen=round(pnl_yen, 2),
            timestamp=book_event_time(book),
            metadata={
                **event_trace_metadata("position_lifecycle", "exit", decision.reason, checks={"engine": closed.engine}),
                "emergency": decision.emergency,
                "hold_seconds": round((book_event_time(book) - closed.entry_time).total_seconds(), 3),
                "take_profit_price": closed.take_profit_price,
                "stop_loss_price": closed.stop_loss_price,
                "take_profit_ticks": closed.take_profit_ticks,
                "signal": signal_snapshot(entry_signal) if entry_signal is not None else None,
            },
        )

    def _trade_summary(self) -> dict[str, object] | None:
        trade = self.trade_slots[0].trade_manager.trade if self.trade_slots else None
        if trade is None:
            return None
        return {
            "symbol": trade.symbol,
            "direction": trade.direction,
            "qty": trade.qty,
            "entry_price": trade.entry_price,
            "entry_time": trade.entry_time.isoformat(),
            "take_profit_price": trade.take_profit_price,
            "stop_loss_price": trade.stop_loss_price,
            "take_profit_ticks": trade.take_profit_ticks,
        }

    def pending_summary(self) -> dict[str, object] | None:
        pending = self.pending_order
        if pending is None:
            return None
        return {
            "symbol": pending.signal.symbol,
            "direction": pending.signal.direction,
            "qty": pending.qty,
            "limit_price": pending.limit_price,
            "created_at": pending.created_at.isoformat(),
            "expires_at": pending.expires_at.isoformat(),
        }

    def stats(self) -> PaperExecutionStats:
        return PaperExecutionStats(self.trades, self.pnl_ticks, self.pnl_yen, 1 if self.pending_order is not None else 0)

    def _reject(self, signal: Signal, book: OrderBook, reason: str) -> ExecutionEvent:
        return ExecutionEvent(
            "execution_reject",
            signal.symbol,
            signal.direction,
            reason=reason,
            timestamp=book_event_time(book),
            metadata={
                **event_trace_metadata("execution_order", "reject", reason, "paper_micro_executor"),
                "signal": signal_snapshot(signal),
            },
        )

    def _evaluate_pending_order(self, book: OrderBook) -> list[ExecutionEvent]:
        pending = self.pending_order
        if pending is None or pending.signal.symbol != book.symbol:
            return []
        event_time = book_event_time(book)
        if event_time >= pending.expires_at:
            self.pending_order = None
            return [
                ExecutionEvent(
                    "paper_cancel",
                    pending.signal.symbol,
                    pending.signal.direction,
                    qty=pending.qty,
                    entry_price=pending.limit_price,
                    reason="pending_timeout",
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata("execution_order", "reject", "pending_timeout", "pending_order_timeout"),
                        "signal": signal_snapshot(pending.signal),
                    },
                )
            ]
        if self._is_touched(pending.signal.direction, pending.limit_price, book):
            self.pending_order = None
            return [self._open_position(pending.signal, book, event_time, "touch_fill")]
        return []

    def _open_position(self, signal: Signal, book: OrderBook, event_time: datetime, reason: str) -> ExecutionEvent:
        trade_manager = MicroTradeManager(self.config.micro_engine, self.config.tick_size_for(signal.symbol))
        trade = trade_manager.open_from_signal(signal, event_time, qty=self.config.micro_engine.qty)
        self.trade_slots.append(_PaperMicroSlot(trade_manager, signal))
        return ExecutionEvent(
            "paper_entry",
            trade.symbol,
            trade.direction,
            qty=trade.qty,
            entry_price=trade.entry_price,
            reason=reason,
            timestamp=event_time,
            metadata={
                **event_trace_metadata("execution_order", "entry", reason, checks={"engine": signal.engine}),
                "fill_model": self.fill_model,
                "take_profit_price": trade.take_profit_price,
                "stop_loss_price": trade.stop_loss_price,
                "take_profit_ticks": trade.take_profit_ticks,
                "book_bid": book.best_bid_price,
                "book_ask": book.best_ask_price,
                "signal": signal_snapshot(signal),
            },
        )

    def _is_touched(self, direction: Direction, limit_price: float, book: OrderBook) -> bool:
        if direction == "long":
            return book.best_ask_price <= limit_price
        if direction == "short":
            return book.best_bid_price >= limit_price
        return False


class PaperMinuteExecutor:
    """Paper-only lifecycle manager for minute-level directional signals."""

    SUPPORTED_ENGINES = MinuteTradeManager.SUPPORTED_ENGINES

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or default_config()
        self.trade_slots: list[_PaperMinuteSlot] = []
        self.trades = 0
        self.pnl_ticks = 0.0
        self.pnl_yen = 0.0

    def on_signal(self, signal: Signal, book: OrderBook) -> list[ExecutionEvent]:
        if signal.engine not in self.SUPPORTED_ENGINES:
            return [_reject_signal(signal, book, "non_minute_signal")]
        if not signal.is_tradeable:
            return [_reject_signal(signal, book, "non_tradeable_signal")]
        if signal.price is None:
            return [_reject_signal(signal, book, "missing_signal_price")]
        if not self.config.is_trade_symbol(signal.symbol):
            return [_reject_signal(signal, book, "unsupported_symbol")]
        if self.position_count(signal.symbol) >= self.config.risk.max_positions_per_symbol:
            return [_reject_signal(signal, book, "max_positions_per_symbol")]
        return [self._open_position(signal, book_event_time(book), "minute_paper_fill")]

    def on_book(self, book: OrderBook) -> list[ExecutionEvent]:
        event_time = book_event_time(book)
        events: list[ExecutionEvent] = []
        active: list[_PaperMinuteSlot] = []
        for slot in self.trade_slots:
            trade = slot.trade_manager.trade
            if trade is None or book.symbol != trade.symbol:
                active.append(slot)
                continue
            decision = slot.trade_manager.evaluate_exit(book)
            if not decision.should_exit or decision.price is None:
                active.append(slot)
                continue
            closed = slot.trade_manager.mark_closed()
            if closed is None:
                continue
            pnl_ticks = slot.trade_manager.pnl_ticks(closed.direction, closed.entry_price, decision.price)
            pnl_yen = pnl_ticks * closed.qty * self.config.tick_value_yen_for(closed.symbol)
            self.trades += 1
            self.pnl_ticks += pnl_ticks
            self.pnl_yen += pnl_yen
            events.append(
                ExecutionEvent(
                    "paper_exit",
                    closed.symbol,
                    closed.direction,
                    qty=closed.qty,
                    entry_price=closed.entry_price,
                    exit_price=decision.price,
                    reason=decision.reason,
                    pnl_ticks=round(pnl_ticks, 4),
                    pnl_yen=round(pnl_yen, 2),
                    timestamp=event_time,
                    metadata={
                        **event_trace_metadata("position_lifecycle", "exit", decision.reason, checks={"engine": closed.engine}),
                        "engine": closed.engine,
                        "emergency": decision.emergency,
                        "hold_seconds": round((event_time - closed.entry_time).total_seconds(), 3),
                        "take_profit_price": closed.take_profit_price,
                        "stop_loss_price": closed.stop_loss_price,
                        "stop_distance_ticks": closed.stop_distance_ticks,
                        "take_profit_ticks": closed.take_profit_ticks,
                        "breakeven_armed": closed.breakeven_armed,
                        "signal": signal_snapshot(slot.entry_signal) if slot.entry_signal is not None else None,
                    },
                )
            )
        self.trade_slots = active
        return events

    def position_summary(self) -> dict[str, object] | None:
        summaries = self.position_summaries()
        return summaries[0] if summaries else None

    def position_summaries(self) -> list[dict[str, object]]:
        summaries: list[dict[str, object]] = []
        for idx, slot in enumerate(self.trade_slots):
            summary = slot.trade_manager.position_summary()
            if summary is None:
                continue
            summary = dict(summary)
            summary["position_id"] = f"minute-{idx + 1}"
            summaries.append(summary)
        return summaries

    def position_count(self, symbol: str | None = None) -> int:
        summaries = self.position_summaries()
        if symbol is None:
            return len(summaries)
        return sum(1 for summary in summaries if summary.get("symbol") == symbol)

    def stats(self) -> PaperExecutionStats:
        return PaperExecutionStats(self.trades, self.pnl_ticks, self.pnl_yen, 0)

    def _open_position(self, signal: Signal, event_time: datetime, reason: str) -> ExecutionEvent:
        trade_manager = MinuteTradeManager(
            self.config.minute_engine,
            self.config.micro_engine.qty,
            self.config.tick_size_for(signal.symbol),
        )
        trade = trade_manager.open_from_signal(signal, event_time, qty=self.config.micro_engine.qty)
        self.trade_slots.append(_PaperMinuteSlot(trade_manager, signal))
        return ExecutionEvent(
            "paper_entry",
            signal.symbol,
            signal.direction,
            qty=self.config.micro_engine.qty,
            entry_price=signal.price,
            reason=reason,
            timestamp=event_time,
            metadata={
                **event_trace_metadata("execution_order", "entry", reason, checks={"engine": signal.engine}),
                "engine": signal.engine,
                "take_profit_price": trade.take_profit_price,
                "stop_loss_price": trade.stop_loss_price,
                "stop_distance_ticks": trade.stop_distance_ticks,
                "take_profit_ticks": trade.take_profit_ticks,
                "take_profit_r": self.config.minute_engine.take_profit_r,
                "breakeven_after_r": self.config.minute_engine.breakeven_after_r,
                "signal": signal_snapshot(signal),
            },
        )


class PaperExecutionController:
    """Controller facade that keeps observe mode side-effect free."""

    def __init__(
        self,
        config: StrategyConfig | None = None,
        trade_mode: TradeMode = "observe",
        paper_fill_model: PaperFillModel = "immediate",
    ) -> None:
        self.config = config or default_config()
        self.trade_mode = trade_mode
        self.micro_executor = PaperMicroExecutor(self.config, paper_fill_model)
        self.minute_executor = PaperMinuteExecutor(self.config)

    def on_signal(self, signal: Signal, book: OrderBook) -> list[ExecutionEvent]:
        if self.trade_mode == "observe":
            if not signal.is_tradeable:
                return []
            return [
                ExecutionEvent(
                    "execution_skip",
                    signal.symbol,
                    signal.direction,
                    reason="observe_mode",
                    timestamp=book_event_time(book),
                    metadata={
                        **event_trace_metadata("execution_order", "reject", "observe_mode", "trade_mode"),
                        "signal": signal_snapshot(signal),
                    },
                )
            ]
        if signal.engine == "micro_book":
            if (
                self._position_count(signal.symbol) + self._pending_entry_count(signal.symbol)
                >= self.config.risk.max_positions_per_symbol
            ):
                return [_reject_signal(signal, book, "max_positions_per_symbol")]
            return self.micro_executor.on_signal(signal, book)
        if signal.engine in PaperMinuteExecutor.SUPPORTED_ENGINES:
            if self.micro_executor.pending_summary() is not None:
                return [_reject_signal(signal, book, "already_has_pending_order")]
            if self._position_count(signal.symbol) >= self.config.risk.max_positions_per_symbol:
                return [_reject_signal(signal, book, "max_positions_per_symbol")]
            return self.minute_executor.on_signal(signal, book)
        return [_reject_signal(signal, book, "unsupported_signal_engine")]

    def on_book(self, book: OrderBook, features: BookFeatures | None = None) -> list[ExecutionEvent]:
        if self.trade_mode == "observe":
            return []
        events = self.micro_executor.on_book(book, features)
        events.extend(self.minute_executor.on_book(book))
        return events

    def heartbeat_metadata(self) -> dict[str, object]:
        micro_stats = self.micro_executor.stats()
        minute_stats = self.minute_executor.stats()
        micro_position = self.micro_executor.position_summary()
        minute_position = self.minute_executor.position_summary()
        positions = self.micro_executor.position_summaries() + self.minute_executor.position_summaries()
        micro_pending = self.micro_executor.pending_summary()
        paper_position = micro_position or minute_position
        return {
            "paper_position": paper_position if self.trade_mode == "paper" else None,
            "paper_positions": positions if self.trade_mode == "paper" else [],
            "paper_position_count": len(positions) if self.trade_mode == "paper" else 0,
            "paper_pending_order": micro_pending if self.trade_mode == "paper" else None,
            "paper_trades": micro_stats.trades + minute_stats.trades,
            "paper_pnl_ticks": round(micro_stats.pnl_ticks + minute_stats.pnl_ticks, 4),
            "paper_pnl_yen": round(micro_stats.pnl_yen + minute_stats.pnl_yen, 2),
            "paper_pending_orders": micro_stats.pending_orders,
            "paper_micro_position": micro_position if self.trade_mode == "paper" else None,
            "paper_minute_position": minute_position if self.trade_mode == "paper" else None,
            "paper_micro_trades": micro_stats.trades,
            "paper_minute_trades": minute_stats.trades,
            "paper_micro_pnl_ticks": round(micro_stats.pnl_ticks, 4),
            "paper_minute_pnl_ticks": round(minute_stats.pnl_ticks, 4),
        }

    def _position_count(self, symbol: str | None = None) -> int:
        return self.micro_executor.position_count(symbol) + self.minute_executor.position_count(symbol)

    def _pending_entry_count(self, symbol: str | None = None) -> int:
        if symbol is None:
            return self.micro_executor.pending_count()
        pending = self.micro_executor.pending_summary()
        return 1 if pending is not None and pending.get("symbol") == symbol else 0


def _pending_timeout(max_hold_seconds: int) -> timedelta:
    return timedelta(seconds=max(1, min(5, max_hold_seconds)))


def _reject_signal(signal: Signal, book: OrderBook, reason: str) -> ExecutionEvent:
    return ExecutionEvent(
        "execution_reject",
        signal.symbol,
        signal.direction,
        reason=reason,
        timestamp=book_event_time(book),
        metadata={
            **event_trace_metadata("execution_order", "reject", reason, "execution_controller"),
            "signal": signal_snapshot(signal),
        },
    )
