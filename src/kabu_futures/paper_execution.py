from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from .config import StrategyConfig, default_config
from .execution import MicroTradeManager
from .models import BookFeatures, Direction, OrderBook, Signal


TradeMode = Literal["observe", "paper"]
PaperFillModel = Literal["immediate", "touch"]
ExecutionEventType = Literal["paper_entry", "paper_exit", "paper_pending", "paper_cancel", "execution_reject", "execution_skip"]


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
class MinutePaperTradeState:
    symbol: str
    direction: Direction
    qty: int
    engine: str
    entry_price: float
    entry_time: datetime
    take_profit_price: float
    stop_loss_price: float
    stop_distance_ticks: float
    breakeven_armed: bool = False

    @property
    def is_long(self) -> bool:
        return self.direction == "long"


class PaperMicroExecutor:
    """Paper-only lifecycle manager for microstructure scalp signals."""

    def __init__(self, config: StrategyConfig | None = None, fill_model: PaperFillModel = "immediate") -> None:
        self.config = config or default_config()
        self.fill_model = fill_model
        self.trade_manager = MicroTradeManager(self.config.micro_engine, self.config.tick_size)
        self.entry_signal: Signal | None = None
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
        if signal.symbol != self.config.symbols.primary:
            return [self._reject(signal, book, "unsupported_symbol")]
        if self.trade_manager.trade is not None:
            return [self._reject(signal, book, "already_has_position")]
        if self.pending_order is not None:
            return [self._reject(signal, book, "already_has_pending_order")]

        event_time = _event_time(book)
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
                        "fill_model": self.fill_model,
                        "expires_at": self.pending_order.expires_at.isoformat(),
                        "signal": _signal_snapshot(signal),
                    },
                )
            ]
        return [self._open_position(signal, book, event_time, "immediate_fill")]

    def on_book(self, book: OrderBook, features: BookFeatures | None = None) -> list[ExecutionEvent]:
        pending_events = self._evaluate_pending_order(book)
        if pending_events:
            return pending_events
        trade = self.trade_manager.trade
        if trade is None or book.symbol != trade.symbol:
            return []
        decision = self.trade_manager.evaluate_exit(book, features)
        if not decision.should_exit or decision.price is None:
            return []
        closed = self.trade_manager.mark_closed()
        if closed is None:
            return []

        pnl_ticks = self._pnl_ticks(closed.direction, closed.entry_price, decision.price)
        pnl_yen = pnl_ticks * closed.qty * self.config.micro225_tick_value
        self.trades += 1
        self.pnl_ticks += pnl_ticks
        self.pnl_yen += pnl_yen
        entry_signal = self.entry_signal
        self.entry_signal = None
        return [
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
                timestamp=_event_time(book),
                metadata={
                    "emergency": decision.emergency,
                    "hold_seconds": round((_event_time(book) - closed.entry_time).total_seconds(), 3),
                    "take_profit_price": closed.take_profit_price,
                    "stop_loss_price": closed.stop_loss_price,
                    "signal": _signal_snapshot(entry_signal) if entry_signal is not None else None,
                },
            )
        ]

    def position_summary(self) -> dict[str, object] | None:
        trade = self.trade_manager.trade
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
            timestamp=_event_time(book),
            metadata={"signal": _signal_snapshot(signal)},
        )

    def _pnl_ticks(self, direction: Direction, entry_price: float, exit_price: float) -> float:
        if direction == "long":
            return (exit_price - entry_price) / self.config.tick_size
        if direction == "short":
            return (entry_price - exit_price) / self.config.tick_size
        return 0.0

    def _evaluate_pending_order(self, book: OrderBook) -> list[ExecutionEvent]:
        pending = self.pending_order
        if pending is None or pending.signal.symbol != book.symbol:
            return []
        event_time = _event_time(book)
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
                    metadata={"signal": _signal_snapshot(pending.signal)},
                )
            ]
        if self._is_touched(pending.signal.direction, pending.limit_price, book):
            self.pending_order = None
            return [self._open_position(pending.signal, book, event_time, "touch_fill")]
        return []

    def _open_position(self, signal: Signal, book: OrderBook, event_time: datetime, reason: str) -> ExecutionEvent:
        trade = self.trade_manager.open_from_signal(signal, event_time, qty=self.config.micro_engine.qty)
        self.entry_signal = signal
        return ExecutionEvent(
            "paper_entry",
            trade.symbol,
            trade.direction,
            qty=trade.qty,
            entry_price=trade.entry_price,
            reason=reason,
            timestamp=event_time,
            metadata={
                "fill_model": self.fill_model,
                "take_profit_price": trade.take_profit_price,
                "stop_loss_price": trade.stop_loss_price,
                "book_bid": book.best_bid_price,
                "book_ask": book.best_ask_price,
                "signal": _signal_snapshot(signal),
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

    SUPPORTED_ENGINES = frozenset(("minute_orb", "minute_vwap", "directional_intraday"))

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or default_config()
        self.trade: MinutePaperTradeState | None = None
        self.entry_signal: Signal | None = None
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
        if signal.symbol != self.config.symbols.primary:
            return [_reject_signal(signal, book, "unsupported_symbol")]
        if self.trade is not None:
            return [_reject_signal(signal, book, "already_has_position")]
        return [self._open_position(signal, _event_time(book), "minute_paper_fill")]

    def on_book(self, book: OrderBook) -> list[ExecutionEvent]:
        trade = self.trade
        if trade is None or book.symbol != trade.symbol:
            return []
        event_time = _event_time(book)
        exit_price = book.best_bid_price if trade.is_long else book.best_ask_price
        self._maybe_arm_breakeven(exit_price)
        reason = ""
        emergency = False
        if trade.is_long and exit_price >= trade.take_profit_price:
            reason = "take_profit"
        elif not trade.is_long and exit_price <= trade.take_profit_price:
            reason = "take_profit"
        elif trade.is_long and exit_price <= trade.stop_loss_price:
            reason = "breakeven_stop" if trade.breakeven_armed else "stop_loss"
            emergency = not trade.breakeven_armed
        elif not trade.is_long and exit_price >= trade.stop_loss_price:
            reason = "breakeven_stop" if trade.breakeven_armed else "stop_loss"
            emergency = not trade.breakeven_armed
        elif (event_time - trade.entry_time).total_seconds() >= self.config.minute_engine.max_hold_minutes * 60:
            reason = "max_hold_minutes"
        if not reason:
            return []

        closed = trade
        self.trade = None
        pnl_ticks = self._pnl_ticks(closed.direction, closed.entry_price, exit_price)
        pnl_yen = pnl_ticks * closed.qty * self.config.micro225_tick_value
        self.trades += 1
        self.pnl_ticks += pnl_ticks
        self.pnl_yen += pnl_yen
        entry_signal = self.entry_signal
        self.entry_signal = None
        return [
            ExecutionEvent(
                "paper_exit",
                closed.symbol,
                closed.direction,
                qty=closed.qty,
                entry_price=closed.entry_price,
                exit_price=exit_price,
                reason=reason,
                pnl_ticks=round(pnl_ticks, 4),
                pnl_yen=round(pnl_yen, 2),
                timestamp=event_time,
                metadata={
                    "engine": closed.engine,
                    "emergency": emergency,
                    "hold_seconds": round((event_time - closed.entry_time).total_seconds(), 3),
                    "take_profit_price": closed.take_profit_price,
                    "stop_loss_price": closed.stop_loss_price,
                    "stop_distance_ticks": closed.stop_distance_ticks,
                    "breakeven_armed": closed.breakeven_armed,
                    "signal": _signal_snapshot(entry_signal) if entry_signal is not None else None,
                },
            )
        ]

    def position_summary(self) -> dict[str, object] | None:
        trade = self.trade
        if trade is None:
            return None
        return {
            "symbol": trade.symbol,
            "direction": trade.direction,
            "qty": trade.qty,
            "engine": trade.engine,
            "entry_price": trade.entry_price,
            "entry_time": trade.entry_time.isoformat(),
            "take_profit_price": trade.take_profit_price,
            "stop_loss_price": trade.stop_loss_price,
            "stop_distance_ticks": trade.stop_distance_ticks,
            "breakeven_armed": trade.breakeven_armed,
        }

    def stats(self) -> PaperExecutionStats:
        return PaperExecutionStats(self.trades, self.pnl_ticks, self.pnl_yen, 0)

    def _open_position(self, signal: Signal, event_time: datetime, reason: str) -> ExecutionEvent:
        stop_distance_ticks = self._stop_distance_ticks(signal)
        stop_distance = stop_distance_ticks * self.config.tick_size
        take_profit_distance = stop_distance * self.config.minute_engine.take_profit_r
        assert signal.price is not None
        if signal.direction == "long":
            take_profit_price = signal.price + take_profit_distance
            stop_loss_price = signal.price - stop_distance
        else:
            take_profit_price = signal.price - take_profit_distance
            stop_loss_price = signal.price + stop_distance
        self.trade = MinutePaperTradeState(
            signal.symbol,
            signal.direction,
            self.config.micro_engine.qty,
            signal.engine,
            signal.price,
            event_time,
            take_profit_price,
            stop_loss_price,
            stop_distance_ticks,
        )
        self.entry_signal = signal
        return ExecutionEvent(
            "paper_entry",
            signal.symbol,
            signal.direction,
            qty=self.config.micro_engine.qty,
            entry_price=signal.price,
            reason=reason,
            timestamp=event_time,
            metadata={
                "engine": signal.engine,
                "take_profit_price": take_profit_price,
                "stop_loss_price": stop_loss_price,
                "stop_distance_ticks": stop_distance_ticks,
                "take_profit_r": self.config.minute_engine.take_profit_r,
                "breakeven_after_r": self.config.minute_engine.breakeven_after_r,
                "signal": _signal_snapshot(signal),
            },
        )

    def _stop_distance_ticks(self, signal: Signal) -> float:
        atr = signal.metadata.get("atr")
        if isinstance(atr, (int, float)) and atr > 0:
            raw_ticks = (float(atr) * self.config.minute_engine.stop_atr_mult) / self.config.tick_size
        else:
            raw_ticks = float(self.config.minute_engine.stop_ticks_min)
        return max(
            float(self.config.minute_engine.stop_ticks_min),
            min(float(self.config.minute_engine.stop_ticks_max), raw_ticks),
        )

    def _maybe_arm_breakeven(self, exit_price: float) -> None:
        trade = self.trade
        if trade is None or trade.breakeven_armed or trade.stop_distance_ticks <= 0:
            return
        trigger_ticks = trade.stop_distance_ticks * self.config.minute_engine.breakeven_after_r
        if self._pnl_ticks(trade.direction, trade.entry_price, exit_price) < trigger_ticks:
            return
        trade.breakeven_armed = True
        trade.stop_loss_price = trade.entry_price

    def _pnl_ticks(self, direction: Direction, entry_price: float, exit_price: float) -> float:
        if direction == "long":
            return (exit_price - entry_price) / self.config.tick_size
        if direction == "short":
            return (entry_price - exit_price) / self.config.tick_size
        return 0.0


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
                    timestamp=_event_time(book),
                    metadata={"signal": _signal_snapshot(signal)},
                )
            ]
        if signal.engine == "micro_book":
            if self.minute_executor.position_summary() is not None:
                return [_reject_signal(signal, book, "already_has_position")]
            return self.micro_executor.on_signal(signal, book)
        if signal.engine in PaperMinuteExecutor.SUPPORTED_ENGINES:
            if self.micro_executor.position_summary() is not None:
                return [_reject_signal(signal, book, "already_has_position")]
            if self.micro_executor.pending_summary() is not None:
                return [_reject_signal(signal, book, "already_has_pending_order")]
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
        paper_position = self.micro_executor.position_summary() or self.minute_executor.position_summary()
        return {
            "paper_position": paper_position if self.trade_mode == "paper" else None,
            "paper_pending_order": self.micro_executor.pending_summary() if self.trade_mode == "paper" else None,
            "paper_trades": micro_stats.trades + minute_stats.trades,
            "paper_pnl_ticks": round(micro_stats.pnl_ticks + minute_stats.pnl_ticks, 4),
            "paper_pnl_yen": round(micro_stats.pnl_yen + minute_stats.pnl_yen, 2),
            "paper_pending_orders": micro_stats.pending_orders,
            "paper_micro_position": self.micro_executor.position_summary() if self.trade_mode == "paper" else None,
            "paper_minute_position": self.minute_executor.position_summary() if self.trade_mode == "paper" else None,
            "paper_micro_trades": micro_stats.trades,
            "paper_minute_trades": minute_stats.trades,
            "paper_micro_pnl_ticks": round(micro_stats.pnl_ticks, 4),
            "paper_minute_pnl_ticks": round(minute_stats.pnl_ticks, 4),
        }


def _event_time(book: OrderBook) -> datetime:
    return book.received_at or book.timestamp


def _pending_timeout(max_hold_seconds: int) -> timedelta:
    return timedelta(seconds=max(1, min(5, max_hold_seconds)))


def _reject_signal(signal: Signal, book: OrderBook, reason: str) -> ExecutionEvent:
    return ExecutionEvent(
        "execution_reject",
        signal.symbol,
        signal.direction,
        reason=reason,
        timestamp=_event_time(book),
        metadata={"signal": _signal_snapshot(signal)},
    )


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
