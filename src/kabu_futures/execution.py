from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from .config import MicroEngineConfig
from .models import BookFeatures, Direction, OrderBook, OrderIntent, Signal
from .orders import KabuFutureOrderBuilder


@dataclass(frozen=True)
class MicroTradeState:
    symbol: str
    direction: Direction
    qty: int
    entry_price: float
    entry_time: datetime
    take_profit_price: float
    stop_loss_price: float
    engine: str = "micro_book"

    @property
    def is_long(self) -> bool:
        return self.direction == "long"


@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: str
    price: float | None = None
    emergency: bool = False


class MicroTradeManager:
    def __init__(self, config: MicroEngineConfig, tick_size: float = 5.0) -> None:
        self.config = config
        self.tick_size = tick_size
        self.trade: MicroTradeState | None = None
        self.orders = KabuFutureOrderBuilder()

    def open_from_signal(self, signal: Signal, timestamp: datetime, qty: int | None = None) -> MicroTradeState:
        if signal.direction not in ("long", "short"):
            raise ValueError("Micro trade can only open from long/short signals")
        if signal.price is None:
            raise ValueError("Micro signal must include an entry price")
        qty = qty or self.config.qty
        if signal.direction == "long":
            tp = signal.price + self.config.take_profit_ticks * self.tick_size
            stop = signal.price - self.config.stop_loss_ticks * self.tick_size
        else:
            tp = signal.price - self.config.take_profit_ticks * self.tick_size
            stop = signal.price + self.config.stop_loss_ticks * self.tick_size
        self.trade = MicroTradeState(signal.symbol, signal.direction, qty, signal.price, timestamp, tp, stop)
        return self.trade

    def evaluate_exit(self, book: OrderBook, features: BookFeatures | None = None) -> ExitDecision:
        if self.trade is None:
            return ExitDecision(False, "no_trade")
        trade = self.trade
        exit_price = book.best_bid_price if trade.is_long else book.best_ask_price
        event_time = book.received_at or book.timestamp
        age_seconds = (event_time - trade.entry_time).total_seconds()
        unrealized_ticks = self._unrealized_ticks(exit_price)

        if trade.is_long and exit_price >= trade.take_profit_price:
            return ExitDecision(True, "take_profit", exit_price)
        if not trade.is_long and exit_price <= trade.take_profit_price:
            return ExitDecision(True, "take_profit", exit_price)
        if trade.is_long and exit_price <= trade.stop_loss_price:
            return ExitDecision(True, "stop_loss", exit_price, emergency=True)
        if not trade.is_long and exit_price >= trade.stop_loss_price:
            return ExitDecision(True, "stop_loss", exit_price, emergency=True)
        if age_seconds >= self.config.max_hold_seconds:
            return ExitDecision(True, "max_hold_seconds", exit_price)
        if age_seconds >= self.config.time_stop_seconds and unrealized_ticks < 1.0:
            return ExitDecision(True, "time_stop", exit_price)
        if features is not None:
            feature_exit = self._feature_exit(features, exit_price)
            if feature_exit.should_exit:
                return feature_exit
        return ExitDecision(False, "hold")

    def build_exit_order(
        self,
        decision: ExitDecision,
        book: OrderBook,
        exchange: int,
        hold_id: str | None = None,
    ) -> OrderIntent | None:
        if self.trade is None or not decision.should_exit:
            return None
        if decision.emergency:
            return self.orders.close_market(self.trade.symbol, exchange, self.trade.direction, self.trade.qty, hold_id=hold_id)
        price = self._aggressive_exit_price(book)
        return self.orders.close_aggressive_limit(
            self.trade.symbol,
            exchange,
            self.trade.direction,
            self.trade.qty,
            price,
            hold_id=hold_id,
        )

    def mark_closed(self) -> MicroTradeState | None:
        closed = self.trade
        self.trade = None
        return closed

    def _feature_exit(self, features: BookFeatures, exit_price: float) -> ExitDecision:
        if features.jump_detected or features.latency_ms > self.config.websocket_latency_stop_ms:
            return ExitDecision(True, "latency_or_jump", exit_price, emergency=True)
        if features.spread_ticks > 2.0:
            return ExitDecision(True, "spread_widened", exit_price, emergency=True)
        if abs(features.imbalance) < self.config.imbalance_exit:
            return ExitDecision(True, "imbalance_neutral", exit_price)
        if self.trade is None:
            return ExitDecision(False, "no_trade")
        if self.trade.is_long:
            if features.ofi_ewma < -max(0.0, features.ofi_threshold):
                return ExitDecision(True, "ofi_reversal", exit_price)
            if features.microprice_edge_ticks <= 0:
                return ExitDecision(True, "microprice_neutral_or_reverse", exit_price)
        else:
            if features.ofi_ewma > max(0.0, features.ofi_threshold):
                return ExitDecision(True, "ofi_reversal", exit_price)
            if features.microprice_edge_ticks >= 0:
                return ExitDecision(True, "microprice_neutral_or_reverse", exit_price)
        return ExitDecision(False, "hold")

    def _unrealized_ticks(self, exit_price: float) -> float:
        if self.trade is None:
            return 0.0
        if self.trade.is_long:
            return (exit_price - self.trade.entry_price) / self.tick_size
        return (self.trade.entry_price - exit_price) / self.tick_size

    def _aggressive_exit_price(self, book: OrderBook) -> float:
        if self.trade is None:
            return book.mid_price
        if self.trade.is_long:
            return book.best_bid_price - self.tick_size
        return book.best_ask_price + self.tick_size
