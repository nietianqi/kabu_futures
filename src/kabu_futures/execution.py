from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .config import MicroEngineConfig, MinuteEngineConfig
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
    stop_loss_price: float | None
    take_profit_ticks: int
    engine: str = "micro_book"

    @property
    def is_long(self) -> bool:
        return self.direction == "long"


@dataclass
class MinuteTradeState:
    symbol: str
    direction: Direction
    qty: int
    engine: str
    entry_price: float
    entry_time: datetime
    take_profit_price: float
    stop_loss_price: float | None
    stop_distance_ticks: float
    take_profit_ticks: int
    breakeven_armed: bool = False

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
        take_profit_ticks = take_profit_ticks_from_signal(signal, signal.price, self.tick_size)
        take_profit_distance = take_profit_ticks * self.tick_size
        if signal.direction == "long":
            tp = signal.price + take_profit_distance
        else:
            tp = signal.price - take_profit_distance
        self.trade = MicroTradeState(signal.symbol, signal.direction, qty, signal.price, timestamp, tp, None, take_profit_ticks)
        return self.trade

    def evaluate_exit(self, book: OrderBook, features: BookFeatures | None = None) -> ExitDecision:
        if self.trade is None:
            return ExitDecision(False, "no_trade")
        trade = self.trade
        exit_price = book.best_bid_price if trade.is_long else book.best_ask_price

        if trade.is_long and exit_price >= trade.take_profit_price:
            return ExitDecision(True, "take_profit", trade.take_profit_price)
        if not trade.is_long and exit_price <= trade.take_profit_price:
            return ExitDecision(True, "take_profit", trade.take_profit_price)
        return ExitDecision(False, "hold")

    def build_exit_order(
        self,
        decision: ExitDecision,
        book: OrderBook,
        exchange: int,
        hold_id: str | None = None,
        symbol: str | None = None,
    ) -> OrderIntent | None:
        if self.trade is None or not decision.should_exit:
            return None
        return _build_close_order(
            self.orders,
            symbol or self.trade.symbol,
            exchange,
            self.trade.direction,
            self.trade.qty,
            decision,
            decision.price if decision.price is not None else self._aggressive_exit_price(book),
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
        return pnl_ticks(self.trade.direction, self.trade.entry_price, exit_price, self.tick_size)

    def _aggressive_exit_price(self, book: OrderBook) -> float:
        if self.trade is None:
            return book.mid_price
        return aggressive_exit_price(self.trade.direction, book, self.tick_size)


class MinuteTradeManager:
    SUPPORTED_ENGINES = frozenset(("minute_orb", "minute_vwap", "directional_intraday"))

    def __init__(self, config: MinuteEngineConfig, default_qty: int = 1, tick_size: float = 5.0) -> None:
        self.config = config
        self.default_qty = default_qty
        self.tick_size = tick_size
        self.trade: MinuteTradeState | None = None
        self.orders = KabuFutureOrderBuilder()

    def open_from_signal(self, signal: Signal, timestamp: datetime, qty: int | None = None) -> MinuteTradeState:
        if signal.engine not in self.SUPPORTED_ENGINES:
            raise ValueError(f"Minute trade cannot open from {signal.engine} signals")
        if signal.direction not in ("long", "short"):
            raise ValueError("Minute trade can only open from long/short signals")
        if signal.price is None:
            raise ValueError("Minute signal must include an entry price")
        trade_qty = qty or self.default_qty
        if trade_qty <= 0:
            raise ValueError("Minute trade quantity must be positive")
        take_profit_ticks = take_profit_ticks_from_signal(signal, signal.price, self.tick_size)
        take_profit_distance = take_profit_ticks * self.tick_size
        if signal.direction == "long":
            take_profit_price = signal.price + take_profit_distance
        else:
            take_profit_price = signal.price - take_profit_distance
        self.trade = MinuteTradeState(
            signal.symbol,
            signal.direction,
            trade_qty,
            signal.engine,
            signal.price,
            timestamp,
            take_profit_price,
            None,
            0.0,
            take_profit_ticks,
        )
        return self.trade

    def evaluate_exit(self, book: OrderBook) -> ExitDecision:
        if self.trade is None:
            return ExitDecision(False, "no_trade")
        trade = self.trade
        if book.symbol != trade.symbol:
            return ExitDecision(False, "different_symbol")
        exit_price = book.best_bid_price if trade.is_long else book.best_ask_price
        if trade.is_long and exit_price >= trade.take_profit_price:
            return ExitDecision(True, "take_profit", trade.take_profit_price)
        if not trade.is_long and exit_price <= trade.take_profit_price:
            return ExitDecision(True, "take_profit", trade.take_profit_price)
        return ExitDecision(False, "hold")

    def build_exit_order(
        self,
        decision: ExitDecision,
        book: OrderBook,
        exchange: int,
        hold_id: str | None = None,
        symbol: str | None = None,
    ) -> OrderIntent | None:
        if self.trade is None or not decision.should_exit:
            return None
        return _build_close_order(
            self.orders,
            symbol or self.trade.symbol,
            exchange,
            self.trade.direction,
            self.trade.qty,
            decision,
            decision.price if decision.price is not None else self._aggressive_exit_price(book),
            hold_id=hold_id,
        )

    def mark_closed(self) -> MinuteTradeState | None:
        closed = self.trade
        self.trade = None
        return closed

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
            "take_profit_ticks": trade.take_profit_ticks,
            "breakeven_armed": trade.breakeven_armed,
        }

    def pnl_ticks(self, direction: Direction, entry_price: float, exit_price: float) -> float:
        return pnl_ticks(direction, entry_price, exit_price, self.tick_size)

    def _stop_distance_ticks(self, signal: Signal) -> float:
        atr = signal.metadata.get("atr")
        if isinstance(atr, (int, float)) and atr > 0:
            raw_ticks = (float(atr) * self.config.stop_atr_mult) / self.tick_size
        else:
            raw_ticks = float(self.config.stop_ticks_min)
        return max(
            float(self.config.stop_ticks_min),
            min(float(self.config.stop_ticks_max), raw_ticks),
        )

    def _maybe_arm_breakeven(self, exit_price: float) -> None:
        trade = self.trade
        if trade is None or trade.breakeven_armed or trade.stop_distance_ticks <= 0:
            return
        trigger_ticks = trade.stop_distance_ticks * self.config.breakeven_after_r
        if self.pnl_ticks(trade.direction, trade.entry_price, exit_price) < trigger_ticks:
            return
        trade.breakeven_armed = True
        trade.stop_loss_price = trade.entry_price

    def _aggressive_exit_price(self, book: OrderBook) -> float:
        if self.trade is None:
            return book.mid_price
        return aggressive_exit_price(self.trade.direction, book, self.tick_size)


def pnl_ticks(direction: Direction, entry_price: float, exit_price: float, tick_size: float) -> float:
    if direction == "long":
        return (exit_price - entry_price) / tick_size
    if direction == "short":
        return (entry_price - exit_price) / tick_size
    return 0.0


def aggressive_exit_price(direction: Direction, book: OrderBook, tick_size: float) -> float:
    if direction == "long":
        return book.best_bid_price - tick_size
    return book.best_ask_price + tick_size


def take_profit_ticks_from_signal(signal: Signal, entry_price: float, tick_size: float) -> int:
    """Use a wider profit target only when actual fill is worse than signal price."""
    signal_price = _float_metadata(signal.metadata.get("live_entry_signal_price"))
    if signal_price is None:
        signal_price = _float_metadata(signal.metadata.get("entry_signal_price"))
    if signal_price is None:
        signal_price = signal.price
    if signal_price is None or tick_size <= 0:
        return 1
    adverse_slippage = entry_price - signal_price if signal.direction == "long" else signal_price - entry_price
    return 2 if adverse_slippage > 1e-9 else 1


def _float_metadata(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _build_close_order(
    orders: KabuFutureOrderBuilder,
    symbol: str,
    exchange: int,
    direction: Direction,
    qty: int,
    decision: ExitDecision,
    close_price: float,
    hold_id: str | None = None,
) -> OrderIntent:
    if decision.emergency:
        return orders.close_market(symbol, exchange, direction, qty, hold_id=hold_id)
    return orders.close_aggressive_limit(symbol, exchange, direction, qty, close_price, hold_id=hold_id)
