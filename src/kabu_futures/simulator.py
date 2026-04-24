from __future__ import annotations

from dataclasses import dataclass

from .config import StrategyConfig, default_config
from .engine import DualStrategyEngine
from .execution import MicroTradeManager
from .models import OrderBook, Signal
from .microstructure import BookFeatureEngine


@dataclass(frozen=True)
class SimulatedTrade:
    entry_signal: Signal
    exit_reason: str
    entry_price: float
    exit_price: float
    pnl_ticks: float


class MicroReplaySimulator:
    """Simple book-path simulator for validating the micro strategy offline."""

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or default_config()
        self.engine = DualStrategyEngine(self.config)
        self.features = BookFeatureEngine(self.config.micro_engine, self.config.tick_size)
        self.trade_manager = MicroTradeManager(self.config.micro_engine, self.config.tick_size)
        self.trades: list[SimulatedTrade] = []
        self._entry_signal: Signal | None = None

    def on_book(self, book: OrderBook) -> list[Signal]:
        emitted = self.engine.on_order_book(book)
        features = self.features.update(book)
        if self.trade_manager.trade is not None:
            decision = self.trade_manager.evaluate_exit(book, features)
            if decision.should_exit and decision.price is not None and self._entry_signal is not None:
                trade = self.trade_manager.mark_closed()
                if trade is not None:
                    if trade.direction == "long":
                        pnl_ticks = (decision.price - trade.entry_price) / self.config.tick_size
                    else:
                        pnl_ticks = (trade.entry_price - decision.price) / self.config.tick_size
                    self.trades.append(
                        SimulatedTrade(
                            entry_signal=self._entry_signal,
                            exit_reason=decision.reason,
                            entry_price=trade.entry_price,
                            exit_price=decision.price,
                            pnl_ticks=pnl_ticks,
                        )
                    )
                    self._entry_signal = None
            return emitted
        for signal in emitted:
            if signal.engine == "micro_book" and signal.is_tradeable:
                self.trade_manager.open_from_signal(signal, book.timestamp)
                self._entry_signal = signal
                break
        return emitted
