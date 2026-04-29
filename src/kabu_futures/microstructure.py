from __future__ import annotations

from bisect import bisect_left, insort
from collections import deque
from datetime import datetime, timedelta, timezone
from math import exp

from .config import MicroEngineConfig, effective_micro_engine_config
from .models import BookFeatures, Level, OrderBook, TradeTick


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


class RollingPercentile:
    """Maintain a rolling percentile without sorting the whole window each tick."""

    def __init__(self, maxlen: int) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen must be positive")
        self.maxlen = maxlen
        self.values: deque[float] = deque()
        self.sorted_values: list[float] = []

    def update(self, value: float) -> float:
        if len(self.values) >= self.maxlen:
            old = self.values.popleft()
            idx = bisect_left(self.sorted_values, old)
            if idx < len(self.sorted_values) and self.sorted_values[idx] == old:
                self.sorted_values.pop(idx)
            else:
                self.sorted_values.remove(old)
        self.values.append(value)
        insort(self.sorted_values, value)
        return value

    def percentile(self, pct: float) -> float:
        if not self.sorted_values:
            return 0.0
        idx = min(
            len(self.sorted_values) - 1,
            max(0, int(round((pct / 100.0) * (len(self.sorted_values) - 1)))),
        )
        return self.sorted_values[idx]

    def __len__(self) -> int:
        return len(self.values)


def weighted_depth(levels: tuple[Level, ...], depth_levels: int) -> float:
    depth = 0.0
    for idx, level in enumerate(levels[:depth_levels], start=1):
        depth += level.qty / idx
    return depth


def weighted_imbalance(book: OrderBook, depth_levels: int) -> tuple[float, float]:
    buy_depth = weighted_depth(book.buy_levels or (Level(book.best_bid_price, book.best_bid_qty),), depth_levels)
    sell_depth = weighted_depth(book.sell_levels or (Level(book.best_ask_price, book.best_ask_qty),), depth_levels)
    total = buy_depth + sell_depth
    if total <= 0:
        return 0.0, 0.0
    return (buy_depth - sell_depth) / total, total


def microprice(book: OrderBook) -> float:
    denom = book.best_bid_qty + book.best_ask_qty
    if denom <= 0:
        return book.mid_price
    return (book.best_ask_price * book.best_bid_qty + book.best_bid_price * book.best_ask_qty) / denom


def order_flow_imbalance(previous: OrderBook | None, current: OrderBook) -> float:
    if previous is None:
        return 0.0
    if current.best_bid_price > previous.best_bid_price:
        bid = current.best_bid_qty
    elif current.best_bid_price == previous.best_bid_price:
        bid = current.best_bid_qty - previous.best_bid_qty
    else:
        bid = -previous.best_bid_qty

    if current.best_ask_price < previous.best_ask_price:
        ask = -current.best_ask_qty
    elif current.best_ask_price == previous.best_ask_price:
        ask = -(current.best_ask_qty - previous.best_ask_qty)
    else:
        ask = previous.best_ask_qty
    return bid + ask


class BookFeatureEngine:
    def __init__(self, config: MicroEngineConfig, tick_size: float = 5.0) -> None:
        self.config = effective_micro_engine_config(config)
        self.tick_size = tick_size
        self.previous: OrderBook | None = None
        self.ofi_ewma = 0.0
        self.abs_ofi_percentile = RollingPercentile(maxlen=2000)
        self.abs_ofi_window = self.abs_ofi_percentile.values

    def update(self, book: OrderBook, now: datetime | None = None) -> BookFeatures:
        book.validate()
        now = now or datetime.now(timezone.utc)
        imbalance, total_depth = weighted_imbalance(book, self.config.depth_levels)
        ofi = order_flow_imbalance(self.previous, book)
        event_gap_ms = self._event_gap_ms(book)
        latency_ms = self._latency_ms(book, now)
        self.ofi_ewma = self._update_ewma(ofi, event_gap_ms)
        self.abs_ofi_percentile.update(abs(ofi))
        ofi_threshold = self.abs_ofi_percentile.percentile(self.config.ofi_percentile)
        mp = microprice(book)
        edge_ticks = (mp - book.mid_price) / self.tick_size
        spread_ticks = book.spread / self.tick_size
        jump_reason = self._jump_reason(book, spread_ticks, event_gap_ms, latency_ms)
        jump_detected = jump_reason is not None
        self.previous = book
        return BookFeatures(
            timestamp=book.timestamp,
            symbol=book.symbol,
            spread_ticks=spread_ticks,
            imbalance=imbalance,
            ofi=ofi,
            ofi_ewma=self.ofi_ewma,
            ofi_threshold=ofi_threshold,
            microprice=mp,
            microprice_edge_ticks=edge_ticks,
            total_depth=total_depth,
            jump_detected=jump_detected,
            jump_reason=jump_reason,
            event_gap_ms=event_gap_ms,
            latency_ms=latency_ms,
        )

    def _update_ewma(self, ofi: float, event_gap_ms: float) -> float:
        half_life_ms = 2000.0
        dt_ms = max(1.0, event_gap_ms or 1.0)
        alpha = 1.0 - exp(-dt_ms / half_life_ms)
        self.ofi_ewma = alpha * ofi + (1.0 - alpha) * self.ofi_ewma
        return self.ofi_ewma

    def _event_gap_ms(self, book: OrderBook) -> float:
        if self.previous is None:
            return 0.0
        current_time = book.received_at or book.timestamp
        previous_time = self.previous.received_at or self.previous.timestamp
        return max(0.0, (current_time - previous_time).total_seconds() * 1000.0)

    def _latency_ms(self, book: OrderBook, now: datetime) -> float:
        if book.received_at is not None:
            # kabu board timestamps are usually last trade timestamps, not quote
            # event timestamps. Treat local receive time as the live event clock.
            return 0.0
        return max(0.0, (now - book.timestamp).total_seconds() * 1000.0)

    def _jump_reason(self, book: OrderBook, spread_ticks: float, event_gap_ms: float, latency_ms: float) -> str | None:
        if spread_ticks > 2.0:
            return "spread_wide"
        if latency_ms > self.config.websocket_latency_stop_ms:
            return "latency_high"
        if book.received_at is None and event_gap_ms > self.config.websocket_latency_stop_ms:
            return "event_gap_high"
        if self.previous is None:
            return None
        mid_move_ticks = abs(book.mid_price - self.previous.mid_price) / self.tick_size
        if mid_move_ticks > 3.0:
            return "mid_move_jump"
        return None


class TapeFeatureEngine:
    def __init__(self, window_seconds: float = 3.0) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window = timedelta(seconds=window_seconds)
        self.trades: deque[TradeTick] = deque()

    def update(self, trade: TradeTick) -> float:
        self.trades.append(trade)
        cutoff = trade.timestamp - self.window
        while self.trades and self.trades[0].timestamp < cutoff:
            self.trades.popleft()
        return self.imbalance()

    def imbalance(self) -> float:
        buy_qty = sum(t.qty for t in self.trades if t.side == "buy")
        sell_qty = sum(t.qty for t in self.trades if t.side == "sell")
        total = buy_qty + sell_qty
        if total <= 0:
            return 0.0
        return (buy_qty - sell_qty) / total
