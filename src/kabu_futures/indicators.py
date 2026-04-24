from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import floor

from .models import Bar


class EMA:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.value: float | None = None
        self.previous: float | None = None

    def update(self, price: float) -> float:
        self.previous = self.value
        if self.value is None:
            self.value = price
        else:
            self.value = self.alpha * price + (1.0 - self.alpha) * self.value
        return self.value

    @property
    def slope(self) -> float:
        if self.value is None or self.previous is None:
            return 0.0
        return self.value - self.previous


class RollingATR:
    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self.period = period
        self.true_ranges: deque[float] = deque(maxlen=period)
        self.previous_close: float | None = None

    def update(self, bar: Bar) -> float | None:
        if self.previous_close is None:
            true_range = bar.high - bar.low
        else:
            true_range = max(
                bar.high - bar.low,
                abs(bar.high - self.previous_close),
                abs(bar.low - self.previous_close),
            )
        self.true_ranges.append(true_range)
        self.previous_close = bar.close
        if len(self.true_ranges) < self.period:
            return None
        return sum(self.true_ranges) / len(self.true_ranges)


class SessionVWAP:
    def __init__(self) -> None:
        self.session_key: str | None = None
        self.price_volume = 0.0
        self.volume = 0.0

    def reset(self, session_key: str) -> None:
        self.session_key = session_key
        self.price_volume = 0.0
        self.volume = 0.0

    def update(self, session_key: str, price: float, volume: float) -> float:
        if self.session_key != session_key:
            self.reset(session_key)
        effective_volume = max(volume, 1.0)
        self.price_volume += price * effective_volume
        self.volume += effective_volume
        return self.value or price

    @property
    def value(self) -> float | None:
        if self.volume <= 0:
            return None
        return self.price_volume / self.volume


@dataclass
class OpeningRange:
    minutes: int
    start: datetime | None = None
    high: float | None = None
    low: float | None = None
    complete: bool = False

    def reset(self, start: datetime) -> None:
        self.start = start
        self.high = None
        self.low = None
        self.complete = False

    def update(self, bar: Bar, session_start: datetime) -> None:
        if self.start != session_start:
            self.reset(session_start)
        end = session_start + timedelta(minutes=self.minutes)
        if bar.start < end:
            self.high = bar.high if self.high is None else max(self.high, bar.high)
            self.low = bar.low if self.low is None else min(self.low, bar.low)
        else:
            self.complete = self.high is not None and self.low is not None


class BarBuilder:
    def __init__(self, period_seconds: int) -> None:
        if period_seconds <= 0:
            raise ValueError("period_seconds must be positive")
        self.period_seconds = period_seconds
        self._current: dict[str, Bar] = {}
        self._last_cumulative_volume: dict[str, float] = {}

    def update(self, symbol: str, timestamp: datetime, price: float, volume: float = 0.0) -> Bar | None:
        bucket_start = self._bucket_start(timestamp)
        bucket_end = bucket_start + timedelta(seconds=self.period_seconds)
        incremental_volume = self._incremental_volume(symbol, volume)
        current = self._current.get(symbol)
        if current is None:
            self._current[symbol] = Bar(symbol, bucket_start, bucket_end, price, price, price, price, incremental_volume)
            return None
        if bucket_start == current.start:
            self._current[symbol] = Bar(
                symbol=symbol,
                start=current.start,
                end=current.end,
                open=current.open,
                high=max(current.high, price),
                low=min(current.low, price),
                close=price,
                volume=current.volume + incremental_volume,
            )
            return None
        closed = current
        self._current[symbol] = Bar(symbol, bucket_start, bucket_end, price, price, price, price, incremental_volume)
        return closed

    def flush(self, symbol: str) -> Bar | None:
        return self._current.pop(symbol, None)

    def _incremental_volume(self, symbol: str, volume: float) -> float:
        if volume <= 0:
            return 0.0
        previous = self._last_cumulative_volume.get(symbol)
        self._last_cumulative_volume[symbol] = volume
        if previous is None:
            return 0.0
        if volume >= previous:
            return volume - previous
        return volume

    def _bucket_start(self, timestamp: datetime) -> datetime:
        seconds = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
        bucket_seconds = floor(seconds / self.period_seconds) * self.period_seconds
        return timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=bucket_seconds)
