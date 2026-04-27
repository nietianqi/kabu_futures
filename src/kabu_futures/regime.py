"""Simple high/low volatility regime classifier.

Per the evolution framework, the first regime split should be the simplest
useful one: high vs low volatility based on rolling realised volatility of
mid-price moves. This module exposes a lightweight rolling classifier and
a function to split a JSONL log by regime for offline analysis.

Intentionally avoids HMM / clustering — keep it auditable.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from statistics import median, pstdev
from typing import Iterable, Iterator, Literal

from .models import OrderBook
from .replay import read_recorded_books


Regime = Literal["low_vol", "high_vol", "warmup"]


@dataclass
class RegimeClassifier:
    """Rolling realised-volatility-based regime classifier.

    Maintains a sliding window of mid-price returns over ``window_seconds``.
    A new sample is classified as ``high_vol`` if the rolling standard
    deviation exceeds ``high_vol_quantile`` of the historical distribution.

    The first ``warmup_samples`` ticks emit ``"warmup"`` (no decision).
    """

    window_seconds: float = 60.0
    warmup_samples: int = 200
    history_size: int = 5000
    high_vol_quantile: float = 0.75

    _mid_buffer: deque[tuple[datetime, float]] = field(default_factory=deque, init=False)
    _vol_history: deque[float] = field(default_factory=deque, init=False)
    _last_regime: Regime = field(default="warmup", init=False)

    def update(self, book: OrderBook) -> Regime:
        """Feed a new book and return the current regime label."""
        event_time = book.received_at or book.timestamp
        mid = book.mid_price
        cutoff = event_time - timedelta(seconds=self.window_seconds)
        self._mid_buffer.append((event_time, mid))
        while self._mid_buffer and self._mid_buffer[0][0] < cutoff:
            self._mid_buffer.popleft()

        if len(self._mid_buffer) < 2:
            self._last_regime = "warmup"
            return "warmup"

        prices = [price for _, price in self._mid_buffer]
        returns = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        if not returns:
            self._last_regime = "warmup"
            return "warmup"

        vol = pstdev(returns) if len(returns) >= 2 else abs(returns[0])
        self._vol_history.append(vol)
        while len(self._vol_history) > self.history_size:
            self._vol_history.popleft()

        if len(self._vol_history) < self.warmup_samples:
            self._last_regime = "warmup"
            return "warmup"

        threshold = self._quantile(list(self._vol_history), self.high_vol_quantile)
        regime: Regime = "high_vol" if vol >= threshold else "low_vol"
        self._last_regime = regime
        return regime

    @property
    def last_regime(self) -> Regime:
        return self._last_regime

    @staticmethod
    def _quantile(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        idx = max(0, min(len(ordered) - 1, int(round(q * (len(ordered) - 1)))))
        return ordered[idx]


def classify_books(books: Iterable[OrderBook], **kwargs: object) -> Iterator[tuple[OrderBook, Regime]]:
    """Yield (book, regime) tuples by streaming a sequence of books through the classifier."""
    classifier = RegimeClassifier(**kwargs)  # type: ignore[arg-type]
    for book in books:
        yield book, classifier.update(book)


def split_log_by_regime(
    path: str | Path,
    window_seconds: float = 60.0,
    warmup_samples: int = 200,
    high_vol_quantile: float = 0.75,
) -> dict[str, list[OrderBook]]:
    """Read a JSONL log and group books by classified regime.

    Useful for running ``analyze_micro_log`` separately on high-vol and
    low-vol slices to compare per-regime parameter performance.
    """
    classifier = RegimeClassifier(
        window_seconds=window_seconds,
        warmup_samples=warmup_samples,
        high_vol_quantile=high_vol_quantile,
    )
    grouped: dict[str, list[OrderBook]] = {"low_vol": [], "high_vol": [], "warmup": []}
    for book in read_recorded_books(path):
        regime = classifier.update(book)
        grouped[regime].append(book)
    return grouped


def regime_distribution(path: str | Path, **kwargs: object) -> dict[str, object]:
    """Quick summary of how many books fall into each regime."""
    grouped = split_log_by_regime(path, **kwargs)  # type: ignore[arg-type]
    total = sum(len(v) for v in grouped.values())
    return {
        "source": str(path),
        "total_books": total,
        "counts": {regime: len(books) for regime, books in grouped.items()},
        "ratios": {
            regime: round(len(books) / total, 4) if total > 0 else 0.0
            for regime, books in grouped.items()
        },
    }
