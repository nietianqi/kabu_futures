"""Intraday volatility regime classifier.

Classifies each OrderBook tick as "high_vol", "low_vol", or "warmup" based
on a rolling realised-volatility window compared to a rolling percentile
threshold.

Usage::

    classifier = RegimeClassifier()
    for book in books:
        regime = classifier.update(book)

    # Or split a pre-loaded list of books:
    by_regime = split_books_by_regime(books)
    high_vol_books = by_regime["high_vol"]
    low_vol_books  = by_regime["low_vol"]
"""
from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Literal

from .models import OrderBook
from .replay import read_recorded_books

Regime = Literal["high_vol", "low_vol", "warmup"]


class _RollingPercentile:
    """Approximate rolling N-th percentile using a sorted deque."""

    def __init__(self, maxlen: int, pct: float = 75.0) -> None:
        self._maxlen = maxlen
        self._pct = pct
        self._buf: deque[float] = deque()

    def update(self, value: float) -> float | None:
        self._buf.append(value)
        if len(self._buf) > self._maxlen:
            self._buf.popleft()
        if not self._buf:
            return None
        sorted_buf = sorted(self._buf)
        idx = (self._pct / 100.0) * (len(sorted_buf) - 1)
        lo, hi = int(idx), min(int(idx) + 1, len(sorted_buf) - 1)
        frac = idx - lo
        return sorted_buf[lo] * (1 - frac) + sorted_buf[hi] * frac


class RegimeClassifier:
    """Rolling intraday volatility regime classifier.

    Computes 1-minute realised volatility (absolute mid-price returns) and
    compares each minute's vol to a rolling percentile threshold.  Until
    ``warmup_periods`` samples have been collected the regime is "warmup".

    Args:
        vol_window_minutes: lookback window for the realised-vol estimate.
        pct_window_periods: number of realised-vol samples for the rolling
            percentile.
        high_vol_percentile: percentile threshold above which a period is
            classified as "high_vol" (default 75th).
        warmup_periods: how many vol samples to collect before classifying.
    """

    def __init__(
        self,
        vol_window_minutes: int = 1,
        pct_window_periods: int = 60,
        high_vol_percentile: float = 75.0,
        warmup_periods: int = 5,
    ) -> None:
        self._vol_window_minutes = vol_window_minutes
        self._warmup_periods = warmup_periods
        self._pct = _RollingPercentile(pct_window_periods, high_vol_percentile)
        self._window: deque[float] = deque()  # mid prices in current minute bucket
        self._current_bucket: str = ""
        self._last_vol: float | None = None
        self._vol_samples: int = 0
        self._current_regime: Regime = "warmup"

    @property
    def current_regime(self) -> Regime:
        return self._current_regime

    def update(self, book: OrderBook) -> Regime:
        """Update classifier with a new book snapshot; return current regime."""
        event_time = book.received_at or book.timestamp
        # 1-minute bucket key
        bucket = event_time.strftime("%Y-%m-%dT%H:%M")

        if bucket != self._current_bucket:
            # Flush previous bucket → compute realised vol
            if self._window:
                vol = self._bucket_vol(list(self._window))
                threshold = self._pct.update(vol)
                self._vol_samples += 1
                if threshold is None or self._vol_samples < self._warmup_periods:
                    self._current_regime = "warmup"
                elif vol >= threshold:
                    self._current_regime = "high_vol"
                else:
                    self._current_regime = "low_vol"
            self._window.clear()
            self._current_bucket = bucket

        self._window.append(book.mid_price)
        return self._current_regime

    @staticmethod
    def _bucket_vol(prices: list[float]) -> float:
        if len(prices) < 2:
            return 0.0
        returns = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
        return sum(returns) / len(returns)


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def classify_books(
    books: list[OrderBook],
    **classifier_kwargs: object,
) -> list[tuple[OrderBook, Regime]]:
    """Return ``(book, regime)`` pairs for a pre-loaded list of books."""
    clf = RegimeClassifier(**classifier_kwargs)  # type: ignore[arg-type]
    return [(book, clf.update(book)) for book in books]


def split_books_by_regime(
    books: list[OrderBook],
    **classifier_kwargs: object,
) -> dict[str, list[OrderBook]]:
    """Split pre-loaded books into ``{"high_vol": [...], "low_vol": [...], "warmup": [...]}``.

    Warmup books are excluded from high/low_vol buckets.
    """
    classified = classify_books(books, **classifier_kwargs)
    result: dict[str, list[OrderBook]] = {"high_vol": [], "low_vol": [], "warmup": []}
    for book, regime in classified:
        result[regime].append(book)
    return result


def split_log_by_regime(
    source: str | Path | list[str | Path],
    **classifier_kwargs: object,
) -> dict[str, list[OrderBook]]:
    """Load books from JSONL source(s) and split by volatility regime."""
    books: list[OrderBook] = []
    if isinstance(source, list):
        for item in source:
            books.extend(read_recorded_books(item))
    else:
        source_path = Path(source)
        if source_path.is_dir():
            for jsonl_file in sorted(source_path.glob("*.jsonl")):
                books.extend(read_recorded_books(jsonl_file))
        else:
            books.extend(read_recorded_books(source_path))
    return split_books_by_regime(books, **classifier_kwargs)


def regime_distribution(books: list[OrderBook], **classifier_kwargs: object) -> dict[str, int]:
    """Return counts of {regime: book_count}."""
    result: dict[str, int] = {"high_vol": 0, "low_vol": 0, "warmup": 0}
    clf = RegimeClassifier(**classifier_kwargs)  # type: ignore[arg-type]
    for book in books:
        regime = clf.update(book)
        result[regime] += 1
    return result
