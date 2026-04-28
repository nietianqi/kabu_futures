from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from .models import OrderBook
from .replay import read_recorded_books


REGIME_BUCKETS = ("warmup", "high_vol", "low_vol")


def iter_books(source: str | Path) -> Iterator[OrderBook]:
    """Yield books from a single JSONL file or every JSONL file in a directory."""
    p = Path(source)
    if p.is_dir():
        for jsonl_file in sorted(p.glob("*.jsonl")):
            yield from read_recorded_books(jsonl_file)
    else:
        yield from read_recorded_books(p)


def max_drawdown(values: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(max_dd, 4)


def pnl_summary(values: Iterable[float]) -> dict[str, object]:
    pnl_values = [float(value) for value in values]
    trades = len(pnl_values)
    wins = sum(1 for value in pnl_values if value > 0)
    losses = sum(1 for value in pnl_values if value < 0)
    net = sum(pnl_values)
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / trades, 4) if trades else 0.0,
        "net_pnl_ticks": round(net, 4),
        "avg_pnl_ticks": round(net / trades, 4) if trades else 0.0,
        "max_drawdown_ticks": max_drawdown(pnl_values),
    }


def markout_summary(values_by_horizon: Mapping[str, Iterable[float]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for horizon, raw_values in sorted(values_by_horizon.items(), key=lambda item: float(item[0])):
        values = [float(value) for value in raw_values]
        if not values:
            continue
        positives = sum(1 for value in values if value > 0)
        sorted_values = sorted(values)
        middle = len(sorted_values) // 2
        median = (
            (sorted_values[middle - 1] + sorted_values[middle]) / 2.0
            if len(sorted_values) % 2 == 0
            else sorted_values[middle]
        )
        summary[horizon] = {
            "count": len(values),
            "avg_ticks": round(sum(values) / len(values), 4),
            "median_ticks": round(median, 4),
            "positive_rate": round(positives / len(values), 4),
            "min_ticks": round(min(values), 4),
            "max_ticks": round(max(values), 4),
        }
    return summary


def counter_to_dict(counter: Mapping[str, int | float]) -> dict[str, int | float]:
    return {key: value for key, value in counter.items()}


def nested_counter_to_dict(counters: Mapping[str, Counter[str]]) -> dict[str, dict[str, int | float]]:
    return {key: dict(counter) for key, counter in sorted(counters.items())}


def regime_counter() -> Counter[str]:
    return Counter({bucket: 0 for bucket in REGIME_BUCKETS})
