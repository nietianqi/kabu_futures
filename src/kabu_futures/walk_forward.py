"""Walk-forward rolling validation for micro-parameter tuning.

Evaluates candidate parameter sets across multiple train/test windows.
A candidate is considered *stable* only when it beats the baseline in
``pass_threshold`` fraction of windows.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Iterator

from .config import StrategyConfig
from .models import OrderBook
from .replay import read_recorded_books
from .tuning import DEFAULT_IMBALANCE_GRID, evaluate_micro_config, _rank_key


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WalkForwardWindow:
    """One rolling train/test window over a list of trading days."""

    index: int
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]

    @property
    def test_day(self) -> str:
        return self.test_days[0] if self.test_days else ""


@dataclass(frozen=True)
class WindowEvaluation:
    """Per-window comparison between baseline and best candidate."""

    window: WalkForwardWindow
    train_recommendation: dict[str, object]
    baseline_test: dict[str, object]
    candidate_test: dict[str, object] | None
    candidate_beats_baseline: bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _day_key(timestamp: datetime) -> str:
    return timestamp.date().isoformat()


def _iter_books_from_source(source: str | Path | list[str | Path]) -> Iterator[OrderBook]:
    """Yield books from a path, directory, or list of paths."""
    if isinstance(source, list):
        for item in source:
            yield from _iter_books_from_source(item)
        return
    source = Path(source)
    if source.is_dir():
        for jsonl_file in sorted(source.glob("*.jsonl")):
            yield from read_recorded_books(jsonl_file)
    else:
        yield from read_recorded_books(source)


def split_books_by_day(
    source: str | Path | list[str | Path],
) -> dict[str, list[OrderBook]]:
    """Group OrderBook objects from one or more JSONL files by trading day.

    Day key is ``YYYY-MM-DD`` (JST date of ``received_at`` or ``timestamp``).
    """
    grouped: dict[str, list[OrderBook]] = defaultdict(list)
    for book in _iter_books_from_source(source):
        event_time = book.received_at or book.timestamp
        grouped[_day_key(event_time)].append(book)
    return dict(grouped)


def make_windows(
    days: list[str],
    train_size: int,
    test_size: int = 1,
    step: int = 1,
) -> list[WalkForwardWindow]:
    """Generate rolling train/test windows from an ordered list of day keys."""
    if train_size <= 0 or test_size <= 0 or step <= 0:
        raise ValueError("train_size, test_size, and step must be positive")
    windows: list[WalkForwardWindow] = []
    cursor = 0
    idx = 0
    while cursor + train_size + test_size <= len(days):
        train_days = tuple(days[cursor: cursor + train_size])
        test_days = tuple(days[cursor + train_size: cursor + train_size + test_size])
        windows.append(WalkForwardWindow(index=idx, train_days=train_days, test_days=test_days))
        cursor += step
        idx += 1
    return windows


def _tune_from_books(
    books: list[OrderBook],
    config: StrategyConfig,
    imbalance_grid: tuple[float, ...],
    min_trades: int,
) -> dict[str, object]:
    """Run grid search over *pre-loaded* books (no path I/O)."""
    from dataclasses import replace

    baseline_params = {"imbalance_entry": config.micro_engine.imbalance_entry}
    baseline = evaluate_micro_config(books, config, baseline_params)

    candidates: list[dict[str, object]] = []
    invalid_combos: list[dict[str, object]] = []
    for imbalance_entry in imbalance_grid:
        params = {"imbalance_entry": float(imbalance_entry)}
        try:
            candidate_config = replace(
                config,
                micro_engine=replace(config.micro_engine, imbalance_entry=float(imbalance_entry)),
            )
            candidate_config.validate()
        except ValueError as exc:
            invalid_combos.append({"parameters": params, "error": str(exc)})
            continue
        candidates.append(evaluate_micro_config(books, candidate_config, params))

    ranked = sorted(candidates, key=_rank_key, reverse=True)
    valid = [c for c in ranked if int(c["trades"]) >= min_trades]
    best = valid[0] if valid else (ranked[0] if ranked else None)
    decision = "no_change"
    if best is not None and int(best["trades"]) >= min_trades and float(best["net_pnl_ticks"]) > float(baseline["net_pnl_ticks"]):
        decision = "recommended"
    return {
        "baseline": baseline,
        "candidates": ranked,
        "invalid_combos": invalid_combos,
        "best": best,
        "decision": decision,
    }


# ---------------------------------------------------------------------------
# Main API
# ---------------------------------------------------------------------------

def walk_forward_micro(
    source: str | Path | list[str | Path],
    config: StrategyConfig,
    train_size: int = 5,
    test_size: int = 1,
    step: int = 1,
    imbalance_grid: tuple[float, ...] | None = None,
    min_trades: int = 5,
    pass_threshold: float = 0.7,
    max_books_per_day: int | None = None,
) -> dict[str, object]:
    """Run rolling walk-forward validation.

    For each window:
        1. Search the imbalance grid using train-day books.
        2. If a recommendation is produced, evaluate it on held-out test days.
        3. Compare candidate test PnL vs baseline test PnL.

    Returns a dict with ``windows``, ``evaluations``, and ``summary``.
    A candidate is *stable* when it beats the baseline in ``pass_threshold``
    fraction of windows **and** at least one window exists.
    """
    grid = imbalance_grid or DEFAULT_IMBALANCE_GRID

    grouped = split_books_by_day(source)
    days = sorted(grouped.keys())
    windows = make_windows(days, train_size=train_size, test_size=test_size, step=step)

    baseline_params = {"imbalance_entry": config.micro_engine.imbalance_entry}

    evaluations: list[dict[str, object]] = []
    pass_count = 0
    baseline_pnls: list[float] = []
    candidate_pnls: list[float] = []
    total_baseline_trades = 0
    total_candidate_trades = 0

    diagnostics: list[str] = []
    if not days:
        diagnostics.append("no_books_loaded")
    if not windows:
        need = train_size + test_size
        diagnostics.append(f"insufficient_days: have {len(days)}, need {need}")

    for window in windows:
        # Build train + test book lists
        train_books: list[OrderBook] = []
        for day in window.train_days:
            day_books = grouped.get(day, [])
            if max_books_per_day is not None:
                day_books = day_books[:max_books_per_day]
            train_books.extend(day_books)

        test_books: list[OrderBook] = []
        for day in window.test_days:
            day_books = grouped.get(day, [])
            if max_books_per_day is not None:
                day_books = day_books[:max_books_per_day]
            test_books.extend(day_books)

        # Tune on train books
        tune = _tune_from_books(train_books, config, grid, min_trades)
        recommendation = tune["best"]

        # Evaluate baseline on test books
        baseline_test = evaluate_micro_config(test_books, config, baseline_params)
        total_baseline_trades += int(baseline_test["trades"])
        baseline_pnls.append(float(baseline_test["net_pnl_ticks"]))

        # Evaluate candidate on test books (if recommendation exists)
        candidate_test: dict[str, object] | None = None
        beats = False
        if recommendation is not None:
            from dataclasses import replace as dc_replace
            rec_imbalance = float(recommendation["parameters"]["imbalance_entry"])  # type: ignore[index]
            try:
                candidate_config = dc_replace(
                    config,
                    micro_engine=dc_replace(config.micro_engine, imbalance_entry=rec_imbalance),
                )
                candidate_config.validate()
                candidate_test = evaluate_micro_config(
                    test_books, candidate_config, {"imbalance_entry": rec_imbalance}
                )
                total_candidate_trades += int(candidate_test["trades"])
                candidate_pnls.append(float(candidate_test["net_pnl_ticks"]))
                beats = float(candidate_test["net_pnl_ticks"]) > float(baseline_test["net_pnl_ticks"])
            except ValueError:
                pass

        if beats:
            pass_count += 1

        evaluations.append({
            "window_index": window.index,
            "train_days": list(window.train_days),
            "test_days": list(window.test_days),
            "train_recommendation": tune,
            "baseline_test": baseline_test,
            "candidate_test": candidate_test,
            "candidate_beats_baseline": beats,
        })

    n_windows = len(windows)
    pass_rate = pass_count / n_windows if n_windows > 0 else 0.0
    stable = pass_rate >= pass_threshold and n_windows > 0 and not diagnostics

    return {
        "config": {
            "train_size": train_size,
            "test_size": test_size,
            "step": step,
            "min_trades": min_trades,
            "pass_threshold": pass_threshold,
            "imbalance_grid": list(grid),
        },
        "days": days,
        "evaluations": evaluations,
        "summary": {
            "windows": n_windows,
            "pass_count": pass_count,
            "pass_rate": round(pass_rate, 4),
            "stable": stable,
            "total_baseline_test_trades": total_baseline_trades,
            "total_candidate_test_trades": total_candidate_trades,
            "median_baseline_pnl_ticks": round(median(baseline_pnls), 4) if baseline_pnls else 0.0,
            "median_candidate_pnl_ticks": round(median(candidate_pnls), 4) if candidate_pnls else 0.0,
            "diagnostics": diagnostics,
        },
    }
