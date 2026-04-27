"""Walk-forward rolling validation for micro-parameter tuning.

Wraps :mod:`kabu_futures.tuning` to evaluate candidate parameter sets across
multiple train/test windows. A candidate is considered stable only if it beats
the baseline in a configurable fraction of windows.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import median

from .config import StrategyConfig
from .replay import BookLogSource, read_recorded_books_many, resolve_recorded_book_paths
from .tuning import (
    default_micro_grid,
    evaluate_micro_config,
    _candidate_config,
)


@dataclass(frozen=True)
class WalkForwardWindow:
    """One rolling train/test window over a list of trading days."""

    index: int
    train_days: tuple[str, ...]
    test_days: tuple[str, ...]

    @property
    def test_day(self) -> str:
        """Backward-compatible first held-out day."""
        return self.test_days[0] if self.test_days else ""


@dataclass(frozen=True)
class WindowEvaluation:
    """Per-window comparison between baseline and recommended candidate."""

    window: WalkForwardWindow
    train_recommendation: dict[str, object]
    baseline_test: dict[str, object]
    candidate_test: dict[str, object] | None
    candidate_beats_baseline: bool


def _day_key(timestamp: datetime) -> str:
    return timestamp.date().isoformat()


def split_books_by_day(path: BookLogSource) -> dict[str, list]:
    """Group recorded books by trading day. Day key is `YYYY-MM-DD`."""
    grouped: dict[str, list] = defaultdict(list)
    for book in read_recorded_books_many(path):
        event_time = book.received_at or book.timestamp
        grouped[_day_key(event_time)].append(book)
    return dict(grouped)


def make_windows(days: list[str], train_size: int, test_size: int = 1, step: int = 1) -> list[WalkForwardWindow]:
    """Generate rolling windows. Test window starts immediately after train.

    Args:
        days: ordered list of trading day keys.
        train_size: number of days used for parameter search.
        test_size: number of days held out for evaluation (default 1).
        step: how many days to advance between windows (default 1).

    Returns:
        List of WalkForwardWindow objects in chronological order.
    """
    if train_size <= 0 or test_size <= 0 or step <= 0:
        raise ValueError("train_size, test_size, and step must be positive")
    windows: list[WalkForwardWindow] = []
    index = 0
    cursor = 0
    while cursor + train_size + test_size <= len(days):
        train_days = tuple(days[cursor : cursor + train_size])
        test_days = tuple(days[cursor + train_size : cursor + train_size + test_size])
        windows.append(WalkForwardWindow(index=index, train_days=train_days, test_days=test_days))
        index += 1
        cursor += step
    return windows


def walk_forward_micro(
    path: BookLogSource,
    config: StrategyConfig,
    train_size: int = 5,
    test_size: int = 1,
    step: int = 1,
    grid: dict[str, tuple] | None = None,
    min_trades: int = 5,
    pass_threshold: float = 0.7,
    max_books_per_day: int | None = None,
) -> dict[str, object]:
    """Run rolling walk-forward over days in a single JSONL log.

    For each window:
        1. Search the parameter grid using the train days (calls tune_micro_params).
        2. If a recommendation is produced, evaluate it on the held-out test day.
        3. Compare candidate test PnL vs baseline test PnL.

    A candidate is considered stable when it beats baseline in
    `pass_threshold` fraction of windows.
    """
    source_paths = resolve_recorded_book_paths(path)
    grouped = split_books_by_day(source_paths)
    days = sorted(grouped.keys())
    windows = make_windows(days, train_size=train_size, test_size=test_size, step=step)

    grid = grid or default_micro_grid(config)
    evaluations: list[WindowEvaluation] = []
    pass_count = 0
    test_baseline_pnls: list[float] = []
    test_candidate_pnls: list[float] = []
    total_baseline_test_trades = 0
    total_candidate_test_trades = 0

    baseline_parameters = {
        "imbalance_entry": config.micro_engine.imbalance_entry,
        "microprice_entry_ticks": config.micro_engine.microprice_entry_ticks,
        "take_profit_ticks": config.micro_engine.take_profit_ticks,
        "stop_loss_ticks": config.micro_engine.stop_loss_ticks,
    }

    for window in windows:
        train_books = []
        for day in window.train_days:
            day_books = grouped.get(day, [])
            if max_books_per_day is not None:
                day_books = day_books[:max_books_per_day]
            train_books.extend(day_books)
        test_books = []
        for day in window.test_days:
            day_books = grouped.get(day, [])
            if max_books_per_day is not None:
                day_books = day_books[:max_books_per_day]
            test_books.extend(day_books)

        # In-window parameter search: reuse tune_micro_params logic but on
        # already-loaded books to avoid re-reading the file.
        train_report = _tune_in_memory(
            train_books,
            config,
            grid=grid,
            min_trades=min_trades,
            baseline_parameters=baseline_parameters,
        )

        baseline_test = evaluate_micro_config(test_books, config, baseline_parameters)
        total_baseline_test_trades += int(baseline_test["trades"])
        candidate_test: dict[str, object] | None = None
        candidate_beats = False
        if train_report["decision"] == "recommended":
            recommended = train_report["recommended"]
            assert isinstance(recommended, dict)
            params = recommended["parameters"]
            assert isinstance(params, dict)
            try:
                candidate_config = _candidate_config(config, params)
                candidate_test = evaluate_micro_config(test_books, candidate_config, params)
                total_candidate_test_trades += int(candidate_test["trades"])
                candidate_beats = float(candidate_test["net_pnl_ticks"]) > float(baseline_test["net_pnl_ticks"])
            except ValueError as exc:
                candidate_test = {"error": str(exc)}
                candidate_beats = False

        if candidate_beats:
            pass_count += 1

        test_baseline_pnls.append(float(baseline_test["net_pnl_ticks"]))
        if candidate_test is not None and "net_pnl_ticks" in candidate_test:
            test_candidate_pnls.append(float(candidate_test["net_pnl_ticks"]))

        evaluations.append(
            WindowEvaluation(
                window=window,
                train_recommendation=train_report,
                baseline_test=baseline_test,
                candidate_test=candidate_test,
                candidate_beats_baseline=candidate_beats,
            )
        )

    pass_rate = pass_count / len(windows) if windows else 0.0
    diagnostics = _walk_forward_diagnostics(
        len(days),
        train_size,
        test_size,
        len(windows),
        total_baseline_test_trades,
        total_candidate_test_trades,
    )
    stable = pass_rate >= pass_threshold and len(windows) > 0 and not diagnostics

    return {
        "source": str(source_paths[0]) if len(source_paths) == 1 else "multiple",
        "sources": [str(source_path) for source_path in source_paths],
        "days": days,
        "windows": [_window_to_dict(ev) for ev in evaluations],
        "summary": {
            "window_count": len(windows),
            "pass_count": pass_count,
            "pass_rate": round(pass_rate, 4),
            "pass_threshold": pass_threshold,
            "stable": stable,
            "insufficient_data": bool(diagnostics),
            "diagnostics": diagnostics,
            "total_baseline_test_trades": total_baseline_test_trades,
            "total_candidate_test_trades": total_candidate_test_trades,
            "test_baseline_pnl_median": round(median(test_baseline_pnls), 4) if test_baseline_pnls else 0.0,
            "test_candidate_pnl_median": round(median(test_candidate_pnls), 4) if test_candidate_pnls else 0.0,
            "test_candidate_pnl_avg": round(sum(test_candidate_pnls) / len(test_candidate_pnls), 4)
            if test_candidate_pnls
            else 0.0,
        },
    }


def _tune_in_memory(
    books: list,
    config: StrategyConfig,
    grid: dict[str, tuple],
    min_trades: int,
    baseline_parameters: dict[str, object],
) -> dict[str, object]:
    """Mirror tune_micro_params but use already-loaded books."""
    from itertools import product

    baseline = evaluate_micro_config(books, config, baseline_parameters)
    candidates: list[dict[str, object]] = []
    invalid_combos: list[dict[str, object]] = []
    for imbalance, microprice, take_profit, stop_loss in product(
        grid["imbalance_entry"],
        grid["microprice_entry_ticks"],
        grid["take_profit_ticks"],
        grid["stop_loss_ticks"],
    ):
        parameters = {
            "imbalance_entry": round(float(imbalance), 6),
            "microprice_entry_ticks": round(float(microprice), 6),
            "take_profit_ticks": int(take_profit),
            "stop_loss_ticks": int(stop_loss),
        }
        if parameters == baseline_parameters:
            continue
        try:
            candidate_config = _candidate_config(config, parameters)
        except ValueError as exc:
            invalid_combos.append({"parameters": parameters, "error": str(exc)})
            continue
        candidates.append(evaluate_micro_config(books, candidate_config, parameters))

    def _rank_key(result: dict[str, object]) -> tuple[float, float, float, int]:
        return (
            float(result["net_pnl_ticks"]),
            float(result["avg_pnl_ticks"]),
            -float(result["max_drawdown_ticks"]),
            int(result["trades"]),
        )

    ranked = sorted(candidates, key=_rank_key, reverse=True)
    valid = [candidate for candidate in ranked if int(candidate["trades"]) >= min_trades]
    best = valid[0] if valid else None
    baseline_net = float(baseline["net_pnl_ticks"])
    max_candidate_trades = max((int(candidate["trades"]) for candidate in candidates), default=0)
    diagnostics: list[str] = []
    if not books:
        diagnostics.append("no_books_loaded")
    if int(baseline["trades"]) == 0 and max_candidate_trades == 0:
        diagnostics.append("no_closed_micro_trades")
    elif max_candidate_trades < min_trades:
        diagnostics.append(f"max_candidate_trades_{max_candidate_trades}_below_min_trades_{min_trades}")
    if best is None:
        decision = "no_change"
        reason = "insufficient_candidate_trades"
    elif float(best["net_pnl_ticks"]) > max(0.0, baseline_net) and float(best["avg_pnl_ticks"]) > 0:
        decision = "recommended"
        reason = "candidate_improved_net_expectancy"
    else:
        decision = "no_change"
        reason = "no_candidate_beats_baseline"

    return {
        "decision": decision,
        "reason": reason,
        "baseline": baseline,
        "recommended": best if decision == "recommended" else None,
        "parameter_trials": len(candidates),
        "invalid_combos": invalid_combos,
        "diagnostics": diagnostics,
        "insufficient_data": bool(diagnostics),
    }


def _window_to_dict(ev: WindowEvaluation) -> dict[str, object]:
    return {
        "index": ev.window.index,
        "train_days": list(ev.window.train_days),
        "test_day": ev.window.test_day,
        "test_days": list(ev.window.test_days),
        "decision": ev.train_recommendation.get("decision"),
        "reason": ev.train_recommendation.get("reason"),
        "recommended_parameters": (
            ev.train_recommendation.get("recommended", {}).get("parameters")
            if isinstance(ev.train_recommendation.get("recommended"), dict)
            else None
        ),
        "baseline_test_pnl_ticks": ev.baseline_test["net_pnl_ticks"],
        "baseline_test_trades": ev.baseline_test["trades"],
        "candidate_test_pnl_ticks": (
            ev.candidate_test.get("net_pnl_ticks") if isinstance(ev.candidate_test, dict) else None
        ),
        "candidate_test_trades": (
            ev.candidate_test.get("trades") if isinstance(ev.candidate_test, dict) else None
        ),
        "candidate_beats_baseline": ev.candidate_beats_baseline,
    }


def _walk_forward_diagnostics(
    day_count: int,
    train_size: int,
    test_size: int,
    window_count: int,
    baseline_test_trades: int,
    candidate_test_trades: int,
) -> list[str]:
    diagnostics: list[str] = []
    required_days = train_size + test_size
    if day_count < required_days:
        diagnostics.append(f"insufficient_days_{day_count}_lt_required_{required_days}")
    if window_count == 0:
        diagnostics.append("no_walk_forward_windows")
    if window_count > 0 and baseline_test_trades == 0 and candidate_test_trades == 0:
        diagnostics.append("no_closed_test_trades")
    return diagnostics
