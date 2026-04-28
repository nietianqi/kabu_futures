from __future__ import annotations

from collections import Counter
from dataclasses import replace
from itertools import product
from pathlib import Path
from typing import Iterable

from .analysis_utils import iter_books, max_drawdown
from .config import StrategyConfig
from .engine import DualStrategyEngine
from .models import OrderBook
from .paper_execution import ExecutionEvent, PaperExecutionController, PaperFillModel


DEFAULT_IMBALANCE_GRID = (0.18, 0.20, 0.22, 0.25, 0.30)


def load_books(path: str | Path, max_books: int | None = None) -> list[OrderBook]:
    books: list[OrderBook] = []
    for book in iter_books(path):
        books.append(book)
        if max_books is not None and len(books) >= max_books:
            break
    return books


def evaluate_micro_config(
    books: Iterable[OrderBook],
    config: StrategyConfig,
    parameters: dict[str, object],
    paper_fill_model: PaperFillModel = "immediate",
) -> dict[str, object]:
    engine = DualStrategyEngine(config)
    execution = PaperExecutionController(config, trade_mode="paper", paper_fill_model=paper_fill_model)
    evaluation_decisions: Counter[str] = Counter()
    reject_reasons: Counter[str] = Counter()
    signal_counts: Counter[str] = Counter()
    paper_events: Counter[str] = Counter()
    exit_reasons: Counter[str] = Counter()
    exits: list[ExecutionEvent] = []
    processed = 0

    for book in books:
        processed += 1
        signals = engine.on_order_book(book, now=book.received_at or book.timestamp)
        for evaluation in engine.latest_signal_evaluations:
            evaluation_decisions[evaluation.decision] += 1
            if evaluation.decision == "reject":
                reject_reasons[evaluation.reason] += 1
        for event in execution.on_book(book, engine.latest_book_features):
            paper_events[event.event_type] += 1
            if event.event_type == "paper_exit":
                exits.append(event)
                exit_reasons[event.reason] += 1
        for signal in signals:
            signal_counts[f"{signal.engine}:{signal.direction}:{signal.reason}"] += 1
            if signal.engine != "micro_book" or not signal.is_tradeable:
                continue
            for event in execution.on_signal(signal, book):
                paper_events[event.event_type] += 1
                if event.event_type == "paper_exit":
                    exits.append(event)
                    exit_reasons[event.reason] += 1

    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    trades = len(pnl_values)
    wins = sum(1 for value in pnl_values if value > 0)
    net = sum(pnl_values)
    return {
        "parameters": dict(parameters),
        "books": processed,
        "evaluations": dict(evaluation_decisions),
        "reject_reasons_top": reject_reasons.most_common(10),
        "signals": dict(signal_counts),
        "paper_events": dict(paper_events),
        "exit_reasons": dict(exit_reasons),
        "trades": trades,
        "win_rate": round(wins / trades, 4) if trades else 0.0,
        "net_pnl_ticks": round(net, 4),
        "avg_pnl_ticks": round(net / trades, 4) if trades else 0.0,
        "max_drawdown_ticks": max_drawdown(pnl_values),
    }


def tune_micro_params(
    path: str | Path,
    config: StrategyConfig,
    imbalance_grid: Iterable[float] = DEFAULT_IMBALANCE_GRID,
    max_books: int | None = None,
    min_trades: int = 20,
    paper_fill_model: PaperFillModel = "immediate",
) -> dict[str, object]:
    books = load_books(path, max_books=max_books)
    baseline_parameters = {"imbalance_entry": config.micro_engine.imbalance_entry}
    baseline = evaluate_micro_config(books, config, baseline_parameters, paper_fill_model=paper_fill_model)

    candidates: list[dict[str, object]] = []
    invalid_combos: list[dict[str, object]] = []
    for imbalance_entry in imbalance_grid:
        parameters = {"imbalance_entry": float(imbalance_entry)}
        try:
            candidate_config = replace(
                config,
                micro_engine=replace(config.micro_engine, imbalance_entry=float(imbalance_entry)),
            )
            candidate_config.validate()
        except ValueError as exc:
            invalid_combos.append({"parameters": parameters, "error": str(exc)})
            continue
        candidates.append(evaluate_micro_config(books, candidate_config, parameters, paper_fill_model=paper_fill_model))

    ranked = sorted(candidates, key=_rank_key, reverse=True)
    valid = [candidate for candidate in ranked if int(candidate["trades"]) >= min_trades]
    best = valid[0] if valid else (ranked[0] if ranked else None)
    decision = "no_change"
    if best is not None and int(best["trades"]) >= min_trades and float(best["net_pnl_ticks"]) > float(baseline["net_pnl_ticks"]):
        decision = "recommended"
    return {
        "source": str(path),
        "books": len(books),
        "grid": {"imbalance_entry": [float(value) for value in imbalance_grid]},
        "min_trades": min_trades,
        "baseline": baseline,
        "candidates": ranked,
        "invalid_combos": invalid_combos,
        "best": best,
        "decision": decision,
        "note": "Candidate parameters are report-only and never overwrite config/local.json.",
    }


def _rank_key(candidate: dict[str, object]) -> tuple[float, float, int]:
    return (
        float(candidate["net_pnl_ticks"]),
        float(candidate["avg_pnl_ticks"]),
        int(candidate["trades"]),
    )
