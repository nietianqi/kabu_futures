from __future__ import annotations

from collections import Counter
from dataclasses import replace
from itertools import product
from pathlib import Path
from typing import Iterable

from .analysis_utils import iter_books, max_drawdown
from .config import MICRO_ENTRY_PROFILE_DEFAULT, StrategyConfig
from .engine import DualStrategyEngine
from .models import OrderBook
from .paper_execution import ExecutionEvent, PaperExecutionController, PaperFillModel


DEFAULT_IMBALANCE_GRID = (0.28, 0.26, 0.24)
DEFAULT_MICROPRICE_GRID = (0.12, 0.10)
DEFAULT_OFI_PERCENTILE_GRID = (70.0, 65.0, 60.0)
DEFAULT_SPREAD_GRID = (1,)


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
    tradeable_micro_signals = 0
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
            tradeable_micro_signals += 1
            for event in execution.on_signal(signal, book):
                paper_events[event.event_type] += 1
                if event.event_type == "paper_exit":
                    exits.append(event)
                    exit_reasons[event.reason] += 1

    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    trades = len(pnl_values)
    wins = sum(1 for value in pnl_values if value > 0)
    net = sum(pnl_values)
    paper_entries = int(paper_events.get("paper_entry", 0))
    return {
        "parameters": dict(parameters),
        "books": processed,
        "evaluations": dict(evaluation_decisions),
        "reject_reasons_top": reject_reasons.most_common(10),
        "signals": dict(signal_counts),
        "paper_events": dict(paper_events),
        "exit_reasons": dict(exit_reasons),
        "tradeable_micro_signals": tradeable_micro_signals,
        "paper_entries": paper_entries,
        "simulated_fill_rate": round(paper_entries / tradeable_micro_signals, 4) if tradeable_micro_signals else 0.0,
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
    microprice_grid: Iterable[float] | None = None,
    ofi_percentile_grid: Iterable[float] | None = None,
    spread_grid: Iterable[int] | None = None,
    max_books: int | None = None,
    min_trades: int = 20,
    max_drawdown_worsen_ratio: float = 0.20,
    min_fill_rate: float = 0.70,
    paper_fill_model: PaperFillModel = "immediate",
) -> dict[str, object]:
    imbalance_values = tuple(float(value) for value in imbalance_grid)
    microprice_values = tuple(float(value) for value in (microprice_grid or DEFAULT_MICROPRICE_GRID))
    effective_micro = config.effective_micro_engine()
    ofi_percentile_values = tuple(float(value) for value in (ofi_percentile_grid or (effective_micro.ofi_percentile,)))
    spread_values = tuple(int(value) for value in (spread_grid or DEFAULT_SPREAD_GRID))
    books = load_books(path, max_books=max_books)
    baseline_parameters = {
        "entry_profile": config.micro_engine.entry_profile,
        "imbalance_entry": effective_micro.imbalance_entry,
        "microprice_entry_ticks": effective_micro.microprice_entry_ticks,
        "ofi_percentile": effective_micro.ofi_percentile,
        "spread_ticks_required": effective_micro.spread_ticks_required,
    }
    baseline = evaluate_micro_config(books, config, baseline_parameters, paper_fill_model=paper_fill_model)

    candidates: list[dict[str, object]] = []
    invalid_combos: list[dict[str, object]] = []
    for imbalance_entry, microprice_entry_ticks, ofi_percentile, spread_ticks_required in product(
        imbalance_values,
        microprice_values,
        ofi_percentile_values,
        spread_values,
    ):
        parameters = {
            "imbalance_entry": imbalance_entry,
            "microprice_entry_ticks": microprice_entry_ticks,
            "ofi_percentile": ofi_percentile,
            "spread_ticks_required": spread_ticks_required,
        }
        try:
            candidate_config = replace(
                config,
                micro_engine=replace(
                    config.micro_engine,
                    entry_profile=MICRO_ENTRY_PROFILE_DEFAULT,
                    imbalance_entry=imbalance_entry,
                    microprice_entry_ticks=microprice_entry_ticks,
                    ofi_percentile=ofi_percentile,
                    spread_ticks_required=spread_ticks_required,
                ),
            )
            candidate_config.validate()
        except ValueError as exc:
            invalid_combos.append({"parameters": parameters, "error": str(exc)})
            continue
        candidate = evaluate_micro_config(books, candidate_config, parameters, paper_fill_model=paper_fill_model)
        candidates.append(_with_reject_reduction(candidate, baseline))

    ranked = sorted(candidates, key=_rank_key, reverse=True)
    valid = [candidate for candidate in ranked if int(candidate["trades"]) >= min_trades]
    best = valid[0] if valid else (ranked[0] if ranked else None)
    recommendation = _recommend_candidate(baseline, ranked, min_trades, max_drawdown_worsen_ratio, min_fill_rate)
    decision = "recommended" if recommendation["decision"] == "recommended" else "no_change"
    return {
        "source": str(path),
        "books": len(books),
        "grid": {
            "imbalance_entry": list(imbalance_values),
            "microprice_entry_ticks": list(microprice_values),
            "ofi_percentile": list(ofi_percentile_values),
            "spread_ticks_required": list(spread_values),
        },
        "min_trades": min_trades,
        "max_drawdown_worsen_ratio": max_drawdown_worsen_ratio,
        "min_fill_rate": min_fill_rate,
        "baseline": baseline,
        "candidates": ranked,
        "invalid_combos": invalid_combos,
        "best": best,
        "best_safe_candidate": recommendation["candidate"] if recommendation["decision"] == "recommended" else None,
        "recommendation": recommendation,
        "decision": decision,
        "note": "Candidate parameters are report-only and never overwrite config/local.json or live rules.",
    }


def _rank_key(candidate: dict[str, object]) -> tuple[float, float, int]:
    return (
        float(candidate["net_pnl_ticks"]),
        float(candidate["avg_pnl_ticks"]),
        int(candidate["trades"]),
    )


def _with_reject_reduction(candidate: dict[str, object], baseline: dict[str, object]) -> dict[str, object]:
    result = dict(candidate)
    baseline_counts = _reject_counts(baseline)
    candidate_counts = _reject_counts(candidate)
    reasons = sorted(set(baseline_counts) | set(candidate_counts))
    by_reason: dict[str, dict[str, float | int]] = {}
    total_reduction = 0
    for reason in reasons:
        baseline_count = baseline_counts.get(reason, 0)
        candidate_count = candidate_counts.get(reason, 0)
        reduction = baseline_count - candidate_count
        total_reduction += reduction
        by_reason[reason] = {
            "baseline": baseline_count,
            "candidate": candidate_count,
            "reduction": reduction,
            "reduction_ratio": round(reduction / baseline_count, 4) if baseline_count else 0.0,
        }
    result["reject_reduction"] = {
        "total": total_reduction,
        "by_reason": by_reason,
        "top_improvements": sorted(
            ((reason, details) for reason, details in by_reason.items() if int(details["reduction"]) > 0),
            key=lambda item: int(item[1]["reduction"]),
            reverse=True,
        )[:10],
    }
    return result


def _reject_counts(report: dict[str, object]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in report.get("reject_reasons_top", []):
        if isinstance(item, (list, tuple)) and len(item) == 2:
            counts[str(item[0])] = int(item[1])
    return counts


def _recommend_candidate(
    baseline: dict[str, object],
    ranked: list[dict[str, object]],
    min_trades: int,
    max_drawdown_worsen_ratio: float,
    min_fill_rate: float,
) -> dict[str, object]:
    for candidate in ranked:
        checks = _recommendation_checks(baseline, candidate, min_trades, max_drawdown_worsen_ratio, min_fill_rate)
        if _passes_candidate_gates(checks):
            return {
                "decision": "recommended",
                "candidate": candidate,
                "checks": checks,
                "trade_delta": int(candidate["trades"]) - int(baseline["trades"]),
                "net_pnl_delta_ticks": round(float(candidate["net_pnl_ticks"]) - float(baseline["net_pnl_ticks"]), 4),
                "avg_pnl_delta_ticks": round(float(candidate["avg_pnl_ticks"]) - float(baseline["avg_pnl_ticks"]), 4),
                "max_drawdown_delta_ticks": round(
                    float(candidate["max_drawdown_ticks"]) - float(baseline["max_drawdown_ticks"]),
                    4,
                ),
                "manual_confirmation_required": True,
            }
    diagnostic = ranked[0] if ranked else None
    checks = _recommendation_checks(baseline, diagnostic, min_trades, max_drawdown_worsen_ratio, min_fill_rate) if diagnostic else {}
    return {
        "decision": "diagnostic_only",
        "candidate": diagnostic,
        "checks": checks,
        "manual_confirmation_required": True,
        "reason": "No grid candidate passed the safety gates for 2x trades, net PnL, drawdown, and simulated fill rate.",
    }


def _recommendation_checks(
    baseline: dict[str, object],
    candidate: dict[str, object] | None,
    min_trades: int,
    max_drawdown_worsen_ratio: float,
    min_fill_rate: float,
) -> dict[str, bool]:
    if candidate is None:
        return {
            "trades_increase": False,
            "trade_opportunity_2x": False,
            "min_trades": False,
            "net_pnl_not_worse": False,
            "net_pnl_improved": False,
            "avg_pnl_not_worse": False,
            "drawdown_not_materially_worse": False,
            "simulated_fill_rate_ok": False,
        }
    baseline_trades = int(baseline["trades"])
    candidate_trades = int(candidate["trades"])
    candidate_fill_rate = float(candidate.get("simulated_fill_rate", 1.0))
    return {
        "trades_increase": candidate_trades > baseline_trades,
        "trade_opportunity_2x": candidate_trades >= max(1, baseline_trades * 2),
        "min_trades": candidate_trades >= min_trades,
        "net_pnl_not_worse": float(candidate["net_pnl_ticks"]) >= float(baseline["net_pnl_ticks"]),
        "net_pnl_improved": float(candidate["net_pnl_ticks"]) > float(baseline["net_pnl_ticks"]),
        "avg_pnl_not_worse": float(candidate["avg_pnl_ticks"]) >= float(baseline["avg_pnl_ticks"]),
        "drawdown_not_materially_worse": _drawdown_not_materially_worse(
            float(baseline["max_drawdown_ticks"]),
            float(candidate["max_drawdown_ticks"]),
            max_drawdown_worsen_ratio,
        ),
        "simulated_fill_rate_ok": candidate_fill_rate >= min_fill_rate,
    }


def _passes_candidate_gates(checks: dict[str, bool]) -> bool:
    required = (
        "trade_opportunity_2x",
        "min_trades",
        "net_pnl_not_worse",
        "drawdown_not_materially_worse",
        "simulated_fill_rate_ok",
    )
    return all(bool(checks.get(key)) for key in required)


def _drawdown_not_materially_worse(baseline_drawdown: float, candidate_drawdown: float, worsen_ratio: float) -> bool:
    baseline_abs = abs(min(baseline_drawdown, 0.0))
    candidate_abs = abs(min(candidate_drawdown, 0.0))
    if candidate_abs <= baseline_abs:
        return True
    if baseline_abs == 0:
        return candidate_abs == 0
    return candidate_abs <= baseline_abs * (1.0 + max(0.0, worsen_ratio))
