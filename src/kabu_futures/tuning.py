from __future__ import annotations

from collections import Counter
from dataclasses import replace
from itertools import product
import json
from pathlib import Path
from typing import Iterable

from .config import StrategyConfig
from .engine import DualStrategyEngine
from .models import OrderBook
from .paper_execution import ExecutionEvent, PaperExecutionController
from .replay import BookLogSource, read_recorded_books_many, resolve_recorded_book_paths


def _unique_sorted(values: Iterable[float | int]) -> tuple:
    return tuple(sorted(set(values)))


def default_micro_grid(config: StrategyConfig) -> dict[str, tuple]:
    micro = config.micro_engine
    imbalance = _unique_sorted((0.18, 0.22, 0.26, 0.30, micro.imbalance_entry))
    microprice = _unique_sorted((0.10, 0.15, 0.20, micro.microprice_entry_ticks))
    take_profit = _unique_sorted((2, 3, micro.take_profit_ticks))
    stop_loss = _unique_sorted((2, 3, 4, micro.stop_loss_ticks))
    return {
        "imbalance_entry": imbalance,
        "microprice_entry_ticks": microprice,
        "take_profit_ticks": take_profit,
        "stop_loss_ticks": stop_loss,
    }


def load_tuning_books(path: BookLogSource, max_books: int | None = None) -> list[OrderBook]:
    books: list[OrderBook] = []
    for book in read_recorded_books_many(path):
        books.append(book)
        if max_books is not None and len(books) >= max_books:
            break
    return books


def evaluate_micro_config(books: list[OrderBook], config: StrategyConfig, parameters: dict[str, object]) -> dict[str, object]:
    engine = DualStrategyEngine(config)
    execution = PaperExecutionController(config, trade_mode="paper", paper_fill_model="immediate")
    signal_counts: Counter[str] = Counter()
    reject_reasons: Counter[str] = Counter()
    exits: list[ExecutionEvent] = []

    for book in books:
        event_time = book.received_at or book.timestamp
        signals = engine.on_order_book(book, now=event_time)
        for evaluation in engine.latest_signal_evaluations:
            if evaluation.decision == "reject":
                reject_reasons[evaluation.reason] += 1
        for event in execution.on_book(book, engine.latest_book_features):
            if event.event_type == "paper_exit":
                exits.append(event)
        for signal in signals:
            signal_counts[f"{signal.engine}:{signal.direction}:{signal.reason}"] += 1
            if signal.engine != "micro_book":
                continue
            for event in execution.on_signal(signal, book):
                if event.event_type == "paper_exit":
                    exits.append(event)

    stats = execution.heartbeat_metadata()
    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    wins = sum(1 for value in pnl_values if value > 0)
    losses = sum(1 for value in pnl_values if value < 0)
    trades = len(pnl_values)
    net_pnl_ticks = sum(pnl_values)
    peak = 0.0
    equity = 0.0
    max_drawdown = 0.0
    for value in pnl_values:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "parameters": parameters,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / trades, 4) if trades else 0.0,
        "net_pnl_ticks": round(net_pnl_ticks, 4),
        "net_pnl_yen": stats["paper_pnl_yen"],
        "avg_pnl_ticks": round(net_pnl_ticks / trades, 4) if trades else 0.0,
        "max_drawdown_ticks": round(max_drawdown, 4),
        "signals": dict(signal_counts),
        "reject_reasons_top": reject_reasons.most_common(10),
    }


def evaluate_strategy_config(books: list[OrderBook], config: StrategyConfig, parameters: dict[str, object]) -> dict[str, object]:
    """Evaluate the complete signal stack with both micro and minute paper execution.

    ``evaluate_micro_config`` intentionally skips non-micro signals so parameter
    search remains isolated to micro-engine knobs. This companion report runs
    the chosen config through the normal paper controller to show how the same
    candidate behaves when minute signals share the execution lane.
    """
    engine = DualStrategyEngine(config)
    execution = PaperExecutionController(config, trade_mode="paper", paper_fill_model="immediate")
    signal_counts: Counter[str] = Counter()
    reject_reasons: Counter[str] = Counter()
    paper_events: Counter[str] = Counter()
    execution_reject_reasons: Counter[str] = Counter()
    engine_entries: Counter[str] = Counter()
    engine_trades: Counter[str] = Counter()
    engine_wins: Counter[str] = Counter()
    engine_losses: Counter[str] = Counter()
    engine_pnl: dict[str, list[float]] = {}
    exits: list[ExecutionEvent] = []

    for book in books:
        event_time = book.received_at or book.timestamp
        signals = engine.on_order_book(book, now=event_time)
        for evaluation in engine.latest_signal_evaluations:
            if evaluation.decision == "reject":
                reject_reasons[evaluation.reason] += 1
        for event in execution.on_book(book, engine.latest_book_features):
            _record_strategy_event(
                event,
                paper_events,
                execution_reject_reasons,
                engine_entries,
                engine_trades,
                engine_wins,
                engine_losses,
                engine_pnl,
                exits,
            )
        for signal in signals:
            signal_counts[f"{signal.engine}:{signal.direction}:{signal.reason}"] += 1
            for event in execution.on_signal(signal, book):
                _record_strategy_event(
                    event,
                    paper_events,
                    execution_reject_reasons,
                    engine_entries,
                    engine_trades,
                    engine_wins,
                    engine_losses,
                    engine_pnl,
                    exits,
                )

    stats = execution.heartbeat_metadata()
    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    trades = len(pnl_values)
    wins = sum(1 for value in pnl_values if value > 0)
    losses = sum(1 for value in pnl_values if value < 0)
    net_pnl_ticks = sum(pnl_values)
    max_drawdown = _max_drawdown(pnl_values)
    return {
        "parameters": parameters,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / trades, 4) if trades else 0.0,
        "net_pnl_ticks": round(net_pnl_ticks, 4),
        "net_pnl_yen": stats["paper_pnl_yen"],
        "avg_pnl_ticks": round(net_pnl_ticks / trades, 4) if trades else 0.0,
        "max_drawdown_ticks": max_drawdown,
        "signals": dict(signal_counts),
        "reject_reasons_top": reject_reasons.most_common(10),
        "paper_events": dict(paper_events),
        "execution_reject_reasons": dict(execution_reject_reasons),
        "by_engine": _strategy_by_engine(engine_entries, engine_trades, engine_wins, engine_losses, engine_pnl),
    }


def _candidate_config(config: StrategyConfig, parameters: dict[str, object]) -> StrategyConfig:
    micro = replace(config.micro_engine, **parameters)
    candidate = replace(config, micro_engine=micro)
    candidate.validate()
    return candidate


def _rank_key(result: dict[str, object]) -> tuple[float, float, float, int]:
    return (
        float(result["net_pnl_ticks"]),
        float(result["avg_pnl_ticks"]),
        -float(result["max_drawdown_ticks"]),
        int(result["trades"]),
    )


def tune_micro_params(
    path: BookLogSource,
    config: StrategyConfig,
    grid: dict[str, tuple] | None = None,
    min_trades: int = 5,
    top_n: int = 10,
    max_books: int | None = None,
) -> dict[str, object]:
    source_paths = resolve_recorded_book_paths(path)
    books = load_tuning_books(path, max_books=max_books)
    observation_days = len({(book.received_at or book.timestamp).date().isoformat() for book in books})
    grid = grid or default_micro_grid(config)
    baseline_parameters = {
        "imbalance_entry": config.micro_engine.imbalance_entry,
        "microprice_entry_ticks": config.micro_engine.microprice_entry_ticks,
        "take_profit_ticks": config.micro_engine.take_profit_ticks,
        "stop_loss_ticks": config.micro_engine.stop_loss_ticks,
    }
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

    ranked = sorted(candidates, key=_rank_key, reverse=True)
    valid = [candidate for candidate in ranked if int(candidate["trades"]) >= min_trades]
    best = valid[0] if valid else None
    baseline_net = float(baseline["net_pnl_ticks"])
    max_candidate_trades = max((int(candidate["trades"]) for candidate in candidates), default=0)
    diagnostics = _data_diagnostics(len(books), observation_days, min_trades, int(baseline["trades"]), max_candidate_trades)
    if best is None:
        decision = "no_change"
        reason = "insufficient_candidate_trades"
    elif float(best["net_pnl_ticks"]) > max(0.0, baseline_net) and float(best["avg_pnl_ticks"]) > 0:
        decision = "recommended"
        reason = "candidate_improved_net_expectancy"
    else:
        decision = "no_change"
        reason = "no_candidate_beats_baseline"

    full_strategy_baseline = evaluate_strategy_config(books, config, baseline_parameters)
    full_strategy_recommended: dict[str, object] | None = None
    if decision == "recommended" and best is not None:
        best_parameters = best["parameters"]
        assert isinstance(best_parameters, dict)
        full_strategy_recommended = evaluate_strategy_config(books, _candidate_config(config, best_parameters), best_parameters)

    return {
        "source": str(source_paths[0]) if len(source_paths) == 1 else "multiple",
        "sources": [str(source_path) for source_path in source_paths],
        "books": len(books),
        "observation_days": observation_days,
        "decision": decision,
        "reason": reason,
        "diagnostics": diagnostics,
        "insufficient_data": bool(diagnostics),
        "min_trades": min_trades,
        "baseline": baseline,
        "recommended": best if decision == "recommended" else None,
        "full_strategy_baseline": full_strategy_baseline,
        "full_strategy_recommended": full_strategy_recommended,
        "top_candidates": ranked[:top_n],
        "search_space": {key: list(value) for key, value in grid.items()},
        "parameter_trials": len(candidates),
        "invalid_combos": invalid_combos,
    }


def challenger_micro_engine_payload(report: dict[str, object]) -> dict[str, dict[str, object]] | None:
    if report.get("decision") != "recommended":
        return None
    recommended = report.get("recommended")
    if not isinstance(recommended, dict):
        return None
    parameters = recommended.get("parameters")
    if not isinstance(parameters, dict):
        return None
    return {"micro_engine": dict(parameters)}


def write_challenger_micro_engine(report: dict[str, object], path: str | Path) -> bool:
    payload = challenger_micro_engine_payload(report)
    if payload is None:
        return False
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return True


def _record_strategy_event(
    event: ExecutionEvent,
    paper_events: Counter[str],
    execution_reject_reasons: Counter[str],
    engine_entries: Counter[str],
    engine_trades: Counter[str],
    engine_wins: Counter[str],
    engine_losses: Counter[str],
    engine_pnl: dict[str, list[float]],
    exits: list[ExecutionEvent],
) -> None:
    paper_events[event.event_type] += 1
    engine_name = _event_signal_engine(event)
    if event.event_type == "paper_entry":
        engine_entries[engine_name] += 1
    elif event.event_type == "paper_exit":
        exits.append(event)
        pnl_ticks = float(event.pnl_ticks or 0.0)
        engine_trades[engine_name] += 1
        engine_pnl.setdefault(engine_name, []).append(pnl_ticks)
        if pnl_ticks > 0:
            engine_wins[engine_name] += 1
        elif pnl_ticks < 0:
            engine_losses[engine_name] += 1
    elif event.event_type == "execution_reject":
        execution_reject_reasons[event.reason] += 1


def _event_signal_engine(event: ExecutionEvent) -> str:
    signal = event.metadata.get("signal")
    if isinstance(signal, dict):
        engine = signal.get("engine")
        if engine:
            return str(engine)
    engine = event.metadata.get("engine")
    if engine:
        return str(engine)
    return "unknown"


def _strategy_by_engine(
    entries: Counter[str],
    trades: Counter[str],
    wins: Counter[str],
    losses: Counter[str],
    pnl_values: dict[str, list[float]],
) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for engine_name in sorted(set(entries) | set(trades) | set(pnl_values)):
        values = pnl_values.get(engine_name, [])
        trade_count = int(trades.get(engine_name, 0))
        net = sum(values)
        result[engine_name] = {
            "entries": int(entries.get(engine_name, 0)),
            "trades": trade_count,
            "wins": int(wins.get(engine_name, 0)),
            "losses": int(losses.get(engine_name, 0)),
            "win_rate": round(wins.get(engine_name, 0) / trade_count, 4) if trade_count else 0.0,
            "net_pnl_ticks": round(net, 4),
            "avg_pnl_ticks": round(net / trade_count, 4) if trade_count else 0.0,
        }
    return result


def _max_drawdown(pnl_values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnl_values:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return round(max_drawdown, 4)


def _data_diagnostics(
    books: int,
    observation_days: int,
    min_trades: int,
    baseline_trades: int,
    max_candidate_trades: int,
) -> list[str]:
    diagnostics: list[str] = []
    if books <= 0:
        diagnostics.append("no_books_loaded")
    if observation_days <= 0:
        diagnostics.append("no_observation_days")
    if baseline_trades == 0 and max_candidate_trades == 0:
        diagnostics.append("no_closed_micro_trades")
    elif max_candidate_trades < min_trades:
        diagnostics.append(f"max_candidate_trades_{max_candidate_trades}_below_min_trades_{min_trades}")
    return diagnostics
