from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from .config import StrategyConfig
from .engine import DualStrategyEngine
from .models import Direction, OrderBook
from .paper_execution import ExecutionEvent, PaperExecutionController, PaperFillModel
from .replay import BookLogSource, read_recorded_books_many, resolve_recorded_book_paths


DEFAULT_MARKOUT_SECONDS = (0.5, 1.0, 3.0, 5.0)


@dataclass
class _PendingMarkout:
    symbol: str
    direction: Direction
    entry_price: float
    signal_engine: str
    signal_reason: str
    targets: dict[float, datetime]


def calculate_markout_ticks(direction: Direction, entry_price: float, future_mid_price: float, tick_size: float) -> float:
    if tick_size <= 0:
        raise ValueError(f"tick_size must be positive, got {tick_size}")
    if direction == "long":
        return (future_mid_price - entry_price) / tick_size
    if direction == "short":
        return (entry_price - future_mid_price) / tick_size
    return 0.0


def analyze_micro_log(
    path: BookLogSource,
    config: StrategyConfig,
    max_books: int | None = None,
    markout_seconds: Iterable[float] = DEFAULT_MARKOUT_SECONDS,
    paper_fill_model: PaperFillModel = "immediate",
) -> dict[str, object]:
    source_paths = resolve_recorded_book_paths(path)
    horizons = tuple(float(value) for value in markout_seconds)
    engine = DualStrategyEngine(config)
    execution = PaperExecutionController(config, trade_mode="paper", paper_fill_model=paper_fill_model)

    books = 0
    evaluation_decisions: Counter[str] = Counter()
    reject_reasons: Counter[str] = Counter()
    allow_reasons: Counter[str] = Counter()
    signal_counts: Counter[str] = Counter()
    signal_reasons: Counter[str] = Counter()
    signal_directions: Counter[str] = Counter()
    paper_events: Counter[str] = Counter()
    execution_reject_reasons: Counter[str] = Counter()
    exit_reasons: Counter[str] = Counter()
    trade_signal_reasons: Counter[str] = Counter()
    hourly: defaultdict[str, Counter[str]] = defaultdict(Counter)
    pending_markouts: list[_PendingMarkout] = []
    markout_values: defaultdict[str, list[float]] = defaultdict(list)
    markout_by_reason: defaultdict[str, defaultdict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    markout_by_engine: defaultdict[str, defaultdict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    exits: list[ExecutionEvent] = []
    paper_engine_entries: Counter[str] = Counter()
    paper_engine_trades: Counter[str] = Counter()
    paper_engine_wins: Counter[str] = Counter()
    paper_engine_losses: Counter[str] = Counter()
    paper_engine_pnl: defaultdict[str, list[float]] = defaultdict(list)
    paper_engine_exit_reasons: defaultdict[str, Counter[str]] = defaultdict(Counter)
    days_seen: set[str] = set()
    entries = 0

    for book in read_recorded_books_many(source_paths):
        books += 1
        event_time = _event_time(book)
        days_seen.add(event_time.date().isoformat())
        _update_pending_markouts(
            book,
            pending_markouts,
            markout_values,
            markout_by_reason,
            markout_by_engine,
            config.tick_size,
        )
        hourly[_hour_key(event_time)]["books"] += 1

        signals = engine.on_order_book(book, now=event_time)
        for evaluation in engine.latest_signal_evaluations:
            evaluation_decisions[evaluation.decision] += 1
            if evaluation.decision == "reject":
                reject_reasons[evaluation.reason] += 1
                hourly[_hour_key(event_time)]["rejects"] += 1
            elif evaluation.decision == "allow":
                allow_reasons[evaluation.reason] += 1
                hourly[_hour_key(event_time)]["allows"] += 1

        book_events = execution.on_book(book, engine.latest_book_features)
        entries += _record_execution_events(
            book_events,
            horizons,
            pending_markouts,
            paper_events,
            execution_reject_reasons,
            exit_reasons,
            trade_signal_reasons,
            exits,
            paper_engine_entries,
            paper_engine_trades,
            paper_engine_wins,
            paper_engine_losses,
            paper_engine_pnl,
            paper_engine_exit_reasons,
            hourly,
        )

        for signal in signals:
            signal_counts[f"{signal.engine}:{signal.direction}:{signal.reason}"] += 1
            signal_reasons[signal.reason] += 1
            signal_directions[signal.direction] += 1
            hourly[_hour_key(event_time)]["signals"] += 1
            signal_events = execution.on_signal(signal, book)
            entries += _record_execution_events(
                signal_events,
                horizons,
                pending_markouts,
                paper_events,
                execution_reject_reasons,
                exit_reasons,
                trade_signal_reasons,
                exits,
                paper_engine_entries,
                paper_engine_trades,
                paper_engine_wins,
                paper_engine_losses,
                paper_engine_pnl,
                paper_engine_exit_reasons,
                hourly,
            )

        if max_books is not None and books >= max_books:
            break

    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    paper_summary = _paper_summary(pnl_values, exits, execution.heartbeat_metadata())
    markout_summary = _markout_summary(markout_values)
    paper_by_engine = _paper_by_engine_summary(
        paper_engine_entries,
        paper_engine_trades,
        paper_engine_wins,
        paper_engine_losses,
        paper_engine_pnl,
        paper_engine_exit_reasons,
    )

    return {
        "source": str(source_paths[0]) if len(source_paths) == 1 else "multiple",
        "sources": [str(source_path) for source_path in source_paths],
        "books": books,
        "observation_days": len(days_seen),
        "markout_seconds": list(horizons),
        "evaluations": {
            "total": sum(evaluation_decisions.values()),
            "decisions": dict(evaluation_decisions),
            "reject_reasons_top": reject_reasons.most_common(10),
            "allow_reasons": dict(allow_reasons),
        },
        "signals": {
            "total": sum(signal_counts.values()),
            "by_key": dict(signal_counts),
            "by_reason": dict(signal_reasons),
            "by_direction": dict(signal_directions),
        },
        "paper": {
            **paper_summary,
            "events": dict(paper_events),
            "execution_reject_reasons": dict(execution_reject_reasons),
            "exit_reasons": dict(exit_reasons),
            "by_signal_reason": dict(trade_signal_reasons),
            "by_engine": paper_by_engine,
        },
        "hourly": _hourly_summary(hourly),
        "markout": {
            "entries": entries,
            "summary": markout_summary,
            "by_signal_reason": {
                reason: _markout_summary(values_by_horizon) for reason, values_by_horizon in sorted(markout_by_reason.items())
            },
            "by_engine": {
                engine_name: _markout_summary(values_by_horizon) for engine_name, values_by_horizon in sorted(markout_by_engine.items())
            },
        },
        "metrics": _promotion_metrics(paper_summary, markout_summary, len(days_seen), _micro_parameters(config)),
    }


def _record_execution_events(
    events: list[ExecutionEvent],
    horizons: tuple[float, ...],
    pending_markouts: list[_PendingMarkout],
    paper_events: Counter[str],
    execution_reject_reasons: Counter[str],
    exit_reasons: Counter[str],
    trade_signal_reasons: Counter[str],
    exits: list[ExecutionEvent],
    paper_engine_entries: Counter[str],
    paper_engine_trades: Counter[str],
    paper_engine_wins: Counter[str],
    paper_engine_losses: Counter[str],
    paper_engine_pnl: defaultdict[str, list[float]],
    paper_engine_exit_reasons: defaultdict[str, Counter[str]],
    hourly: defaultdict[str, Counter[str]],
) -> int:
    entries = 0
    for event in events:
        paper_events[event.event_type] += 1
        hour = _hour_key(event.timestamp)
        if event.event_type == "paper_entry" and event.entry_price is not None:
            entries += 1
            signal_engine = _event_signal_engine(event)
            signal_reason = _event_signal_reason(event)
            paper_engine_entries[signal_engine] += 1
            pending_markouts.append(
                _PendingMarkout(
                    event.symbol,
                    event.direction,
                    float(event.entry_price),
                    signal_engine,
                    signal_reason,
                    {horizon: event.timestamp + timedelta(seconds=horizon) for horizon in horizons},
                )
            )
            hourly[hour]["entries"] += 1
        elif event.event_type == "paper_exit":
            exits.append(event)
            exit_reasons[event.reason] += 1
            trade_signal_reasons[_event_signal_reason(event)] += 1
            signal_engine = _event_signal_engine(event)
            pnl_ticks = float(event.pnl_ticks or 0.0)
            paper_engine_trades[signal_engine] += 1
            paper_engine_pnl[signal_engine].append(pnl_ticks)
            paper_engine_exit_reasons[signal_engine][event.reason] += 1
            if pnl_ticks > 0:
                paper_engine_wins[signal_engine] += 1
            elif pnl_ticks < 0:
                paper_engine_losses[signal_engine] += 1
            hourly[hour]["trades"] += 1
            hourly[hour]["pnl_ticks"] += pnl_ticks
        elif event.event_type == "execution_reject":
            execution_reject_reasons[event.reason] += 1
            hourly[hour]["execution_rejects"] += 1
        elif event.event_type == "paper_cancel":
            hourly[hour]["cancels"] += 1
        elif event.event_type == "paper_pending":
            hourly[hour]["pending"] += 1
    return entries


def _update_pending_markouts(
    book: OrderBook,
    pending_markouts: list[_PendingMarkout],
    markout_values: defaultdict[str, list[float]],
    markout_by_reason: defaultdict[str, defaultdict[str, list[float]]],
    markout_by_engine: defaultdict[str, defaultdict[str, list[float]]],
    tick_size: float,
) -> None:
    if not pending_markouts:
        return
    event_time = _event_time(book)
    active: list[_PendingMarkout] = []
    for entry in pending_markouts:
        if entry.symbol != book.symbol:
            active.append(entry)
            continue
        remaining: dict[float, datetime] = {}
        for horizon, target_time in entry.targets.items():
            if event_time >= target_time:
                label = _horizon_label(horizon)
                value = round(calculate_markout_ticks(entry.direction, entry.entry_price, book.mid_price, tick_size), 4)
                markout_values[label].append(value)
                markout_by_reason[entry.signal_reason][label].append(value)
                markout_by_engine[entry.signal_engine][label].append(value)
            else:
                remaining[horizon] = target_time
        if remaining:
            entry.targets = remaining
            active.append(entry)
    pending_markouts[:] = active


def _paper_summary(pnl_values: list[float], exits: list[ExecutionEvent], heartbeat: dict[str, object]) -> dict[str, object]:
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
        "net_pnl_yen": heartbeat["paper_pnl_yen"],
        "avg_pnl_ticks": round(net / trades, 4) if trades else 0.0,
        "max_drawdown_ticks": _max_drawdown(pnl_values),
        "max_consecutive_losses": _max_consecutive_losses(pnl_values),
        "open_position": heartbeat["paper_position"],
        "pending_order": heartbeat["paper_pending_order"],
    }


def _max_drawdown(pnl_values: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnl_values:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return round(max_drawdown, 4)


def _max_consecutive_losses(pnl_values: list[float]) -> int:
    current = 0
    worst = 0
    for value in pnl_values:
        if value < 0:
            current += 1
            worst = max(worst, current)
        else:
            current = 0
    return worst


def _paper_by_engine_summary(
    entries: Counter[str],
    trades: Counter[str],
    wins: Counter[str],
    losses: Counter[str],
    pnl_values: defaultdict[str, list[float]],
    exit_reasons: defaultdict[str, Counter[str]],
) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    engines = sorted(set(entries) | set(trades) | set(pnl_values))
    for engine_name in engines:
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
            "exit_reasons": dict(exit_reasons.get(engine_name, Counter())),
        }
    return result


def _markout_summary(values_by_horizon: dict[str, list[float]]) -> dict[str, dict[str, float | int]]:
    summary: dict[str, dict[str, float | int]] = {}
    for label, values in sorted(values_by_horizon.items(), key=lambda item: float(item[0])):
        count = len(values)
        positive = sum(1 for value in values if value > 0)
        total = sum(values)
        summary[label] = {
            "count": count,
            "avg_ticks": round(total / count, 4) if count else 0.0,
            "positive_rate": round(positive / count, 4) if count else 0.0,
        }
    return summary


def _hourly_summary(hourly: defaultdict[str, Counter[str]]) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    for hour, values in sorted(hourly.items()):
        row: dict[str, float | int] = dict(values)
        if "pnl_ticks" in row:
            row["pnl_ticks"] = round(float(row["pnl_ticks"]), 4)
        result[hour] = row
    return result


def _event_signal_reason(event: ExecutionEvent) -> str:
    signal = event.metadata.get("signal")
    if isinstance(signal, dict):
        reason = signal.get("reason")
        if reason:
            return str(reason)
    return "unknown"


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


def _promotion_metrics(
    paper_summary: dict[str, object],
    markout_summary: dict[str, dict[str, float | int]],
    observation_days: int,
    parameters: dict[str, object],
) -> dict[str, object]:
    return {
        "net_pnl_ticks": paper_summary["net_pnl_ticks"],
        "max_drawdown_ticks": paper_summary["max_drawdown_ticks"],
        "trades": paper_summary["trades"],
        "avg_pnl_ticks": paper_summary["avg_pnl_ticks"],
        "avg_markout_ticks_3s": _markout_avg(markout_summary, "3"),
        "observation_days": observation_days,
        "max_consecutive_losses": paper_summary["max_consecutive_losses"],
        "parameters": parameters,
    }


def _micro_parameters(config: StrategyConfig) -> dict[str, object]:
    micro = config.micro_engine
    return {
        "imbalance_entry": micro.imbalance_entry,
        "microprice_entry_ticks": micro.microprice_entry_ticks,
        "take_profit_ticks": micro.take_profit_ticks,
        "stop_loss_ticks": micro.stop_loss_ticks,
    }


def _markout_avg(summary: dict[str, dict[str, float | int]], label: str) -> float:
    row = summary.get(label) or summary.get(f"{label}.0")
    if not row:
        return 0.0
    return float(row.get("avg_ticks", 0.0))


def _event_time(book: OrderBook) -> datetime:
    return book.received_at or book.timestamp


def _hour_key(timestamp: datetime) -> str:
    return timestamp.replace(minute=0, second=0, microsecond=0).isoformat()


def _horizon_label(horizon: float) -> str:
    return str(int(horizon)) if horizon.is_integer() else str(horizon)
