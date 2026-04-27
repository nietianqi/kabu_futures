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
from .replay import read_recorded_books
from typing import Iterator


def _iter_books(source: str | Path) -> Iterator[OrderBook]:
    """Yield books from a single JSONL file or a directory of JSONL files."""
    p = Path(source)
    if p.is_dir():
        for jsonl_file in sorted(p.glob("*.jsonl")):
            yield from read_recorded_books(jsonl_file)
    else:
        yield from read_recorded_books(p)


DEFAULT_MARKOUT_SECONDS = (0.5, 1.0, 3.0, 5.0, 30.0, 60.0)


@dataclass
class _PendingMarkout:
    symbol: str
    direction: Direction
    entry_price: float
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
    path: str | Path,
    config: StrategyConfig,
    max_books: int | None = None,
    markout_seconds: Iterable[float] = DEFAULT_MARKOUT_SECONDS,
    paper_fill_model: PaperFillModel = "immediate",
) -> dict[str, object]:
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
    exits: list[ExecutionEvent] = []
    entries = 0

    for book in _iter_books(path):
        books += 1
        event_time = _event_time(book)
        _update_pending_markouts(book, pending_markouts, markout_values, markout_by_reason, config.tick_size)
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

        entries += _record_execution_events(
            execution.on_book(book, engine.latest_book_features),
            horizons,
            pending_markouts,
            paper_events,
            execution_reject_reasons,
            exit_reasons,
            trade_signal_reasons,
            exits,
            hourly,
        )

        for signal in signals:
            signal_counts[f"{signal.engine}:{signal.direction}:{signal.reason}"] += 1
            signal_reasons[signal.reason] += 1
            signal_directions[signal.direction] += 1
            hourly[_hour_key(event_time)]["signals"] += 1
            if not signal.is_tradeable:
                continue
            entries += _record_execution_events(
                execution.on_signal(signal, book),
                horizons,
                pending_markouts,
                paper_events,
                execution_reject_reasons,
                exit_reasons,
                trade_signal_reasons,
                exits,
                hourly,
            )

        if max_books is not None and books >= max_books:
            break

    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    return {
        "source": str(path),
        "books": books,
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
            **_paper_summary(pnl_values, execution.heartbeat_metadata()),
            "events": dict(paper_events),
            "execution_reject_reasons": dict(execution_reject_reasons),
            "exit_reasons": dict(exit_reasons),
            "by_signal_reason": dict(trade_signal_reasons),
        },
        "hourly": _hourly_summary(hourly),
        "markout": {
            "entries": entries,
            "summary": _markout_summary(markout_values),
            "by_signal_reason": {
                reason: _markout_summary(values_by_horizon) for reason, values_by_horizon in sorted(markout_by_reason.items())
            },
        },
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
    hourly: defaultdict[str, Counter[str]],
) -> int:
    entries = 0
    for event in events:
        paper_events[event.event_type] += 1
        hour = _hour_key(event.timestamp)
        if event.event_type == "paper_entry" and event.entry_price is not None:
            entries += 1
            signal_reason = _event_signal_reason(event)
            pending_markouts.append(
                _PendingMarkout(
                    event.symbol,
                    event.direction,
                    float(event.entry_price),
                    signal_reason,
                    {horizon: event.timestamp + timedelta(seconds=horizon) for horizon in horizons},
                )
            )
            hourly[hour]["entries"] += 1
        elif event.event_type == "paper_exit":
            exits.append(event)
            exit_reasons[event.reason] += 1
            trade_signal_reasons[_event_signal_reason(event)] += 1
            hourly[hour]["trades"] += 1
            hourly[hour]["pnl_ticks"] += float(event.pnl_ticks or 0.0)
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
                value = calculate_markout_ticks(entry.direction, entry.entry_price, book.mid_price, tick_size)
                markout_values[label].append(value)
                markout_by_reason[entry.signal_reason][label].append(value)
            else:
                remaining[horizon] = target_time
        if remaining:
            entry.targets = remaining
            active.append(entry)
    pending_markouts[:] = active


def _paper_summary(pnl_values: list[float], heartbeat: dict[str, object]) -> dict[str, object]:
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
        "open_position": heartbeat["paper_position"],
        "pending_order": heartbeat["paper_pending_order"],
        "paper_micro_trades": heartbeat.get("paper_micro_trades", 0),
        "paper_minute_trades": heartbeat.get("paper_minute_trades", 0),
    }


def _max_drawdown(values: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_dd = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(max_dd, 4)


def _markout_summary(values_by_horizon: dict[str, list[float]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for horizon, values in sorted(values_by_horizon.items(), key=lambda item: float(item[0])):
        if not values:
            continue
        positives = sum(1 for value in values if value > 0)
        sorted_values = sorted(values)
        summary[horizon] = {
            "count": len(values),
            "avg_ticks": round(sum(values) / len(values), 4),
            "median_ticks": round(sorted_values[len(sorted_values) // 2], 4),
            "positive_rate": round(positives / len(values), 4),
            "min_ticks": round(min(values), 4),
            "max_ticks": round(max(values), 4),
        }
    return summary


def _hourly_summary(hourly: defaultdict[str, Counter[str]]) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    for hour, counts in sorted(hourly.items()):
        result[hour] = {key: round(value, 4) if isinstance(value, float) else value for key, value in sorted(counts.items())}
    return result


def _event_signal_reason(event: ExecutionEvent) -> str:
    signal = event.metadata.get("signal") if isinstance(event.metadata, dict) else None
    if isinstance(signal, dict):
        return str(signal.get("reason") or signal.get("engine") or "unknown")
    return "unknown"


def _event_time(book: OrderBook) -> datetime:
    return book.received_at or book.timestamp


def _hour_key(timestamp: datetime) -> str:
    return timestamp.replace(minute=0, second=0, microsecond=0).isoformat()


def _horizon_label(horizon: float) -> str:
    return str(int(horizon)) if horizon.is_integer() else str(horizon)
