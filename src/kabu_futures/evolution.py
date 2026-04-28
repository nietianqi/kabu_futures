from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from .analysis_utils import REGIME_BUCKETS, iter_books, markout_summary, pnl_summary, regime_counter
from .config import StrategyConfig
from .engine import DualStrategyEngine
from .models import Direction, OrderBook
from .paper_execution import ExecutionEvent, PaperExecutionController, PaperFillModel
from .regime import RegimeClassifier
from .serialization import event_time as book_event_time


DEFAULT_MARKOUT_SECONDS = (0.5, 1.0, 3.0, 5.0, 30.0, 60.0)


@dataclass
class _PendingMarkout:
    symbol: str
    direction: Direction
    entry_price: float
    signal_reason: str
    targets: dict[float, datetime]
    regime: str | None = None


@dataclass
class _RegimeAttribution:
    distribution: Counter[str] = field(default_factory=regime_counter)
    evaluation_decisions: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    reject_reasons: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    allow_reasons: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    signal_counts: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    signal_reasons: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    signal_directions: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    paper_events: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    execution_reject_reasons: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    exit_reasons: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    trade_signal_reasons: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    pnl_ticks: defaultdict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    pnl_yen: defaultdict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    markout_entries: Counter[str] = field(default_factory=regime_counter)
    markout_values: defaultdict[str, defaultdict[str, list[float]]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(list)))
    markout_by_reason: defaultdict[str, defaultdict[str, defaultdict[str, list[float]]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    )


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
    include_regime: bool = True,
    regime_kwargs: dict[str, object] | None = None,
) -> dict[str, object]:
    horizons = tuple(float(value) for value in markout_seconds)
    engine = DualStrategyEngine(config)
    execution = PaperExecutionController(config, trade_mode="paper", paper_fill_model=paper_fill_model)
    regime_classifier = RegimeClassifier(**(regime_kwargs or {})) if include_regime else None
    regime_stats = _RegimeAttribution() if include_regime else None

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

    for book in iter_books(path):
        books += 1
        event_time = book_event_time(book)
        current_regime = regime_classifier.update(book) if regime_classifier is not None else None
        if regime_stats is not None and current_regime is not None:
            regime_stats.distribution[current_regime] += 1

        _update_pending_markouts(book, pending_markouts, markout_values, markout_by_reason, config.tick_size, regime_stats)
        hourly[_hour_key(event_time)]["books"] += 1

        signals = engine.on_order_book(book, now=event_time)
        for evaluation in engine.latest_signal_evaluations:
            evaluation_decisions[evaluation.decision] += 1
            if regime_stats is not None and current_regime is not None:
                regime_stats.evaluation_decisions[current_regime][evaluation.decision] += 1
            if evaluation.decision == "reject":
                reject_reasons[evaluation.reason] += 1
                if regime_stats is not None and current_regime is not None:
                    regime_stats.reject_reasons[current_regime][evaluation.reason] += 1
                hourly[_hour_key(event_time)]["rejects"] += 1
            elif evaluation.decision == "allow":
                allow_reasons[evaluation.reason] += 1
                if regime_stats is not None and current_regime is not None:
                    regime_stats.allow_reasons[current_regime][evaluation.reason] += 1
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
            current_regime,
            regime_stats,
        )

        for signal in signals:
            signal_key = f"{signal.engine}:{signal.direction}:{signal.reason}"
            signal_counts[signal_key] += 1
            signal_reasons[signal.reason] += 1
            signal_directions[signal.direction] += 1
            if regime_stats is not None and current_regime is not None:
                regime_stats.signal_counts[current_regime][signal_key] += 1
                regime_stats.signal_reasons[current_regime][signal.reason] += 1
                regime_stats.signal_directions[current_regime][signal.direction] += 1
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
                current_regime,
                regime_stats,
            )

        if max_books is not None and books >= max_books:
            break

    pnl_values = [float(event.pnl_ticks or 0.0) for event in exits]
    report: dict[str, object] = {
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
            "summary": markout_summary(markout_values),
            "by_signal_reason": {
                reason: markout_summary(values_by_horizon) for reason, values_by_horizon in sorted(markout_by_reason.items())
            },
        },
    }
    if regime_stats is not None:
        report["regime"] = _regime_summary(regime_stats)
    return report


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
    current_regime: str | None = None,
    regime_stats: _RegimeAttribution | None = None,
) -> int:
    entries = 0
    for event in events:
        paper_events[event.event_type] += 1
        if regime_stats is not None and current_regime is not None:
            regime_stats.paper_events[current_regime][event.event_type] += 1
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
                    current_regime,
                )
            )
            if regime_stats is not None and current_regime is not None:
                regime_stats.markout_entries[current_regime] += 1
            hourly[hour]["entries"] += 1
        elif event.event_type == "paper_exit":
            exits.append(event)
            exit_reasons[event.reason] += 1
            trade_signal_reasons[_event_signal_reason(event)] += 1
            if regime_stats is not None and current_regime is not None:
                regime_stats.exit_reasons[current_regime][event.reason] += 1
                regime_stats.trade_signal_reasons[current_regime][_event_signal_reason(event)] += 1
                regime_stats.pnl_ticks[current_regime].append(float(event.pnl_ticks or 0.0))
                regime_stats.pnl_yen[current_regime].append(float(event.pnl_yen or 0.0))
            hourly[hour]["trades"] += 1
            hourly[hour]["pnl_ticks"] += float(event.pnl_ticks or 0.0)
        elif event.event_type == "execution_reject":
            execution_reject_reasons[event.reason] += 1
            if regime_stats is not None and current_regime is not None:
                regime_stats.execution_reject_reasons[current_regime][event.reason] += 1
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
    regime_stats: _RegimeAttribution | None = None,
) -> None:
    if not pending_markouts:
        return
    event_time = book_event_time(book)
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
                if regime_stats is not None and entry.regime is not None:
                    regime_stats.markout_values[entry.regime][label].append(value)
                    regime_stats.markout_by_reason[entry.regime][entry.signal_reason][label].append(value)
            else:
                remaining[horizon] = target_time
        if remaining:
            entry.targets = remaining
            active.append(entry)
    pending_markouts[:] = active


def _paper_summary(pnl_values: list[float], heartbeat: dict[str, object]) -> dict[str, object]:
    return {
        **pnl_summary(pnl_values),
        "net_pnl_yen": heartbeat["paper_pnl_yen"],
        "open_position": heartbeat["paper_position"],
        "pending_order": heartbeat["paper_pending_order"],
        "paper_micro_trades": heartbeat.get("paper_micro_trades", 0),
        "paper_minute_trades": heartbeat.get("paper_minute_trades", 0),
    }


def _regime_summary(stats: _RegimeAttribution) -> dict[str, object]:
    regimes = _regime_keys(stats)
    return {
        "distribution": {regime: int(stats.distribution.get(regime, 0)) for regime in regimes},
        "evaluations": {
            regime: {
                "total": sum(stats.evaluation_decisions[regime].values()),
                "decisions": dict(stats.evaluation_decisions[regime]),
                "reject_reasons_top": stats.reject_reasons[regime].most_common(10),
                "allow_reasons": dict(stats.allow_reasons[regime]),
            }
            for regime in regimes
        },
        "signals": {
            regime: {
                "total": sum(stats.signal_counts[regime].values()),
                "by_key": dict(stats.signal_counts[regime]),
                "by_reason": dict(stats.signal_reasons[regime]),
                "by_direction": dict(stats.signal_directions[regime]),
            }
            for regime in regimes
        },
        "paper": {regime: _regime_paper_summary(stats, regime) for regime in regimes},
        "markout": {
            regime: {
                "entries": int(stats.markout_entries.get(regime, 0)),
                "summary": markout_summary(stats.markout_values[regime]),
                "by_signal_reason": {
                    reason: markout_summary(values_by_horizon)
                    for reason, values_by_horizon in sorted(stats.markout_by_reason[regime].items())
                },
            }
            for regime in regimes
        },
    }


def _regime_paper_summary(stats: _RegimeAttribution, regime: str) -> dict[str, object]:
    pnl_ticks = stats.pnl_ticks[regime]
    pnl_yen = stats.pnl_yen[regime]
    trades = len(pnl_yen)
    return {
        **pnl_summary(pnl_ticks),
        "net_pnl_yen": round(sum(pnl_yen), 2),
        "avg_pnl_yen": round(sum(pnl_yen) / trades, 2) if trades else 0.0,
        "events": dict(stats.paper_events[regime]),
        "execution_reject_reasons": dict(stats.execution_reject_reasons[regime]),
        "exit_reasons": dict(stats.exit_reasons[regime]),
        "by_signal_reason": dict(stats.trade_signal_reasons[regime]),
    }


def _regime_keys(stats: _RegimeAttribution) -> list[str]:
    keys = list(REGIME_BUCKETS)
    for mapping in (
        stats.distribution,
        stats.evaluation_decisions,
        stats.signal_counts,
        stats.paper_events,
        stats.pnl_ticks,
        stats.markout_values,
    ):
        for key in mapping:
            if key not in keys:
                keys.append(key)
    return keys


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


def _hour_key(timestamp: datetime) -> str:
    return timestamp.replace(minute=0, second=0, microsecond=0).isoformat()


def _horizon_label(horizon: float) -> str:
    return str(int(horizon)) if horizon.is_integer() else str(horizon)
