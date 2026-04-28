from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Iterable

from .analysis_utils import REGIME_BUCKETS, iter_books, markout_summary, pnl_summary, regime_counter
from .config import StrategyConfig
from .engine import DualStrategyEngine
from .models import Direction, OrderBook, SignalEvaluation
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


@dataclass
class _EntryDiagnostics:
    micro_rejects: int = 0
    failed_checks: Counter[str] = field(default_factory=Counter)
    reject_reasons: Counter[str] = field(default_factory=Counter)
    candidate_directions: Counter[str] = field(default_factory=Counter)
    blocked_by: Counter[str] = field(default_factory=Counter)
    reject_stages: Counter[str] = field(default_factory=Counter)
    policy_reasons: Counter[str] = field(default_factory=Counter)
    recorded_execution_rejects: Counter[str] = field(default_factory=Counter)
    recorded_live_unsupported_engines: Counter[str] = field(default_factory=Counter)


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
    entry_diagnostics = _EntryDiagnostics()

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
                _record_entry_diagnostics(evaluation, entry_diagnostics)
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
    _record_logged_execution_rejects(path, entry_diagnostics)
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
        "entry_diagnostics": _entry_diagnostics_summary(entry_diagnostics),
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


def _record_entry_diagnostics(evaluation: SignalEvaluation, diagnostics: _EntryDiagnostics) -> None:
    if evaluation.engine != "micro_book" or evaluation.decision != "reject":
        return
    diagnostics.micro_rejects += 1
    diagnostics.reject_reasons[evaluation.reason] += 1
    diagnostics.candidate_directions[evaluation.candidate_direction] += 1
    metadata = evaluation.metadata if isinstance(evaluation.metadata, dict) else {}
    for reason in _failed_entry_checks(metadata):
        diagnostics.failed_checks[reason] += 1
    blocked_by = metadata.get("blocked_by")
    if blocked_by:
        diagnostics.blocked_by[str(blocked_by)] += 1
    reject_stage = metadata.get("reject_stage")
    if reject_stage:
        diagnostics.reject_stages[str(reject_stage)] += 1
    policy_reason = metadata.get("policy_reason")
    if policy_reason and policy_reason != "ok":
        diagnostics.policy_reasons[str(policy_reason)] += 1


def _failed_entry_checks(metadata: dict[str, object]) -> list[str]:
    checks: list[str] = []
    if metadata.get("jump_detected") is True:
        checks.append("jump_detected")
    if metadata.get("spread_ok") is False:
        checks.append("spread_not_required_width")
    if metadata.get("too_soon") is True:
        checks.append("min_order_interval")
    if _both_false(metadata, "imbalance_long_ok", "imbalance_short_ok"):
        checks.append("imbalance_not_met")
    if _both_false(metadata, "ofi_long_ok", "ofi_short_ok"):
        checks.append("ofi_not_met")
    if _both_false(metadata, "microprice_long_ok", "microprice_short_ok"):
        checks.append("microprice_not_met")

    candidate_direction = metadata.get("candidate_direction")
    if candidate_direction == "long":
        if metadata.get("minute_long_ok") is False:
            checks.append("minute_bias_conflict_long")
        if metadata.get("topix_long_ok") is False:
            checks.append("topix_bias_conflict_long")
    elif candidate_direction == "short":
        if metadata.get("minute_short_ok") is False:
            checks.append("minute_bias_conflict_short")
        if metadata.get("topix_short_ok") is False:
            checks.append("topix_bias_conflict_short")
    else:
        if _both_false(metadata, "minute_long_ok", "minute_short_ok"):
            checks.append("minute_bias_conflict")
        if _both_false(metadata, "topix_long_ok", "topix_short_ok"):
            checks.append("topix_bias_conflict")

    blocked_by = metadata.get("blocked_by")
    if blocked_by:
        checks.append(f"blocked_by:{blocked_by}")
    reject_stage = metadata.get("reject_stage")
    if reject_stage:
        checks.append(f"reject_stage:{reject_stage}")
    return checks


def _both_false(metadata: dict[str, object], left_key: str, right_key: str) -> bool:
    return metadata.get(left_key) is False and metadata.get(right_key) is False


def _record_logged_execution_rejects(path: str | Path, diagnostics: _EntryDiagnostics) -> None:
    for jsonl_file in _jsonl_files(path):
        try:
            handle = jsonl_file.open(encoding="utf-8")
        except OSError:
            continue
        with handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("kind") != "execution_reject":
                    continue
                payload = row.get("payload")
                if not isinstance(payload, dict):
                    continue
                reason = payload.get("reason") or payload.get("event_type")
                if reason:
                    diagnostics.recorded_execution_rejects[str(reason)] += 1
                if reason == "live_unsupported_signal_engine":
                    signal = _payload_signal(payload)
                    engine = signal.get("engine") if isinstance(signal, dict) else None
                    diagnostics.recorded_live_unsupported_engines[str(engine or "unknown")] += 1


def _jsonl_files(path: str | Path) -> list[Path]:
    source = Path(path)
    if source.is_dir():
        return sorted(source.glob("*.jsonl"))
    return [source]


def _payload_signal(payload: dict[str, object]) -> dict[str, object]:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        signal = metadata.get("signal")
        if isinstance(signal, dict):
            return signal
    return {}


def _entry_diagnostics_summary(diagnostics: _EntryDiagnostics) -> dict[str, object]:
    return {
        "micro_rejects": diagnostics.micro_rejects,
        "failed_checks": dict(diagnostics.failed_checks),
        "failed_checks_top": diagnostics.failed_checks.most_common(10),
        "reject_reasons": dict(diagnostics.reject_reasons),
        "reject_reasons_top": diagnostics.reject_reasons.most_common(10),
        "candidate_directions": dict(diagnostics.candidate_directions),
        "blocked_by": dict(diagnostics.blocked_by),
        "reject_stages": dict(diagnostics.reject_stages),
        "policy_reasons": dict(diagnostics.policy_reasons),
        "recorded_execution_rejects": dict(diagnostics.recorded_execution_rejects),
        "recorded_live_unsupported_engines": dict(diagnostics.recorded_live_unsupported_engines),
    }


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
