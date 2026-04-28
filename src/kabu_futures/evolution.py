from __future__ import annotations

from bisect import bisect_left
from collections import Counter, defaultdict, deque
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
from .sessions import classify_jst_session


DEFAULT_MARKOUT_SECONDS = (0.5, 1.0, 3.0, 5.0, 30.0, 60.0)
DEFAULT_ENTRY_FILL_SLIPPAGE_TICKS = (0, 1, 2)
DEFAULT_ENTRY_FILL_LATENCY_MS = (0, 100, 250, 500, 1000)


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


@dataclass(frozen=True)
class _BookQuote:
    timestamp: datetime
    symbol: str
    best_bid_price: float
    best_ask_price: float


@dataclass(frozen=True)
class _EntrySignalSample:
    timestamp: datetime
    symbol: str
    direction: Direction
    signal_price: float
    reason: str


@dataclass
class _EntryFillDiagnostics:
    entry_submitted: int = 0
    own_fills_detected: int = 0
    expired_unfilled: int = 0
    api_errors: int = 0
    rejected: int = 0
    timeout_after_grace: int = 0
    cooldown_rejects: int = 0
    positions_detected: int = 0
    external_or_unknown_positions_detected: int = 0
    by_slippage_ticks: defaultdict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))


@dataclass
class _LiveTradeDiagnostics:
    direct_events: int = 0
    reconstructed_trades: int = 0
    pnl_ticks: list[float] = field(default_factory=list)
    pnl_yen: list[float] = field(default_factory=list)
    by_symbol_ticks: defaultdict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    by_symbol_yen: defaultdict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    by_exit_reason: Counter[str] = field(default_factory=Counter)
    entry_price_mismatches: int = 0
    mismatch_samples: list[dict[str, object]] = field(default_factory=list)


@dataclass
class _LoggedPosition:
    symbol: str
    direction: Direction
    qty: int
    entry_price: float
    position_key: str
    hold_id: str | None = None


@dataclass
class _LoggedExitOrder:
    order_id: str
    position_key: str
    symbol: str
    direction: Direction
    qty: int
    exit_reason: str


@dataclass
class _JumpDiagnostics:
    total: int = 0
    by_symbol: Counter[str] = field(default_factory=Counter)
    by_session_phase: Counter[str] = field(default_factory=Counter)
    by_spread_ticks: Counter[str] = field(default_factory=Counter)


@dataclass
class _LoggedDiagnostics:
    entry_fill: _EntryFillDiagnostics = field(default_factory=_EntryFillDiagnostics)
    live_trade: _LiveTradeDiagnostics = field(default_factory=_LiveTradeDiagnostics)
    jump: _JumpDiagnostics = field(default_factory=_JumpDiagnostics)
    recorded_execution_rejects: Counter[str] = field(default_factory=Counter)
    recorded_live_unsupported_engines: Counter[str] = field(default_factory=Counter)
    pending_entry_bucket_by_order_id: dict[str, str] = field(default_factory=dict)
    pending_entry_ids_by_symbol_direction: defaultdict[tuple[str, str], deque[str]] = field(default_factory=lambda: defaultdict(deque))
    live_positions_by_key: dict[str, _LoggedPosition] = field(default_factory=dict)
    live_exit_orders_by_id: dict[str, _LoggedExitOrder] = field(default_factory=dict)
    live_filled_exit_status_by_order_id: dict[str, float] = field(default_factory=dict)
    live_direct_exit_order_ids: set[str] = field(default_factory=set)
    live_entry_order_ids: defaultdict[tuple[str, str], deque[str]] = field(default_factory=lambda: defaultdict(deque))
    live_entry_order_key_by_id: dict[str, tuple[str, str]] = field(default_factory=dict)
    live_entry_executions_by_order_id: dict[str, float] = field(default_factory=dict)


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
    entry_fill_slippage_ticks: Iterable[int] = DEFAULT_ENTRY_FILL_SLIPPAGE_TICKS,
    entry_fill_latency_ms: Iterable[int] = DEFAULT_ENTRY_FILL_LATENCY_MS,
    logged_diagnostics_max_rows: int | None = None,
) -> dict[str, object]:
    horizons = tuple(float(value) for value in markout_seconds)
    fill_slippage_ticks = _non_negative_int_tuple(entry_fill_slippage_ticks, "entry_fill_slippage_ticks")
    fill_latency_ms = _non_negative_int_tuple(entry_fill_latency_ms, "entry_fill_latency_ms")
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
    book_quotes: list[_BookQuote] = []
    entry_signal_samples: list[_EntrySignalSample] = []

    for book in iter_books(path):
        books += 1
        event_time = book_event_time(book)
        book_quotes.append(_BookQuote(event_time, book.symbol, book.best_bid_price, book.best_ask_price))
        current_regime = regime_classifier.update(book) if regime_classifier is not None else None
        if regime_stats is not None and current_regime is not None:
            regime_stats.distribution[current_regime] += 1

        _update_pending_markouts(book, pending_markouts, markout_values, markout_by_reason, config, regime_stats)
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
            if signal.engine == "micro_book" and signal.is_tradeable and signal.price is not None:
                entry_signal_samples.append(
                    _EntrySignalSample(event_time, signal.symbol, signal.direction, float(signal.price), signal.reason)
                )
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
    logged_diagnostics = _record_logged_diagnostics(path, config, logged_diagnostics_max_rows)
    entry_diagnostics.recorded_execution_rejects.update(logged_diagnostics.recorded_execution_rejects)
    entry_diagnostics.recorded_live_unsupported_engines.update(logged_diagnostics.recorded_live_unsupported_engines)
    entry_fill_simulation = _simulate_fak_entry_fills(
        entry_signal_samples,
        book_quotes,
        fill_slippage_ticks,
        fill_latency_ms,
        config,
    )
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
        "entry_fill_diagnostics": _entry_fill_diagnostics_summary(logged_diagnostics.entry_fill, entry_fill_simulation),
        "live_trade_diagnostics": _live_trade_diagnostics_summary(logged_diagnostics.live_trade),
        "jump_diagnostics": _jump_diagnostics_summary(logged_diagnostics.jump),
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


def _non_negative_int_tuple(values: Iterable[int], name: str) -> tuple[int, ...]:
    result = tuple(int(value) for value in values)
    for value in result:
        if value < 0:
            raise ValueError(f"{name} values must be non-negative, got {value}")
    return result


def _record_logged_diagnostics(
    path: str | Path,
    config: StrategyConfig,
    max_rows: int | None = None,
) -> _LoggedDiagnostics:
    diagnostics = _LoggedDiagnostics()
    for row in _iter_jsonl_rows(path, max_rows=max_rows):
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        event_type = _payload_event_type(row, payload)
        reason = str(payload.get("reason") or "")
        _record_logged_execution_reject(diagnostics, event_type, payload)
        _record_logged_entry_fill(diagnostics, event_type, reason, payload, config)
        _record_logged_live_trade(diagnostics, event_type, reason, payload, config)
        _record_logged_jump(diagnostics, row, payload, config)

    for order_id, execution_price in diagnostics.live_filled_exit_status_by_order_id.items():
        if order_id in diagnostics.live_direct_exit_order_ids:
            continue
        exit_order = diagnostics.live_exit_orders_by_id.get(order_id)
        if exit_order is not None:
            _record_reconstructed_live_trade(
                diagnostics.live_trade,
                diagnostics.live_positions_by_key,
                exit_order,
                execution_price,
                config,
            )
    return diagnostics


def _record_logged_execution_reject(
    diagnostics: _LoggedDiagnostics,
    event_type: str,
    payload: dict[str, object],
) -> None:
    if event_type != "execution_reject":
        return
    reason = payload.get("reason") or payload.get("event_type")
    if reason:
        diagnostics.recorded_execution_rejects[str(reason)] += 1
    if reason == "live_unsupported_signal_engine":
        signal = _payload_signal(payload)
        engine = signal.get("engine") if isinstance(signal, dict) else None
        diagnostics.recorded_live_unsupported_engines[str(engine or "unknown")] += 1


def _record_logged_entry_fill(
    diagnostics: _LoggedDiagnostics,
    event_type: str,
    reason: str,
    payload: dict[str, object],
    config: StrategyConfig,
) -> None:
    observed = diagnostics.entry_fill
    if event_type == "live_order_submitted" and _is_entry_submission(payload):
        bucket = _entry_slippage_bucket(payload, config.tick_size_for(_payload_symbol(payload)))
        observed.entry_submitted += 1
        observed.by_slippage_ticks[bucket]["entry_submitted"] += 1
        order_id = _payload_order_id(payload)
        if order_id:
            diagnostics.pending_entry_bucket_by_order_id[order_id] = bucket
            diagnostics.pending_entry_ids_by_symbol_direction[_payload_symbol_direction(payload)].append(order_id)
    elif event_type == "live_position_detected":
        observed.positions_detected += 1
        metadata = _payload_metadata(payload)
        if metadata.get("own_entry_detected") is True:
            bucket = _consume_pending_entry_bucket(
                payload,
                diagnostics.pending_entry_bucket_by_order_id,
                diagnostics.pending_entry_ids_by_symbol_direction,
            )
            observed.own_fills_detected += 1
            observed.by_slippage_ticks[bucket]["own_fills_detected"] += 1
        else:
            observed.external_or_unknown_positions_detected += 1
    elif event_type == "live_order_expired" and reason == "entry_order_expired_or_unfilled":
        bucket = _pending_bucket_for_payload(
            payload,
            diagnostics.pending_entry_bucket_by_order_id,
            config.tick_size_for(_payload_symbol(payload)),
        )
        observed.expired_unfilled += 1
        observed.by_slippage_ticks[bucket]["expired_unfilled"] += 1
    elif event_type == "live_sync_error" and reason == "pending_entry_timeout_after_grace":
        bucket = _pending_bucket_for_payload(
            payload,
            diagnostics.pending_entry_bucket_by_order_id,
            config.tick_size_for(_payload_symbol(payload)),
        )
        observed.timeout_after_grace += 1
        observed.by_slippage_ticks[bucket]["timeout_after_grace"] += 1
    elif event_type == "live_order_error" and reason == "entry_order_api_error":
        bucket = _entry_slippage_bucket(payload, config.tick_size_for(_payload_symbol(payload)))
        observed.api_errors += 1
        observed.by_slippage_ticks[bucket]["api_errors"] += 1
    elif event_type == "live_order_error" and reason == "entry_order_rejected":
        bucket = _entry_slippage_bucket(payload, config.tick_size_for(_payload_symbol(payload)))
        observed.rejected += 1
        observed.by_slippage_ticks[bucket]["rejected"] += 1
    elif event_type == "execution_reject" and reason == "entry_failure_cooldown":
        observed.cooldown_rejects += 1
        observed.by_slippage_ticks["unknown"]["cooldown_rejects"] += 1


def _record_logged_live_trade(
    diagnostics: _LoggedDiagnostics,
    event_type: str,
    reason: str,
    payload: dict[str, object],
    config: StrategyConfig,
) -> None:
    metadata = _payload_metadata(payload)
    if event_type == "live_order_submitted" and _is_entry_submission(payload):
        order_id = _payload_order_id(payload)
        if order_id:
            key = _payload_symbol_direction(payload)
            diagnostics.live_entry_order_ids[key].append(order_id)
            diagnostics.live_entry_order_key_by_id[order_id] = key
    elif event_type == "live_order_submitted" and _is_exit_submission(payload):
        order_id = _payload_order_id(payload)
        position_key = _metadata_text(metadata, "position_key")
        if order_id and position_key:
            diagnostics.live_exit_orders_by_id[order_id] = _LoggedExitOrder(
                order_id,
                position_key,
                _payload_symbol(payload),
                _payload_direction(payload),
                int(payload.get("qty") or 1),
                str(metadata.get("exit_reason") or reason or "unknown"),
            )
    elif event_type == "live_order_status":
        _record_logged_order_status(diagnostics, payload, metadata)
    elif event_type == "live_order_expired" and reason == "entry_order_expired_or_unfilled":
        _discard_logged_entry_order(diagnostics, _payload_order_id(payload))
    elif event_type == "live_sync_error" and reason == "pending_entry_timeout_after_grace":
        _discard_logged_entry_order(diagnostics, _payload_order_id(payload))
    elif event_type == "live_position_detected":
        _record_logged_position_detected(diagnostics, payload, metadata)
    elif event_type == "live_trade_closed":
        diagnostics.live_trade.direct_events += 1
        exit_order_id = _metadata_text(metadata, "exit_order_id")
        if exit_order_id:
            diagnostics.live_direct_exit_order_ids.add(exit_order_id)
        _record_direct_live_trade(diagnostics.live_trade, payload, metadata, config)


def _record_logged_order_status(
    diagnostics: _LoggedDiagnostics,
    payload: dict[str, object],
    metadata: dict[str, object],
) -> None:
    order_id = _payload_order_id(payload)
    status = metadata.get("order_status")
    if not order_id or not isinstance(status, dict):
        return
    execution_price = _status_execution_price(status)
    if execution_price is None:
        return
    if order_id in diagnostics.live_exit_orders_by_id and order_id not in diagnostics.live_direct_exit_order_ids:
        exit_order = diagnostics.live_exit_orders_by_id[order_id]
        if _status_is_filled(status, exit_order.qty):
            diagnostics.live_filled_exit_status_by_order_id[order_id] = execution_price
    elif _status_is_filled(status, 1):
        diagnostics.live_entry_executions_by_order_id[order_id] = execution_price


def _discard_logged_entry_order(diagnostics: _LoggedDiagnostics, order_id: str | None) -> None:
    if order_id is None:
        return
    _remove_entry_order_id(order_id, diagnostics.live_entry_order_ids, diagnostics.live_entry_order_key_by_id)
    diagnostics.live_entry_executions_by_order_id.pop(order_id, None)


def _record_logged_position_detected(
    diagnostics: _LoggedDiagnostics,
    payload: dict[str, object],
    metadata: dict[str, object],
) -> None:
    position = _logged_position_from_payload(payload)
    if position is not None:
        diagnostics.live_positions_by_key[position.position_key] = position
    if metadata.get("entry_price_mismatch") is True:
        _record_entry_price_mismatch(diagnostics.live_trade, payload, metadata)
    elif metadata.get("own_entry_detected") is True:
        order_id = _metadata_text(metadata, "entry_order_id") or _consume_entry_order_id(
            payload,
            diagnostics.live_entry_order_ids,
        )
        if order_id is not None:
            diagnostics.live_entry_order_key_by_id.pop(order_id, None)
        execution_price = _optional_payload_float(metadata.get("entry_execution_price"))
        if execution_price is None and order_id is not None:
            execution_price = diagnostics.live_entry_executions_by_order_id.get(order_id)
        position_entry_price = _optional_payload_float(metadata.get("position_entry_price") or payload.get("entry_price"))
        if execution_price is not None and position_entry_price is not None and abs(execution_price - position_entry_price) > 1e-9:
            _record_entry_price_mismatch(
                diagnostics.live_trade,
                payload,
                {
                    **metadata,
                    "entry_order_id": order_id,
                    "entry_execution_price": execution_price,
                    "position_entry_price": position_entry_price,
                },
            )


def _record_logged_jump(
    diagnostics: _LoggedDiagnostics,
    row: dict[str, object],
    payload: dict[str, object],
    config: StrategyConfig,
) -> None:
    kind = str(row.get("kind") or "")
    if kind not in {"signal_eval", "signal_eval_summary"}:
        return
    reason = str(payload.get("reason") or "")
    metadata = _jump_metadata(payload)
    if reason != "jump_detected" and metadata.get("jump_detected") is not True:
        return
    count = int(payload.get("count") or 1)
    symbol = str(payload.get("symbol") or "unknown")
    timestamp = _payload_timestamp(payload, "start_timestamp") or _payload_timestamp(payload, "timestamp")
    session_phase = classify_jst_session(timestamp, config.session_schedule).phase if timestamp is not None else "unknown"
    spread_ticks = _optional_payload_float(metadata.get("spread_ticks"))
    spread_bucket = _spread_bucket(spread_ticks)
    diagnostics.jump.total += count
    diagnostics.jump.by_symbol[symbol] += count
    diagnostics.jump.by_session_phase[session_phase] += count
    diagnostics.jump.by_spread_ticks[spread_bucket] += count


def _iter_jsonl_rows(path: str | Path, max_rows: int | None = None) -> Iterable[dict[str, object]]:
    yielded = 0
    limit = None if max_rows is None or max_rows <= 0 else int(max_rows)
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
                if isinstance(row, dict):
                    if limit is not None and yielded >= limit:
                        return
                    yielded += 1
                    yield row


def _payload_event_type(row: dict[str, object], payload: dict[str, object]) -> str:
    return str(payload.get("event_type") or payload.get("event") or row.get("kind") or "")


def _is_entry_submission(payload: dict[str, object]) -> bool:
    if payload.get("reason") == "entry_limit_fak_submitted":
        return True
    metadata = _payload_metadata(payload)
    if metadata.get("exit_reason"):
        return False
    order_payload = metadata.get("order_payload")
    return isinstance(order_payload, dict) and str(order_payload.get("TradeType")) == "1"


def _is_exit_submission(payload: dict[str, object]) -> bool:
    metadata = _payload_metadata(payload)
    if metadata.get("exit_reason"):
        return True
    order_payload = metadata.get("order_payload")
    return isinstance(order_payload, dict) and str(order_payload.get("TradeType")) == "2"


def _payload_metadata(payload: dict[str, object]) -> dict[str, object]:
    metadata = payload.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _payload_order_id(payload: dict[str, object]) -> str | None:
    metadata = _payload_metadata(payload)
    for source in (metadata, payload):
        order_id = source.get("order_id") or source.get("OrderId") or source.get("OrderID") or source.get("ID")
        if order_id:
            return str(order_id)
    response = metadata.get("response")
    if isinstance(response, dict):
        order_id = response.get("OrderId") or response.get("OrderID")
        if order_id:
            return str(order_id)
    return None


def _payload_symbol_direction(payload: dict[str, object]) -> tuple[str, str]:
    return (_payload_symbol(payload), str(payload.get("direction") or ""))


def _payload_direction(payload: dict[str, object]) -> Direction:
    direction = str(payload.get("direction") or "")
    return direction if direction in ("long", "short") else "flat"


def _payload_symbol(payload: dict[str, object]) -> str:
    symbol = payload.get("symbol")
    if symbol:
        return str(symbol)
    signal = _payload_signal(payload)
    return str(signal.get("symbol") or "NK225micro")


def _pending_bucket_for_payload(
    payload: dict[str, object],
    pending_by_order_id: dict[str, str],
    tick_size: float,
) -> str:
    order_id = _payload_order_id(payload)
    if order_id is not None:
        bucket = pending_by_order_id.pop(order_id, None)
        if bucket is not None:
            return bucket
    return _entry_slippage_bucket(payload, tick_size)


def _consume_pending_entry_bucket(
    payload: dict[str, object],
    pending_by_order_id: dict[str, str],
    pending_by_symbol_direction: defaultdict[tuple[str, str], deque[str]],
) -> str:
    queue = pending_by_symbol_direction[_payload_symbol_direction(payload)]
    while queue:
        order_id = queue.popleft()
        bucket = pending_by_order_id.pop(order_id, None)
        if bucket is not None:
            return bucket
    return "unknown"


def _consume_entry_order_id(
    payload: dict[str, object],
    entry_order_ids: defaultdict[tuple[str, str], deque[str]],
) -> str | None:
    queue = entry_order_ids[_payload_symbol_direction(payload)]
    while queue:
        return queue.popleft()
    return None


def _remove_entry_order_id(
    order_id: str,
    entry_order_ids: defaultdict[tuple[str, str], deque[str]],
    entry_order_key_by_id: dict[str, tuple[str, str]],
) -> None:
    key = entry_order_key_by_id.pop(order_id, None)
    if key is None:
        return
    queue = entry_order_ids.get(key)
    if queue is None:
        return
    try:
        queue.remove(order_id)
    except ValueError:
        return


def _entry_slippage_bucket(payload: dict[str, object], tick_size: float) -> str:
    metadata = _payload_metadata(payload)
    raw_slippage = metadata.get("entry_slippage_ticks")
    if isinstance(raw_slippage, (int, float, str)) and str(raw_slippage).strip() != "":
        return str(int(float(raw_slippage)))

    signal = _payload_signal(payload)
    direction = str(payload.get("direction") or signal.get("direction") or "")
    signal_price = _float_from_any(metadata.get("entry_signal_price") or signal.get("price") or payload.get("entry_price"))
    order_price = _float_from_any(metadata.get("entry_order_price"))
    order_payload = metadata.get("order_payload")
    if order_price is None and isinstance(order_payload, dict):
        order_price = _float_from_any(order_payload.get("Price"))
    if signal_price is None or order_price is None or tick_size <= 0:
        return "unknown"

    if direction == "long":
        ticks = (order_price - signal_price) / tick_size
    elif direction == "short":
        ticks = (signal_price - order_price) / tick_size
    else:
        return "unknown"
    return str(max(0, int(round(ticks))))


def _float_from_any(value: object) -> float | None:
    if isinstance(value, (int, float, str)) and str(value).strip() != "":
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _optional_payload_float(value: object) -> float | None:
    return _float_from_any(value)


def _metadata_text(metadata: dict[str, object], key: str) -> str | None:
    value = metadata.get(key)
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _logged_position_from_payload(payload: dict[str, object]) -> _LoggedPosition | None:
    metadata = _payload_metadata(payload)
    entry_price = _optional_payload_float(payload.get("entry_price") or metadata.get("position_entry_price"))
    if entry_price is None:
        return None
    position_key = _metadata_text(metadata, "position_key")
    if position_key is None:
        hold_id = _metadata_text(metadata, "hold_id")
        symbol_code = _metadata_text(metadata, "symbol_code") or _payload_symbol(payload)
        position_key = f"hold:{hold_id}" if hold_id else f"single_position_without_hold_id:{symbol_code}"
    qty = int(payload.get("qty") or 1)
    return _LoggedPosition(
        _payload_symbol(payload),
        _payload_direction(payload),
        qty,
        entry_price,
        position_key,
        _metadata_text(metadata, "hold_id"),
    )


def _record_direct_live_trade(
    diagnostics: _LiveTradeDiagnostics,
    payload: dict[str, object],
    metadata: dict[str, object],
    config: StrategyConfig,
) -> None:
    symbol = _payload_symbol(payload)
    pnl_ticks = _optional_payload_float(payload.get("pnl_ticks") or metadata.get("pnl_ticks"))
    pnl_yen = _optional_payload_float(payload.get("pnl_yen") or metadata.get("pnl_yen"))
    if pnl_ticks is None:
        entry_price = _optional_payload_float(payload.get("entry_price") or metadata.get("entry_price"))
        exit_price = _optional_payload_float(payload.get("exit_price") or metadata.get("exit_price"))
        if entry_price is not None and exit_price is not None:
            pnl_ticks = calculate_markout_ticks(_payload_direction(payload), entry_price, exit_price, config.tick_size_for(symbol))
    if pnl_ticks is None:
        return
    qty = int(payload.get("qty") or metadata.get("qty") or 1)
    if pnl_yen is None:
        pnl_yen = pnl_ticks * qty * config.tick_value_yen_for(symbol)
    _append_live_trade(
        diagnostics,
        symbol,
        str(payload.get("reason") or metadata.get("exit_reason") or "unknown"),
        pnl_ticks,
        pnl_yen,
    )


def _record_reconstructed_live_trade(
    diagnostics: _LiveTradeDiagnostics,
    positions_by_key: dict[str, _LoggedPosition],
    exit_order: _LoggedExitOrder,
    exit_price: float,
    config: StrategyConfig,
) -> None:
    position = positions_by_key.get(exit_order.position_key)
    if position is None:
        return
    pnl_ticks = calculate_markout_ticks(position.direction, position.entry_price, exit_price, config.tick_size_for(position.symbol))
    pnl_yen = pnl_ticks * exit_order.qty * config.tick_value_yen_for(position.symbol)
    diagnostics.reconstructed_trades += 1
    _append_live_trade(diagnostics, position.symbol, exit_order.exit_reason, pnl_ticks, pnl_yen)


def _append_live_trade(
    diagnostics: _LiveTradeDiagnostics,
    symbol: str,
    exit_reason: str,
    pnl_ticks: float,
    pnl_yen: float,
) -> None:
    diagnostics.pnl_ticks.append(float(pnl_ticks))
    diagnostics.pnl_yen.append(float(pnl_yen))
    diagnostics.by_symbol_ticks[symbol].append(float(pnl_ticks))
    diagnostics.by_symbol_yen[symbol].append(float(pnl_yen))
    diagnostics.by_exit_reason[exit_reason] += 1


def _record_entry_price_mismatch(
    diagnostics: _LiveTradeDiagnostics,
    payload: dict[str, object],
    metadata: dict[str, object],
) -> None:
    diagnostics.entry_price_mismatches += 1
    if len(diagnostics.mismatch_samples) >= 10:
        return
    diagnostics.mismatch_samples.append(
        {
            "timestamp": payload.get("timestamp"),
            "symbol": _payload_symbol(payload),
            "direction": payload.get("direction"),
            "position_key": metadata.get("position_key"),
            "entry_order_id": metadata.get("entry_order_id"),
            "position_entry_price": metadata.get("position_entry_price") or payload.get("entry_price"),
            "entry_execution_price": metadata.get("entry_execution_price"),
            "entry_execution_id": metadata.get("entry_execution_id"),
        }
    )


def _status_execution_price(status: dict[str, object]) -> float | None:
    direct = _optional_payload_float(status.get("execution_price"))
    if direct is not None:
        return direct
    details = status.get("details")
    if not isinstance(details, list):
        return _optional_payload_float(status.get("price")) if _optional_payload_float(status.get("cum_qty")) else None
    executions: list[tuple[float, float]] = []
    for detail in details:
        if not isinstance(detail, dict):
            continue
        price = _optional_payload_float(detail.get("price"))
        if price is None:
            continue
        if detail.get("execution_id") or str(detail.get("rec_type")) == "8":
            executions.append((price, _optional_payload_float(detail.get("qty")) or 1.0))
    if executions:
        total_qty = sum(qty for _, qty in executions)
        return sum(price * qty for price, qty in executions) / total_qty if total_qty > 0 else executions[-1][0]
    if _optional_payload_float(status.get("cum_qty")):
        return _optional_payload_float(status.get("price"))
    return None


def _status_is_filled(status: dict[str, object], qty: int) -> bool:
    cum_qty = _optional_payload_float(status.get("cum_qty")) or 0.0
    state = str(status.get("state") or "")
    order_state = str(status.get("order_state") or "")
    return cum_qty >= float(qty) and (state in {"5", "5.0"} or order_state in {"5", "5.0"})


def _jump_metadata(payload: dict[str, object]) -> dict[str, object]:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    first = payload.get("first_metadata")
    if isinstance(first, dict):
        return first
    last = payload.get("last_metadata")
    if isinstance(last, dict):
        return last
    return {}


def _payload_timestamp(payload: dict[str, object], key: str) -> datetime | None:
    value = payload.get(key)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _spread_bucket(spread_ticks: float | None) -> str:
    if spread_ticks is None:
        return "unknown"
    if spread_ticks < 1:
        return "<1"
    if spread_ticks >= 10:
        return ">=10"
    return str(int(spread_ticks))


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


def _entry_fill_diagnostics_summary(
    observed: _EntryFillDiagnostics,
    simulation: dict[str, object],
) -> dict[str, object]:
    return {
        "observed_live_funnel": {
            "entry_submitted": observed.entry_submitted,
            "own_fills_detected": observed.own_fills_detected,
            "expired_unfilled": observed.expired_unfilled,
            "api_errors": observed.api_errors,
            "rejected": observed.rejected,
            "timeout_after_grace": observed.timeout_after_grace,
            "cooldown_rejects": observed.cooldown_rejects,
            "positions_detected": observed.positions_detected,
            "external_or_unknown_positions_detected": observed.external_or_unknown_positions_detected,
            "fill_rate": _safe_ratio(observed.own_fills_detected, observed.entry_submitted),
            "by_slippage_ticks": {
                bucket: _entry_fill_bucket_summary(counts)
                for bucket, counts in sorted(observed.by_slippage_ticks.items(), key=lambda item: _bucket_sort_key(item[0]))
            },
        },
        "fak_fill_simulation": simulation,
    }


def _live_trade_diagnostics_summary(diagnostics: _LiveTradeDiagnostics) -> dict[str, object]:
    trades = len(diagnostics.pnl_ticks)
    net_yen = sum(diagnostics.pnl_yen)
    return {
        **pnl_summary(diagnostics.pnl_ticks),
        "net_pnl_yen": round(net_yen, 2),
        "avg_pnl_yen": round(net_yen / trades, 2) if trades else 0.0,
        "direct_events": diagnostics.direct_events,
        "reconstructed_trades": diagnostics.reconstructed_trades,
        "by_symbol": {
            symbol: {
                **pnl_summary(diagnostics.by_symbol_ticks[symbol]),
                "net_pnl_yen": round(sum(diagnostics.by_symbol_yen[symbol]), 2),
            }
            for symbol in sorted(diagnostics.by_symbol_ticks)
        },
        "by_exit_reason": dict(diagnostics.by_exit_reason),
        "entry_price_mismatches": diagnostics.entry_price_mismatches,
        "entry_price_mismatch_samples": diagnostics.mismatch_samples,
    }


def _jump_diagnostics_summary(diagnostics: _JumpDiagnostics) -> dict[str, object]:
    return {
        "total": diagnostics.total,
        "by_symbol": dict(diagnostics.by_symbol),
        "by_session_phase": dict(diagnostics.by_session_phase),
        "by_spread_ticks": dict(diagnostics.by_spread_ticks),
    }


def _entry_fill_bucket_summary(counts: Counter[str]) -> dict[str, object]:
    submitted = int(counts.get("entry_submitted", 0))
    fills = int(counts.get("own_fills_detected", 0))
    return {
        "entry_submitted": submitted,
        "own_fills_detected": fills,
        "expired_unfilled": int(counts.get("expired_unfilled", 0)),
        "api_errors": int(counts.get("api_errors", 0)),
        "rejected": int(counts.get("rejected", 0)),
        "timeout_after_grace": int(counts.get("timeout_after_grace", 0)),
        "cooldown_rejects": int(counts.get("cooldown_rejects", 0)),
        "fill_rate": _safe_ratio(fills, submitted),
    }


def _simulate_fak_entry_fills(
    samples: list[_EntrySignalSample],
    book_quotes: list[_BookQuote],
    slippage_ticks: tuple[int, ...],
    latency_ms: tuple[int, ...],
    config: StrategyConfig,
) -> dict[str, object]:
    quotes_by_symbol: defaultdict[str, list[_BookQuote]] = defaultdict(list)
    for quote in book_quotes:
        quotes_by_symbol[quote.symbol].append(quote)
    quote_times_by_symbol: dict[str, list[datetime]] = {}
    for symbol, quotes in quotes_by_symbol.items():
        quotes.sort(key=lambda quote: quote.timestamp)
        quote_times_by_symbol[symbol] = [quote.timestamp for quote in quotes]

    by_slippage_latency: dict[str, dict[str, dict[str, object]]] = {}
    for slippage in slippage_ticks:
        slippage_key = str(slippage)
        by_slippage_latency[slippage_key] = {}
        for latency in latency_ms:
            fills = 0
            no_future_quote = 0
            for sample in samples:
                quote = _future_quote(sample, latency, quotes_by_symbol, quote_times_by_symbol)
                if quote is None:
                    no_future_quote += 1
                    continue
                limit_price = _simulated_entry_limit(
                    sample.direction,
                    sample.signal_price,
                    slippage,
                    config.tick_size_for(sample.symbol),
                )
                if _would_fak_fill(sample.direction, limit_price, quote):
                    fills += 1
            total = len(samples)
            misses = max(0, total - fills - no_future_quote)
            by_slippage_latency[slippage_key][str(latency)] = {
                "signals": total,
                "fills": fills,
                "misses": misses,
                "no_future_quote": no_future_quote,
                "fill_rate": _safe_ratio(fills, total),
            }

    return {
        "total_signals": len(samples),
        "slippage_ticks": list(slippage_ticks),
        "latency_ms": list(latency_ms),
        "by_slippage_latency": by_slippage_latency,
    }


def _future_quote(
    sample: _EntrySignalSample,
    latency_ms: int,
    quotes_by_symbol: dict[str, list[_BookQuote]],
    quote_times_by_symbol: dict[str, list[datetime]],
) -> _BookQuote | None:
    quotes = quotes_by_symbol.get(sample.symbol)
    times = quote_times_by_symbol.get(sample.symbol)
    if not quotes or not times:
        return None
    target_time = sample.timestamp + timedelta(milliseconds=latency_ms)
    index = bisect_left(times, target_time)
    if index >= len(quotes):
        return None
    return quotes[index]


def _simulated_entry_limit(direction: Direction, signal_price: float, slippage_ticks: int, tick_size: float) -> float:
    offset = slippage_ticks * tick_size
    if direction == "long":
        return signal_price + offset
    if direction == "short":
        return signal_price - offset
    return signal_price


def _would_fak_fill(direction: Direction, limit_price: float, quote: _BookQuote) -> bool:
    if direction == "long":
        return limit_price >= quote.best_ask_price
    if direction == "short":
        return limit_price <= quote.best_bid_price
    return False


def _safe_ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _bucket_sort_key(value: str) -> tuple[int, float | str]:
    try:
        return (0, float(value))
    except ValueError:
        return (1, value)


def _update_pending_markouts(
    book: OrderBook,
    pending_markouts: list[_PendingMarkout],
    markout_values: defaultdict[str, list[float]],
    markout_by_reason: defaultdict[str, defaultdict[str, list[float]]],
    config: StrategyConfig,
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
                value = calculate_markout_ticks(
                    entry.direction,
                    entry.entry_price,
                    book.mid_price,
                    config.tick_size_for(entry.symbol),
                )
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
