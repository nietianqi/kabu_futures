from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Literal

from .api import KabuStationClient, build_future_registration_symbols
from .config import StrategyConfig
from .engine import DualStrategyEngine
from .live_execution import LiveExecutionController
from .marketdata import BufferedJsonlMarketRecorder, KabuBoardNormalizer, KabuWebSocketStream, MarketDataError, MarketDataSkip
from .micro_candidates import candidate_metadata_snapshot, near_miss_key
from .models import OrderBook, SignalEvaluation
from .paper_execution import ExecutionEvent, PaperExecutionController, PaperFillModel, TradeMode
from .runtime import live_startup_self_check
from .serialization import signal_to_dict
from .sessions import SessionState, classify_jst_session


BookLogMode = Literal["full", "sample", "signals_only", "off"]
TickLogMode = Literal["off", "sample", "changes", "all"]
SignalEvalLogMode = Literal["full", "summary", "allow_only", "off"]


@dataclass(frozen=True)
class LiveRunOptions:
    production: bool = True
    exchanges: tuple[int, ...] = (23, 24)
    future_codes: tuple[str, ...] = ()
    deriv_month: int | None = None
    dry_run: bool = False
    log_dir: Path = Path("logs")
    reconnect_seconds: int = 5
    max_events: int | None = None
    book_log_mode: BookLogMode = "full"
    log_batch_size: int = 256
    log_flush_interval_seconds: float = 1.0
    error_console_sample_rate: int = 20
    heartbeat_interval_events: int = 100
    tick_log_mode: TickLogMode = "off"
    tick_log_interval_events: int = 1
    signal_eval_log_mode: SignalEvalLogMode = "summary"
    clear_registered_symbols: bool = True
    trade_mode: TradeMode = "observe"
    paper_fill_model: PaperFillModel = "immediate"
    paper_console: bool = True
    live_orders: bool = False


def tick_to_dict(book: OrderBook, processed: int) -> dict[str, Any]:
    return {
        "event": "tick",
        "books": processed,
        "symbol": book.symbol,
        "raw_symbol": book.raw_symbol,
        "ts": book.timestamp.isoformat(),
        "bid": book.best_bid_price,
        "bid_qty": book.best_bid_qty,
        "ask": book.best_ask_price,
        "ask_qty": book.best_ask_qty,
        "spread": book.spread,
        "mid": book.mid_price,
        "last": book.last_price,
        "volume": book.volume,
    }


def _tick_state(book: OrderBook) -> tuple[float, float, float, float, float | None, float]:
    return (
        book.best_bid_price,
        book.best_ask_price,
        book.best_bid_qty,
        book.best_ask_qty,
        book.last_price,
        book.volume,
    )


def _should_print_tick(
    mode: TickLogMode,
    processed: int,
    interval: int,
    book: OrderBook,
    last_tick_state: dict[str, tuple[float, float, float, float, float | None, float]],
) -> bool:
    if mode == "off":
        return False
    if mode == "all":
        return True
    if mode == "sample":
        return processed % max(1, interval) == 0
    return last_tick_state.get(book.symbol) != _tick_state(book)


def _symbol_aliases(resolved: list[dict[str, Any]]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for item in resolved:
        raw_symbol = item.get("Symbol")
        future_code = item.get("FutureCode")
        if raw_symbol is not None and future_code is not None:
            aliases[str(raw_symbol)] = str(future_code)
    return aliases


def _symbol_codes(resolved: list[dict[str, Any]]) -> dict[str, str]:
    codes: dict[str, str] = {}
    for item in resolved:
        future_code = item.get("FutureCode")
        symbol = item.get("Symbol")
        if future_code is not None and symbol is not None:
            codes[str(future_code)] = str(symbol)
    return codes


def _execution_exchange(session_phase: str, exchanges: tuple[int, ...]) -> int:
    if session_phase.startswith("night") and 24 in exchanges:
        return 24
    if session_phase.startswith("day") and 23 in exchanges:
        return 23
    if session_phase == "between_sessions" and 24 in exchanges:
        return 24
    if session_phase in ("api_prepare", "day_preopen") and 23 in exchanges:
        return 23
    return exchanges[0] if exchanges else 23


def _should_log_book(mode: BookLogMode, processed: int, sample_interval: int) -> bool:
    if mode == "full":
        return True
    if mode == "sample":
        return processed % max(1, sample_interval) == 0
    return False


def _compact_eval_metadata(metadata: dict[str, object]) -> dict[str, object]:
    keys = (
        "spread_ticks",
        "spread_required_ticks",
        "spread_ok",
        "imbalance",
        "imbalance_entry",
        "imbalance_long_ok",
        "imbalance_short_ok",
        "ofi_ewma",
        "ofi_threshold",
        "ofi_percentile",
        "ofi_long_ok",
        "ofi_short_ok",
        "microprice_edge_ticks",
        "microprice_entry_ticks",
        "microprice_long_ok",
        "microprice_short_ok",
        "minute_bias",
        "topix_bias",
        "jump_detected",
        "jump_reason",
        "too_soon",
        "latency_ms",
        "long_failed_checks",
        "short_failed_checks",
        "long_near_miss",
        "short_near_miss",
        "near_miss",
        "near_miss_direction",
        "near_miss_missing",
        "reject_stage",
        "throttle_ok",
        "throttle_reason",
        "mtf_ok",
        "mtf_reason",
        "risk_ok",
        "risk_reason",
        "alpha_ok",
        "alpha_reason",
        "total_score",
        "veto_reason",
        "position_scale",
    )
    return {key: metadata[key] for key in keys if key in metadata}


class SignalEvaluationLogger:
    """Write signal evaluations with optional reject aggregation."""

    def __init__(self, recorder: BufferedJsonlMarketRecorder, mode: SignalEvalLogMode = "summary") -> None:
        self.recorder = recorder
        self.mode = mode
        self._summary: dict[str, object] | None = None
        self._summary_key: tuple[object, ...] | None = None

    def write(self, evaluation: SignalEvaluation) -> None:
        if self.mode == "off":
            return
        if self.mode == "full":
            self.recorder.write("signal_eval", evaluation)
            return
        if evaluation.decision == "allow":
            self.flush()
            if self.mode in ("summary", "allow_only"):
                self.recorder.write("signal_eval", evaluation, force_flush=True)
            return
        if self.mode == "allow_only":
            return

        metadata = evaluation.metadata
        key = (
            evaluation.engine,
            evaluation.symbol,
            evaluation.reason,
            evaluation.candidate_direction,
            metadata.get("reject_stage"),
            metadata.get("near_miss"),
            metadata.get("near_miss_direction"),
            metadata.get("near_miss_missing"),
        )
        if self._summary_key != key:
            self.flush()
            self._summary_key = key
            self._summary = {
                "engine": evaluation.engine,
                "symbol": evaluation.symbol,
                "decision": evaluation.decision,
                "reason": evaluation.reason,
                "candidate_direction": evaluation.candidate_direction,
                "reject_stage": metadata.get("reject_stage"),
                "count": 0,
                "start_timestamp": evaluation.timestamp.isoformat(),
                "end_timestamp": evaluation.timestamp.isoformat(),
                "first_metadata": _compact_eval_metadata(metadata),
                "last_metadata": _compact_eval_metadata(metadata),
            }
        assert self._summary is not None
        self._summary["count"] = int(self._summary["count"]) + 1
        self._summary["end_timestamp"] = evaluation.timestamp.isoformat()
        self._summary["last_metadata"] = _compact_eval_metadata(metadata)

    def flush(self) -> None:
        if self._summary is None:
            return
        self.recorder.write("signal_eval_summary", self._summary)
        self._summary = None
        self._summary_key = None


class MicroCandidateEmitter:
    """Emit ``micro_candidate`` JSONL events for near-miss micro evaluations.

    A candidate is recorded only when the underlying ``SignalEvaluation``
    metadata flags ``near_miss=True``. The emitter throttles per
    ``(symbol, direction, missing_check)`` so a quiet near-miss only logs once
    every ``min_interval_seconds`` (default 30s). Aggregation counts how many
    near-misses were collapsed into the throttled window.

    The emitter does NOT generate any orders or strategy signals — it is purely
    diagnostic. Output is the JSONL ``kind=micro_candidate`` event with the same
    metadata shape SignalEvaluation already produces, plus a ``candidate_count``
    field for the throttled total.
    """

    def __init__(
        self,
        recorder: BufferedJsonlMarketRecorder,
        min_interval_seconds: float = 30.0,
        enabled: bool = True,
    ) -> None:
        self.recorder = recorder
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.enabled = enabled
        self._last_emit_at: dict[tuple[str, str, str], datetime] = {}
        self._pending_count: dict[tuple[str, str, str], int] = {}
        self.total_candidates = 0
        self.emitted_candidates = 0

    def write(self, evaluation: SignalEvaluation) -> None:
        if not self.enabled:
            return
        metadata = evaluation.metadata
        if not isinstance(metadata, dict):
            return
        if not metadata.get("near_miss"):
            return
        # Use SignalEvaluation.engine to scope this to micro evaluations only.
        if evaluation.engine != "micro_book":
            return
        # Identify which direction is the near-miss and the single missing check.
        key_parts = near_miss_key(metadata)
        if key_parts is None:
            return
        direction, missing = key_parts
        key = (str(evaluation.symbol), str(direction), str(missing))
        self.total_candidates += 1
        self._pending_count[key] = self._pending_count.get(key, 0) + 1
        last = self._last_emit_at.get(key)
        ts = evaluation.timestamp
        if last is not None and self.min_interval_seconds > 0:
            elapsed = (ts - last).total_seconds()
            if elapsed < self.min_interval_seconds:
                return
        self._emit(evaluation, direction, missing, key, ts)

    def _emit(
        self,
        evaluation: SignalEvaluation,
        direction: object,
        missing: object,
        key: tuple[str, str, str],
        timestamp: datetime,
    ) -> None:
        candidate_count = self._pending_count.get(key, 1)
        payload: dict[str, object] = {
            "engine": evaluation.engine,
            "symbol": evaluation.symbol,
            "candidate_direction": evaluation.candidate_direction,
            "near_miss_direction": direction,
            "near_miss_missing": missing,
            "candidate_count": candidate_count,
            "timestamp": timestamp.isoformat(),
            "metadata": candidate_metadata_snapshot(evaluation.metadata),
        }
        self.recorder.write("micro_candidate", payload)
        self._last_emit_at[key] = timestamp
        self._pending_count[key] = 0
        self.emitted_candidates += 1

def _write_execution_events(
    recorder: BufferedJsonlMarketRecorder,
    events: list[ExecutionEvent],
    console: bool,
) -> None:
    for event in events:
        payload = event.to_dict()
        recorder.write(event.event_type, payload, force_flush=True)
        if console:
            print(json.dumps(payload, ensure_ascii=False))


def _session_state_to_dict(state: SessionState) -> dict[str, Any]:
    return {
        "session_phase": state.phase,
        "session_window_jst": state.window_jst,
        "new_entries_allowed": state.new_entries_allowed,
        "api_window_status": state.api_window_status,
    }


def run_live(config: StrategyConfig, password: str, options: LiveRunOptions | None = None) -> int:
    options = options or LiveRunOptions()
    startup_session = classify_jst_session(datetime.now(timezone.utc), config.session_schedule)
    if startup_session.phase == "api_maintenance":
        print(
            json.dumps(
                {
                    "event": "startup_blocked",
                    "reason": "api_maintenance",
                    **_session_state_to_dict(startup_session),
                },
                ensure_ascii=False,
            )
        )
        return 2
    if options.trade_mode == "live" and not options.live_orders:
        print(
            json.dumps(
                {
                    "event": "startup_blocked",
                    "reason": "live_orders_flag_required",
                    "message": "Pass --live-orders together with --trade-mode live to send real kabu orders.",
                    **_session_state_to_dict(startup_session),
                },
                ensure_ascii=False,
            )
        )
        return 2
    future_codes = options.future_codes or tuple(dict.fromkeys((*config.trade_symbols(), config.symbols.filter)))
    deriv_month = options.deriv_month if options.deriv_month is not None else config.symbols.deriv_month
    client = KabuStationClient(password, config.api, production=options.production)
    token = client.authenticate()
    register_payload, resolved = build_future_registration_symbols(
        client,
        list(future_codes),
        deriv_month,
        list(options.exchanges),
    )
    if not options.dry_run:
        if options.clear_registered_symbols:
            client.unregister_all()
        client.register(register_payload)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = options.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    recorder = BufferedJsonlMarketRecorder(
        log_dir / f"live_{run_ts}.jsonl",
        batch_size=options.log_batch_size,
        flush_interval_seconds=options.log_flush_interval_seconds,
    )
    startup_self_check = live_startup_self_check(config)
    recorder.write(
        "startup",
        {
            "mode": "production" if options.production else "sandbox",
            "base_url": client.base_url,
            "websocket_url": client.websocket_base_url(),
            "token_present": bool(token),
            "deriv_month": deriv_month,
            "exchanges": options.exchanges,
            "resolved": resolved,
            "register_symbols": register_payload,
            "dry_run": options.dry_run,
            "book_log_mode": options.book_log_mode,
            "log_batch_size": options.log_batch_size,
            "log_flush_interval_seconds": options.log_flush_interval_seconds,
            "tick_log_mode": options.tick_log_mode,
            "signal_eval_log_mode": options.signal_eval_log_mode,
            "clear_registered_symbols": options.clear_registered_symbols,
            "trade_mode": options.trade_mode,
            "paper_fill_model": options.paper_fill_model,
            "paper_console": options.paper_console,
            "live_orders": options.live_orders,
            **startup_self_check,
            **_session_state_to_dict(startup_session),
            "no_new_entry_windows_jst": config.micro_engine.no_new_entry_windows_jst,
        },
        force_flush=True,
    )
    print(
        json.dumps(
            {
                "event": "startup",
                "log": str(recorder.path),
                "resolved": resolved,
                "trade_mode": options.trade_mode,
                "live_orders": options.live_orders,
                "code_fingerprint": startup_self_check["code_fingerprint"],
                "config_fingerprint": startup_self_check["config_fingerprint"],
                "micro_entry_profile": startup_self_check["micro_entry_profile"],
                "micro_effective_thresholds": startup_self_check["micro_effective_thresholds"],
                "live_minute_atr_filter": startup_self_check["live_minute_atr_filter"],
                "min_execution_score_to_chase": startup_self_check["min_execution_score_to_chase"],
                **_session_state_to_dict(startup_session),
            },
            ensure_ascii=False,
        )
    )
    if options.dry_run:
        recorder.write("dry_run_complete", {"register_symbols": register_payload}, force_flush=True)
        recorder.close()
        print(json.dumps({"event": "dry_run_complete", "register_symbols": register_payload}, ensure_ascii=False))
        return 0

    engine = DualStrategyEngine(config)
    if options.trade_mode == "live":
        execution = LiveExecutionController(client, config, _symbol_codes(resolved))
    else:
        execution = PaperExecutionController(config, options.trade_mode, options.paper_fill_model)
    normalizer = KabuBoardNormalizer(symbol_aliases=_symbol_aliases(resolved))
    stream = KabuWebSocketStream(client.websocket_base_url(), normalizer)
    signal_eval_logger = SignalEvaluationLogger(recorder, options.signal_eval_log_mode)
    micro_candidate_emitter = MicroCandidateEmitter(recorder)
    processed = 0
    market_data_errors = 0
    market_data_skips = 0
    signals_count = 0
    signal_eval_count = 0
    signal_allow_count = 0
    signal_reject_count = 0
    execution_events_count = 0
    process_time_total_ms = 0.0
    started_perf = time.perf_counter()
    heartbeat_interval = max(1, options.heartbeat_interval_events)
    error_sample_rate = max(1, options.error_console_sample_rate)
    tick_interval = max(1, options.tick_log_interval_events)
    last_tick_state: dict[str, tuple[float, float, float, float, float | None, float]] = {}
    try:
        while True:
            try:
                for raw, received_at in stream.iter_raw():
                    event_start = time.perf_counter()
                    try:
                        book = normalizer.normalize_raw(raw, received_at=received_at)
                    except MarketDataSkip as exc:
                        market_data_skips += 1
                        recorder.write(
                            "market_data_skip",
                            exc.to_payload(raw, received_at),
                            force_flush=market_data_skips == 1 or market_data_skips % error_sample_rate == 0,
                        )
                        if market_data_skips == 1 or market_data_skips % error_sample_rate == 0:
                            print(
                                json.dumps(
                                    {
                                        "event": "market_data_skip",
                                        "reason": str(exc),
                                        "count": market_data_skips,
                                    },
                                    ensure_ascii=False,
                                )
                            )
                        continue
                    except (MarketDataError, ValueError, json.JSONDecodeError) as exc:
                        market_data_errors += 1
                        recorder.write(
                            "market_data_error",
                            {"error": str(exc), "raw": raw, "received_at": received_at.isoformat()},
                            force_flush=True,
                        )
                        if market_data_errors == 1 or market_data_errors % error_sample_rate == 0:
                            print(
                                json.dumps(
                                    {
                                        "event": "market_data_error",
                                        "error": str(exc),
                                        "count": market_data_errors,
                                    },
                                    ensure_ascii=False,
                                )
                            )
                        continue
                    processed += 1
                    if _should_log_book(options.book_log_mode, processed, heartbeat_interval):
                        recorder.write_book(book)
                    if _should_print_tick(options.tick_log_mode, processed, tick_interval, book, last_tick_state):
                        print(json.dumps(tick_to_dict(book, processed), ensure_ascii=False))
                    last_tick_state[book.symbol] = _tick_state(book)
                    session_state = classify_jst_session(book.timestamp, config.session_schedule)
                    exchange = _execution_exchange(session_state.phase, options.exchanges)
                    signals = engine.on_order_book(book)
                    signal_evaluations = engine.latest_signal_evaluations
                    signal_eval_count += len(signal_evaluations)
                    signal_allow_count += sum(1 for evaluation in signal_evaluations if evaluation.decision == "allow")
                    signal_reject_count += sum(1 for evaluation in signal_evaluations if evaluation.decision == "reject")
                    for evaluation in signal_evaluations:
                        signal_eval_logger.write(evaluation)
                        micro_candidate_emitter.write(evaluation)
                    if options.trade_mode == "live":
                        execution_events = execution.on_book(book, engine.latest_book_features, exchange)
                    else:
                        execution_events = execution.on_book(book, engine.latest_book_features)
                    execution_events_count += len(execution_events)
                    _write_execution_events(recorder, execution_events, options.paper_console)
                    signals_count += len(signals)
                    for signal in signals:
                        recorder.write_signal(signal, force_flush=True)
                        print(json.dumps({"event": "signal", **signal_to_dict(signal)}, ensure_ascii=False))
                        if not signal.is_tradeable:
                            continue
                        if options.trade_mode == "live":
                            execution_events = execution.on_signal(signal, book, exchange)
                        else:
                            execution_events = execution.on_signal(signal, book)
                        execution_events_count += len(execution_events)
                        _write_execution_events(recorder, execution_events, options.paper_console)
                    process_time_total_ms += (time.perf_counter() - event_start) * 1000.0
                    if processed % heartbeat_interval == 0:
                        signal_eval_logger.flush()
                        elapsed = max(0.001, time.perf_counter() - started_perf)
                        heartbeat = {
                            "event": "heartbeat",
                            "books": processed,
                            "books_per_sec": round(processed / elapsed, 2),
                            "avg_process_ms": round(process_time_total_ms / processed, 4),
                            "market_data_errors": market_data_errors,
                            "market_data_skips": market_data_skips,
                            "signals_count": signals_count,
                            "signal_eval_count": signal_eval_count,
                            "signal_allow_count": signal_allow_count,
                            "signal_reject_count": signal_reject_count,
                            "micro_candidate_count": micro_candidate_emitter.total_candidates,
                            "micro_candidate_events": micro_candidate_emitter.emitted_candidates,
                            "execution_events_count": execution_events_count,
                            "last_symbol": book.symbol,
                            "raw_symbol": book.raw_symbol,
                            "ts": book.timestamp.isoformat(),
                            "websocket_last_receive_at": received_at.isoformat(),
                            "websocket_seconds_since_last_message": round(
                                (datetime.now(timezone.utc) - received_at).total_seconds(),
                                4,
                            ),
                            **_session_state_to_dict(session_state),
                        }
                        heartbeat.update(execution.heartbeat_metadata())
                        recorder.write("heartbeat", heartbeat)
                        print(json.dumps(heartbeat, ensure_ascii=False))
                    if options.max_events is not None and processed >= options.max_events:
                        signal_eval_logger.flush()
                        recorder.write("max_events_reached", {"books": processed}, force_flush=True)
                        print(json.dumps({"event": "max_events_reached", "books": processed}, ensure_ascii=False))
                        return 0
            except (RuntimeError, OSError) as exc:
                signal_eval_logger.flush()
                recorder.write("live_error", {"error": str(exc), "books": processed}, force_flush=True)
                print(json.dumps({"event": "live_error", "error": str(exc), "reconnect_seconds": options.reconnect_seconds}, ensure_ascii=False))
                time.sleep(options.reconnect_seconds)
    except KeyboardInterrupt:
        signal_eval_logger.flush()
        recorder.write("stopped_by_user", {"books": processed}, force_flush=True)
        print(json.dumps({"event": "stopped_by_user", "books": processed}, ensure_ascii=False))
        return 0
    finally:
        signal_eval_logger.flush()
        recorder.close()
