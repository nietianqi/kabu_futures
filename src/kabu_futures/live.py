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
from .marketdata import BufferedJsonlMarketRecorder, KabuBoardNormalizer, KabuWebSocketStream, MarketDataError
from .models import OrderBook, Signal
from .paper_execution import ExecutionEvent, PaperExecutionController, PaperFillModel, TradeMode
from .sessions import SessionState, classify_jst_session


BookLogMode = Literal["full", "sample", "signals_only", "off"]
TickLogMode = Literal["off", "sample", "changes", "all"]


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
    clear_registered_symbols: bool = True
    trade_mode: TradeMode = "observe"
    paper_fill_model: PaperFillModel = "immediate"
    paper_console: bool = True
    live_orders: bool = False


def signal_to_dict(signal: Signal) -> dict[str, Any]:
    return {
        "engine": signal.engine,
        "symbol": signal.symbol,
        "direction": signal.direction,
        "confidence": signal.confidence,
        "price": signal.price,
        "reason": signal.reason,
        "metadata": signal.metadata,
        "score": signal.score,
        "signal_horizon": signal.signal_horizon,
        "expected_hold_seconds": signal.expected_hold_seconds,
        "risk_budget_pct": signal.risk_budget_pct,
        "veto_reason": signal.veto_reason,
        "position_scale": signal.position_scale,
    }


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
    future_codes = options.future_codes or (config.symbols.primary, config.symbols.filter)
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
            "clear_registered_symbols": options.clear_registered_symbols,
            "trade_mode": options.trade_mode,
            "paper_fill_model": options.paper_fill_model,
            "paper_console": options.paper_console,
            "live_orders": options.live_orders,
            "live_supported_engines": config.live_execution.supported_engines,
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
    processed = 0
    market_data_errors = 0
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
                        recorder.write("signal_eval", evaluation)
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
                        elapsed = max(0.001, time.perf_counter() - started_perf)
                        heartbeat = {
                            "event": "heartbeat",
                            "books": processed,
                            "books_per_sec": round(processed / elapsed, 2),
                            "avg_process_ms": round(process_time_total_ms / processed, 4),
                            "market_data_errors": market_data_errors,
                            "signals_count": signals_count,
                            "signal_eval_count": signal_eval_count,
                            "signal_allow_count": signal_allow_count,
                            "signal_reject_count": signal_reject_count,
                            "execution_events_count": execution_events_count,
                            "last_symbol": book.symbol,
                            "raw_symbol": book.raw_symbol,
                            "ts": book.timestamp.isoformat(),
                            **_session_state_to_dict(session_state),
                        }
                        heartbeat.update(execution.heartbeat_metadata())
                        recorder.write("heartbeat", heartbeat)
                        print(json.dumps(heartbeat, ensure_ascii=False))
                    if options.max_events is not None and processed >= options.max_events:
                        recorder.write("max_events_reached", {"books": processed}, force_flush=True)
                        print(json.dumps({"event": "max_events_reached", "books": processed}, ensure_ascii=False))
                        return 0
            except (RuntimeError, OSError) as exc:
                recorder.write("live_error", {"error": str(exc), "books": processed}, force_flush=True)
                print(json.dumps({"event": "live_error", "error": str(exc), "reconnect_seconds": options.reconnect_seconds}, ensure_ascii=False))
                time.sleep(options.reconnect_seconds)
    except KeyboardInterrupt:
        recorder.write("stopped_by_user", {"books": processed}, force_flush=True)
        print(json.dumps({"event": "stopped_by_user", "books": processed}, ensure_ascii=False))
        return 0
    finally:
        recorder.close()
