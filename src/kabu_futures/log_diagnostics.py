from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import Any, Iterator

from .config import StrategyConfig, default_config


LOSS_SAMPLE_LIMIT = 200
MINUTE_SIGNAL_ENGINES = frozenset(("minute_orb", "minute_vwap", "directional_intraday"))


def diagnose_log(source: str | Path, config: StrategyConfig | None = None, max_rows: int | None = None) -> dict[str, object]:
    cfg = config or default_config()
    paths = _jsonl_paths(source)
    counters: dict[str, Any] = {
        "files": [str(path) for path in paths],
        "rows": 0,
        "events": Counter(),
        "api_errors": Counter(),
        "expired_orders": Counter(),
        "symbol_mapping_issues": Counter(),
        "minute_live_filter_violations": 0,
        "startup_checks": [],
        "loss_samples": [],
        "loss_sample_limit": LOSS_SAMPLE_LIMIT,
        "losses_total": 0,
        "signal_eval_decisions": Counter(),
        "signal_eval_engines": Counter(),
        "signal_eval_reject_reasons": Counter(),
        "signal_eval_failed_checks": Counter(),
        "execution_rejects": Counter(),
        "execution_reject_blocked_by": Counter(),
        "live_unsupported_minute_signals": 0,
        "live_entry_orders_submitted": 0,
        "live_entry_orders_expired": 0,
        "live_entry_order_errors": 0,
        "live_positions_detected": 0,
        "live_trades_closed": 0,
        "live_exit_orders_submitted": 0,
        "suspected_old_live_policy": False,
    }
    pnl_by_engine: dict[str, dict[str, float]] = defaultdict(_pnl_bucket)
    pnl_by_exit_reason: dict[str, dict[str, float]] = defaultdict(_pnl_bucket)
    symbol_issue_samples: list[dict[str, object]] = []
    entry_signals_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    hold_entries: dict[str, dict[str, Any]] = {}
    exit_orders: dict[str, dict[str, Any]] = {}
    filled_exit_records: list[dict[str, Any]] = []
    recorded_exit_order_ids: set[str] = set()

    for row in _iter_jsonl_rows(paths):
        if max_rows is not None and counters["rows"] >= max_rows:
            break
        counters["rows"] += 1
        kind = str(row.get("kind") or row.get("event") or "unknown")
        payload = _payload(row)
        event = str(payload.get("event") or payload.get("event_type") or kind)
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        assert isinstance(metadata, dict)
        counters["events"][event] += 1

        if event == "startup" or kind == "startup":
            _record_startup(payload, counters)
        if kind in {"signal_eval", "signal_eval_summary"} or event in {"signal_eval", "signal_eval_summary"}:
            _record_signal_evaluation(payload, counters)
        if event == "execution_reject":
            _record_execution_reject(payload, metadata, counters)
        if event in {"live_order_error", "live_sync_error", "market_data_error", "live_error"}:
            _record_error(payload, metadata, counters)
        if event == "live_order_expired":
            counters["expired_orders"][str(payload.get("reason") or metadata.get("reason") or "unknown")] += 1
            if str(payload.get("reason") or "").startswith("entry_order_"):
                counters["live_entry_orders_expired"] += 1

        if event == "live_order_submitted" and payload.get("reason") == "entry_limit_fak_submitted":
            counters["live_entry_orders_submitted"] += 1
            _remember_entry_signal(payload, metadata, entry_signals_by_key)
        if event == "live_order_submitted" and payload.get("reason") == "exit_order_submitted":
            counters["live_exit_orders_submitted"] += 1
        if event == "live_position_detected":
            counters["live_positions_detected"] += 1
            _remember_position_entry(payload, metadata, entry_signals_by_key, hold_entries)
        if event == "live_order_submitted" and payload.get("reason") == "exit_order_submitted":
            _remember_exit_order(payload, metadata, hold_entries, exit_orders)
        if event == "live_order_status":
            filled = _reconstruct_filled_exit(payload, metadata, exit_orders, recorded_exit_order_ids, cfg)
            if filled is not None:
                filled_exit_records.append(filled)
        if event == "live_position_flat":
            _record_position_flat(
                payload,
                metadata,
                filled_exit_records,
                recorded_exit_order_ids,
                pnl_by_engine,
                pnl_by_exit_reason,
                counters,
            )
        _record_symbol_mapping_issue(event, metadata, counters, symbol_issue_samples)
        if _is_minute_live_filter_violation(event, payload, metadata, cfg):
            counters["minute_live_filter_violations"] += 1
            counters["suspected_old_live_policy"] = True
        if event == "live_trade_closed":
            counters["live_trades_closed"] += 1
            _record_closed_trade(payload, metadata, pnl_by_engine, pnl_by_exit_reason, counters)
            exit_order_id = str(metadata.get("exit_order_id") or "")
            if exit_order_id:
                recorded_exit_order_ids.add(exit_order_id)

    return {
        "files": counters["files"],
        "rows": counters["rows"],
        "events": dict(counters["events"]),
        "strategy_pnl": _round_pnl_map(pnl_by_engine),
        "exit_reason_pnl": _round_pnl_map(pnl_by_exit_reason),
        "losses_total": counters["losses_total"],
        "loss_samples": counters["loss_samples"],
        "loss_sample_limit": counters["loss_sample_limit"],
        "api_errors": dict(counters["api_errors"]),
        "expired_orders": dict(counters["expired_orders"]),
        "symbol_mapping_issues": dict(counters["symbol_mapping_issues"]),
        "symbol_issue_samples": symbol_issue_samples[:20],
        "minute_live_filter_violations": counters["minute_live_filter_violations"],
        "micro_entry_funnel": _micro_entry_funnel(counters),
        "startup_checks": counters["startup_checks"],
        "diagnosis_notes": _diagnosis_notes(counters),
        "suspected_old_live_policy": bool(counters["suspected_old_live_policy"]),
    }


def _jsonl_paths(source: str | Path) -> list[Path]:
    path = Path(source)
    if path.is_dir():
        return sorted(path.glob("*.jsonl"))
    return [path]


def _iter_jsonl_rows(paths: list[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    yield row


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if isinstance(payload, dict):
        return payload
    return row


def _record_startup(payload: dict[str, Any], counters: dict[str, Any]) -> None:
    check = {
        "code_fingerprint": payload.get("code_fingerprint"),
        "config_fingerprint": payload.get("config_fingerprint"),
        "live_minute_atr_filter": payload.get("live_minute_atr_filter"),
        "min_execution_score_to_chase": payload.get("min_execution_score_to_chase"),
        "live_supported_engines": payload.get("live_supported_engines"),
    }
    counters["startup_checks"].append(check)
    if payload.get("live_minute_atr_filter") is not True:
        counters["suspected_old_live_policy"] = True


def _record_error(payload: dict[str, Any], metadata: dict[str, Any], counters: dict[str, Any]) -> None:
    reason = str(payload.get("reason") or metadata.get("reason") or payload.get("event") or "unknown")
    counters["api_errors"][reason] += 1
    if reason.startswith("entry_order_"):
        counters["live_entry_order_errors"] += 1
    error = metadata.get("error") or payload.get("error")
    if error:
        counters["api_errors"][f"error:{str(error)[:120]}"] += 1


def _record_signal_evaluation(payload: dict[str, Any], counters: dict[str, Any]) -> None:
    count = _event_count(payload)
    decision = str(payload.get("decision") or "unknown")
    engine = str(payload.get("engine") or "unknown")
    reason = str(payload.get("reason") or "unknown")
    counters["signal_eval_decisions"][decision] += count
    counters["signal_eval_engines"][engine] += count
    if decision == "reject":
        counters["signal_eval_reject_reasons"][reason] += count
        metadata = _signal_eval_metadata(payload)
        for failed_check in _failed_checks(reason, metadata):
            counters["signal_eval_failed_checks"][failed_check] += count


def _record_execution_reject(payload: dict[str, Any], metadata: dict[str, Any], counters: dict[str, Any]) -> None:
    reason = str(payload.get("reason") or "unknown")
    counters["execution_rejects"][reason] += 1
    counters["execution_reject_blocked_by"][str(metadata.get("blocked_by") or "unknown")] += 1
    signal = metadata.get("signal")
    engine = signal.get("engine") if isinstance(signal, dict) else metadata.get("engine")
    if reason == "live_unsupported_signal_engine" and engine in MINUTE_SIGNAL_ENGINES:
        counters["live_unsupported_minute_signals"] += 1


def _event_count(payload: dict[str, Any]) -> int:
    value = payload.get("count")
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return 1
    return parsed if parsed > 0 else 1


def _signal_eval_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("metadata", "last_metadata", "first_metadata"):
        metadata = payload.get(key)
        if isinstance(metadata, dict):
            return metadata
    return {}


def _failed_checks(reason: str, metadata: dict[str, Any]) -> set[str]:
    checks: set[str] = set()
    if reason == "jump_detected" or metadata.get("jump_detected") is True:
        checks.add("jump_detected")
    if reason == "spread_not_required_width" or metadata.get("spread_ok") is False:
        checks.add("spread_not_required_width")
    if reason == "min_order_interval" or metadata.get("too_soon") is True:
        checks.add("min_order_interval")
    if reason.startswith("minute_bias_conflict"):
        checks.add("minute_bias_conflict")
    if reason.startswith("topix_bias_conflict"):
        checks.add("topix_bias_conflict")
    if reason in {"multi_timeframe_score_below_threshold", "execution_score_below_threshold", "position_scale_none"}:
        checks.add(reason)
    if metadata.get("mtf_ok") is False:
        checks.add(str(metadata.get("mtf_reason") or "multi_timeframe"))
    if metadata.get("throttle_ok") is False:
        checks.add(str(metadata.get("throttle_reason") or "throttle"))
    if metadata.get("risk_ok") is False:
        checks.add(str(metadata.get("risk_reason") or "risk"))
    if metadata.get("alpha_ok") is False:
        checks.add(str(metadata.get("alpha_reason") or "alpha_stack"))
    if _both_false(metadata, "imbalance_long_ok", "imbalance_short_ok") or reason == "imbalance_not_met":
        checks.add("imbalance_not_met")
    if _both_false(metadata, "ofi_long_ok", "ofi_short_ok") or reason == "ofi_not_met":
        checks.add("ofi_not_met")
    if _both_false(metadata, "microprice_long_ok", "microprice_short_ok") or reason == "microprice_not_met":
        checks.add("microprice_not_met")
    if not checks and reason and reason != "unknown":
        checks.add(reason)
    return checks


def _both_false(metadata: dict[str, Any], long_key: str, short_key: str) -> bool:
    return metadata.get(long_key) is False and metadata.get(short_key) is False


def _record_symbol_mapping_issue(
    event: str,
    metadata: dict[str, Any],
    counters: dict[str, Any],
    samples: list[dict[str, object]],
) -> None:
    payload = metadata.get("order_payload")
    if event not in {"live_order_submitted", "live_order_error"} or not isinstance(payload, dict):
        return
    if int(payload.get("TradeType") or 0) != 2:
        return
    symbol = str(payload.get("Symbol") or "")
    if symbol.isdigit():
        return
    counters["symbol_mapping_issues"]["close_order_non_numeric_symbol"] += 1
    if symbol in {"NK225micro", "TOPIXmini"}:
        counters["symbol_mapping_issues"]["close_order_human_symbol"] += 1
    if len(samples) < 20:
        samples.append({"event": event, "symbol": symbol, "order_payload": payload})


def _remember_entry_signal(
    payload: dict[str, Any],
    metadata: dict[str, Any],
    entry_signals_by_key: dict[tuple[str, str, str], dict[str, Any]],
) -> None:
    signal = metadata.get("signal")
    if not isinstance(signal, dict):
        return
    engine = str(signal.get("engine") or "")
    if not engine:
        return
    key = (str(payload.get("symbol") or signal.get("symbol") or ""), str(payload.get("direction") or signal.get("direction") or ""), engine)
    entry_signals_by_key[key] = signal


def _remember_position_entry(
    payload: dict[str, Any],
    metadata: dict[str, Any],
    entry_signals_by_key: dict[tuple[str, str, str], dict[str, Any]],
    hold_entries: dict[str, dict[str, Any]],
) -> None:
    hold_id = str(metadata.get("hold_id") or "")
    if not hold_id:
        return
    engine = str(metadata.get("engine") or "unknown")
    key = (str(payload.get("symbol") or ""), str(payload.get("direction") or ""), engine)
    signal = entry_signals_by_key.get(key, {})
    hold_entries[hold_id] = {
        "engine": engine,
        "signal_reason": signal.get("reason"),
        "signal": signal,
        "entry_price": _to_float(payload.get("entry_price")),
        "direction": payload.get("direction"),
        "symbol": payload.get("symbol"),
        "qty": _to_float(payload.get("qty")) or 1.0,
    }


def _remember_exit_order(
    payload: dict[str, Any],
    metadata: dict[str, Any],
    hold_entries: dict[str, dict[str, Any]],
    exit_orders: dict[str, dict[str, Any]],
) -> None:
    order_id = str(metadata.get("order_id") or "")
    if not order_id:
        return
    hold_id = _first_close_hold_id(metadata.get("order_payload"))
    hold_entry = hold_entries.get(hold_id or "", {})
    exit_orders[order_id] = {
        "order_id": order_id,
        "hold_id": hold_id,
        "symbol": payload.get("symbol") or hold_entry.get("symbol"),
        "direction": payload.get("direction") or hold_entry.get("direction"),
        "qty": _to_float(payload.get("qty")) or hold_entry.get("qty") or 1.0,
        "entry_price": _to_float(payload.get("entry_price")) or hold_entry.get("entry_price"),
        "engine": metadata.get("engine") or hold_entry.get("engine") or "unknown",
        "signal_reason": hold_entry.get("signal_reason"),
        "signal": hold_entry.get("signal"),
        "exit_reason": metadata.get("exit_reason") or "unknown",
        "submitted_at": payload.get("timestamp"),
    }


def _reconstruct_filled_exit(
    payload: dict[str, Any],
    metadata: dict[str, Any],
    exit_orders: dict[str, dict[str, Any]],
    recorded_exit_order_ids: set[str],
    config: StrategyConfig,
) -> dict[str, Any] | None:
    order_id = str(metadata.get("order_id") or "")
    if not order_id or order_id in recorded_exit_order_ids or order_id not in exit_orders:
        return None
    order_status = metadata.get("order_status")
    if not isinstance(order_status, dict) or not _order_status_filled(order_status):
        return None
    exit_order = exit_orders[order_id]
    entry_price = _to_float(exit_order.get("entry_price"))
    exit_price = _order_execution_price(order_status)
    symbol = str(exit_order.get("symbol") or payload.get("symbol") or "")
    direction = str(exit_order.get("direction") or payload.get("direction") or "")
    if entry_price is None or exit_price is None or direction not in {"long", "short"}:
        return None
    qty = int(float(exit_order.get("qty") or 1))
    tick_size = config.tick_size_for(symbol) if symbol else config.tick_size
    tick_value = config.tick_value_yen_for(symbol) if symbol else config.micro225_tick_value
    pnl_ticks = _pnl_ticks(direction, entry_price, exit_price, tick_size)
    pnl_yen = pnl_ticks * qty * tick_value
    return {
        "event": "live_trade_closed",
        "symbol": symbol,
        "direction": direction,
        "qty": qty,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "reason": exit_order.get("exit_reason"),
        "pnl_ticks": pnl_ticks,
        "pnl_yen": pnl_yen,
        "timestamp": payload.get("timestamp"),
        "metadata": {
            "engine": exit_order.get("engine"),
            "signal_reason": exit_order.get("signal_reason"),
            "signal": exit_order.get("signal"),
            "exit_reason": exit_order.get("exit_reason"),
            "exit_order_id": order_id,
            "hold_id": exit_order.get("hold_id"),
            "reconstructed_from": "live_order_status",
            "order_status": order_status,
        },
    }


def _record_position_flat(
    payload: dict[str, Any],
    metadata: dict[str, Any],
    filled_exit_records: list[dict[str, Any]],
    recorded_exit_order_ids: set[str],
    pnl_by_engine: dict[str, dict[str, float]],
    pnl_by_exit_reason: dict[str, dict[str, float]],
    counters: dict[str, Any],
) -> None:
    live_trade = metadata.get("live_trade")
    if isinstance(live_trade, dict):
        exit_order_id = str(live_trade.get("exit_order_id") or "")
        if exit_order_id and exit_order_id in recorded_exit_order_ids:
            return
        synthetic = {
            "event": "live_trade_closed",
            "symbol": live_trade.get("symbol") or payload.get("symbol"),
            "direction": live_trade.get("direction") or payload.get("direction"),
            "qty": live_trade.get("qty") or payload.get("qty"),
            "entry_price": live_trade.get("entry_price"),
            "exit_price": live_trade.get("exit_price"),
            "reason": live_trade.get("exit_reason"),
            "pnl_ticks": live_trade.get("pnl_ticks"),
            "pnl_yen": live_trade.get("pnl_yen"),
            "timestamp": payload.get("timestamp"),
            "metadata": live_trade,
        }
        counters["live_trades_closed"] += 1
        _record_closed_trade(synthetic, live_trade, pnl_by_engine, pnl_by_exit_reason, counters)
        if exit_order_id:
            recorded_exit_order_ids.add(exit_order_id)
        return

    for index, record in enumerate(tuple(filled_exit_records)):
        if record["metadata"].get("exit_order_id") in recorded_exit_order_ids:
            continue
        if record.get("symbol") != payload.get("symbol") or record.get("direction") != payload.get("direction"):
            continue
        filled_exit_records.pop(index)
        _record_closed_trade(record, record["metadata"], pnl_by_engine, pnl_by_exit_reason, counters)
        counters["live_trades_closed"] += 1
        recorded_exit_order_ids.add(str(record["metadata"].get("exit_order_id")))
        return


def _micro_entry_funnel(counters: dict[str, Any]) -> dict[str, object]:
    submitted = int(counters["live_entry_orders_submitted"])
    detected = int(counters["live_positions_detected"])
    return {
        "signal_eval": {
            "decisions": dict(counters["signal_eval_decisions"]),
            "engines": dict(counters["signal_eval_engines"]),
            "reject_reasons_top": counters["signal_eval_reject_reasons"].most_common(12),
            "failed_checks_top": counters["signal_eval_failed_checks"].most_common(12),
        },
        "live_execution": {
            "entry_orders_submitted": submitted,
            "entry_orders_expired": int(counters["live_entry_orders_expired"]),
            "entry_order_errors": int(counters["live_entry_order_errors"]),
            "positions_detected": detected,
            "entry_fill_rate": round(detected / submitted, 4) if submitted else 0.0,
            "exit_orders_submitted": int(counters["live_exit_orders_submitted"]),
            "trades_closed": int(counters["live_trades_closed"]),
            "execution_rejects": dict(counters["execution_rejects"]),
            "execution_reject_blocked_by": dict(counters["execution_reject_blocked_by"]),
            "live_unsupported_minute_signals": int(counters["live_unsupported_minute_signals"]),
        },
    }


def _diagnosis_notes(counters: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if counters["suspected_old_live_policy"]:
        notes.append("startup_missing_current_live_safety_fields_or_old_policy_detected")
    if counters["live_unsupported_minute_signals"]:
        notes.append("minute_signals_were_blocked_by_live_supported_engines")
    failed_checks = counters["signal_eval_failed_checks"]
    for reason, _ in failed_checks.most_common(3):
        notes.append(f"micro_front_gate_top_blocker:{reason}")
    return notes


def _is_minute_live_filter_violation(
    event: str,
    payload: dict[str, Any],
    metadata: dict[str, Any],
    config: StrategyConfig,
) -> bool:
    if event != "live_order_submitted" or payload.get("reason") != "entry_limit_fak_submitted":
        return False
    signal = metadata.get("signal") if isinstance(metadata.get("signal"), dict) else {}
    assert isinstance(signal, dict)
    engine = signal.get("engine") or metadata.get("engine")
    if engine not in {"minute_orb", "minute_vwap", "directional_intraday"}:
        return False
    signal_metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    assert isinstance(signal_metadata, dict)
    atr = _to_float(signal_metadata.get("atr") or metadata.get("atr"))
    execution_score = _to_float(signal_metadata.get("execution_score") or metadata.get("execution_score"))
    threshold = max(10.0, float(config.multi_timeframe.min_execution_score_to_chase))
    return atr is None or atr <= 0 or execution_score is None or execution_score < threshold


def _record_closed_trade(
    payload: dict[str, Any],
    metadata: dict[str, Any],
    pnl_by_engine: dict[str, dict[str, float]],
    pnl_by_exit_reason: dict[str, dict[str, float]],
    counters: dict[str, Any],
) -> None:
    pnl_ticks = _to_float(payload.get("pnl_ticks"))
    if pnl_ticks is None:
        pnl_ticks = _to_float(metadata.get("pnl_ticks"))
    if pnl_ticks is None:
        return
    pnl_yen = _to_float(payload.get("pnl_yen"))
    if pnl_yen is None:
        pnl_yen = _to_float(metadata.get("pnl_yen")) or 0.0
    engine = str(metadata.get("engine") or _nested(metadata, "signal", "engine") or "unknown")
    exit_reason = str(metadata.get("exit_reason") or payload.get("reason") or "unknown")
    _add_trade(pnl_by_engine[engine], pnl_ticks, pnl_yen)
    _add_trade(pnl_by_exit_reason[exit_reason], pnl_ticks, pnl_yen)
    if pnl_ticks >= 0:
        return
    counters["losses_total"] += 1
    if len(counters["loss_samples"]) >= LOSS_SAMPLE_LIMIT:
        return
    counters["loss_samples"].append(
        {
            "timestamp": payload.get("timestamp"),
            "symbol": payload.get("symbol") or metadata.get("symbol"),
            "engine": engine,
            "signal_reason": metadata.get("signal_reason") or _nested(metadata, "signal", "reason"),
            "exit_reason": exit_reason,
            "direction": payload.get("direction"),
            "pnl_ticks": round(pnl_ticks, 4),
            "pnl_yen": round(pnl_yen, 2),
        }
    )


def _pnl_bucket() -> dict[str, float]:
    return {"trades": 0.0, "wins": 0.0, "losses": 0.0, "pnl_ticks": 0.0, "pnl_yen": 0.0}


def _add_trade(bucket: dict[str, float], pnl_ticks: float, pnl_yen: float) -> None:
    bucket["trades"] += 1
    bucket["pnl_ticks"] += pnl_ticks
    bucket["pnl_yen"] += pnl_yen
    if pnl_ticks > 0:
        bucket["wins"] += 1
    elif pnl_ticks < 0:
        bucket["losses"] += 1


def _round_pnl_map(values: dict[str, dict[str, float]]) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    for key, bucket in values.items():
        trades = int(bucket["trades"])
        result[key] = {
            "trades": trades,
            "wins": int(bucket["wins"]),
            "losses": int(bucket["losses"]),
            "win_rate": round(bucket["wins"] / trades, 4) if trades else 0.0,
            "pnl_ticks": round(bucket["pnl_ticks"], 4),
            "pnl_yen": round(bucket["pnl_yen"], 2),
        }
    return result


def _nested(mapping: dict[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_close_hold_id(order_payload: Any) -> str | None:
    if not isinstance(order_payload, dict):
        return None
    close_positions = order_payload.get("ClosePositions")
    if not isinstance(close_positions, list) or not close_positions:
        return None
    first = close_positions[0]
    if not isinstance(first, dict):
        return None
    hold_id = first.get("HoldID")
    return str(hold_id) if hold_id else None


def _order_status_filled(order_status: dict[str, Any]) -> bool:
    state = str(order_status.get("state") or order_status.get("order_state") or "")
    if state == "5":
        return True
    if (_to_float(order_status.get("cum_qty")) or 0.0) > 0:
        return True
    details = order_status.get("details")
    return isinstance(details, list) and any(isinstance(item, dict) and str(item.get("rec_type")) == "8" for item in details)


def _order_execution_price(order_status: dict[str, Any]) -> float | None:
    price = _to_float(order_status.get("execution_price"))
    if price is not None and price > 0:
        return price
    details = order_status.get("details")
    if isinstance(details, list):
        for item in reversed(details):
            if not isinstance(item, dict) or str(item.get("rec_type")) != "8":
                continue
            price = _to_float(item.get("price"))
            if price is not None and price > 0:
                return price
    price = _to_float(order_status.get("price"))
    return price if price is not None and price > 0 else None


def _pnl_ticks(direction: str, entry_price: float, exit_price: float, tick_size: float) -> float:
    if direction == "long":
        return (exit_price - entry_price) / tick_size
    if direction == "short":
        return (entry_price - exit_price) / tick_size
    return 0.0


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
