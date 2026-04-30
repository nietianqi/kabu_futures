from __future__ import annotations

from typing import Any


def micro_entry_funnel(counters: dict[str, Any]) -> dict[str, object]:
    submitted = int(counters["live_entry_orders_submitted"])
    detected = int(counters["live_positions_detected"])
    submitted_by_engine = dict(counters["live_entry_orders_submitted_by_engine"])
    tradeable_by_engine = dict(counters["live_tradeable_signals_by_engine"])
    micro_tradeable = int(tradeable_by_engine.get("micro_book", 0))
    micro_submitted = int(submitted_by_engine.get("micro_book", 0))
    return {
        "signal_eval": {
            "decisions": dict(counters["signal_eval_decisions"]),
            "engines": dict(counters["signal_eval_engines"]),
            "reject_reasons_top": counters["signal_eval_reject_reasons"].most_common(12),
            "failed_checks_top": counters["signal_eval_failed_checks"].most_common(12),
            "jump_reasons": dict(counters["signal_eval_jump_reasons"]),
            "near_miss_count": int(counters["near_miss_from_signal_eval"]),
            "near_miss_missing_checks": counters["near_miss_missing_checks"].most_common(12),
            "near_miss_directions": dict(counters["near_miss_directions"]),
            "non_tradeable_jump_reasons": {
                key: value
                for key, value in counters["signal_eval_jump_reasons"].items()
                if key in {"spread_wide", "mid_move_jump", "latency_high", "event_gap_high"}
            },
        },
        "micro_candidates": {
            "events": int(counters["micro_candidate_events"]),
            "candidate_count": int(counters["micro_candidate_count"]),
            "missing_checks": counters["micro_candidate_missing_checks"].most_common(12),
            "directions": dict(counters["micro_candidate_directions"]),
        },
        "signals": {
            "by_engine": dict(counters["live_signals_by_engine"]),
            "tradeable_by_engine": tradeable_by_engine,
        },
        "live_execution": {
            "entry_orders_submitted": submitted,
            "entry_orders_submitted_by_engine": submitted_by_engine,
            "micro_tradeable_signals_without_entry_submission": max(0, micro_tradeable - micro_submitted),
            "entry_orders_cancelled": int(counters["live_entry_orders_cancelled"]),
            "entry_orders_expired": int(counters["live_entry_orders_expired"]),
            "entry_order_errors": int(counters["live_entry_order_errors"]),
            "positions_detected": detected,
            "entry_fill_rate": round(detected / submitted, 4) if submitted else 0.0,
            "exit_orders_submitted": int(counters["live_exit_orders_submitted"]),
            "exit_orders_cancelled": int(counters["live_exit_orders_cancelled"]),
            "trades_closed": int(counters["live_trades_closed"]),
            "execution_rejects": dict(counters["execution_rejects"]),
            "execution_reject_blocked_by": dict(counters["execution_reject_blocked_by"]),
            "position_sync_blocked_rejects": int(counters["position_sync_blocked_rejects"]),
            "live_unsupported_minute_signals": int(counters["live_unsupported_minute_signals"]),
            "api_error_categories": dict(counters["api_error_categories"]),
            "loss_hold_guard_events": int(counters["loss_hold_guard_events"]),
            "kill_switch_events": int(counters["kill_switch_events"]),
        },
    }


def diagnosis_notes(counters: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if counters["suspected_old_live_policy"]:
        notes.append("startup_missing_current_live_safety_fields_or_old_policy_detected")
    if counters["live_unsupported_minute_signals"]:
        notes.append("minute_signals_were_blocked_by_live_supported_engines")
    api_categories = counters["api_error_categories"]
    if api_categories.get("auth_error") or api_categories.get("auth_recovery_failed"):
        notes.append("kabu_auth_errors_blocked_live_entry")
    if api_categories.get("kabu_station_wrong_instance"):
        notes.append("kabu_station_wrong_instance_detected_switch_to_orderable_instance")
    if counters["position_sync_blocked_rejects"]:
        notes.append("position_sync_blocked_prevented_live_entry")
    if counters["loss_hold_guard_events"]:
        notes.append("loss_hold_guard_requires_manual_review_no_auto_loss_close")
    if counters["kill_switch_events"]:
        notes.append("kill_switch_was_active")
    failed_checks = counters["signal_eval_failed_checks"]
    for reason, _ in failed_checks.most_common(3):
        notes.append(f"micro_front_gate_top_blocker:{reason}")
    for missing, count in counters["near_miss_missing_checks"].most_common(3):
        notes.append(f"micro_near_miss_one_check:{missing}:{count}")
    return notes


def latency_summary(values: dict[str, list[float]]) -> dict[str, dict[str, float]]:
    return {key: _latency_bucket(samples) for key, samples in values.items() if samples}


def _latency_bucket(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "count": float(len(ordered)),
        "p50": round(_percentile(ordered, 0.50), 4),
        "p95": round(_percentile(ordered, 0.95), 4),
        "max": round(ordered[-1], 4) if ordered else 0.0,
    }


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, max(0, int(round((len(values) - 1) * pct))))
    return values[index]
