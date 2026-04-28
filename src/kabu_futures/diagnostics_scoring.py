from __future__ import annotations

from typing import Any

from .config import StrategyConfig


def live_readiness_score(counters: dict[str, Any], config: StrategyConfig) -> dict[str, object]:
    modules = {
        "instrument_rules": _score_module(7, 10, ["tick_value_configured", "sq_rollover_gate_estimated", "fees_need_configuration"]),
        "kabu_api_stability": _score_module(11, 15, _api_score_notes(counters)),
        "market_data_quality": _score_module(11, 15, _market_quality_notes(counters)),
        "order_fill_sync": _score_module(14, 20, _order_sync_notes(counters)),
        "risk_controls": _score_module(14 if config.live_execution.max_positions_per_symbol <= 1 else 8, 20, _risk_notes(counters, config)),
        "strategy_edge": _score_module(3, 10, ["sample_size_too_small", "net_expectancy_not_proven"]),
        "replay_simulation": _score_module(7, 10, ["replay_analyze_tune_available", "live_cost_bias_still_needs_more_samples"]),
    }
    total = sum(int(item["score"]) for item in modules.values())
    return {
        "total": total,
        "max_score": 100,
        "rating": _readiness_rating(total),
        "live_smoke_test_allowed": total >= 60,
        "manual_supervision_required": total < 90,
        "unattended_live_allowed": total >= 90,
        "modules": modules,
        "assumptions": {
            "auto_loss_close_disabled": True,
            "loss_hold_guard_blocks_entries_only": True,
            "live_max_positions_per_symbol": config.live_execution.max_positions_per_symbol,
        },
    }


def _score_module(score: int, max_score: int, notes: list[str]) -> dict[str, object]:
    return {"score": score, "max": max_score, "notes": notes}


def _api_score_notes(counters: dict[str, Any]) -> list[str]:
    categories = counters["api_error_categories"]
    notes = ["401_reauth_supported", "wrong_instance_classified", "429_503_backoff_supported"]
    if categories:
        notes.append(f"observed_api_errors:{dict(categories)}")
    return notes


def _market_quality_notes(counters: dict[str, Any]) -> list[str]:
    notes = ["spread_jump_latency_gates_present"]
    jump_reasons = counters["signal_eval_jump_reasons"]
    if jump_reasons:
        notes.append(f"jump_reasons:{dict(jump_reasons)}")
    return notes


def _order_sync_notes(counters: dict[str, Any]) -> list[str]:
    submitted = int(counters["live_entry_orders_submitted"])
    detected = int(counters["live_positions_detected"])
    notes = ["order_poll_and_position_sync_present", "cancelorder_lifecycle_supported"]
    if submitted:
        notes.append(f"entry_fill_rate:{round(detected / submitted, 4)}")
    if counters["live_entry_orders_cancelled"]:
        notes.append(f"entry_cancelled:{counters['live_entry_orders_cancelled']}")
    return notes


def _risk_notes(counters: dict[str, Any], config: StrategyConfig) -> list[str]:
    notes = [
        "tp_only_loss_positions_are_not_auto_closed",
        f"live_max_positions_per_symbol:{config.live_execution.max_positions_per_symbol}",
        f"loss_hold_guard_ticks:{config.live_execution.loss_hold_guard_ticks}",
        f"daily_loss_limit_yen:{config.live_execution.daily_loss_limit_yen}",
    ]
    if counters["loss_hold_guard_events"]:
        notes.append("loss_hold_guard_triggered")
    if config.live_execution.kill_switch_enabled:
        notes.append("kill_switch_enabled")
    return notes


def _readiness_rating(total: int) -> str:
    if total >= 90:
        return "ready_for_small_live_and_gradual_scale"
    if total >= 75:
        return "one_lot_smoke_test_only"
    if total >= 60:
        return "paper_or_supervised_smoke_only"
    return "not_live_ready"
