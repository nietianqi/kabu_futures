"""Champion vs Challenger promotion gate.

Encodes the hard thresholds and veto conditions from
``docs/ai_strategy_evolution_framework.md`` section 7 step 5.

A challenger is **eligible** (decision == ``"promote"``) only if it passes
**all** hard gates. Any veto trigger forces decision == ``"reject"`` regardless
of net PnL improvement. Borderline cases get ``"hold"``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class PromotionThresholds:
    """Default hard gates for promoting a challenger to champion.

    All thresholds match the recommended defaults documented in the
    evolution framework. Override per deployment as needed.
    """

    min_pnl_delta_ticks: float = 0.0
    """Challenger net PnL must beat champion by at least this many ticks (median across windows)."""

    max_drawdown_ratio: float = 1.2
    """Challenger max drawdown must be <= champion * this ratio."""

    min_trades_per_day: float = 10.0
    """Each evaluation window should produce at least this many trades."""

    min_avg_markout_ticks: float = 0.5
    """Average markout (typically 3s) must clear this threshold to claim real edge."""

    max_slippage_ratio: float = 1.1
    """Avg slippage in ticks must be <= champion * this ratio."""

    max_consecutive_losses_ratio: float = 1.2
    """Max consecutive losses must be <= champion * this ratio."""

    walk_forward_pass_threshold: float = 0.7
    """Walk-forward pass rate must be >= this (e.g. 0.7 means 70% of windows)."""

    min_observation_days: int = 14
    """Challenger must have observation data over at least this many trading days."""

    max_param_change_ratio: float = 0.5
    """Any parameter change > champion * this ratio triggers a manual review veto."""


@dataclass(frozen=True)
class PromotionDecision:
    """Outcome of evaluating a challenger against a champion."""

    decision: str  # "promote" | "hold" | "reject"
    reason: str
    passed_gates: tuple[str, ...] = field(default_factory=tuple)
    failed_gates: tuple[str, ...] = field(default_factory=tuple)
    veto_triggers: tuple[str, ...] = field(default_factory=tuple)
    metrics: dict[str, object] = field(default_factory=dict)


def evaluate_challenger(
    champion: Mapping[str, object],
    challenger: Mapping[str, object],
    walk_forward_summary: Mapping[str, object] | None = None,
    thresholds: PromotionThresholds | None = None,
) -> PromotionDecision:
    """Compare a challenger configuration's metrics against the champion.

    Both ``champion`` and ``challenger`` should provide at minimum:
        - net_pnl_ticks (float)
        - max_drawdown_ticks (float)
        - trades (int)
        - avg_pnl_ticks (float)

    Optional fields (used when present):
        - avg_markout_ticks_3s (float): average 3-second markout
        - avg_slippage_ticks (float)
        - max_consecutive_losses (int)
        - parameters (dict): for parameter-jump veto
        - observation_days (int)

    ``walk_forward_summary`` is the ``summary`` block produced by
    :func:`kabu_futures.walk_forward.walk_forward_micro`.
    """
    thresholds = thresholds or PromotionThresholds()

    passed: list[str] = []
    failed: list[str] = []
    vetoes: list[str] = []
    metrics: dict[str, object] = {}

    # Hard gate 1: net PnL improvement
    champion_pnl = float(champion.get("net_pnl_ticks", 0.0))
    challenger_pnl = float(challenger.get("net_pnl_ticks", 0.0))
    pnl_delta = challenger_pnl - champion_pnl
    metrics["pnl_delta_ticks"] = round(pnl_delta, 4)
    if pnl_delta >= thresholds.min_pnl_delta_ticks:
        passed.append("net_pnl_improvement")
    else:
        failed.append(f"net_pnl_delta_{pnl_delta:.4f}_below_threshold")

    # Hard gate 2: max drawdown not significantly worse
    champion_dd = float(champion.get("max_drawdown_ticks", 0.0))
    challenger_dd = float(challenger.get("max_drawdown_ticks", 0.0))
    dd_ratio = (challenger_dd / champion_dd) if champion_dd > 0 else 0.0
    metrics["drawdown_ratio"] = round(dd_ratio, 4)
    if champion_dd <= 0 and challenger_dd <= 0:
        passed.append("drawdown_within_limit")
    elif challenger_dd <= champion_dd * thresholds.max_drawdown_ratio:
        passed.append("drawdown_within_limit")
    else:
        failed.append(f"drawdown_ratio_{dd_ratio:.4f}_exceeds_limit")

    # Hard gate 3: enough trades to be statistically meaningful
    challenger_trades = int(challenger.get("trades", 0))
    obs_days = int(challenger.get("observation_days", 1) or 1)
    trades_per_day = challenger_trades / obs_days if obs_days > 0 else 0.0
    metrics["trades_per_day"] = round(trades_per_day, 4)
    if trades_per_day >= thresholds.min_trades_per_day:
        passed.append("sufficient_trade_count")
    else:
        failed.append(f"trades_per_day_{trades_per_day:.2f}_below_minimum")

    # Hard gate 4: average markout (real predictive edge)
    avg_markout = challenger.get("avg_markout_ticks_3s")
    if avg_markout is not None:
        metrics["avg_markout_ticks_3s"] = round(float(avg_markout), 4)
        if float(avg_markout) >= thresholds.min_avg_markout_ticks:
            passed.append("markout_predictive_edge")
        else:
            failed.append(f"avg_markout_{float(avg_markout):.4f}_below_threshold")

    # Hard gate 5: slippage not significantly worse
    champion_slip = champion.get("avg_slippage_ticks")
    challenger_slip = challenger.get("avg_slippage_ticks")
    if champion_slip is not None and challenger_slip is not None:
        c_slip = float(champion_slip) if float(champion_slip) > 0 else 0.0001
        slip_ratio = float(challenger_slip) / c_slip
        metrics["slippage_ratio"] = round(slip_ratio, 4)
        if slip_ratio <= thresholds.max_slippage_ratio:
            passed.append("slippage_within_limit")
        else:
            failed.append(f"slippage_ratio_{slip_ratio:.4f}_exceeds_limit")

    # Hard gate 6: consecutive losses
    champion_cl = champion.get("max_consecutive_losses")
    challenger_cl = challenger.get("max_consecutive_losses")
    if champion_cl is not None and challenger_cl is not None:
        c_cl = int(champion_cl) if int(champion_cl) > 0 else 1
        cl_ratio = int(challenger_cl) / c_cl
        metrics["consecutive_losses_ratio"] = round(cl_ratio, 4)
        if cl_ratio <= thresholds.max_consecutive_losses_ratio:
            passed.append("consecutive_losses_within_limit")
        else:
            failed.append(f"consecutive_losses_ratio_{cl_ratio:.4f}_exceeds_limit")

    # Hard gate 7: walk-forward pass rate
    if walk_forward_summary is not None:
        wf_pass_rate = float(walk_forward_summary.get("pass_rate", 0.0))
        metrics["walk_forward_pass_rate"] = wf_pass_rate
        if wf_pass_rate >= thresholds.walk_forward_pass_threshold:
            passed.append("walk_forward_stable")
        else:
            failed.append(f"walk_forward_pass_rate_{wf_pass_rate:.4f}_below_threshold")

    # Hard gate 8: observation period
    metrics["observation_days"] = obs_days
    if obs_days >= thresholds.min_observation_days:
        passed.append("observation_period_sufficient")
    else:
        failed.append(f"observation_days_{obs_days}_below_minimum")

    # Veto trigger: parameter jump too large (suggests overfit / outlier)
    champion_params = champion.get("parameters")
    challenger_params = challenger.get("parameters")
    if isinstance(champion_params, dict) and isinstance(challenger_params, dict):
        for key, c_value in champion_params.items():
            if key not in challenger_params:
                continue
            try:
                c_num = float(c_value)
                ch_num = float(challenger_params[key])
            except (TypeError, ValueError):
                continue
            if c_num == 0:
                continue
            change_ratio = abs(ch_num - c_num) / abs(c_num)
            if change_ratio > thresholds.max_param_change_ratio:
                vetoes.append(
                    f"param_{key}_changed_{change_ratio:.2%}_requires_manual_review"
                )

    # Veto trigger: search hit grid boundary (signal that space is too narrow)
    if challenger.get("hit_grid_boundary"):
        vetoes.append("recommended_parameters_at_grid_boundary")

    # Decision matrix
    if vetoes:
        decision = "reject"
        reason = "veto_triggered"
    elif not failed:
        decision = "promote"
        reason = "all_hard_gates_passed"
    elif len(failed) <= 2 and len(passed) >= 4:
        decision = "hold"
        reason = "mostly_passed_observe_longer"
    else:
        decision = "reject"
        reason = "multiple_hard_gates_failed"

    return PromotionDecision(
        decision=decision,
        reason=reason,
        passed_gates=tuple(passed),
        failed_gates=tuple(failed),
        veto_triggers=tuple(vetoes),
        metrics=metrics,
    )


def decision_to_dict(decision: PromotionDecision) -> dict[str, object]:
    """Serialise a PromotionDecision to a JSON-friendly dict."""
    return {
        "decision": decision.decision,
        "reason": decision.reason,
        "passed_gates": list(decision.passed_gates),
        "failed_gates": list(decision.failed_gates),
        "veto_triggers": list(decision.veto_triggers),
        "metrics": decision.metrics,
    }
