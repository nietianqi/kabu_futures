"""Champion / Challenger promotion gate.

Evaluates whether a challenger parameter set should replace the current
champion, be held for further observation, or be rejected outright.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PromotionThresholds:
    """Hard gates and veto conditions for challenger promotion.

    All ratios compare challenger value vs champion value (challenger/champion).
    """

    # --- Hard gates (all must pass for "promote") ---
    min_pnl_delta_ticks: float = 0.0
    """Challenger net_pnl_ticks must exceed champion by at least this amount."""

    max_drawdown_ratio: float = 1.2
    """Challenger max_drawdown may not be more than this multiple of champion's."""

    min_trades_per_day: float = 10.0
    """Challenger must produce at least this many trades per day on average."""

    min_avg_markout_ticks: float = 0.5
    """Challenger avg_pnl_ticks must meet this floor (positive edge requirement)."""

    max_slippage_ratio: float = 1.1
    """Challenger avg negative pnl trades ratio vs champion (proxy for slippage)."""

    max_consecutive_loss_ratio: float = 1.2
    """Challenger consecutive-loss proxy must not exceed champion by this ratio."""

    walk_forward_pass_threshold: float = 0.7
    """Fraction of walk-forward windows where challenger must beat baseline."""

    min_observation_days: int = 0
    """Minimum trading days of log data required before any promotion is valid."""

    # --- Soft / veto conditions ---
    max_param_change_ratio: float = 0.5
    """Reject if any candidate parameter changes by more than this fraction."""

    allow_grid_boundary: bool = False
    """Reject if recommended parameter is at the edge of the search grid."""


@dataclass
class PromotionDecision:
    decision: str  # "promote" | "hold" | "reject"
    passed_gates: list[str] = field(default_factory=list)
    failed_gates: list[str] = field(default_factory=list)
    veto_reasons: list[str] = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "decision": self.decision,
            "passed_gates": self.passed_gates,
            "failed_gates": self.failed_gates,
            "veto_reasons": self.veto_reasons,
            "notes": self.notes,
        }


def evaluate_challenger(
    champion: dict[str, object],
    challenger: dict[str, object],
    walk_forward_summary: dict[str, object] | None = None,
    thresholds: PromotionThresholds | None = None,
    observation_days: int = 0,
) -> PromotionDecision:
    """Decide whether to promote a challenger over the current champion.

    Args:
        champion: metrics dict for the current live config (may be empty if no champion).
        challenger: ``tune_micro_params`` result dict containing ``best`` and ``candidates``.
        walk_forward_summary: ``walk_forward_micro`` result ``summary`` dict.
        thresholds: gate thresholds; defaults to ``PromotionThresholds()``.
        observation_days: number of actual trading days observed so far.

    Returns:
        ``PromotionDecision`` with decision in {"promote", "hold", "reject"}.
    """
    t = thresholds or PromotionThresholds()
    passed: list[str] = []
    failed: list[str] = []
    vetoes: list[str] = []

    best: dict[str, object] = {}
    if isinstance(challenger.get("best"), dict):
        best = challenger["best"]  # type: ignore[assignment]
    elif isinstance(challenger.get("recommendation"), dict):
        best = challenger["recommendation"]  # type: ignore[assignment]

    # If no champion exists, treat all champion metrics as 0
    champ_trades: float = float(champion.get("trades", 0))
    champ_pnl: float = float(champion.get("net_pnl_ticks", 0.0))
    champ_dd: float = float(champion.get("max_drawdown_ticks", 0.0))

    chal_trades: float = float(best.get("trades", 0))
    chal_pnl: float = float(best.get("net_pnl_ticks", 0.0))
    chal_dd: float = float(best.get("max_drawdown_ticks", 0.0))
    chal_avg_pnl: float = float(best.get("avg_pnl_ticks", 0.0))

    # --- Gate 1: observation days ---
    if observation_days >= t.min_observation_days:
        passed.append("observation_days")
    else:
        failed.append("observation_days")

    # --- Gate 2: PnL improvement ---
    pnl_delta = chal_pnl - champ_pnl
    if pnl_delta >= t.min_pnl_delta_ticks:
        passed.append("pnl_delta")
    else:
        failed.append("pnl_delta")

    # --- Gate 3: drawdown ratio ---
    if abs(champ_dd) < 1e-9:
        # No champion drawdown → any drawdown is considered acceptable
        passed.append("drawdown_ratio")
    elif chal_dd != 0 and abs(chal_dd) <= abs(champ_dd) * t.max_drawdown_ratio:
        passed.append("drawdown_ratio")
    else:
        failed.append("drawdown_ratio")

    # --- Gate 4: minimum trades per day ---
    actual_days = max(1, observation_days)
    trades_per_day = chal_trades / actual_days
    if trades_per_day >= t.min_trades_per_day:
        passed.append("trades_per_day")
    else:
        failed.append("trades_per_day")

    # --- Gate 5: avg markout (positive edge) ---
    if chal_avg_pnl >= t.min_avg_markout_ticks:
        passed.append("avg_markout")
    else:
        failed.append("avg_markout")

    # --- Gate 6: walk-forward pass rate ---
    if walk_forward_summary is not None:
        wf_pass_rate = float(walk_forward_summary.get("pass_rate", 0.0))
        if wf_pass_rate >= t.walk_forward_pass_threshold:
            passed.append("walk_forward")
        else:
            failed.append("walk_forward")

    # --- Veto: parameter change too large ---
    champ_params: dict[str, object] = champion.get("parameters", {})  # type: ignore[assignment]
    chal_params: dict[str, object] = best.get("parameters", {})  # type: ignore[assignment]
    if isinstance(champ_params, dict) and isinstance(chal_params, dict):
        for key, chal_val in chal_params.items():
            champ_val = champ_params.get(key)
            if champ_val is None or float(champ_val) == 0:
                continue
            change_ratio = abs(float(chal_val) - float(champ_val)) / abs(float(champ_val))
            if change_ratio > t.max_param_change_ratio:
                vetoes.append(f"param_jump:{key}:{change_ratio:.2f}")

    # --- Veto: parameter on grid boundary ---
    if not t.allow_grid_boundary:
        candidates: list[dict[str, object]] = challenger.get("candidates", [])  # type: ignore[assignment]
        if candidates and best.get("parameters"):
            grid_vals = challenger.get("grid", {})
            if isinstance(grid_vals, dict) and isinstance(chal_params, dict):
                for key, chal_val in chal_params.items():
                    grid_for_key = grid_vals.get(key, [])
                    if isinstance(grid_for_key, list) and len(grid_for_key) >= 2:
                        lo, hi = float(grid_for_key[0]), float(grid_for_key[-1])
                        val = float(chal_val)
                        if val <= lo or val >= hi:
                            vetoes.append(f"grid_boundary:{key}={val} in [{lo}, {hi}]")

    # --- Final decision ---
    if vetoes:
        return PromotionDecision(
            decision="reject",
            passed_gates=passed,
            failed_gates=failed,
            veto_reasons=vetoes,
            notes="Rejected due to veto conditions.",
        )
    if not failed:
        return PromotionDecision(
            decision="promote",
            passed_gates=passed,
            failed_gates=failed,
            notes="All gates passed.",
        )
    # Hold if ≤2 failed and ≥4 passed (borderline — needs more data)
    if len(failed) <= 2 and len(passed) >= 4:
        return PromotionDecision(
            decision="hold",
            passed_gates=passed,
            failed_gates=failed,
            notes="Borderline: accumulate more observation days.",
        )
    return PromotionDecision(
        decision="reject",
        passed_gates=passed,
        failed_gates=failed,
        notes=f"{len(failed)} gate(s) failed.",
    )
