"""CLI for evaluating a challenger against champion via promotion gate.

Reads two JSON metric files (champion and challenger) plus an optional
walk-forward summary, and prints a promotion decision per the framework
in ``docs/ai_strategy_evolution_framework.md``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from kabu_futures.promotion import (
    PromotionThresholds,
    decision_to_dict,
    evaluate_challenger,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="promotion_check.py",
        description="Evaluate a challenger config's metrics against champion. "
        "Inputs are JSON files with at minimum: net_pnl_ticks, max_drawdown_ticks, trades, avg_pnl_ticks.",
    )
    parser.add_argument("--champion", required=True, help="Path to champion metrics JSON.")
    parser.add_argument("--challenger", required=True, help="Path to challenger metrics JSON.")
    parser.add_argument(
        "--walk-forward",
        help="Optional path to walk-forward summary JSON (the 'summary' block from walk_forward.py output).",
    )
    parser.add_argument(
        "--min-pnl-delta",
        type=float,
        help="Override default minimum PnL delta in ticks.",
    )
    parser.add_argument(
        "--max-drawdown-ratio",
        type=float,
        help="Override default max drawdown ratio (challenger / champion).",
    )
    parser.add_argument(
        "--min-trades-per-day",
        type=float,
        help="Override default minimum trades per day.",
    )
    parser.add_argument(
        "--min-avg-markout",
        type=float,
        help="Override default minimum average markout in ticks.",
    )
    parser.add_argument(
        "--walk-forward-pass",
        type=float,
        help="Override default walk-forward pass rate threshold.",
    )
    parser.add_argument(
        "--min-observation-days",
        type=int,
        help="Override default minimum observation days.",
    )
    parser.add_argument("--output", help="Optional JSON output path.")
    return parser


def _load_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _build_thresholds(args: argparse.Namespace) -> PromotionThresholds:
    defaults = PromotionThresholds()
    return PromotionThresholds(
        min_pnl_delta_ticks=args.min_pnl_delta if args.min_pnl_delta is not None else defaults.min_pnl_delta_ticks,
        max_drawdown_ratio=args.max_drawdown_ratio if args.max_drawdown_ratio is not None else defaults.max_drawdown_ratio,
        min_trades_per_day=args.min_trades_per_day if args.min_trades_per_day is not None else defaults.min_trades_per_day,
        min_avg_markout_ticks=args.min_avg_markout if args.min_avg_markout is not None else defaults.min_avg_markout_ticks,
        max_slippage_ratio=defaults.max_slippage_ratio,
        max_consecutive_losses_ratio=defaults.max_consecutive_losses_ratio,
        walk_forward_pass_threshold=args.walk_forward_pass if args.walk_forward_pass is not None else defaults.walk_forward_pass_threshold,
        min_observation_days=args.min_observation_days if args.min_observation_days is not None else defaults.min_observation_days,
        max_param_change_ratio=defaults.max_param_change_ratio,
    )


def main() -> int:
    args = build_parser().parse_args()
    champion = _load_json(args.champion)
    challenger = _load_json(args.challenger)
    walk_forward_summary = _load_json(args.walk_forward) if args.walk_forward else None
    thresholds = _build_thresholds(args)

    decision = evaluate_challenger(
        champion,
        challenger,
        walk_forward_summary=walk_forward_summary,
        thresholds=thresholds,
    )
    payload = {
        "champion_source": args.champion,
        "challenger_source": args.challenger,
        "walk_forward_source": args.walk_forward,
        "thresholds": {
            "min_pnl_delta_ticks": thresholds.min_pnl_delta_ticks,
            "max_drawdown_ratio": thresholds.max_drawdown_ratio,
            "min_trades_per_day": thresholds.min_trades_per_day,
            "min_avg_markout_ticks": thresholds.min_avg_markout_ticks,
            "walk_forward_pass_threshold": thresholds.walk_forward_pass_threshold,
            "min_observation_days": thresholds.min_observation_days,
            "max_param_change_ratio": thresholds.max_param_change_ratio,
        },
        "result": decision_to_dict(decision),
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if decision.decision == "promote" else 1


if __name__ == "__main__":
    raise SystemExit(main())
