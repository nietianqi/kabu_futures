"""Champion / Challenger promotion check CLI.

Usage::

    # Compare tune report to champion (no walk-forward)
    python scripts/promotion_check.py champion.json challenger_tune.json

    # Include walk-forward result
    python scripts/promotion_check.py champion.json challenger_tune.json \\
        --walk-forward reports/wf.json

    # Override thresholds
    python scripts/promotion_check.py champion.json challenger_tune.json \\
        --min-pnl-delta 1.0 --walk-forward reports/wf.json

Exit codes:
    0 → promote
    1 → hold
    2 → reject
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

from kabu_futures.promotion import PromotionThresholds, evaluate_challenger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="promotion_check.py",
        description=(
            "Evaluate a challenger tune report against the current champion. "
            "Exit 0 = promote, 1 = hold, 2 = reject."
        ),
    )
    parser.add_argument("champion", help="Champion metrics JSON (output of a previous tune run or manual file).")
    parser.add_argument("challenger", help="Challenger tune report JSON (output of tune_micro_params.py).")
    parser.add_argument("--walk-forward", help="Walk-forward summary JSON (output of walk_forward.py).")
    parser.add_argument("--observation-days", type=int, default=0, help="Actual trading days observed so far.")
    parser.add_argument("--min-pnl-delta", type=float, default=0.0)
    parser.add_argument("--max-drawdown-ratio", type=float, default=1.2)
    parser.add_argument("--min-trades-per-day", type=float, default=10.0)
    parser.add_argument("--min-avg-markout", type=float, default=0.5)
    parser.add_argument("--walk-forward-pass-threshold", type=float, default=0.7)
    parser.add_argument("--min-observation-days", type=int, default=0)
    parser.add_argument("--output", help="Optional JSON decision output path.")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    champion_path = Path(args.champion)
    champion: dict[str, object] = {}
    if champion_path.exists():
        champion = json.loads(champion_path.read_text(encoding="utf-8"))

    challenger = json.loads(Path(args.challenger).read_text(encoding="utf-8"))

    wf_summary: dict[str, object] | None = None
    if args.walk_forward:
        wf_path = Path(args.walk_forward)
        if wf_path.exists():
            wf_data = json.loads(wf_path.read_text(encoding="utf-8"))
            wf_summary = wf_data.get("summary", wf_data)

    thresholds = PromotionThresholds(
        min_pnl_delta_ticks=args.min_pnl_delta,
        max_drawdown_ratio=args.max_drawdown_ratio,
        min_trades_per_day=args.min_trades_per_day,
        min_avg_markout_ticks=args.min_avg_markout,
        walk_forward_pass_threshold=args.walk_forward_pass_threshold,
        min_observation_days=args.min_observation_days,
    )

    decision = evaluate_challenger(
        champion,
        challenger,
        walk_forward_summary=wf_summary,
        thresholds=thresholds,
        observation_days=args.observation_days,
    )

    result = decision.to_dict()
    text = json.dumps(result, ensure_ascii=False, indent=2)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
        print(f"Decision written to {output_path}", file=sys.stderr)

    print(text)

    exit_codes = {"promote": 0, "hold": 1, "reject": 2}
    return exit_codes.get(decision.decision, 2)


if __name__ == "__main__":
    raise SystemExit(main())
