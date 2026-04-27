"""Walk-forward rolling validation CLI.

Usage::

    python scripts/walk_forward.py logs/
    python scripts/walk_forward.py logs/ --train-size 2 --test-size 1
    python scripts/walk_forward.py logs/ --output reports/wf.json
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

from kabu_futures.config import default_config, load_json_config
from kabu_futures.tuning import DEFAULT_IMBALANCE_GRID
from kabu_futures.walk_forward import walk_forward_micro


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _resolve_source(path_str: str) -> str | Path:
    p = Path(path_str)
    if p.is_dir():
        # Return sorted list of JSONL files
        return p
    return p


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="walk_forward.py",
        description=(
            "Rolling walk-forward validation: tune imbalance grid on train days, "
            "evaluate on held-out test days. Report-only; never rewrites live config."
        ),
    )
    parser.add_argument(
        "path",
        help="JSONL log file, or directory of JSONL files (one per trading session).",
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "config" / "local.json"),
        help="Strategy config JSON (default: config/local.json).",
    )
    parser.add_argument("--train-size", type=int, default=1, help="Training window in days (default: 1).")
    parser.add_argument("--test-size", type=int, default=1, help="Test window in days (default: 1).")
    parser.add_argument("--step", type=int, default=1, help="Days to advance between windows (default: 1).")
    parser.add_argument(
        "--imbalance-grid",
        default=",".join(str(v) for v in DEFAULT_IMBALANCE_GRID),
        help="Comma-separated imbalance_entry candidates.",
    )
    parser.add_argument("--min-trades", type=int, default=5, help="Minimum trades to consider a candidate valid.")
    parser.add_argument(
        "--pass-threshold",
        type=float,
        default=0.7,
        help="Fraction of windows where candidate must beat baseline to be 'stable' (default: 0.7).",
    )
    parser.add_argument("--max-books-per-day", type=int, help="Cap books loaded per day (for quick tests).")
    parser.add_argument("--output", help="Optional JSON report output path.")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    config_path = Path(args.config)
    config = load_json_config(config_path) if config_path.exists() else default_config()

    report = walk_forward_micro(
        args.path,
        config,
        train_size=args.train_size,
        test_size=args.test_size,
        step=args.step,
        imbalance_grid=_float_tuple(args.imbalance_grid),
        min_trades=args.min_trades,
        pass_threshold=args.pass_threshold,
        max_books_per_day=args.max_books_per_day,
    )

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
        print(f"Report written to {output_path}", file=sys.stderr)

    print(text)

    # Print summary to stderr for quick reading
    s = report["summary"]
    print(
        f"\n=== Walk-Forward Summary ===\n"
        f"  Days: {report['days']}\n"
        f"  Windows: {s['windows']}  Pass rate: {s['pass_rate']:.0%}  Stable: {s['stable']}\n"
        f"  Baseline test trades: {s['total_baseline_test_trades']}\n"
        f"  Candidate test trades: {s['total_candidate_test_trades']}\n"
        f"  Diagnostics: {s['diagnostics'] or 'none'}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
