from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from kabu_futures.config import load_json_config
from kabu_futures.tuning import default_micro_grid, tune_micro_params, write_challenger_micro_engine


def _float_grid(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _int_grid(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tune_micro_params.py",
        description="Offline micro-parameter tuner. It prints recommendations only and never rewrites live config.",
    )
    parser.add_argument("paths", nargs="+", help="Live JSONL log path(s), directory, or glob(s) to replay.")
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--top", type=int, default=10, help="Number of top candidates to print.")
    parser.add_argument("--min-trades", type=int, default=5, help="Minimum closed paper trades required for a recommendation.")
    parser.add_argument("--max-books", type=int, help="Limit replayed books for quick smoke tests.")
    parser.add_argument("--imbalance-grid", help="Comma-separated imbalance_entry values, e.g. 0.18,0.22,0.26,0.30")
    parser.add_argument("--microprice-grid", help="Comma-separated microprice_entry_ticks values.")
    parser.add_argument("--take-profit-grid", help="Comma-separated take_profit_ticks values.")
    parser.add_argument("--stop-loss-grid", help="Comma-separated stop_loss_ticks values.")
    parser.add_argument(
        "--write-challenger",
        help="Optional path for a recommended micro_engine override JSON. No file is written for no_change decisions.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = load_json_config(args.config)
    grid = default_micro_grid(config)
    if args.imbalance_grid:
        grid["imbalance_entry"] = _float_grid(args.imbalance_grid)
    if args.microprice_grid:
        grid["microprice_entry_ticks"] = _float_grid(args.microprice_grid)
    if args.take_profit_grid:
        grid["take_profit_ticks"] = _int_grid(args.take_profit_grid)
    if args.stop_loss_grid:
        grid["stop_loss_ticks"] = _int_grid(args.stop_loss_grid)
    report = tune_micro_params(
        args.paths,
        config,
        grid=grid,
        min_trades=args.min_trades,
        top_n=args.top,
        max_books=args.max_books,
    )
    if args.write_challenger:
        report["challenger_written"] = write_challenger_micro_engine(report, args.write_challenger)
        report["challenger_path"] = str(Path(args.write_challenger))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
