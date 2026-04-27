"""CLI for walk-forward rolling validation of micro-parameter tuning."""
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
from kabu_futures.tuning import default_micro_grid
from kabu_futures.walk_forward import walk_forward_micro


def _float_grid(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _int_grid(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="walk_forward.py",
        description="Walk-forward rolling validation. Splits the JSONL log by trading day, "
        "trains parameters on a rolling window, and tests on the held-out day(s).",
    )
    parser.add_argument("paths", nargs="+", help="Live JSONL log path(s), directory, or glob(s) to replay.")
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--train-size", type=int, default=5, help="Train window size in trading days (default 5).")
    parser.add_argument("--test-size", type=int, default=1, help="Test window size in trading days (default 1).")
    parser.add_argument("--step", type=int, default=1, help="Roll step in trading days (default 1).")
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument(
        "--pass-threshold",
        type=float,
        default=0.7,
        help="Fraction of windows challenger must beat baseline to be considered stable (default 0.7).",
    )
    parser.add_argument("--max-books-per-day", type=int, help="Limit per-day books for smoke tests.")
    parser.add_argument("--imbalance-grid")
    parser.add_argument("--microprice-grid")
    parser.add_argument("--take-profit-grid")
    parser.add_argument("--stop-loss-grid")
    parser.add_argument("--output", help="Optional JSON report path.")
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
    report = walk_forward_micro(
        args.paths,
        config,
        train_size=args.train_size,
        test_size=args.test_size,
        step=args.step,
        grid=grid,
        min_trades=args.min_trades,
        pass_threshold=args.pass_threshold,
        max_books_per_day=args.max_books_per_day,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
