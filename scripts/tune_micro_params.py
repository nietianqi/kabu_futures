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
from kabu_futures.tuning import (
    DEFAULT_IMBALANCE_GRID,
    DEFAULT_MICROPRICE_GRID,
    DEFAULT_OFI_PERCENTILE_GRID,
    DEFAULT_SPREAD_GRID,
    tune_micro_params,
)


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _int_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tune_micro_params.py",
        description="Replay JSONL books across a conservative micro parameter grid. Report-only; never rewrites live config.",
    )
    parser.add_argument("path", help="Live JSONL log path to replay.")
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--max-books", type=int, help="Limit replayed books for quick smoke tests.")
    parser.add_argument("--min-trades", type=int, default=20, help="Minimum trades required before recommending a candidate.")
    parser.add_argument(
        "--imbalance-grid",
        default=",".join(str(value) for value in DEFAULT_IMBALANCE_GRID),
        help="Comma-separated imbalance_entry candidates. Default: 0.28,0.26,0.24.",
    )
    parser.add_argument(
        "--microprice-grid",
        default=",".join(str(value) for value in DEFAULT_MICROPRICE_GRID),
        help="Comma-separated microprice_entry_ticks candidates. Default: 0.12,0.10.",
    )
    parser.add_argument(
        "--ofi-percentile-grid",
        default=",".join(str(value) for value in DEFAULT_OFI_PERCENTILE_GRID),
        help="Comma-separated OFI percentile candidates. Default: 70,65,60.",
    )
    parser.add_argument(
        "--spread-grid",
        default=",".join(str(value) for value in DEFAULT_SPREAD_GRID),
        help="Comma-separated spread_ticks_required candidates. Default: 1.",
    )
    parser.add_argument(
        "--max-drawdown-worsen-ratio",
        type=float,
        default=0.20,
        help="Allowed relative drawdown deterioration before a candidate stops being recommended.",
    )
    parser.add_argument("--min-fill-rate", type=float, default=0.70, help="Minimum simulated entry fill rate for recommendation.")
    parser.add_argument(
        "--paper-fill-model",
        choices=("immediate", "touch"),
        default="immediate",
        help="Paper fill model used during replay.",
    )
    parser.add_argument("--output", help="Optional JSON report path.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = tune_micro_params(
        args.path,
        load_json_config(args.config),
        imbalance_grid=_float_tuple(args.imbalance_grid),
        microprice_grid=_float_tuple(args.microprice_grid),
        ofi_percentile_grid=_float_tuple(args.ofi_percentile_grid),
        spread_grid=_int_tuple(args.spread_grid),
        max_books=args.max_books,
        min_trades=args.min_trades,
        max_drawdown_worsen_ratio=args.max_drawdown_worsen_ratio,
        min_fill_rate=args.min_fill_rate,
        paper_fill_model=args.paper_fill_model,
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
