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
from kabu_futures.tuning import DEFAULT_IMBALANCE_GRID, tune_micro_params


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tune_micro_params.py",
        description="Replay JSONL books across a conservative micro imbalance grid. Report-only; never rewrites live config.",
    )
    parser.add_argument("path", help="Live JSONL log path to replay.")
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--max-books", type=int, help="Limit replayed books for quick smoke tests.")
    parser.add_argument("--min-trades", type=int, default=20, help="Minimum trades required before recommending a candidate.")
    parser.add_argument(
        "--imbalance-grid",
        default=",".join(str(value) for value in DEFAULT_IMBALANCE_GRID),
        help="Comma-separated imbalance_entry candidates. Default: 0.18,0.20,0.22,0.25,0.30.",
    )
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
        max_books=args.max_books,
        min_trades=args.min_trades,
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
