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
from kabu_futures.evolution import DEFAULT_MARKOUT_SECONDS, analyze_micro_log


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="analyze_micro_evolution.py",
        description="Offline micro-evolution replay report with signal attribution, paper PnL, and markout metrics.",
    )
    parser.add_argument("paths", nargs="+", help="Live JSONL log path(s), directory, or glob(s) to replay.")
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--max-books", type=int, help="Limit replayed books for quick smoke tests.")
    parser.add_argument(
        "--markout-seconds",
        default=",".join(str(value) for value in DEFAULT_MARKOUT_SECONDS),
        help="Comma-separated markout horizons, e.g. 0.5,1,3,5.",
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
    config = load_json_config(args.config)
    report = analyze_micro_log(
        args.paths,
        config,
        max_books=args.max_books,
        markout_seconds=_float_tuple(args.markout_seconds),
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
