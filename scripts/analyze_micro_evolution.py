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
from kabu_futures.evolution import (
    DEFAULT_ENTRY_FILL_LATENCY_MS,
    DEFAULT_ENTRY_FILL_SLIPPAGE_TICKS,
    DEFAULT_MARKOUT_SECONDS,
    analyze_micro_log,
)


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _int_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="analyze_micro_evolution.py",
        description="Offline replay report with signal attribution, paper PnL, and markout metrics.",
    )
    parser.add_argument("path", help="Live JSONL log path to replay.")
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--max-books", type=int, help="Limit replayed books for quick smoke tests.")
    parser.add_argument(
        "--markout-seconds",
        default=",".join(str(value) for value in DEFAULT_MARKOUT_SECONDS),
        help="Comma-separated markout horizons, e.g. 0.5,1,3,5,30,60.",
    )
    parser.add_argument(
        "--paper-fill-model",
        choices=("immediate", "touch"),
        default="immediate",
        help="Paper fill model used during replay.",
    )
    parser.add_argument(
        "--no-regime",
        action="store_true",
        help="Disable volatility-regime attribution in the JSON report.",
    )
    parser.add_argument(
        "--regime-warmup-periods",
        type=int,
        default=5,
        help="Regime classifier warmup periods before high/low-vol labeling.",
    )
    parser.add_argument(
        "--regime-high-vol-percentile",
        type=float,
        default=75.0,
        help="Rolling percentile threshold used to label high-vol books.",
    )
    parser.add_argument(
        "--entry-fill-slippage-grid",
        default=",".join(str(value) for value in DEFAULT_ENTRY_FILL_SLIPPAGE_TICKS),
        help="Comma-separated entry slippage ticks for FAK fill simulation, e.g. 0,1,2.",
    )
    parser.add_argument(
        "--entry-fill-latency-ms",
        default=",".join(str(value) for value in DEFAULT_ENTRY_FILL_LATENCY_MS),
        help="Comma-separated latency assumptions in milliseconds for FAK fill simulation.",
    )
    parser.add_argument(
        "--diagnostics-max-rows",
        type=int,
        default=None,
        help=(
            "Limit logged live-diagnostic JSONL rows. Defaults to --max-books for smoke tests; "
            "use 0 for full-log diagnostics."
        ),
    )
    parser.add_argument("--output", help="Optional JSON report path.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = analyze_micro_log(
        args.path,
        load_json_config(args.config),
        max_books=args.max_books,
        markout_seconds=_float_tuple(args.markout_seconds),
        paper_fill_model=args.paper_fill_model,
        include_regime=not args.no_regime,
        regime_kwargs={
            "warmup_periods": args.regime_warmup_periods,
            "high_vol_percentile": args.regime_high_vol_percentile,
        },
        entry_fill_slippage_ticks=_int_tuple(args.entry_fill_slippage_grid),
        entry_fill_latency_ms=_int_tuple(args.entry_fill_latency_ms),
        logged_diagnostics_max_rows=args.diagnostics_max_rows
        if args.diagnostics_max_rows is not None
        else args.max_books,
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
