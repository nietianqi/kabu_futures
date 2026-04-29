"""auto_evolve.py -- Single-command AI evolution pipeline.

Runs the complete evolution loop in one shot:
    1. analyze      -> signal attribution + paper PnL
    2. tune         -> imbalance grid search
    3. walk_forward -> rolling train/test validation
    4. promote      -> champion/challenger gate decision

All outputs are written to --output-dir (default: reports/).
The script never modifies live config; it only produces JSON reports
and a final decision file.

Usage::

    python scripts/auto_evolve.py logs/
    python scripts/auto_evolve.py logs/ --train-size 2 --output-dir reports/

Exit codes:
    0 -> promote  (stable candidate found; review decision_YYYYMMDD.json)
    1 -> hold     (borderline; accumulate more data)
    2 -> reject   (no stable candidate or gates failed)
    3 -> no_data  (insufficient days to run walk_forward)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from kabu_futures.config import MICRO_ENTRY_PROFILE_DEFAULT, default_config, load_json_config
from kabu_futures.evolution import analyze_micro_log
from kabu_futures.promotion import PromotionThresholds, evaluate_challenger
from kabu_futures.tuning import DEFAULT_IMBALANCE_GRID, tune_micro_params
from kabu_futures.walk_forward import split_books_by_day, walk_forward_micro


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="auto_evolve.py",
        description=(
            "End-to-end AI evolution pipeline: analyze -> tune -> walk_forward -> promote. "
            "Report-only; never rewrites live config."
        ),
    )
    parser.add_argument(
        "path",
        help="JSONL log file, or directory of JSONL files (one per trading session).",
    )
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument(
        "--champion",
        default=str(ROOT / "reports" / "champion.json"),
        help="Champion metrics JSON. Skipped if file does not exist.",
    )
    parser.add_argument("--train-size", type=int, default=1)
    parser.add_argument("--test-size", type=int, default=1)
    parser.add_argument("--step", type=int, default=1)
    parser.add_argument(
        "--min-days",
        type=int,
        default=2,
        help="Minimum trading days required to run walk_forward (default: 2).",
    )
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--pass-threshold", type=float, default=0.7)
    parser.add_argument("--observation-days", type=int, default=0)
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument(
        "--imbalance-grid",
        default=",".join(str(v) for v in DEFAULT_IMBALANCE_GRID),
    )
    parser.add_argument("--max-books", type=int, help="Limit books for analysis/tune (smoke test).")
    parser.add_argument(
        "--regime",
        action="store_true",
        help="Run regime-aware tuning (split logs by high/low vol before tuning).",
    )
    return parser


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _write(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = build_parser().parse_args()
    today = date.today().isoformat()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    config_path = Path(args.config)
    config = load_json_config(config_path) if config_path.exists() else default_config()
    imbalance_grid = _float_tuple(args.imbalance_grid)

    # ------------------------------------------------------------------
    # Step 1: Analyze
    # ------------------------------------------------------------------
    print("=== [1/4] Analyze logs ===")
    baseline_report = analyze_micro_log(args.path, config, max_books=args.max_books)
    analyze_out = out / f"analyze_{today}.json"
    _write(analyze_out, baseline_report)
    signals_total = baseline_report["signals"]["total"]
    paper_trades = baseline_report["paper"]["trades"]
    print(f"  books={baseline_report['books']}  signals={signals_total}  paper_trades={paper_trades}")
    print(f"  -> {analyze_out}")

    # ------------------------------------------------------------------
    # Step 2: Check available days
    # ------------------------------------------------------------------
    print("=== [2/4] Check available days ===")
    grouped = split_books_by_day(args.path)
    days = sorted(grouped.keys())
    print(f"  available days: {len(days)} ({', '.join(days) or 'none'})")

    if len(days) < args.min_days:
        print(f"  insufficient days (need >= {args.min_days}), skipping walk_forward.")
        decision_data = {
            "decision": "no_data",
            "reason": f"only {len(days)} days available, need {args.min_days}",
            "date": today,
            "days": days,
        }
        _write(out / f"decision_{today}.json", decision_data)
        print(f"  -> {out / f'decision_{today}.json'}")
        return 3

    # ------------------------------------------------------------------
    # Step 3: Tune (+ optional regime-aware tuning)
    # ------------------------------------------------------------------
    print("=== [3/4] Parameter grid search ===")
    tune_report = tune_micro_params(
        args.path,
        config,
        imbalance_grid=imbalance_grid,
        max_books=args.max_books,
        min_trades=args.min_trades,
    )
    tune_out = out / f"tune_{today}.json"
    _write(tune_out, tune_report)
    best = tune_report.get("best")
    decision_flag = tune_report.get("decision", "no_change")
    print(f"  decision={decision_flag}  best={best['parameters'] if best else None}")
    print(f"  -> {tune_out}")

    # Optional: regime-aware tuning
    if args.regime:
        try:
            from kabu_futures.regime import split_books_by_regime
            from kabu_futures.walk_forward import _iter_books_from_source
            from kabu_futures.tuning import evaluate_micro_config, _rank_key
            from dataclasses import replace

            all_books = list(_iter_books_from_source(args.path))
            regime_books = split_books_by_regime(all_books)
            effective_micro = config.effective_micro_engine()
            for regime_name, r_books in regime_books.items():
                if regime_name == "warmup" or len(r_books) < 500:
                    continue
                r_candidates: list[dict] = []
                for imbalance in imbalance_grid:
                    params = {"imbalance_entry": float(imbalance)}
                    try:
                        r_cfg = replace(
                            config,
                            micro_engine=replace(
                                config.micro_engine,
                                entry_profile=MICRO_ENTRY_PROFILE_DEFAULT,
                                imbalance_entry=float(imbalance),
                                microprice_entry_ticks=effective_micro.microprice_entry_ticks,
                                ofi_percentile=effective_micro.ofi_percentile,
                            ),
                        )
                        r_cfg.validate()
                    except ValueError:
                        continue
                    r_candidates.append(evaluate_micro_config(r_books, r_cfg, params))
                ranked = sorted(r_candidates, key=_rank_key, reverse=True)
                regime_out = out / f"tune_{regime_name}_{today}.json"
                _write(regime_out, {"regime": regime_name, "books": len(r_books), "candidates": ranked})
                best_r = ranked[0] if ranked else None
                print(f"  [{regime_name}] books={len(r_books)}  best={best_r['parameters'] if best_r else None}  -> {regime_out}")
        except Exception as exc:
            print(f"  [warning] regime tuning error: {exc}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Step 4: Walk-forward
    # ------------------------------------------------------------------
    print("=== [4/4] Walk-forward validation ===")
    wf_report = walk_forward_micro(
        args.path,
        config,
        train_size=args.train_size,
        test_size=args.test_size,
        step=args.step,
        imbalance_grid=imbalance_grid,
        min_trades=args.min_trades,
        pass_threshold=args.pass_threshold,
    )
    wf_out = out / f"walk_forward_{today}.json"
    _write(wf_out, wf_report)
    s = wf_report["summary"]
    print(f"  windows={s['windows']}  pass_rate={s['pass_rate']:.0%}  stable={s['stable']}")
    print(f"  baseline_trades={s['total_baseline_test_trades']}  candidate_trades={s['total_candidate_test_trades']}")
    print(f"  -> {wf_out}")

    # ------------------------------------------------------------------
    # Step 5: Promotion check
    # ------------------------------------------------------------------
    champion_path = Path(args.champion)
    champion: dict = {}
    if champion_path.exists():
        champion = json.loads(champion_path.read_text(encoding="utf-8"))

    thresholds = PromotionThresholds(
        min_trades_per_day=args.min_trades,
        walk_forward_pass_threshold=args.pass_threshold,
    )

    promotion = evaluate_challenger(
        champion,
        tune_report,
        walk_forward_summary=wf_report["summary"],
        thresholds=thresholds,
        observation_days=args.observation_days,
    )

    decision_out = out / f"decision_{today}.json"
    _write(decision_out, promotion.to_dict())

    print(f"\n{'='*50}")
    print(f"  DECISION: {promotion.decision.upper()}")
    print(f"  passed:   {promotion.passed_gates}")
    print(f"  failed:   {promotion.failed_gates}")
    if promotion.veto_reasons:
        print(f"  veto:     {promotion.veto_reasons}")
    print(f"  -> {decision_out}")
    print(f"{'='*50}")

    exit_codes = {"promote": 0, "hold": 1, "reject": 2, "no_data": 3}
    return exit_codes.get(promotion.decision, 2)


if __name__ == "__main__":
    raise SystemExit(main())
