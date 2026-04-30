from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from kabu_futures.analysis_utils import iter_books, markout_summary
from kabu_futures.config import StrategyConfig, default_config, load_json_config, micro_entry_profile_metadata
from kabu_futures.evolution import analyze_micro_log, calculate_markout_ticks
from kabu_futures.log_diagnostics import diagnose_log
from kabu_futures.models import OrderBook
from kabu_futures.promotion import PromotionThresholds, evaluate_challenger
from kabu_futures.tuning import (
    DEFAULT_IMBALANCE_GRID,
    DEFAULT_MICROPRICE_GRID,
    DEFAULT_OFI_PERCENTILE_GRID,
    tune_micro_params,
)
from kabu_futures.walk_forward import walk_forward_micro


DEFAULT_SLIPPAGE_GRID = (0, 1)
DEFAULT_FILL_LATENCY_MS = (0, 100, 250)
DEFAULT_JUMP_MARKOUT_SECONDS = (1.0, 5.0, 30.0)


def _float_tuple(value: str) -> tuple[float, ...]:
    return tuple(float(item.strip()) for item in value.split(",") if item.strip())


def _int_tuple(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _optional_positive_int(value: int | None) -> int | None:
    if value is None or value <= 0:
        return None
    return int(value)


def _load_config(path: str | Path) -> StrategyConfig:
    config_path = Path(path)
    if config_path.exists():
        return load_json_config(config_path)
    return default_config()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="candidate_review.py",
        description=(
            "Run a read-only conservative micro candidate review: diagnostics, "
            "candidate replay, FAK fill simulation, walk-forward, and promotion gates."
        ),
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=str(ROOT / "logs"),
        help="JSONL log file or directory. Defaults to logs/.",
    )
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--output-dir", default=str(ROOT / "reports"))
    parser.add_argument("--output", help="Optional explicit report path.")
    parser.add_argument("--tag", help="Report tag. Defaults to today's date.")
    parser.add_argument("--baseline", default=str(ROOT / "reports" / "baseline.json"))
    parser.add_argument("--max-books", type=int, help="Limit replayed books for quick smoke tests.")
    parser.add_argument(
        "--diagnostics-max-rows",
        type=int,
        default=None,
        help="Limit raw JSONL rows read by diagnostics. Defaults to full log; use with --max-books for smoke tests.",
    )
    parser.add_argument("--analyze-max-books", type=int, help="Override max books for evolution analysis.")
    parser.add_argument("--tune-max-books", type=int, help="Override max books for candidate replay.")
    parser.add_argument("--walk-forward-max-books-per-day", type=int, help="Cap books per day for walk-forward.")
    parser.add_argument("--skip-walk-forward", action="store_true", help="Skip walk-forward for a faster smoke review.")
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
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--max-drawdown-worsen-ratio", type=float, default=0.20)
    parser.add_argument("--min-fill-rate", type=float, default=0.70)
    parser.add_argument("--walk-forward-train-size", type=int, default=1)
    parser.add_argument("--walk-forward-test-size", type=int, default=1)
    parser.add_argument("--walk-forward-step", type=int, default=1)
    parser.add_argument("--walk-forward-pass-threshold", type=float, default=0.70)
    parser.add_argument(
        "--entry-fill-slippage-grid",
        default=",".join(str(value) for value in DEFAULT_SLIPPAGE_GRID),
        help="FAK fill slippage ticks for the read-only simulation. Default: 0,1.",
    )
    parser.add_argument(
        "--entry-fill-latency-ms",
        default=",".join(str(value) for value in DEFAULT_FILL_LATENCY_MS),
        help="FAK fill latency assumptions in milliseconds. Default: 0,100,250.",
    )
    parser.add_argument(
        "--jump-markout-seconds",
        default=",".join(str(value) for value in DEFAULT_JUMP_MARKOUT_SECONDS),
        help="Jump post-hoc markout horizons in seconds. Default: 1,5,30.",
    )
    return parser


def build_report(args: argparse.Namespace) -> dict[str, object]:
    config = _load_config(args.config)
    source = Path(args.path)
    diagnostics_max_rows = _optional_positive_int(args.diagnostics_max_rows)
    max_books = _optional_positive_int(args.max_books)
    analyze_max_books = _optional_positive_int(args.analyze_max_books) or max_books
    tune_max_books = _optional_positive_int(args.tune_max_books) or max_books

    imbalance_grid = _float_tuple(args.imbalance_grid)
    microprice_grid = _float_tuple(args.microprice_grid)
    ofi_grid = _float_tuple(args.ofi_percentile_grid)
    slippage_grid = _int_tuple(args.entry_fill_slippage_grid)
    latency_grid = _int_tuple(args.entry_fill_latency_ms)
    jump_horizons = _float_tuple(args.jump_markout_seconds)

    diagnostics = diagnose_log(source, config, max_rows=diagnostics_max_rows)
    tune_report = tune_micro_params(
        source,
        config,
        imbalance_grid=imbalance_grid,
        microprice_grid=microprice_grid,
        ofi_percentile_grid=ofi_grid,
        max_books=tune_max_books,
        min_trades=args.min_trades,
        max_drawdown_worsen_ratio=args.max_drawdown_worsen_ratio,
        min_fill_rate=args.min_fill_rate,
    )
    evolution_report = analyze_micro_log(
        source,
        config,
        max_books=analyze_max_books,
        include_regime=True,
        entry_fill_slippage_ticks=slippage_grid,
        entry_fill_latency_ms=latency_grid,
        logged_diagnostics_max_rows=diagnostics_max_rows,
    )
    jump_posthoc = jump_markout_report(
        source,
        config,
        horizons=jump_horizons,
        max_books=analyze_max_books,
        max_rows=diagnostics_max_rows,
    )
    walk_forward_report = None
    if not args.skip_walk_forward:
        walk_forward_report = walk_forward_micro(
            source,
            config,
            train_size=args.walk_forward_train_size,
            test_size=args.walk_forward_test_size,
            step=args.walk_forward_step,
            imbalance_grid=imbalance_grid,
            min_trades=args.min_trades,
            pass_threshold=args.walk_forward_pass_threshold,
            max_books_per_day=_optional_positive_int(args.walk_forward_max_books_per_day),
        )

    champion = load_or_build_champion(Path(args.baseline), tune_report)
    promotion = promotion_decision(
        champion,
        tune_report,
        walk_forward_report,
        max_drawdown_worsen_ratio=args.max_drawdown_worsen_ratio,
        walk_forward_pass_threshold=args.walk_forward_pass_threshold,
        observation_days=_observation_days(diagnostics, walk_forward_report),
    )
    gates = safety_gates(
        tune_report,
        walk_forward_report,
        promotion,
        min_fill_rate=args.min_fill_rate,
        walk_forward_pass_threshold=args.walk_forward_pass_threshold,
    )
    final_decision = final_candidate_decision(tune_report, gates, promotion)
    profile_metadata = micro_entry_profile_metadata(config)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "config": str(args.config),
        "read_only": True,
        "live_config_changed": False,
        "live_rules_unchanged": {
            "qty": 1,
            "micro_entry_profile": profile_metadata["micro_entry_profile"],
            "micro_effective_thresholds": profile_metadata["micro_effective_thresholds"],
            "spread_ticks_required": config.effective_micro_engine().spread_ticks_required,
            "max_positions_per_symbol": config.live_execution.max_positions_per_symbol,
            "minute_atr_filter": True,
            "minute_execution_score_filter": True,
            "loss_auto_close": False,
        },
        "claude_claims_review": {
            "micro_candidate_emitter_connected": True,
            "candidate_emitter_note": "MicroCandidateEmitter is already instantiated and fed from run_live.",
            "fak_fill_rate_note": (
                "Use entry submitted to own position detected, not all live_order_submitted including exits/retries."
            ),
            "jump_live_behavior_changed": False,
        },
        "diagnostics": diagnostics,
        "candidate_replay": tune_report,
        "fak_fill_simulation": _fak_section(evolution_report),
        "jump_posthoc": jump_posthoc,
        "walk_forward": walk_forward_report,
        "promotion": promotion,
        "safety_gates": gates,
        "final_decision": final_decision,
        "notes": [
            "Candidate parameters are report-only and never overwrite config/local.json.",
            "micro_candidate remains diagnostic-only and is not routed to LiveExecutionController.",
            "Jump handling is diagnostic-only; live strategy still blocks jump_detected.",
            "Loss-hold policy remains no automatic loss close: block entries, cancel pending entries, alert/manual review.",
            "Promotion order is champion/baseline first, challenger tune report second.",
        ],
    }


def load_or_build_champion(baseline_path: Path, tune_report: dict[str, object]) -> dict[str, object]:
    if baseline_path.exists():
        try:
            raw = json.loads(baseline_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raw = {}
        champion = _normalize_champion(raw)
        if champion:
            return {**champion, "source": str(baseline_path)}
    baseline = tune_report.get("baseline")
    if isinstance(baseline, dict):
        return {**baseline, "source": "temporary_champion_from_current_tune_baseline"}
    return {"source": "empty_temporary_champion"}


def _normalize_champion(raw: dict[str, object]) -> dict[str, object]:
    if _looks_like_metrics(raw):
        return raw
    for key in ("champion", "baseline", "best_safe_candidate", "best"):
        value = raw.get(key)
        if isinstance(value, dict) and _looks_like_metrics(value):
            return value
    return raw if raw else {}


def _looks_like_metrics(value: dict[str, object]) -> bool:
    return "trades" in value or "net_pnl_ticks" in value or "parameters" in value


def promotion_decision(
    champion: dict[str, object],
    tune_report: dict[str, object],
    walk_forward_report: dict[str, object] | None,
    *,
    max_drawdown_worsen_ratio: float,
    walk_forward_pass_threshold: float,
    observation_days: int,
) -> dict[str, object]:
    thresholds = PromotionThresholds(
        min_pnl_delta_ticks=0.0,
        max_drawdown_ratio=1.0 + max(0.0, max_drawdown_worsen_ratio),
        min_trades_per_day=0.0,
        min_avg_markout_ticks=0.0,
        walk_forward_pass_threshold=walk_forward_pass_threshold,
        min_observation_days=0,
        allow_grid_boundary=True,
    )
    wf_summary = None
    if isinstance(walk_forward_report, dict):
        summary = walk_forward_report.get("summary")
        if isinstance(summary, dict):
            wf_summary = summary
    decision = evaluate_challenger(
        champion,
        tune_report,
        walk_forward_summary=wf_summary,
        thresholds=thresholds,
        observation_days=observation_days,
    )
    return {
        **decision.to_dict(),
        "champion_source": champion.get("source", "unknown"),
        "promotion_order": ["champion", "challenger"],
        "thresholds": {
            "min_pnl_delta_ticks": thresholds.min_pnl_delta_ticks,
            "max_drawdown_ratio": thresholds.max_drawdown_ratio,
            "min_trades_per_day": thresholds.min_trades_per_day,
            "min_avg_markout_ticks": thresholds.min_avg_markout_ticks,
            "walk_forward_pass_threshold": thresholds.walk_forward_pass_threshold,
            "allow_grid_boundary": thresholds.allow_grid_boundary,
        },
    }


def safety_gates(
    tune_report: dict[str, object],
    walk_forward_report: dict[str, object] | None,
    promotion: dict[str, object],
    *,
    min_fill_rate: float,
    walk_forward_pass_threshold: float,
) -> dict[str, object]:
    recommendation = tune_report.get("recommendation") if isinstance(tune_report.get("recommendation"), dict) else {}
    checks = recommendation.get("checks", {}) if isinstance(recommendation, dict) else {}
    if not isinstance(checks, dict):
        checks = {}
    wf_summary = walk_forward_report.get("summary") if isinstance(walk_forward_report, dict) else None
    wf_pass_rate = float(wf_summary.get("pass_rate", 0.0)) if isinstance(wf_summary, dict) else 0.0
    wf_windows = int(wf_summary.get("windows", 0)) if isinstance(wf_summary, dict) else 0
    wf_skipped = walk_forward_report is None
    gate_values = {
        "trade_opportunity_2x": bool(checks.get("trade_opportunity_2x")),
        "net_pnl_not_worse": bool(checks.get("net_pnl_not_worse")),
        "drawdown_not_materially_worse": bool(checks.get("drawdown_not_materially_worse")),
        "simulated_fill_rate_ok": bool(checks.get("simulated_fill_rate_ok")),
        "walk_forward_pass_rate_ok": (wf_pass_rate >= walk_forward_pass_threshold and wf_windows > 0) if not wf_skipped else False,
        "promotion_not_reject": promotion.get("decision") != "reject",
        "live_config_unchanged": True,
    }
    return {
        "required": gate_values,
        "passed": all(gate_values.values()),
        "min_fill_rate": min_fill_rate,
        "walk_forward_pass_threshold": walk_forward_pass_threshold,
        "walk_forward_skipped": wf_skipped,
        "tune_recommendation_decision": recommendation.get("decision") if isinstance(recommendation, dict) else None,
    }


def final_candidate_decision(
    tune_report: dict[str, object],
    gates: dict[str, object],
    promotion: dict[str, object],
) -> str:
    if tune_report.get("decision") != "recommended":
        return "no_change"
    if not bool(gates.get("passed")):
        return "no_change"
    if promotion.get("decision") == "reject":
        return "no_change"
    return "candidate_ready_for_manual_gray_review"


def _fak_section(evolution_report: dict[str, object]) -> dict[str, object]:
    entry_fill = evolution_report.get("entry_fill_diagnostics")
    if not isinstance(entry_fill, dict):
        return {}
    observed = entry_fill.get("observed_live_funnel", {})
    simulation = entry_fill.get("fak_fill_simulation", {})
    return {
        "observed_live_funnel": observed if isinstance(observed, dict) else {},
        "slippage_0_1_tick_simulation": _filter_slippage(simulation, {"0", "1"}),
        "source_note": "Observed fill rate counts entry submissions to own fills, not exit orders or retries.",
    }


def _filter_slippage(simulation: object, allowed: set[str]) -> dict[str, object]:
    if not isinstance(simulation, dict):
        return {}
    by_slippage = simulation.get("by_slippage_latency")
    if not isinstance(by_slippage, dict):
        return simulation
    filtered = {key: value for key, value in by_slippage.items() if str(key) in allowed}
    return {**simulation, "by_slippage_latency": filtered, "slippage_ticks": sorted(int(key) for key in filtered)}


def _observation_days(diagnostics: dict[str, object], walk_forward_report: dict[str, object] | None) -> int:
    if isinstance(walk_forward_report, dict):
        days = walk_forward_report.get("days")
        if isinstance(days, list):
            return len(days)
    files = diagnostics.get("files")
    if isinstance(files, list):
        return len({Path(str(file)).stem for file in files})
    return 0


class _Quote:
    __slots__ = ("timestamp", "symbol", "mid", "spread_ticks")

    def __init__(self, timestamp: datetime, symbol: str, mid: float, spread_ticks: float) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.mid = mid
        self.spread_ticks = spread_ticks


def jump_markout_report(
    source: str | Path,
    config: StrategyConfig,
    *,
    horizons: Iterable[float] = DEFAULT_JUMP_MARKOUT_SECONDS,
    max_books: int | None = None,
    max_rows: int | None = None,
) -> dict[str, object]:
    horizon_values = tuple(float(value) for value in horizons)
    required_spread_ticks = config.effective_micro_engine().spread_ticks_required
    quotes_by_symbol, times_by_symbol, books_loaded = _load_quote_index(source, config, max_books=max_books)
    by_reason: dict[str, dict[str, Any]] = defaultdict(_jump_bucket)
    rows_read = 0
    for row in _iter_jsonl_rows(source):
        if max_rows is not None and rows_read >= max_rows:
            break
        rows_read += 1
        kind = str(row.get("kind") or row.get("event") or "")
        payload = _payload(row)
        if kind not in {"signal_eval", "signal_eval_summary"} and payload.get("event") not in {"signal_eval", "signal_eval_summary"}:
            continue
        metadata = _evaluation_metadata(payload)
        reason = str(metadata.get("jump_reason") or "unknown")
        if payload.get("reason") != "jump_detected" and metadata.get("jump_detected") is not True:
            continue
        count = _event_count(payload)
        symbol = str(payload.get("symbol") or "unknown")
        direction = str(payload.get("candidate_direction") or metadata.get("near_miss_direction") or "flat")
        timestamp = _event_timestamp(payload)
        bucket = by_reason[reason]
        bucket["events"] += count
        bucket["directions"][direction] += count
        bucket["symbols"][symbol] += count
        if timestamp is None or symbol == "unknown":
            bucket["missing_quote_samples"] += 1
            continue
        quote = _future_quote(symbol, timestamp, 0.0, quotes_by_symbol, times_by_symbol)
        if quote is None:
            bucket["missing_quote_samples"] += 1
            continue
        bucket["samples"] += 1
        for horizon in horizon_values:
            future = _future_quote(symbol, timestamp + timedelta(seconds=horizon), 0.0, quotes_by_symbol, times_by_symbol)
            if future is None:
                continue
            future_tradeable = bucket["future_tradeable_quotes"][str(horizon)]
            future_tradeable["samples"] += count
            if round(future.spread_ticks) == required_spread_ticks:
                future_tradeable["tradeable_quotes"] += count
            tick_size = config.tick_size_for(symbol)
            abs_move = abs(future.mid - quote.mid) / tick_size
            bucket["absolute_move"][str(horizon)].append(abs_move)
            if direction in {"long", "short"}:
                markout = calculate_markout_ticks(direction, quote.mid, future.mid, tick_size)  # type: ignore[arg-type]
                bucket["directional_markout"][str(horizon)].append(markout)

    return {
        "books_loaded": books_loaded,
        "rows_read": rows_read,
        "horizons_seconds": list(horizon_values),
        "required_spread_ticks": required_spread_ticks,
        "live_behavior_changed": False,
        "by_jump_reason": {
            reason: {
                "events": int(bucket["events"]),
                "samples": int(bucket["samples"]),
                "missing_quote_samples": int(bucket["missing_quote_samples"]),
                "directions": dict(bucket["directions"]),
                "symbols": dict(bucket["symbols"]),
                "future_tradeable_quotes": _future_tradeability_summary(bucket["future_tradeable_quotes"]),
                "absolute_move_ticks": markout_summary(bucket["absolute_move"]),
                "directional_markout_ticks": markout_summary(bucket["directional_markout"]),
            }
            for reason, bucket in sorted(by_reason.items())
        },
        "note": "Jump markout is post-hoc diagnostics only; live jump_detected behavior is unchanged.",
    }


def _jump_bucket() -> dict[str, Any]:
    return {
        "events": 0,
        "samples": 0,
        "missing_quote_samples": 0,
        "directions": Counter(),
        "symbols": Counter(),
        "future_tradeable_quotes": defaultdict(lambda: {"samples": 0, "tradeable_quotes": 0}),
        "absolute_move": defaultdict(list),
        "directional_markout": defaultdict(list),
    }


def _future_tradeability_summary(raw: dict[str, dict[str, int]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for horizon, values in sorted(raw.items(), key=lambda item: float(item[0])):
        samples = int(values.get("samples", 0))
        tradeable = int(values.get("tradeable_quotes", 0))
        summary[horizon] = {
            "samples": float(samples),
            "tradeable_quotes": float(tradeable),
            "tradeable_rate": round(tradeable / samples, 4) if samples else 0.0,
        }
    return summary


def _load_quote_index(
    source: str | Path,
    config: StrategyConfig,
    *,
    max_books: int | None = None,
) -> tuple[dict[str, list[_Quote]], dict[str, list[datetime]], int]:
    quotes_by_symbol: dict[str, list[_Quote]] = defaultdict(list)
    books_loaded = 0
    for book in iter_books(source):
        books_loaded += 1
        quotes_by_symbol[book.symbol].append(_book_quote(book, config))
        if max_books is not None and books_loaded >= max_books:
            break
    times_by_symbol: dict[str, list[datetime]] = {}
    for symbol, quotes in quotes_by_symbol.items():
        quotes.sort(key=lambda quote: quote.timestamp)
        times_by_symbol[symbol] = [quote.timestamp for quote in quotes]
    return quotes_by_symbol, times_by_symbol, books_loaded


def _book_quote(book: OrderBook, config: StrategyConfig) -> _Quote:
    timestamp = book.received_at or book.timestamp
    tick_size = config.tick_size_for(book.symbol)
    spread_ticks = (book.best_ask_price - book.best_bid_price) / tick_size if tick_size > 0 else 0.0
    return _Quote(timestamp, book.symbol, (book.best_bid_price + book.best_ask_price) / 2.0, spread_ticks)


def _future_quote(
    symbol: str,
    timestamp: datetime,
    latency_seconds: float,
    quotes_by_symbol: dict[str, list[_Quote]],
    times_by_symbol: dict[str, list[datetime]],
) -> _Quote | None:
    quotes = quotes_by_symbol.get(symbol)
    times = times_by_symbol.get(symbol)
    if not quotes or not times:
        return None
    target = timestamp + timedelta(seconds=latency_seconds)
    index = bisect_left(times, target)
    if index >= len(quotes):
        return None
    return quotes[index]


def _iter_jsonl_rows(source: str | Path) -> Iterable[dict[str, Any]]:
    path = Path(source)
    paths = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    for jsonl_path in paths:
        try:
            handle = jsonl_path.open("r", encoding="utf-8")
        except OSError:
            continue
        with handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    yield row


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    return payload if isinstance(payload, dict) else row


def _evaluation_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("metadata", "last_metadata", "first_metadata"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _event_count(payload: dict[str, Any]) -> int:
    value = payload.get("count")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return 1
    return parsed if parsed > 0 else 1


def _event_timestamp(payload: dict[str, Any]) -> datetime | None:
    for key in ("timestamp", "start_timestamp", "end_timestamp"):
        value = payload.get(key)
        if not value:
            continue
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            continue
    return None


def _default_output_path(args: argparse.Namespace) -> Path:
    if args.output:
        return Path(args.output)
    tag = args.tag or datetime.now().date().isoformat()
    return Path(args.output_dir) / f"candidate_review_{tag}.json"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = build_report(args)
    output_path = _default_output_path(args)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(
        "Candidate review written to "
        f"{output_path} | decision={report['final_decision']} | "
        f"promotion={report['promotion']['decision']}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
