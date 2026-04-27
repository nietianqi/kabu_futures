from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from kabu_futures.api import KabuApiError, KabuStationClient, build_future_registration_symbols
from kabu_futures.config import default_config, load_json_config
from kabu_futures.live import LiveRunOptions, run_live
from kabu_futures.replay import replay_jsonl


def password_from_config(config, override: str | None = None) -> str:
    if override:
        return override
    env_value = os.environ.get(config.api.api_password_env)
    if env_value:
        return env_value
    if config.api.api_password:
        return config.api.api_password
    raise SystemExit(
        f"kabu API password is missing. Set api.api_password in {ROOT / 'config' / 'local.json'} "
        f"or set ${config.api.api_password_env}."
    )


def register_symbols(args: argparse.Namespace) -> int:
    config = load_json_config(args.config) if args.config else load_json_config(ROOT / "config" / "local.json")
    password = password_from_config(config, args.password)
    future_codes = args.symbols or [config.symbols.primary, config.symbols.filter]
    deriv_month = args.deriv_month if args.deriv_month is not None else config.symbols.deriv_month
    exchanges = args.exchanges or [23, 24]
    production = not args.sandbox
    client = KabuStationClient(password, config.api, production=production)
    try:
        client.authenticate()
        register_payload, resolved = build_future_registration_symbols(client, future_codes, deriv_month, exchanges)
        if args.dry_run:
            response = {"dry_run": True}
        else:
            unregister_response = None if args.keep_registered_symbols else client.unregister_all()
            register_response = client.register(register_payload)
            response = {"unregister_all": unregister_response, "register": register_response}
    except KabuApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    result = {
        "mode": "production" if production else "sandbox",
        "base_url": client.base_url,
        "deriv_month": deriv_month,
        "exchanges": exchanges,
        "resolved": resolved,
        "register_symbols": register_payload,
        "register_response": response,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def live_loop(args: argparse.Namespace) -> int:
    config = load_json_config(args.config) if args.config else load_json_config(ROOT / "config" / "local.json")
    password = password_from_config(config, args.password)
    future_codes = tuple(args.symbols or [config.symbols.primary, config.symbols.filter])
    deriv_month = args.deriv_month if args.deriv_month is not None else config.symbols.deriv_month
    exchanges = tuple(args.exchanges or [23, 24])
    return run_live(
        config,
        password,
        LiveRunOptions(
            production=not args.sandbox,
            exchanges=exchanges,
            future_codes=future_codes,
            deriv_month=deriv_month,
            dry_run=args.dry_run,
            log_dir=ROOT / "logs",
            max_events=args.max_events,
            book_log_mode=args.book_log_mode,
            log_batch_size=args.log_batch_size,
            log_flush_interval_seconds=args.log_flush_interval,
            heartbeat_interval_events=args.heartbeat_events,
            tick_log_mode=args.tick_log_mode,
            tick_log_interval_events=args.tick_log_interval,
            signal_eval_log_mode=args.signal_eval_log_mode,
            clear_registered_symbols=not args.keep_registered_symbols,
            trade_mode=args.trade_mode,
            paper_fill_model=args.paper_fill_model,
            paper_console=not args.no_paper_console,
        ),
    )


def replay_sample(args: argparse.Namespace) -> int:
    config_path = args.config or ROOT / "config" / "local.json"
    replay_path = args.path or ROOT / "data" / "sample_market_data.jsonl"
    for event in replay_jsonl(replay_path, config_path, trade_mode=args.trade_mode):
        print(json.dumps(event, ensure_ascii=False))
    return 0


def run_tests() -> int:
    import unittest

    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Default: production live loop, register NK225micro/TOPIXmini day+night, then keep WebSocket running.",
    )
    parser.add_argument("--config", default=str(ROOT / "config" / "local.json"))
    parser.add_argument("--password")
    parser.add_argument("--sandbox", action="store_true", help="Use 18081 sandbox instead of production 18080.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve symbols but do not call /register.")
    parser.add_argument("--symbol", action="append", dest="symbols", help="FutureCode to register. Can be repeated.")
    parser.add_argument("--deriv-month", type=int)
    parser.add_argument("--exchange", action="append", type=int, dest="exchanges", help="Exchange to register. Default: 23 and 24.")
    parser.add_argument("--replay-sample", action="store_true")
    parser.add_argument("--path", help="JSONL path for --replay-sample.")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--register-only", action="store_true", help="Register symbols and exit instead of running the live loop.")
    parser.add_argument("--max-events", type=int, help="Stop live loop after N WebSocket book events.")
    parser.add_argument(
        "--book-log-mode",
        choices=("full", "sample", "signals_only", "off"),
        default="full",
        help="Book logging mode. Default keeps full audit logs with buffered writes.",
    )
    parser.add_argument("--log-batch-size", type=int, default=256, help="Buffered JSONL rows per disk write.")
    parser.add_argument("--log-flush-interval", type=float, default=1.0, help="Buffered JSONL flush interval in seconds.")
    parser.add_argument("--heartbeat-events", type=int, default=100, help="Print heartbeat every N valid book events.")
    parser.add_argument(
        "--tick-log-mode",
        choices=("off", "sample", "changes", "all"),
        default="off",
        help="Print tick snapshots to console. Default: off, so heartbeat/signal/paper/error logs remain readable.",
    )
    parser.add_argument("--tick-log-interval", type=int, default=1, help="Console tick interval when --tick-log-mode sample is used.")
    parser.add_argument(
        "--signal-eval-log-mode",
        choices=("full", "summary", "allow_only", "off"),
        default="summary",
        help="Signal evaluation logging. summary aggregates repeated rejects while preserving allow events.",
    )
    parser.add_argument(
        "--trade-mode",
        choices=("observe", "paper"),
        default="observe",
        help="Execution mode. Default observe never creates paper or live orders.",
    )
    parser.add_argument(
        "--paper-fill-model",
        choices=("immediate", "touch"),
        default="immediate",
        help="Paper fill model. immediate fills at signal price; touch waits until the book touches the signal price.",
    )
    parser.add_argument(
        "--no-paper-console",
        action="store_true",
        help="Write paper execution events to logs only, without printing them to console.",
    )
    parser.add_argument(
        "--keep-registered-symbols",
        action="store_true",
        help="Do not call /unregister/all before /register. Default clears stale PUSH symbols first.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.test:
        return run_tests()
    if args.replay_sample:
        return replay_sample(args)
    if args.register_only:
        return register_symbols(args)
    return live_loop(args)


if __name__ == "__main__":
    raise SystemExit(main())
