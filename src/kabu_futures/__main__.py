from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from .api import KabuApiError, KabuStationClient, build_future_registration_symbols
from .config import default_config, load_json_config
from .replay import replay_jsonl


def _password_from_args(password: str | None, env_name: str, config_password: str = "") -> str:
    if password:
        return password
    env_value = os.environ.get(env_name)
    if env_value:
        return env_value
    if config_password:
        return config_password
    print(f"kabu API password is missing. Set ${env_name}, pass --password, or set api.api_password in a local config file.", file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(prog="kabu_futures")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay")
    replay.add_argument("path")
    replay.add_argument("--config")
    replay.add_argument("--trade-mode", choices=("observe", "paper"), default="observe")
    api_register = sub.add_parser("api-register")
    api_register.add_argument("--config")
    api_register.add_argument("--production", action="store_true")
    api_register.add_argument("--password")
    api_register.add_argument("--password-env")
    api_register.add_argument("--symbol", action="append", dest="symbols")
    api_register.add_argument("--deriv-month", type=int)
    api_register.add_argument("--exchange", action="append", type=int, dest="exchanges")
    api_register.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.command == "replay":
        replay_path = Path(args.path)
        if not replay_path.exists():
            print(f"Replay file not found: {replay_path}", file=sys.stderr)
            print("Use a real JSONL path, for example: D:\\kabu_futures\\data\\sample_market_data.jsonl", file=sys.stderr)
            raise SystemExit(2)
        for event in replay_jsonl(args.path, args.config, trade_mode=args.trade_mode):
            print(json.dumps(event, ensure_ascii=False))
    elif args.command == "api-register":
        config = load_json_config(args.config) if args.config else default_config()
        password_env = args.password_env or config.api.api_password_env
        password = _password_from_args(args.password, password_env, config.api.api_password)
        future_codes = args.symbols or list(dict.fromkeys((*config.trade_symbols(), config.symbols.filter)))
        deriv_month = args.deriv_month if args.deriv_month is not None else config.symbols.deriv_month
        exchanges = args.exchanges or [23]
        client = KabuStationClient(password, config.api, production=args.production)
        try:
            client.authenticate()
            register_symbols, resolved = build_future_registration_symbols(client, future_codes, deriv_month, exchanges)
            response = {"dry_run": True} if args.dry_run else client.register(register_symbols)
        except KabuApiError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(1) from exc
        result = {
            "authenticated": True,
            "base_url": client.base_url,
            "deriv_month": deriv_month,
            "exchanges": exchanges,
            "resolved": resolved,
            "register_symbols": register_symbols,
            "register_response": response,
        }
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
