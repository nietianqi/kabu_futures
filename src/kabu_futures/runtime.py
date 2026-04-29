from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from typing import Any

from .config import StrategyConfig, micro_entry_profile_metadata


FINGERPRINT_SOURCE_FILES = (
    "config.py",
    "policy.py",
    "live_safety.py",
    "live_execution.py",
    "execution.py",
    "live.py",
)


def code_fingerprint() -> str:
    digest = hashlib.sha256()
    source_dir = Path(__file__).resolve().parent
    for name in FINGERPRINT_SOURCE_FILES:
        path = source_dir / name
        digest.update(name.encode("utf-8"))
        if path.exists():
            digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def config_fingerprint(config: StrategyConfig) -> str:
    payload = json.dumps(_jsonable(asdict(config)), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def live_startup_self_check(config: StrategyConfig) -> dict[str, object]:
    return {
        "code_fingerprint": code_fingerprint(),
        "config_fingerprint": config_fingerprint(config),
        **micro_entry_profile_metadata(config),
        "live_minute_atr_filter": True,
        "live_minute_execution_score_filter": True,
        "min_execution_score_to_chase": config.multi_timeframe.min_execution_score_to_chase,
        "live_supported_engines": config.live_execution.supported_engines,
        "live_safety": {
            "max_positions_per_symbol": config.live_execution.max_positions_per_symbol,
            "minute_cooldown_seconds": config.live_execution.minute_cooldown_seconds,
            "micro_loss_pause_seconds": config.live_execution.micro_loss_pause_seconds,
            "max_consecutive_micro_small_losses": config.live_execution.max_consecutive_micro_small_losses,
            "loss_hold_guard_ticks": config.live_execution.loss_hold_guard_ticks,
            "daily_loss_limit_yen": config.live_execution.daily_loss_limit_yen,
            "kill_switch_enabled": config.live_execution.kill_switch_enabled,
        },
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value
