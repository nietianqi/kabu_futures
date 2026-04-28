from __future__ import annotations

from typing import Any


SOFT_DIRECTIONAL_CHECKS = ("imbalance", "ofi", "microprice")


def failed_directional_checks(
    imbalance_ok: bool,
    ofi_ok: bool,
    microprice_ok: bool,
    minute_ok: bool,
    topix_ok: bool,
) -> list[str]:
    """Return failed directional checks in stable order."""
    failed: list[str] = []
    if not imbalance_ok:
        failed.append("imbalance")
    if not ofi_ok:
        failed.append("ofi")
    if not microprice_ok:
        failed.append("microprice")
    if not minute_ok:
        failed.append("minute_bias")
    if not topix_ok:
        failed.append("topix_bias")
    return failed


def is_near_miss(
    spread_ok: bool,
    jump_detected: bool,
    too_soon: bool,
    failed_checks: list[str],
) -> bool:
    """True when exactly one soft micro check blocks an otherwise clean setup."""
    if not spread_ok or jump_detected or too_soon:
        return False
    if len(failed_checks) != 1:
        return False
    return failed_checks[0] in SOFT_DIRECTIONAL_CHECKS


def near_miss_key(metadata: dict[str, Any]) -> tuple[str, str] | None:
    """Return ``(direction, missing_check)`` for a stable micro near-miss."""
    if not metadata.get("near_miss"):
        inferred = infer_near_miss_key(metadata)
        return inferred
    direction = metadata.get("near_miss_direction")
    missing = metadata.get("near_miss_missing")
    if direction in ("long", "short") and isinstance(missing, str):
        return str(direction), missing
    if direction == "both":
        return "both", "mixed" if not isinstance(missing, str) else missing
    return None


def infer_near_miss_key(metadata: dict[str, Any]) -> tuple[str, str] | None:
    """Infer near-miss status from older signal-eval metadata without explicit flags."""
    if metadata.get("spread_ok") is not True:
        return None
    if metadata.get("jump_detected") is True or metadata.get("too_soon") is True:
        return None
    long_failed = failed_directional_checks(
        metadata.get("imbalance_long_ok") is True,
        metadata.get("ofi_long_ok") is True,
        metadata.get("microprice_long_ok") is True,
        metadata.get("minute_long_ok") is not False,
        metadata.get("topix_long_ok") is not False,
    )
    short_failed = failed_directional_checks(
        metadata.get("imbalance_short_ok") is True,
        metadata.get("ofi_short_ok") is True,
        metadata.get("microprice_short_ok") is True,
        metadata.get("minute_short_ok") is not False,
        metadata.get("topix_short_ok") is not False,
    )
    long_near = is_near_miss(True, False, False, long_failed)
    short_near = is_near_miss(True, False, False, short_failed)
    if long_near and not short_near:
        return "long", long_failed[0]
    if short_near and not long_near:
        return "short", short_failed[0]
    if long_near and short_near:
        return "both", "mixed"
    return None


def candidate_metadata_snapshot(metadata: dict[str, object]) -> dict[str, object]:
    """Return compact metadata used by ``micro_candidate`` JSONL events."""
    keys = (
        "imbalance",
        "imbalance_entry",
        "imbalance_long_ok",
        "imbalance_short_ok",
        "ofi_ewma",
        "ofi_threshold",
        "ofi_percentile",
        "ofi_long_ok",
        "ofi_short_ok",
        "microprice_edge_ticks",
        "microprice_entry_ticks",
        "microprice_long_ok",
        "microprice_short_ok",
        "spread_ticks",
        "spread_required_ticks",
        "jump_detected",
        "jump_reason",
        "too_soon",
        "long_failed_checks",
        "short_failed_checks",
        "long_near_miss",
        "short_near_miss",
        "near_miss",
        "near_miss_direction",
        "near_miss_missing",
        "minute_bias",
        "topix_bias",
    )
    return {key: metadata[key] for key in keys if key in metadata}
