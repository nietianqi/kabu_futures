from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import SessionScheduleConfig


JST = timezone(timedelta(hours=9))

DEFAULT_SESSION_WINDOWS: dict[str, str] = {
    "api_maintenance": "06:15-06:30",
    "api_prepare": "06:30-08:00",
    "day_preopen": "08:00-08:45",
    "day_continuous": "08:45-15:40",
    "day_closing_call": "15:40-15:45",
    "between_sessions": "15:45-16:45",
    "night_preopen": "16:45-17:00",
    "night_continuous": "17:00-05:55",
    "night_closing_call": "05:55-06:00",
    "post_close": "06:00-06:15",
}

DEFAULT_ALLOW_NEW_ENTRY_PHASES = ("day_continuous", "night_continuous")

SESSION_PHASE_ORDER = (
    "api_maintenance",
    "api_prepare",
    "day_preopen",
    "day_continuous",
    "day_closing_call",
    "between_sessions",
    "night_preopen",
    "night_continuous",
    "night_closing_call",
    "post_close",
)


@dataclass(frozen=True)
class SessionState:
    phase: str
    window_jst: str
    new_entries_allowed: bool
    api_window_status: str


def classify_jst_session(timestamp: datetime, config: "SessionScheduleConfig | None" = None) -> SessionState:
    """Classify a timestamp into the configured JST futures/API session phase."""

    current = _jst_time(timestamp)
    for phase in SESSION_PHASE_ORDER:
        window = _window_for(phase, config)
        if time_in_window(current, window):
            return SessionState(
                phase=phase,
                window_jst=window,
                new_entries_allowed=phase in _allowed_phases(config),
                api_window_status=_api_window_status(phase),
            )
    return SessionState(
        phase="api_maintenance",
        window_jst=_window_for("api_maintenance", config),
        new_entries_allowed=False,
        api_window_status="maintenance",
    )


def new_entries_allowed(timestamp: datetime, config: "SessionScheduleConfig | None" = None) -> bool:
    return classify_jst_session(timestamp, config).new_entries_allowed


def time_in_jst_window(timestamp: datetime, window: str) -> bool:
    return time_in_window(_jst_time(timestamp), window)


def time_in_window(current: dt_time, window: str) -> bool:
    try:
        start_text, end_text = window.split("-", 1)
        start = parse_hhmm(start_text)
        end = parse_hhmm(end_text)
    except ValueError:
        return False
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def parse_hhmm(value: str) -> dt_time:
    hour_text, minute_text = value.strip().split(":", 1)
    return dt_time(int(hour_text), int(minute_text))


def _jst_time(timestamp: datetime) -> dt_time:
    if timestamp.tzinfo is None:
        return timestamp.time()
    return timestamp.astimezone(JST).time()


def _window_for(phase: str, config: "SessionScheduleConfig | None") -> str:
    if config is None:
        return DEFAULT_SESSION_WINDOWS[phase]
    value = getattr(config, phase, DEFAULT_SESSION_WINDOWS[phase])
    return str(value)


def _allowed_phases(config: "SessionScheduleConfig | None") -> tuple[str, ...]:
    if config is None:
        return DEFAULT_ALLOW_NEW_ENTRY_PHASES
    return tuple(getattr(config, "allow_new_entry_phases", DEFAULT_ALLOW_NEW_ENTRY_PHASES))


def _api_window_status(phase: str) -> str:
    if phase == "api_maintenance":
        return "maintenance"
    if phase == "api_prepare":
        return "prepare"
    return "available"
