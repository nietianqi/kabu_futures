from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SymbolsConfig:
    primary: str = "NK225micro"
    filter: str = "TOPIXmini"
    deriv_month: int = 0
    rollover_business_days_before_last_trade: int = 3


@dataclass(frozen=True)
class MinuteEngineConfig:
    orb_minutes: int = 5
    atr_period: int = 14
    risk_per_trade_pct: float = 0.25
    stop_ticks_min: int = 6
    stop_ticks_max: int = 12
    stop_atr_mult: float = 0.8
    take_profit_r: float = 1.5
    breakeven_after_r: float = 1.0
    max_hold_minutes: int = 45
    trend_pullback_long_observe_only: bool = False
    trend_pullback_short_observe_only: bool = False
    directional_intraday_long_observe_only: bool = False


@dataclass(frozen=True)
class MicroEngineConfig:
    mode: str = "observe_only"
    qty: int = 1
    depth_levels: int = 5
    invert_direction: bool = False
    imbalance_entry: float = 0.30
    imbalance_exit: float = 0.10
    microprice_entry_ticks: float = 0.15
    spread_ticks_required: int = 1
    take_profit_ticks: int = 1
    stop_loss_ticks: int = 3
    time_stop_seconds: int = 20
    max_hold_seconds: int = 30
    min_order_interval_seconds: int = 3
    max_new_entries_per_minute: int = 6
    websocket_latency_stop_ms: int = 500
    consecutive_loss_stop: int = 5
    avg_slippage_stop_ticks: float = 2.0
    no_new_entry_windows_jst: tuple[str, ...] = ()


@dataclass(frozen=True)
class SessionScheduleConfig:
    api_maintenance: str = "06:15-06:30"
    api_prepare: str = "06:30-08:00"
    day_preopen: str = "08:00-08:45"
    day_continuous: str = "08:45-15:40"
    day_closing_call: str = "15:40-15:45"
    between_sessions: str = "15:45-16:45"
    night_preopen: str = "16:45-17:00"
    night_continuous: str = "17:00-05:55"
    night_closing_call: str = "05:55-06:00"
    post_close: str = "06:00-06:15"
    allow_new_entry_phases: tuple[str, ...] = ("day_continuous", "night_continuous")


@dataclass(frozen=True)
class LiveExecutionConfig:
    max_order_qty: int = 1
    supported_engines: tuple[str, ...] = ("micro_book",)
    entry_slippage_ticks: int = 0
    entry_time_in_force: int = 2
    exit_time_in_force: int = 1
    position_poll_interval_seconds: float = 1.0
    max_pending_entry_seconds: int = 8
    pending_entry_grace_seconds: int = 20
    max_consecutive_exit_failures: int = 3
    max_consecutive_entry_failures: int = 3
    entry_failure_cooldown_seconds: int = 30


@dataclass(frozen=True)
class NTSpreadConfig:
    enabled: bool = True
    mode: str = "shadow"
    zscore_windows: tuple[int, int, int] = (20, 60, 250)
    entry_zscore: float = 2.0
    exit_zscore: float = 0.5
    stop_expansion_sigma: float = 1.0
    min_history: int = 20
    risk_per_trade_pct: float = 0.50
    max_hold_days: int = 15
    micro225_multiplier: float = 10.0
    topix_mini_multiplier: float = 1000.0
    default_micro_per_topix_mini: float = 7.0


@dataclass(frozen=True)
class LeadLagConfig:
    enabled: bool = True
    optional_external_data: bool = True
    us_open_start: str = "22:30"
    us_open_end: str = "00:00"
    morning_handoff_start: str = "08:45"
    morning_handoff_end: str = "09:30"
    min_factor_alignment: int = 2
    event_risk_veto: bool = True


@dataclass(frozen=True)
class AlphaStackConfig:
    enabled: bool = True
    block_directional_when_nt_conflicts: bool = True
    reduce_directional_when_nt_active: bool = True
    external_factor_optional: bool = True
    external_factor_conflict_veto: bool = True
    max_external_score_adjustment: int = 10


@dataclass(frozen=True)
class MultiTimeframeConfig:
    enabled: bool = True
    regime_score_max: int = 30
    bias_score_max: int = 30
    setup_score_max: int = 25
    execution_score_max: int = 15
    min_total_score_to_trade: int = 70
    min_execution_score_to_chase: int = 10
    score_70_79_position_scale: str = "micro225_1"
    score_80_89_position_scale: str = "standard_minute_position"
    score_90_plus_position_scale: str = "extend_target_only"
    hard_veto_extreme_higher_timeframe_reversal: bool = True
    hard_veto_daily_extreme_volatility: bool = True
    hard_veto_hourly_minute_strong_conflict: bool = True
    hard_veto_book_latency_or_depth_failure: bool = True


@dataclass(frozen=True)
class RiskConfig:
    account_daily_loss_pct: float = 1.0
    account_daily_loss_r: float = 3.0
    max_micro225_net_qty: int = 5
    max_positions_per_symbol: int = 5
    margin_buffer_ratio: float = 1.5
    directional_risk_per_trade_pct_min: float = 0.25
    directional_risk_per_trade_pct_max: float = 0.40
    micro_risk_per_trade_pct_min: float = 0.05
    micro_risk_per_trade_pct_max: float = 0.10
    nt_spread_risk_pct: float = 0.50
    max_portfolio_nikkei_beta: float = 0.30
    night_size_scale: float = 0.50
    event_risk_position_scale: float = 0.50


@dataclass(frozen=True)
class ApiConfig:
    api_password: str = ""
    api_password_env: str = "KABU_API_PASSWORD"
    production_url: str = "http://localhost:18080/kabusapi"
    sandbox_url: str = "http://localhost:18081/kabusapi"
    order_requests_per_second: int = 5
    info_requests_per_second: int = 10
    max_registered_symbols: int = 50
    max_active_orders_same_symbol: int = 5


@dataclass(frozen=True)
class StrategyConfig:
    symbols: SymbolsConfig = field(default_factory=SymbolsConfig)
    minute_engine: MinuteEngineConfig = field(default_factory=MinuteEngineConfig)
    micro_engine: MicroEngineConfig = field(default_factory=MicroEngineConfig)
    session_schedule: SessionScheduleConfig = field(default_factory=SessionScheduleConfig)
    live_execution: LiveExecutionConfig = field(default_factory=LiveExecutionConfig)
    nt_spread: NTSpreadConfig = field(default_factory=NTSpreadConfig)
    lead_lag: LeadLagConfig = field(default_factory=LeadLagConfig)
    alpha_stack: AlphaStackConfig = field(default_factory=AlphaStackConfig)
    multi_timeframe: MultiTimeframeConfig = field(default_factory=MultiTimeframeConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    tick_size: float = 5.0
    micro225_tick_value: float = 50.0

    def validate(self) -> None:
        """Raise ValueError if parameter combinations are inconsistent."""
        m = self.micro_engine
        if m.imbalance_entry <= m.imbalance_exit:
            raise ValueError(
                f"imbalance_entry ({m.imbalance_entry}) must be greater than imbalance_exit ({m.imbalance_exit})"
            )
        if m.stop_loss_ticks <= 0:
            raise ValueError(f"stop_loss_ticks must be positive, got {m.stop_loss_ticks}")
        if m.take_profit_ticks <= 0:
            raise ValueError(f"take_profit_ticks must be positive, got {m.take_profit_ticks}")
        if self.live_execution.entry_slippage_ticks < 0:
            raise ValueError(
                f"live_execution.entry_slippage_ticks must be non-negative, got {self.live_execution.entry_slippage_ticks}"
            )
        if self.live_execution.position_poll_interval_seconds <= 0:
            raise ValueError(
                "live_execution.position_poll_interval_seconds must be positive, "
                f"got {self.live_execution.position_poll_interval_seconds}"
            )
        if self.live_execution.max_pending_entry_seconds <= 0:
            raise ValueError(
                f"live_execution.max_pending_entry_seconds must be positive, got {self.live_execution.max_pending_entry_seconds}"
            )
        if self.live_execution.pending_entry_grace_seconds < self.live_execution.max_pending_entry_seconds:
            raise ValueError(
                "live_execution.pending_entry_grace_seconds must be greater than or equal to "
                f"max_pending_entry_seconds ({self.live_execution.max_pending_entry_seconds}), "
                f"got {self.live_execution.pending_entry_grace_seconds}"
            )
        if self.live_execution.max_consecutive_exit_failures <= 0:
            raise ValueError(
                f"live_execution.max_consecutive_exit_failures must be positive, got {self.live_execution.max_consecutive_exit_failures}"
            )
        if self.live_execution.max_consecutive_entry_failures <= 0:
            raise ValueError(
                "live_execution.max_consecutive_entry_failures must be positive, "
                f"got {self.live_execution.max_consecutive_entry_failures}"
            )
        if self.live_execution.entry_failure_cooldown_seconds < 0:
            raise ValueError(
                "live_execution.entry_failure_cooldown_seconds must be non-negative, "
                f"got {self.live_execution.entry_failure_cooldown_seconds}"
            )
        if self.risk.max_positions_per_symbol <= 0:
            raise ValueError(f"max_positions_per_symbol must be positive, got {self.risk.max_positions_per_symbol}")
        if self.tick_size <= 0:
            raise ValueError(f"tick_size must be positive, got {self.tick_size}")
        windows = self.nt_spread.zscore_windows
        if len(windows) < 1 or any(w <= 0 for w in windows):
            raise ValueError(f"nt_spread.zscore_windows must be positive integers, got {windows}")


def default_config() -> StrategyConfig:
    config = StrategyConfig()
    config.validate()
    return config


def _merge_dataclass(cls: type, values: dict[str, Any]) -> Any:
    valid = {field_.name for field_ in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
    return cls(**{key: value for key, value in values.items() if key in valid})


def load_json_config(path: str | Path) -> StrategyConfig:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    config = StrategyConfig(
        symbols=_merge_dataclass(SymbolsConfig, data.get("symbols", {})),
        minute_engine=_merge_dataclass(MinuteEngineConfig, data.get("minute_engine", {})),
        micro_engine=_merge_dataclass(MicroEngineConfig, data.get("micro_engine", {})),
        session_schedule=_merge_dataclass(SessionScheduleConfig, data.get("session_schedule", {})),
        live_execution=_merge_dataclass(LiveExecutionConfig, data.get("live_execution", {})),
        nt_spread=_merge_dataclass(NTSpreadConfig, data.get("nt_spread", {})),
        lead_lag=_merge_dataclass(LeadLagConfig, data.get("lead_lag", {})),
        alpha_stack=_merge_dataclass(AlphaStackConfig, data.get("alpha_stack", {})),
        multi_timeframe=_merge_dataclass(MultiTimeframeConfig, data.get("multi_timeframe", {})),
        risk=_merge_dataclass(RiskConfig, data.get("risk", {})),
        api=_merge_dataclass(ApiConfig, data.get("api", {})),
    )
    config.validate()
    return config
