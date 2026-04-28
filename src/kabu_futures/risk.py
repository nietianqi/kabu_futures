from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .config import MicroEngineConfig, RiskConfig
from .models import PositionState, Signal


@dataclass
class RiskState:
    daily_pnl: float = 0.0
    daily_r: float = 0.0
    consecutive_losses: int = 0
    micro_consecutive_losses: int = 0
    micro_halted: bool = False
    account_halted: bool = False
    last_reason: str = ""
    slippage_ticks: deque[float] = field(default_factory=lambda: deque(maxlen=20))


class OrderThrottle:
    def __init__(self, min_interval_seconds: int, max_per_minute: int) -> None:
        self.min_interval = timedelta(seconds=min_interval_seconds)
        self.max_per_minute = max_per_minute
        self.timestamps: deque[datetime] = deque(maxlen=max_per_minute * 2)

    def allow(self, now: datetime) -> tuple[bool, str]:
        if self.timestamps and now - self.timestamps[-1] < self.min_interval:
            return False, "min_order_interval"
        cutoff = now - timedelta(minutes=1)
        recent = [ts for ts in self.timestamps if ts >= cutoff]
        if len(recent) >= self.max_per_minute:
            return False, "max_orders_per_minute"
        return True, "ok"

    def record(self, now: datetime) -> None:
        self.timestamps.append(now)


class RiskManager:
    def __init__(self, risk_config: RiskConfig, micro_config: MicroEngineConfig) -> None:
        self.config = risk_config
        self.micro_config = micro_config
        self.state = RiskState()

    def validate_signal(self, signal: Signal, position: PositionState) -> tuple[bool, str]:
        if self.state.account_halted:
            return False, self.state.last_reason or "account_halted"
        if signal.engine == "micro_book" and self.state.micro_halted:
            return False, self.state.last_reason or "micro_halted"
        if position.qty >= self.config.max_micro225_net_qty:
            return False, "max_net_qty"
        return True, "ok"

    def record_trade_result(self, engine: str, pnl: float, risk_r: float, slippage_ticks: float = 0.0) -> None:
        self.state.daily_pnl += pnl
        self.state.daily_r += risk_r
        if pnl < 0:
            self.state.consecutive_losses += 1
            if engine == "micro_book":
                self.state.micro_consecutive_losses += 1
        else:
            self.state.consecutive_losses = 0
            if engine == "micro_book":
                self.state.micro_consecutive_losses = 0
        if engine == "micro_book":
            self.state.slippage_ticks.append(abs(slippage_ticks))
        self._refresh_halts()

    def _refresh_halts(self) -> None:
        if self.state.daily_r <= -self.config.account_daily_loss_r:
            self.state.account_halted = True
            self.state.last_reason = "daily_r_loss_limit"
        if self.state.micro_consecutive_losses >= self.micro_config.consecutive_loss_stop:
            self.state.micro_halted = True
            self.state.last_reason = "micro_consecutive_loss_stop"
        if self.state.slippage_ticks:
            avg_slippage = sum(self.state.slippage_ticks) / len(self.state.slippage_ticks)
            if avg_slippage > self.micro_config.avg_slippage_stop_ticks:
                self.state.micro_halted = True
                self.state.last_reason = "micro_slippage_stop"
