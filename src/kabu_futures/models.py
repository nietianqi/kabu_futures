from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Direction = Literal["long", "short", "flat"]
EngineName = Literal[
    "minute_orb",
    "minute_vwap",
    "micro_book",
    "directional_intraday",
    "microstructure_scalp",
    "nt_ratio_spread",
    "us_japan_lead_lag",
    "event_calendar",
    "risk",
    "system",
]
TradeSide = Literal["buy", "sell", "unknown"]
PositionScale = Literal["none", "micro225_1", "standard_minute_position", "extend_target_only"]
SignalHorizon = Literal["micro", "intraday", "swing", "event_filter", "system"]
StrategyAction = Literal["allow", "reject", "observe_only", "flatten_only"]
SignalDecision = Literal["allow", "reject"]

@dataclass(frozen=True)
class Level:
    """Single price level in an order book depth snapshot."""

    price: float
    qty: float


@dataclass(frozen=True)
class OrderBook:
    """Normalised order book snapshot from kabu PUSH board data.

    Note: kabu encodes the best *sell* quote as BidPrice and best *buy* quote as AskPrice,
    which is the reverse of typical market data conventions. KabuBoardNormalizer corrects
    this so best_bid_price is always the highest buyer price and best_ask_price the lowest
    seller price.
    """

    symbol: str
    timestamp: datetime
    best_bid_price: float
    best_bid_qty: float
    best_ask_price: float
    best_ask_qty: float
    buy_levels: tuple[Level, ...] = ()
    sell_levels: tuple[Level, ...] = ()
    last_price: float | None = None
    volume: float = 0.0
    received_at: datetime | None = None
    raw_symbol: str | None = None

    @property
    def spread(self) -> float:
        return self.best_ask_price - self.best_bid_price

    @property
    def mid_price(self) -> float:
        return (self.best_bid_price + self.best_ask_price) / 2.0

    def validate(self) -> None:
        if self.best_ask_price <= self.best_bid_price:
            raise ValueError("Invalid order book: best ask must be greater than best bid")
        if self.best_ask_qty < 0 or self.best_bid_qty < 0:
            raise ValueError("Invalid order book: quantities must be non-negative")


@dataclass(frozen=True)
class Bar:
    """Completed OHLCV bar aggregated from tick data by BarBuilder."""

    symbol: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open


@dataclass(frozen=True)
class Signal:
    """Tradeable or informational signal emitted by a strategy engine."""

    engine: EngineName
    symbol: str
    direction: Direction
    confidence: float
    price: float | None = None
    reason: str = ""
    metadata: dict[str, object] = field(default_factory=dict)
    score: int | None = None
    signal_horizon: SignalHorizon | None = None
    expected_hold_seconds: int | None = None
    risk_budget_pct: float | None = None
    veto_reason: str | None = None
    position_scale: PositionScale | None = None

    @property
    def is_tradeable(self) -> bool:
        return self.direction in ("long", "short") and self.confidence > 0

    @property
    def engine_name(self) -> str:
        return self.engine


@dataclass(frozen=True)
class SignalEvaluation:
    engine: EngineName
    symbol: str
    timestamp: datetime
    decision: SignalDecision
    reason: str
    candidate_direction: Direction = "flat"
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def is_allow(self) -> bool:
        return self.decision == "allow"


@dataclass(frozen=True)
class BookFeatures:
    """Derived microstructure features computed from a single OrderBook snapshot."""

    timestamp: datetime
    symbol: str
    spread_ticks: float
    imbalance: float
    ofi: float
    ofi_ewma: float
    ofi_threshold: float
    microprice: float
    microprice_edge_ticks: float
    total_depth: float
    jump_detected: bool = False
    jump_reason: str | None = None
    event_gap_ms: float = 0.0
    latency_ms: float = 0.0


@dataclass(frozen=True)
class MultiTimeframeSnapshot:
    timestamp: datetime
    symbol: str
    yearly_trend: Direction = "flat"
    monthly_trend: Direction = "flat"
    weekly_trend: Direction = "flat"
    daily_trend: Direction = "flat"
    hourly_trend: Direction = "flat"
    daily_volatility_extreme: bool = False
    higher_timeframe_extreme_reversal: bool = False


@dataclass(frozen=True)
class MultiTimeframeScore:
    regime_score: int
    bias_score: int
    setup_score: int
    execution_score: int
    total_score: int
    bias: Direction
    veto_reason: str | None
    position_scale: PositionScale

    @property
    def can_trade(self) -> bool:
        return self.veto_reason is None and self.position_scale != "none"

    def as_metadata(self) -> dict[str, object]:
        return {
            "regime_score": self.regime_score,
            "bias_score": self.bias_score,
            "setup_score": self.setup_score,
            "execution_score": self.execution_score,
            "total_score": self.total_score,
            "multi_timeframe_bias": self.bias,
            "veto_reason": self.veto_reason,
            "position_scale": self.position_scale,
        }


@dataclass(frozen=True)
class HedgeLeg:
    symbol: str
    direction: Direction
    qty: int
    notional: float


@dataclass(frozen=True)
class AlphaSignal:
    engine: EngineName
    symbol: str
    direction: Direction
    score: int
    horizon: SignalHorizon
    expected_hold_seconds: int | None = None
    risk_budget_pct: float = 0.0
    veto_reason: str | None = None
    position_scale: PositionScale = "none"
    hedge_legs: tuple[HedgeLeg, ...] = ()
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def engine_name(self) -> str:
        return self.engine

    @property
    def is_active(self) -> bool:
        return self.direction in ("long", "short") and self.score > 0

    def as_metadata(self) -> dict[str, object]:
        metadata = dict(self.metadata)
        metadata.update(
            {
                "engine_name": self.engine_name,
                "signal_horizon": self.horizon,
                "alpha_score": self.score,
                "expected_hold_seconds": self.expected_hold_seconds,
                "risk_budget_pct": self.risk_budget_pct,
                "veto_reason": self.veto_reason,
                "position_scale": self.position_scale,
            }
        )
        if self.hedge_legs:
            metadata["hedge_legs"] = tuple(leg.__dict__ for leg in self.hedge_legs)
        return metadata


@dataclass(frozen=True)
class ExternalFactorsSnapshot:
    timestamp: datetime
    es_momentum_1m: float = 0.0
    nq_momentum_1m: float = 0.0
    sox_bias: Direction = "flat"
    usdjpy_bias: Direction = "flat"
    us10y_bias: Direction = "flat"
    bank_bias: Direction = "flat"
    event_risk_flag: bool = False
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ExternalFactorScore:
    score: int
    bias: Direction
    veto_reason: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def as_metadata(self) -> dict[str, object]:
        metadata = dict(self.metadata)
        metadata.update(
            {
                "external_factor_score": self.score,
                "external_factor_bias": self.bias,
                "external_factor_veto_reason": self.veto_reason,
            }
        )
        return metadata


@dataclass(frozen=True)
class PortfolioExposure:
    micro225_net_qty: int = 0
    topix_mini_net_qty: int = 0
    nikkei_beta: float = 0.0
    gross_risk_pct: float = 0.0
    active_nt_spread: bool = False
    event_risk_flag: bool = False


@dataclass(frozen=True)
class StrategyIntent:
    action: StrategyAction
    symbol: str
    direction: Direction
    engine_name: str
    score: int
    signal_horizon: SignalHorizon
    expected_hold_seconds: int | None
    risk_budget_pct: float
    veto_reason: str | None
    position_scale: PositionScale
    reason: str
    hedge_legs: tuple[HedgeLeg, ...] = ()
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def allowed(self) -> bool:
        return self.action == "allow"

    def as_metadata(self) -> dict[str, object]:
        metadata = dict(self.metadata)
        metadata.update(
            {
                "strategy_action": self.action,
                "engine_name": self.engine_name,
                "signal_horizon": self.signal_horizon,
                "alpha_stack_score": self.score,
                "expected_hold_seconds": self.expected_hold_seconds,
                "risk_budget_pct": self.risk_budget_pct,
                "alpha_veto_reason": self.veto_reason,
                "position_scale": self.position_scale,
                "arbiter_reason": self.reason,
            }
        )
        if self.hedge_legs:
            metadata["hedge_legs"] = tuple(leg.__dict__ for leg in self.hedge_legs)
        return metadata


@dataclass(frozen=True)
class TradeTick:
    symbol: str
    timestamp: datetime
    price: float
    qty: float
    side: TradeSide = "unknown"
    received_at: datetime | None = None


@dataclass
class PositionState:
    """Mutable tracker for a single instrument's open position."""

    symbol: str
    direction: Direction = "flat"
    qty: int = 0
    entry_price: float | None = None
    engine: str | None = None

    @property
    def is_flat(self) -> bool:
        return self.direction == "flat" or self.qty == 0


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    exchange: int
    trade_type: int
    side: str
    qty: int
    front_order_type: int
    price: float
    time_in_force: int
    expire_day: int = 0
    close_position_order: int | None = None
    close_positions: tuple[dict[str, object], ...] = ()
    reverse_limit_order: dict[str, object] | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "Symbol": self.symbol,
            "Exchange": self.exchange,
            "TradeType": self.trade_type,
            "TimeInForce": self.time_in_force,
            "Side": self.side,
            "Qty": self.qty,
            "FrontOrderType": self.front_order_type,
            "Price": self.price,
            "ExpireDay": self.expire_day,
        }
        if self.close_position_order is not None:
            payload["ClosePositionOrder"] = self.close_position_order
        if self.close_positions:
            payload["ClosePositions"] = list(self.close_positions)
        if self.reverse_limit_order is not None:
            payload["ReverseLimitOrder"] = self.reverse_limit_order
        return payload
