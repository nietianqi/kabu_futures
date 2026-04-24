from __future__ import annotations

from collections import deque
from datetime import datetime, time
from statistics import fmean, pstdev

from .config import AlphaStackConfig, LeadLagConfig, NTSpreadConfig, RiskConfig
from .models import (
    AlphaSignal,
    Direction,
    ExternalFactorScore,
    ExternalFactorsSnapshot,
    HedgeLeg,
    MultiTimeframeScore,
    PortfolioExposure,
    PositionScale,
    Signal,
    SignalHorizon,
    StrategyIntent,
)
from .utils import opposite


def zscore(values: list[float] | tuple[float, ...], window: int) -> float | None:
    if window < 2 or len(values) < window:
        return None
    sample = list(values[-window:])
    mean = fmean(sample)
    deviation = pstdev(sample)
    if deviation <= 0:
        return 0.0
    return (sample[-1] - mean) / deviation


def compute_nt_ratio(nikkei_price: float, topix_price: float) -> float:
    if nikkei_price <= 0 or topix_price <= 0:
        raise ValueError("nikkei_price and topix_price must be positive")
    return nikkei_price / topix_price


def compute_micro225_per_topix_mini(
    nikkei_price: float,
    topix_price: float,
    micro225_multiplier: float = 10.0,
    topix_mini_multiplier: float = 1000.0,
) -> float:
    if nikkei_price <= 0 or topix_price <= 0:
        raise ValueError("nikkei_price and topix_price must be positive")
    micro_notional = nikkei_price * micro225_multiplier
    topix_notional = topix_price * topix_mini_multiplier
    return topix_notional / micro_notional


def _direction_vote(direction: Direction) -> int:
    if direction == "long":
        return 1
    if direction == "short":
        return -1
    return 0


def _signed_to_direction(value: int) -> Direction:
    if value > 0:
        return "long"
    if value < 0:
        return "short"
    return "flat"


def _parse_time(value: str) -> time:
    hour, minute = value.split(":", maxsplit=1)
    return time(int(hour), int(minute))


def in_time_window(ts: datetime, start: str, end: str) -> bool:
    start_time = _parse_time(start)
    end_time = _parse_time(end)
    current = ts.time()
    if start_time <= end_time:
        return start_time <= current <= end_time
    return current >= start_time or current <= end_time


class NTRatioSpreadEngine:
    """Shadow-first Nikkei/TOPIX relative-value signal engine."""

    def __init__(self, config: NTSpreadConfig) -> None:
        self.config = config
        # Default windows: 20 ~= 1 month, 60 ~= 1 quarter, 250 ~= 1 year.
        self.ratios: deque[float] = deque(maxlen=max(config.zscore_windows))
        self.last_signal: AlphaSignal | None = None

    def update(
        self,
        timestamp: datetime,
        nikkei_price: float,
        topix_price: float,
        structural_bias: Direction = "flat",
    ) -> AlphaSignal | None:
        if not self.config.enabled:
            return None
        ratio = compute_nt_ratio(nikkei_price, topix_price)
        self.ratios.append(ratio)
        zscores = {window: zscore(tuple(self.ratios), window) for window in self.config.zscore_windows}
        usable_zscores = [value for value in zscores.values() if value is not None]
        if len(self.ratios) < self.config.min_history or not usable_zscores:
            return None

        composite_z = max(usable_zscores, key=abs)
        direction: Direction = "flat"
        reason = "nt_ratio_inside_observe_band"
        if composite_z >= self.config.entry_zscore:
            direction = "short"
            reason = "nt_ratio_expensive_short_nikkei_long_topix"
        elif composite_z <= -self.config.entry_zscore:
            direction = "long"
            reason = "nt_ratio_cheap_long_nikkei_short_topix"

        hedge_ratio = compute_micro225_per_topix_mini(
            nikkei_price,
            topix_price,
            self.config.micro225_multiplier,
            self.config.topix_mini_multiplier,
        )
        hedge_legs = self._hedge_legs(direction, nikkei_price, topix_price, hedge_ratio)
        score = min(100, int(round(abs(composite_z) / self.config.entry_zscore * 70)))
        veto_reason = "nt_shadow_only" if direction != "flat" and self.config.mode != "live" else None
        position_scale: PositionScale = "micro225_1" if direction != "flat" and veto_reason is None else "none"
        metadata: dict[str, object] = {
            "nt_ratio": ratio,
            "nt_zscore_20": zscores.get(20),
            "nt_zscore_60": zscores.get(60),
            "nt_zscore_250": zscores.get(250),
            "nt_composite_zscore": composite_z,
            "hedge_ratio": hedge_ratio,
            "structural_bias": structural_bias,
            "mode": self.config.mode,
            "reason": reason,
            "timestamp": timestamp.isoformat(),
        }
        signal = AlphaSignal(
            "nt_ratio_spread",
            "NT_RATIO",
            direction,
            score,
            "swing",
            expected_hold_seconds=self.config.max_hold_days * 24 * 60 * 60,
            risk_budget_pct=self.config.risk_per_trade_pct,
            veto_reason=veto_reason,
            position_scale=position_scale,
            hedge_legs=hedge_legs,
            metadata=metadata,
        )
        self.last_signal = signal
        return signal

    def _hedge_legs(
        self,
        direction: Direction,
        nikkei_price: float,
        topix_price: float,
        hedge_ratio: float,
    ) -> tuple[HedgeLeg, ...]:
        if direction == "flat":
            return ()
        micro_qty = max(1, int(round(hedge_ratio)))
        micro_notional = nikkei_price * self.config.micro225_multiplier * micro_qty
        topix_notional = topix_price * self.config.topix_mini_multiplier
        if direction == "long":
            return (
                HedgeLeg("NK225micro", "long", micro_qty, micro_notional),
                HedgeLeg("TOPIXmini", "short", 1, topix_notional),
            )
        return (
            HedgeLeg("NK225micro", "short", micro_qty, micro_notional),
            HedgeLeg("TOPIXmini", "long", 1, topix_notional),
        )


class USJapanLeadLagScorer:
    def __init__(self, config: LeadLagConfig) -> None:
        self.config = config
        self.snapshot: ExternalFactorsSnapshot | None = None

    def update_snapshot(self, snapshot: ExternalFactorsSnapshot) -> ExternalFactorScore:
        self.snapshot = snapshot
        return self.score("flat", snapshot)

    def score(self, direction: Direction, snapshot: ExternalFactorsSnapshot | None = None) -> ExternalFactorScore:
        if not self.config.enabled:
            return ExternalFactorScore(0, "flat", metadata={"lead_lag_enabled": False})
        snapshot = snapshot or self.snapshot
        if snapshot is None:
            return ExternalFactorScore(0, "flat", metadata={"external_data_missing": True})
        if snapshot.event_risk_flag and self.config.event_risk_veto:
            return ExternalFactorScore(0, "flat", "event_risk_flag", {"event_risk_flag": True})

        signed = self._signed_alignment(snapshot)
        bias = _signed_to_direction(signed)
        if direction == "short":
            directional_alignment = -signed
        elif direction == "long":
            directional_alignment = signed
        else:
            directional_alignment = abs(signed)
        score = max(
            -self.config.min_factor_alignment * 2,
            min(self.config.min_factor_alignment * 5, directional_alignment * 2),
        )
        metadata = {
            "external_signed_alignment": signed,
            "external_factor_window": self._window_name(snapshot.timestamp),
            "es_momentum_1m": snapshot.es_momentum_1m,
            "nq_momentum_1m": snapshot.nq_momentum_1m,
            "sox_bias": snapshot.sox_bias,
            "usdjpy_bias": snapshot.usdjpy_bias,
            "us10y_bias": snapshot.us10y_bias,
            "bank_bias": snapshot.bank_bias,
        }
        metadata.update(snapshot.metadata)
        veto = None
        if direction in ("long", "short") and directional_alignment <= -self.config.min_factor_alignment:
            veto = "external_factor_conflict"
        return ExternalFactorScore(score, bias, veto, metadata)

    def _signed_alignment(self, snapshot: ExternalFactorsSnapshot) -> int:
        signed = 0
        signed += 1 if snapshot.es_momentum_1m > 0 else -1 if snapshot.es_momentum_1m < 0 else 0
        signed += 1 if snapshot.nq_momentum_1m > 0 else -1 if snapshot.nq_momentum_1m < 0 else 0
        signed += _direction_vote(snapshot.sox_bias)
        signed += _direction_vote(snapshot.usdjpy_bias)
        signed -= _direction_vote(snapshot.us10y_bias)
        signed += _direction_vote(snapshot.bank_bias)
        return signed

    def _window_name(self, ts: datetime) -> str:
        if in_time_window(ts, self.config.us_open_start, self.config.us_open_end):
            return "us_open_lead_lag"
        if in_time_window(ts, self.config.morning_handoff_start, self.config.morning_handoff_end):
            return "morning_handoff"
        return "regular"


class StrategyArbiter:
    def __init__(self, config: AlphaStackConfig, risk_config: RiskConfig) -> None:
        self.config = config
        self.risk_config = risk_config

    def evaluate(
        self,
        signal: Signal,
        mtf_score: MultiTimeframeScore,
        nt_signal: AlphaSignal | None = None,
        external_score: ExternalFactorScore | None = None,
        portfolio: PortfolioExposure | None = None,
    ) -> StrategyIntent:
        portfolio = portfolio or PortfolioExposure()
        base_horizon = self._horizon_for_signal(signal)
        risk_budget = self._risk_budget_for_signal(signal)
        score = self._bounded_score(mtf_score.total_score + self._external_adjustment(external_score))
        scale = mtf_score.position_scale
        metadata: dict[str, object] = {
            "regime_score": mtf_score.regime_score,
            "bias_score": mtf_score.bias_score,
            "setup_score": mtf_score.setup_score,
            "execution_score": mtf_score.execution_score,
            "total_score": mtf_score.total_score,
            "portfolio_beta": portfolio.nikkei_beta,
            "gross_risk_pct": portfolio.gross_risk_pct,
        }
        if nt_signal is not None:
            metadata.update({f"nt_{key}": value for key, value in nt_signal.as_metadata().items()})
        if external_score is not None:
            metadata.update(external_score.as_metadata())

        reject_reason = self._reject_reason(signal, nt_signal, external_score, portfolio)
        if reject_reason is not None:
            return StrategyIntent(
                "reject",
                signal.symbol,
                signal.direction,
                signal.engine_name,
                score,
                base_horizon,
                self._expected_hold_seconds(signal),
                risk_budget,
                reject_reason,
                "none",
                reject_reason,
                metadata=metadata,
            )

        if portfolio.active_nt_spread and self.config.reduce_directional_when_nt_active and signal.engine != "micro_book":
            scale = "micro225_1"
            metadata["position_scale_reduction_reason"] = "active_nt_spread"

        return StrategyIntent(
            "allow",
            signal.symbol,
            signal.direction,
            signal.engine_name,
            score,
            base_horizon,
            self._expected_hold_seconds(signal),
            risk_budget,
            None,
            scale,
            "alpha_stack_allow",
            metadata=metadata,
        )

    def _reject_reason(
        self,
        signal: Signal,
        nt_signal: AlphaSignal | None,
        external_score: ExternalFactorScore | None,
        portfolio: PortfolioExposure,
    ) -> str | None:
        if not self.config.enabled:
            return None
        if portfolio.event_risk_flag:
            return "portfolio_event_risk_flag"
        if abs(portfolio.nikkei_beta) > self.risk_config.max_portfolio_nikkei_beta:
            return "portfolio_beta_limit"
        if (
            external_score is not None
            and external_score.veto_reason is not None
            and (self.config.external_factor_conflict_veto or external_score.veto_reason == "event_risk_flag")
        ):
            return external_score.veto_reason
        if (
            nt_signal is not None
            and nt_signal.is_active
            and nt_signal.veto_reason is None
            and self.config.block_directional_when_nt_conflicts
            and opposite(signal.direction, nt_signal.direction)
        ):
            return "nt_spread_direction_conflict"
        return None

    def _external_adjustment(self, external_score: ExternalFactorScore | None) -> int:
        if external_score is None:
            return 0
        limit = self.config.max_external_score_adjustment
        return max(-limit, min(limit, external_score.score))

    def _horizon_for_signal(self, signal: Signal) -> SignalHorizon:
        if signal.signal_horizon is not None:
            return signal.signal_horizon
        if signal.engine == "micro_book":
            return "micro"
        if signal.engine in ("minute_orb", "minute_vwap", "directional_intraday"):
            return "intraday"
        if signal.engine == "nt_ratio_spread":
            return "swing"
        return "system"

    def _expected_hold_seconds(self, signal: Signal) -> int | None:
        if signal.expected_hold_seconds is not None:
            return signal.expected_hold_seconds
        if signal.engine == "micro_book":
            return 30
        if signal.engine in ("minute_orb", "minute_vwap", "directional_intraday"):
            return 45 * 60
        return None

    def _risk_budget_for_signal(self, signal: Signal) -> float:
        if signal.risk_budget_pct is not None:
            return signal.risk_budget_pct
        if signal.engine == "micro_book":
            return self.risk_config.micro_risk_per_trade_pct_max
        if signal.engine == "nt_ratio_spread":
            return self.risk_config.nt_spread_risk_pct
        return self.risk_config.directional_risk_per_trade_pct_max

    def _bounded_score(self, score: int) -> int:
        return max(0, min(100, score))
