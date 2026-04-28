from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from .alpha import NTRatioSpreadEngine, StrategyArbiter, USJapanLeadLagScorer
from .config import StrategyConfig, default_config
from .indicators import BarBuilder
from .microstructure import BookFeatureEngine
from .models import (
    AlphaSignal,
    BookFeatures,
    Direction,
    ExternalFactorsSnapshot,
    MultiTimeframeScore,
    MultiTimeframeSnapshot,
    OrderBook,
    PortfolioExposure,
    PositionState,
    Signal,
    SignalEvaluation,
    StrategyIntent,
)
from .multitimeframe import MultiTimeframeScorer
from .policy import StrategyEntryPolicy
from .risk import OrderThrottle, RiskManager
from .strategies import MicroStrategyEngine, MinuteStrategyEngine


class DualStrategyEngine:
    """Orchestrates minute-bar, micro-book, NT-spread, and lead-lag engines into a unified signal pipeline."""

    def __init__(self, config: StrategyConfig | None = None) -> None:
        self.config = config or default_config()
        self.config.validate()
        self.minute_engine = MinuteStrategyEngine(
            self.config.minute_engine,
            self.config.symbols,
            tick_size=self.config.tick_size,
        )
        self.micro_engine = MicroStrategyEngine(self.config.micro_engine, tick_size=self.config.tick_size)
        self.book_features = BookFeatureEngine(self.config.micro_engine, tick_size=self.config.tick_size)
        self.multi_timeframe = MultiTimeframeScorer(self.config.multi_timeframe)
        self.risk = RiskManager(self.config.risk, self.config.micro_engine)
        self.throttle = OrderThrottle(
            self.config.micro_engine.min_order_interval_seconds,
            self.config.micro_engine.max_new_entries_per_minute,
        )
        self.entry_policy = StrategyEntryPolicy(self.config, self.risk, self.throttle)
        self.bar_builder_1m = BarBuilder(60)
        self.nt_spread = NTRatioSpreadEngine(self.config.nt_spread)
        self.lead_lag = USJapanLeadLagScorer(self.config.lead_lag)
        self.alpha_stack = StrategyArbiter(self.config.alpha_stack, self.config.risk)
        self.last_minute_bias: Direction = "flat"
        self.last_topix_bias: Direction = "flat"
        self.last_nt_signal: AlphaSignal | None = None
        self.latest_prices: dict[str, float] = {}
        self.portfolio_exposure = PortfolioExposure()
        self.position = PositionState(self.config.symbols.primary)
        self.latest_book_features: BookFeatures | None = None
        self.latest_signal_evaluations: list[SignalEvaluation] = []

    def update_multi_timeframe(self, snapshot: MultiTimeframeSnapshot) -> None:
        self.multi_timeframe.update_snapshot(snapshot)

    def update_external_factors(self, snapshot: ExternalFactorsSnapshot) -> None:
        self.lead_lag.update_snapshot(snapshot)

    def update_portfolio_exposure(self, exposure: PortfolioExposure) -> None:
        self.portfolio_exposure = exposure

    def on_order_book(self, book: OrderBook, now: datetime | None = None) -> list[Signal]:
        signals: list[Signal] = []
        latest_features = None
        self.latest_book_features = None
        self.latest_signal_evaluations = []
        if book.symbol == self.config.symbols.primary:
            latest_features = self.book_features.update(book, now=now)
            self.latest_book_features = latest_features
        price = book.last_price if book.last_price is not None else book.mid_price
        self._update_latest_price(book.symbol, price, book.timestamp)
        closed_bar = self.bar_builder_1m.update(book.symbol, book.timestamp, price, book.volume)
        if closed_bar is not None:
            minute_signal = self.minute_engine.on_bar(closed_bar)
            if closed_bar.symbol == self.config.symbols.primary:
                self.last_minute_bias = self.minute_engine.trend_bias(closed_bar.symbol)
            elif closed_bar.symbol == self.config.symbols.filter:
                self.last_topix_bias = self.minute_engine.trend_bias(closed_bar.symbol)
            if minute_signal is not None:
                mtf_score = self.multi_timeframe.score(minute_signal.direction, minute_signal=minute_signal, book_features=latest_features)
                intent = self._alpha_intent(minute_signal, mtf_score)
                scored_minute_signal = self._with_strategy_metadata(minute_signal, mtf_score, intent)
                decision = self.entry_policy.evaluate_minute(
                    scored_minute_signal,
                    closed_bar.end,
                    mtf_score,
                    intent,
                    self.position,
                )
                if decision.allowed:
                    signals.append(_with_extra_metadata(scored_minute_signal, decision.merged_metadata))
                else:
                    metadata = dict(scored_minute_signal.metadata)
                    metadata.update(decision.merged_metadata)
                    signals.append(
                        Signal(
                            "risk",
                            minute_signal.symbol,
                            "flat",
                            0.0,
                            reason=decision.reason,
                            score=0,
                            signal_horizon="system",
                            metadata=metadata,
                        )
                    )

        if book.symbol == self.config.symbols.primary:
            micro_signal, micro_evaluation = self.micro_engine.evaluate_book(
                book,
                self.last_minute_bias,
                self.last_topix_bias,
                now=now,
                features=latest_features,
            )
            self.latest_signal_evaluations.append(micro_evaluation)
            if micro_signal is not None:
                mtf_score = self.multi_timeframe.score(micro_signal.direction, book_features=latest_features)
                intent = self._alpha_intent(micro_signal, mtf_score)
                scored_micro_signal = self._with_strategy_metadata(micro_signal, mtf_score, intent)
                decision = self.entry_policy.evaluate_micro(
                    scored_micro_signal,
                    book.timestamp,
                    mtf_score,
                    intent,
                    self.position,
                )
                evaluation_metadata = dict(micro_evaluation.metadata)
                evaluation_metadata.update(decision.merged_metadata)
                evaluation_metadata.update(mtf_score.as_metadata())
                evaluation_metadata.update(intent.as_metadata())
                if decision.allowed:
                    self.throttle.record(book.timestamp)
                    signals.append(_with_extra_metadata(scored_micro_signal, decision.merged_metadata))
                    self.latest_signal_evaluations[-1] = replace(
                        micro_evaluation,
                        decision="allow",
                        reason=scored_micro_signal.reason,
                        candidate_direction=scored_micro_signal.direction,
                        metadata=evaluation_metadata,
                    )
                else:
                    risk_metadata = dict(scored_micro_signal.metadata)
                    risk_metadata.update(decision.merged_metadata)
                    signals.append(
                        Signal(
                            "risk",
                            book.symbol,
                            "flat",
                            0.0,
                            reason=decision.reason,
                            score=0,
                            signal_horizon="system",
                            metadata=risk_metadata,
                        )
                    )
                    self.latest_signal_evaluations[-1] = replace(
                        micro_evaluation,
                        decision="reject",
                        reason=decision.reason,
                        candidate_direction=micro_signal.direction,
                        metadata=evaluation_metadata,
                    )
        return signals

    def _new_entry_block(self, timestamp: datetime):
        return self.entry_policy.new_entry_block(timestamp)

    def _minute_observe_only_reason(self, signal: Signal) -> str | None:
        return self.entry_policy.minute_observe_only_reason(signal)

    def _update_latest_price(self, symbol: str, price: float, timestamp: datetime) -> None:
        self.latest_prices[symbol] = price
        primary = self.latest_prices.get(self.config.symbols.primary)
        topix = self.latest_prices.get(self.config.symbols.filter)
        if primary is None or topix is None:
            return
        structural_bias = self.lead_lag.snapshot.sox_bias if self.lead_lag.snapshot is not None else "flat"
        nt_signal = self.nt_spread.update(timestamp, primary, topix, structural_bias=structural_bias)
        if nt_signal is not None:
            self.last_nt_signal = nt_signal

    def _validate_signal(self, signal: Signal, mtf_score: MultiTimeframeScore, intent: StrategyIntent) -> tuple[bool, str]:
        mtf_ok, mtf_reason = self._validate_mtf_score(mtf_score, require_execution=False)
        if not mtf_ok:
            return False, mtf_reason
        if not intent.allowed:
            return False, intent.veto_reason or intent.reason
        return self.risk.validate_signal(signal, self.position)

    def _validate_mtf_score(self, score: MultiTimeframeScore, require_execution: bool) -> tuple[bool, str]:
        return self.entry_policy.validate_mtf_score(score, require_execution)

    def _alpha_intent(self, signal: Signal, score: MultiTimeframeScore) -> StrategyIntent:
        external_score = self.lead_lag.score(signal.direction)
        return self.alpha_stack.evaluate(
            signal,
            score,
            nt_signal=self.last_nt_signal,
            external_score=external_score,
            portfolio=self.portfolio_exposure,
        )

    def _with_strategy_metadata(self, signal: Signal, score: MultiTimeframeScore, intent: StrategyIntent) -> Signal:
        metadata = dict(signal.metadata)
        metadata.update(score.as_metadata())
        metadata.update(intent.as_metadata())
        return Signal(
            signal.engine,
            signal.symbol,
            signal.direction,
            signal.confidence,
            signal.price,
            signal.reason,
            metadata,
            score=intent.score,
            signal_horizon=intent.signal_horizon,
            expected_hold_seconds=intent.expected_hold_seconds,
            risk_budget_pct=intent.risk_budget_pct,
            veto_reason=intent.veto_reason,
            position_scale=intent.position_scale,
        )


def _with_extra_metadata(signal: Signal, extra_metadata: dict[str, object]) -> Signal:
    metadata = dict(signal.metadata)
    metadata.update(extra_metadata)
    return Signal(
        signal.engine,
        signal.symbol,
        signal.direction,
        signal.confidence,
        signal.price,
        signal.reason,
        metadata,
        signal.score,
        signal.signal_horizon,
        signal.expected_hold_seconds,
        signal.risk_budget_pct,
        signal.veto_reason,
        signal.position_scale,
    )
