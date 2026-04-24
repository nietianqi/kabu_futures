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
                allowed, reason = self._validate_signal(scored_minute_signal, mtf_score, intent)
                if allowed:
                    signals.append(scored_minute_signal)
                else:
                    signals.append(Signal("risk", minute_signal.symbol, "flat", 0.0, reason=reason, score=0, signal_horizon="system", metadata=scored_minute_signal.metadata))

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
                throttle_ok, throttle_reason = self.throttle.allow(book.timestamp)
                mtf_score = self.multi_timeframe.score(micro_signal.direction, book_features=latest_features)
                intent = self._alpha_intent(micro_signal, mtf_score)
                scored_micro_signal = self._with_strategy_metadata(micro_signal, mtf_score, intent)
                mtf_ok, mtf_reason = self._validate_mtf_score(mtf_score, require_execution=True)
                risk_ok, risk_reason = self.risk.validate_signal(scored_micro_signal, self.position)
                alpha_ok = intent.allowed
                evaluation_metadata = dict(micro_evaluation.metadata)
                evaluation_metadata.update(
                    {
                        "throttle_ok": throttle_ok,
                        "throttle_reason": throttle_reason,
                        "mtf_ok": mtf_ok,
                        "mtf_reason": mtf_reason,
                        "risk_ok": risk_ok,
                        "risk_reason": risk_reason,
                        "alpha_ok": alpha_ok,
                        "alpha_reason": intent.veto_reason or intent.reason,
                    }
                )
                evaluation_metadata.update(mtf_score.as_metadata())
                evaluation_metadata.update(intent.as_metadata())
                if throttle_ok and mtf_ok and risk_ok and alpha_ok:
                    self.throttle.record(book.timestamp)
                    signals.append(scored_micro_signal)
                    self.latest_signal_evaluations[-1] = replace(
                        micro_evaluation,
                        decision="allow",
                        reason=scored_micro_signal.reason,
                        candidate_direction=scored_micro_signal.direction,
                        metadata=evaluation_metadata,
                    )
                else:
                    reject_stage = (
                        "throttle"
                        if not throttle_ok
                        else "multi_timeframe"
                        if not mtf_ok
                        else "risk"
                        if not risk_ok
                        else "alpha_stack"
                    )
                    reason = (
                        throttle_reason
                        if not throttle_ok
                        else mtf_reason
                        if not mtf_ok
                        else risk_reason
                        if not risk_ok
                        else intent.veto_reason or intent.reason
                    )
                    evaluation_metadata["reject_stage"] = reject_stage
                    signals.append(
                        Signal(
                            "risk",
                            book.symbol,
                            "flat",
                            0.0,
                            reason=reason,
                            score=0,
                            signal_horizon="system",
                            metadata=scored_micro_signal.metadata,
                        )
                    )
                    self.latest_signal_evaluations[-1] = replace(
                        micro_evaluation,
                        decision="reject",
                        reason=reason,
                        candidate_direction=micro_signal.direction,
                        metadata=evaluation_metadata,
                    )
        return signals

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
        if score.veto_reason is not None:
            return False, score.veto_reason
        if score.total_score < self.config.multi_timeframe.min_total_score_to_trade:
            return False, "multi_timeframe_score_below_threshold"
        if require_execution and score.execution_score < self.config.multi_timeframe.min_execution_score_to_chase:
            return False, "execution_score_below_threshold"
        if score.position_scale == "none":
            return False, "position_scale_none"
        return True, "ok"

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
