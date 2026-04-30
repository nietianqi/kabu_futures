from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta

from .config import MinuteEngineConfig, MicroEngineConfig, SymbolsConfig, effective_micro_engine_config
from .indicators import EMA, OpeningRange, RollingATR, SessionVWAP
from .micro_candidates import failed_directional_checks, is_near_miss
from .microstructure import BookFeatureEngine
from .models import Bar, BookFeatures, Direction, OrderBook, Signal, SignalEvaluation


@dataclass
class InstrumentMinuteState:
    ema20: EMA = field(default_factory=lambda: EMA(20))
    ema50: EMA = field(default_factory=lambda: EMA(50))
    atr: RollingATR = field(default_factory=lambda: RollingATR(14))
    vwap: SessionVWAP = field(default_factory=SessionVWAP)
    orb: OpeningRange = field(default_factory=lambda: OpeningRange(5))
    bars: deque[Bar] = field(default_factory=lambda: deque(maxlen=30))
    last_atr: float | None = None
    last_vwap: float | None = None


def session_name(ts: datetime) -> str:
    t = ts.time()
    if time(8, 45) <= t <= time(15, 45):
        return "day"
    return "night"


def session_start(ts: datetime) -> datetime:
    if session_name(ts) == "day":
        return ts.replace(hour=8, minute=45, second=0, microsecond=0)
    if ts.time() >= time(17, 0):
        return ts.replace(hour=17, minute=0, second=0, microsecond=0)
    return (ts - timedelta(days=1)).replace(hour=17, minute=0, second=0, microsecond=0)


def session_key(ts: datetime) -> str:
    start = session_start(ts)
    return f"{start.date().isoformat()}:{session_name(ts)}"


class MinuteStrategyEngine:
    """Minute-bar ORB, VWAP-pullback, and trend-continuation signal generator."""

    def __init__(self, config: MinuteEngineConfig, symbols: SymbolsConfig, tick_size: float = 5.0) -> None:
        self.config = config
        self.symbols = symbols
        self.tick_size = tick_size
        self.states: dict[str, InstrumentMinuteState] = {
            symbols.primary: self._new_state(),
            symbols.filter: self._new_state(),
        }

    def on_bar(self, bar: Bar) -> Signal | None:
        state = self.states.setdefault(bar.symbol, self._new_state())
        key = session_key(bar.start)
        state.last_vwap = state.vwap.update(key, bar.close, bar.volume)
        state.ema20.update(bar.close)
        state.ema50.update(bar.close)
        state.last_atr = state.atr.update(bar)
        state.orb.update(bar, session_start(bar.start))
        state.bars.append(bar)

        if bar.symbol != self.symbols.primary:
            return None
        if state.last_vwap is None:
            return None
        orb_signal = self._orb_signal(bar, state)
        if orb_signal is not None:
            return orb_signal
        vwap_signal = self._vwap_signal(bar, state)
        if vwap_signal is not None:
            return vwap_signal
        return self._trend_continuation_signal(bar, state)

    def trend_bias(self, symbol: str) -> Direction:
        state = self.states.get(symbol)
        if state is None or not state.bars or state.last_vwap is None:
            return "flat"
        if state.ema20.value is None or state.ema50.value is None:
            return "flat"
        close = state.bars[-1].close
        atr_proxy = state.last_atr or max(self.tick_size * 2.0, abs(state.ema20.value - state.ema50.value))
        trend_strength = abs(state.ema20.value - state.ema50.value) / max(self.tick_size, atr_proxy)
        if (
            close > state.last_vwap
            and state.ema20.value >= state.ema50.value
            and state.ema20.slope >= 0
            and trend_strength >= 0.20
        ):
            return "long"
        if (
            close < state.last_vwap
            and state.ema20.value <= state.ema50.value
            and state.ema20.slope <= 0
            and trend_strength >= 0.20
        ):
            return "short"
        return "flat"

    def _orb_signal(self, bar: Bar, state: InstrumentMinuteState) -> Signal | None:
        if not state.orb.complete or state.orb.high is None or state.orb.low is None:
            return None
        close_location = self._close_location(bar)
        range_ratio = self._range_ratio(state, bar)
        volume_ratio = self._volume_ratio(state, bar)
        trend_bias = self.trend_bias(bar.symbol)
        topix_long = self._topix_allows("long")
        topix_short = self._topix_allows("short")
        quality_long = (
            close_location >= 0.65  # bar closed in top 35%, showing bullish conviction
            and range_ratio >= 1.10  # breakout bar at least 10% wider than average range
            and volume_ratio >= 1.00  # at least average volume to validate the move
            and trend_bias == "long"
        )
        quality_short = (
            close_location <= 0.35  # bar closed in bottom 35%, showing bearish conviction
            and range_ratio >= 1.10
            and volume_ratio >= 1.00
            and trend_bias == "short"
        )
        if bar.close > state.orb.high and bar.close > (state.last_vwap or bar.close) and state.ema20.slope > 0 and topix_long and quality_long:
            confidence = min(0.82, 0.68 + min(0.08, (range_ratio - 1.0) * 0.10) + min(0.06, (volume_ratio - 1.0) * 0.06))
            return Signal(
                "minute_orb",
                bar.symbol,
                "long",
                confidence,
                bar.close,
                "orb_breakout_long",
                self._setup_metadata("orb_breakout_long", state, bar, trend_bias, close_location, range_ratio, volume_ratio),
            )
        if bar.close < state.orb.low and bar.close < (state.last_vwap or bar.close) and state.ema20.slope < 0 and topix_short and quality_short:
            confidence = min(0.82, 0.68 + min(0.08, (range_ratio - 1.0) * 0.10) + min(0.06, (volume_ratio - 1.0) * 0.06))
            return Signal(
                "minute_orb",
                bar.symbol,
                "short",
                confidence,
                bar.close,
                "orb_breakout_short",
                self._setup_metadata("orb_breakout_short", state, bar, trend_bias, close_location, range_ratio, volume_ratio),
            )
        return None

    def _vwap_signal(self, bar: Bar, state: InstrumentMinuteState) -> Signal | None:
        if state.last_vwap is None or len(state.bars) < 2:
            return None
        previous = state.bars[-2]
        close_location = self._close_location(bar)
        range_ratio = self._range_ratio(state, bar)
        volume_ratio = self._volume_ratio(state, bar)
        trend_bias = self.trend_bias(bar.symbol)
        trend_long = trend_bias == "long" and bar.close >= state.last_vwap and state.ema20.value is not None and bar.close >= state.ema20.value
        trend_short = trend_bias == "short" and bar.close <= state.last_vwap and state.ema20.value is not None and bar.close <= state.ema20.value
        atr_proxy = state.last_atr or max(self.tick_size * 2.0, self._average_range(list(state.bars)[-5:]))
        vwap_band = atr_proxy * 0.35
        long_pullback = previous.low <= state.last_vwap + vwap_band and bar.is_bullish and close_location >= 0.60
        short_pullback = previous.high >= state.last_vwap - vwap_band and bar.is_bearish and close_location <= 0.40
        if trend_long and long_pullback and volume_ratio >= 0.90 and not self._topix_strong("short"):
            confidence = min(0.78, 0.60 + min(0.06, range_ratio * 0.04) + min(0.04, volume_ratio * 0.03))
            return Signal(
                "minute_vwap",
                bar.symbol,
                "long",
                confidence,
                bar.close,
                "trend_pullback_long",
                self._setup_metadata("trend_pullback_long", state, bar, trend_bias, close_location, range_ratio, volume_ratio),
            )
        if trend_short and short_pullback and volume_ratio >= 0.90 and not self._topix_strong("long"):
            confidence = min(0.78, 0.60 + min(0.06, range_ratio * 0.04) + min(0.04, volume_ratio * 0.03))
            return Signal(
                "minute_vwap",
                bar.symbol,
                "short",
                confidence,
                bar.close,
                "trend_pullback_short",
                self._setup_metadata("trend_pullback_short", state, bar, trend_bias, close_location, range_ratio, volume_ratio),
            )
        return None

    def _trend_continuation_signal(self, bar: Bar, state: InstrumentMinuteState) -> Signal | None:
        if state.last_vwap is None or len(state.bars) < 4:
            return None
        trend_bias = self.trend_bias(bar.symbol)
        close_location = self._close_location(bar)
        range_ratio = self._range_ratio(state, bar)
        volume_ratio = self._volume_ratio(state, bar)
        recent = list(state.bars)[-4:-1]
        recent_high = max(candidate.high for candidate in recent)
        recent_low = min(candidate.low for candidate in recent)
        atr_proxy = state.last_atr or max(self.tick_size * 2.0, self._average_range(list(state.bars)[-6:]))
        compression = self._average_range(recent) <= atr_proxy * 0.85
        if (
            trend_bias == "long"
            and compression
            and bar.close > recent_high
            and close_location >= 0.65
            and range_ratio >= 1.0
            and volume_ratio >= 0.95
            and self._topix_allows("long")
        ):
            confidence = min(0.80, 0.61 + min(0.07, (range_ratio - 1.0) * 0.10) + min(0.05, (volume_ratio - 1.0) * 0.05))
            return Signal(
                "directional_intraday",
                bar.symbol,
                "long",
                confidence,
                bar.close,
                "trend_continuation_long",
                self._setup_metadata("trend_continuation_long", state, bar, trend_bias, close_location, range_ratio, volume_ratio),
            )
        if (
            trend_bias == "short"
            and compression
            and bar.close < recent_low
            and close_location <= 0.35
            and range_ratio >= 1.0
            and volume_ratio >= 0.95
            and self._topix_allows("short")
        ):
            confidence = min(0.80, 0.61 + min(0.07, (range_ratio - 1.0) * 0.10) + min(0.05, (volume_ratio - 1.0) * 0.05))
            return Signal(
                "directional_intraday",
                bar.symbol,
                "short",
                confidence,
                bar.close,
                "trend_continuation_short",
                self._setup_metadata("trend_continuation_short", state, bar, trend_bias, close_location, range_ratio, volume_ratio),
            )
        return None

    def _topix_allows(self, direction: Direction) -> bool:
        topix_bias = self.trend_bias(self.symbols.filter)
        if direction == "long":
            return topix_bias != "short"
        if direction == "short":
            return topix_bias != "long"
        return topix_bias == "flat"

    def _topix_strong(self, direction: Direction) -> bool:
        return self.trend_bias(self.symbols.filter) == direction

    def _new_state(self) -> InstrumentMinuteState:
        return InstrumentMinuteState(atr=RollingATR(self.config.atr_period), orb=OpeningRange(self.config.orb_minutes))

    def _close_location(self, bar: Bar) -> float:
        bar_range = bar.high - bar.low
        if bar_range <= 0:
            return 0.5
        return (bar.close - bar.low) / bar_range

    def _average_range(self, bars: list[Bar]) -> float:
        if not bars:
            return self.tick_size
        return sum(candidate.high - candidate.low for candidate in bars) / len(bars)

    def _average_volume(self, bars: list[Bar]) -> float:
        if not bars:
            return 0.0
        return sum(candidate.volume for candidate in bars) / len(bars)

    def _range_ratio(self, state: InstrumentMinuteState, bar: Bar) -> float:
        previous_bars = list(state.bars)[:-1]
        avg_range = self._average_range(previous_bars[-10:])
        if avg_range <= 0:
            return 1.0
        return (bar.high - bar.low) / avg_range

    def _volume_ratio(self, state: InstrumentMinuteState, bar: Bar) -> float:
        previous_bars = list(state.bars)[:-1]
        avg_volume = self._average_volume(previous_bars[-10:])
        if avg_volume <= 0:
            return 1.0
        return bar.volume / avg_volume

    def _setup_metadata(
        self,
        setup_type: str,
        state: InstrumentMinuteState,
        bar: Bar,
        trend_bias: Direction,
        close_location: float,
        range_ratio: float,
        volume_ratio: float,
    ) -> dict[str, object]:
        if "short" in setup_type:
            directional_close_quality = 1.0 - close_location
        elif "long" in setup_type:
            directional_close_quality = close_location
        else:
            directional_close_quality = max(close_location, 1.0 - close_location)
        return {
            "setup_type": setup_type,
            "trend_bias": trend_bias,
            "session_vwap": state.last_vwap,
            "atr": state.last_atr,
            "ema20": state.ema20.value,
            "ema50": state.ema50.value,
            "close_location": round(close_location, 4),
            "range_ratio": round(range_ratio, 4),
            "volume_ratio": round(volume_ratio, 4),
            "setup_quality": round(
                min(
                    1.0,
                    max(
                        0.0,
                        0.4 * directional_close_quality
                        + 0.35 * min(1.5, range_ratio) / 1.5
                        + 0.25 * min(1.5, volume_ratio) / 1.5,
                    ),
                ),
                4,
            ),
        }


class MicroStrategyEngine:
    """Tick-level order-book imbalance and OFI scalping signal generator."""

    def __init__(self, config: MicroEngineConfig, tick_size: float = 5.0) -> None:
        self.config = effective_micro_engine_config(config)
        self.features = BookFeatureEngine(self.config, tick_size=tick_size)
        self.last_signal_time: datetime | None = None
        self.last_evaluation: SignalEvaluation | None = None

    def on_book(
        self,
        book: OrderBook,
        minute_bias: Direction = "flat",
        topix_bias: Direction = "flat",
        now: datetime | None = None,
        features: BookFeatures | None = None,
    ) -> Signal | None:
        signal, _ = self.evaluate_book(book, minute_bias, topix_bias, now=now, features=features)
        return signal

    def evaluate_book(
        self,
        book: OrderBook,
        minute_bias: Direction = "flat",
        topix_bias: Direction = "flat",
        now: datetime | None = None,
        features: BookFeatures | None = None,
    ) -> tuple[Signal | None, SignalEvaluation]:
        features = features or self.features.update(book, now=now)
        ofi_threshold = max(0.0, features.ofi_threshold)
        spread_ok = round(features.spread_ticks) == self.config.spread_ticks_required
        too_soon = self._too_soon(book.timestamp)
        imbalance_long_ok = features.imbalance >= self.config.imbalance_entry
        imbalance_short_ok = features.imbalance <= -self.config.imbalance_entry
        ofi_long_ok = features.ofi_ewma > ofi_threshold
        ofi_short_ok = features.ofi_ewma < -ofi_threshold
        microprice_long_ok = features.microprice_edge_ticks >= self.config.microprice_entry_ticks
        microprice_short_ok = features.microprice_edge_ticks <= -self.config.microprice_entry_ticks
        minute_long_ok = minute_bias != "short"
        minute_short_ok = minute_bias != "long"
        topix_long_ok = topix_bias != "short"
        topix_short_ok = topix_bias != "long"
        long_ok = imbalance_long_ok and ofi_long_ok and microprice_long_ok and minute_long_ok and topix_long_ok
        short_ok = imbalance_short_ok and ofi_short_ok and microprice_short_ok and minute_short_ok and topix_short_ok
        long_edge_score = sum(
            (
                int(imbalance_long_ok),
                int(ofi_long_ok),
                int(microprice_long_ok),
                int(minute_long_ok),
                int(topix_long_ok),
            )
        )
        short_edge_score = sum(
            (
                int(imbalance_short_ok),
                int(ofi_short_ok),
                int(microprice_short_ok),
                int(minute_short_ok),
                int(topix_short_ok),
            )
        )
        candidate_direction: Direction
        if long_ok:
            candidate_direction = "long"
        elif short_ok:
            candidate_direction = "short"
        elif long_edge_score > short_edge_score:
            candidate_direction = "long"
        elif short_edge_score > long_edge_score:
            candidate_direction = "short"
        else:
            candidate_direction = "flat"

        # Per-direction failed checks (only the directional micro filters; quality
        # gates such as spread/jump/too_soon are tracked separately).
        long_failed_checks = failed_directional_checks(
            imbalance_long_ok, ofi_long_ok, microprice_long_ok, minute_long_ok, topix_long_ok
        )
        short_failed_checks = failed_directional_checks(
            imbalance_short_ok, ofi_short_ok, microprice_short_ok, minute_short_ok, topix_short_ok
        )
        # Near-miss: book quality is fine (spread=required, no jump, not throttled)
        # and only ONE soft directional check is missing (imbalance / ofi /
        # microprice). Bias conflicts do NOT count as near-miss because flipping
        # bias is not a parameter-level fix.
        long_near_miss = is_near_miss(spread_ok, features.jump_detected, too_soon, long_failed_checks)
        short_near_miss = is_near_miss(spread_ok, features.jump_detected, too_soon, short_failed_checks)

        metadata = {
            "mode": self.config.mode,
            "invert_direction": self.config.invert_direction,
            "spread_ticks": features.spread_ticks,
            "spread_required_ticks": self.config.spread_ticks_required,
            "spread_ok": spread_ok,
            "imbalance": features.imbalance,
            "imbalance_entry": self.config.imbalance_entry,
            "imbalance_long_ok": imbalance_long_ok,
            "imbalance_short_ok": imbalance_short_ok,
            "ofi_ewma": features.ofi_ewma,
            "ofi_threshold": features.ofi_threshold,
            "ofi_percentile": self.config.ofi_percentile,
            "ofi_long_ok": ofi_long_ok,
            "ofi_short_ok": ofi_short_ok,
            "microprice_edge_ticks": features.microprice_edge_ticks,
            "microprice_entry_ticks": self.config.microprice_entry_ticks,
            "microprice_long_ok": microprice_long_ok,
            "microprice_short_ok": microprice_short_ok,
            "minute_bias": minute_bias,
            "topix_bias": topix_bias,
            "minute_long_ok": minute_long_ok,
            "minute_short_ok": minute_short_ok,
            "topix_long_ok": topix_long_ok,
            "topix_short_ok": topix_short_ok,
            "jump_detected": features.jump_detected,
            "jump_reason": features.jump_reason,
            "too_soon": too_soon,
            "min_order_interval_seconds": self.config.min_order_interval_seconds,
            "latency_ms": features.latency_ms,
            "candidate_direction": candidate_direction,
            "long_edge_score": long_edge_score,
            "short_edge_score": short_edge_score,
            "long_failed_checks": list(long_failed_checks),
            "short_failed_checks": list(short_failed_checks),
            "long_near_miss": long_near_miss,
            "short_near_miss": short_near_miss,
            "near_miss": long_near_miss or short_near_miss,
            "near_miss_direction": (
                "long" if long_near_miss and not short_near_miss
                else "short" if short_near_miss and not long_near_miss
                else "both" if long_near_miss and short_near_miss
                else None
            ),
            "near_miss_missing": (
                long_failed_checks[0] if long_near_miss and not short_near_miss
                else short_failed_checks[0] if short_near_miss and not long_near_miss
                else None
            ),
        }

        if features.jump_detected:
            evaluation = SignalEvaluation("micro_book", book.symbol, book.timestamp, "reject", "jump_detected", candidate_direction, metadata)
            self.last_evaluation = evaluation
            return None, evaluation
        if not spread_ok:
            evaluation = SignalEvaluation(
                "micro_book",
                book.symbol,
                book.timestamp,
                "reject",
                "spread_not_required_width",
                candidate_direction,
                metadata,
            )
            self.last_evaluation = evaluation
            return None, evaluation
        if too_soon:
            evaluation = SignalEvaluation(
                "micro_book",
                book.symbol,
                book.timestamp,
                "reject",
                "min_order_interval",
                candidate_direction,
                metadata,
            )
            self.last_evaluation = evaluation
            return None, evaluation

        if long_ok:
            self.last_signal_time = book.timestamp
            signal = self._build_signal(book, "long", metadata)
            evaluation = SignalEvaluation("micro_book", book.symbol, book.timestamp, "allow", signal.reason, signal.direction, metadata)
            self.last_evaluation = evaluation
            return signal, evaluation
        if short_ok:
            self.last_signal_time = book.timestamp
            signal = self._build_signal(book, "short", metadata)
            evaluation = SignalEvaluation("micro_book", book.symbol, book.timestamp, "allow", signal.reason, signal.direction, metadata)
            self.last_evaluation = evaluation
            return signal, evaluation

        evaluation = SignalEvaluation(
            "micro_book",
            book.symbol,
            book.timestamp,
            "reject",
            self._reject_reason(
                imbalance_long_ok,
                imbalance_short_ok,
                ofi_long_ok,
                ofi_short_ok,
                microprice_long_ok,
                microprice_short_ok,
                minute_long_ok,
                minute_short_ok,
                topix_long_ok,
                topix_short_ok,
            ),
            candidate_direction,
            metadata,
        )
        self.last_evaluation = evaluation
        return None, evaluation

    def _build_signal(self, book: OrderBook, raw_direction: Direction, metadata: dict[str, object]) -> Signal:
        direction = _opposite(raw_direction) if self.config.invert_direction else raw_direction
        signal_metadata = dict(metadata)
        signal_metadata["raw_signal_direction"] = raw_direction
        signal_metadata["executed_signal_direction"] = direction
        if self.config.invert_direction:
            signal_metadata["direction_inverted"] = True
        price = book.best_ask_price if direction == "long" else book.best_bid_price
        reason = f"micro_book_{direction}" if not self.config.invert_direction else f"micro_book_inverted_{direction}"
        return Signal("micro_book", book.symbol, direction, 0.65, price, reason, signal_metadata)

    def _too_soon(self, timestamp: datetime) -> bool:
        if self.last_signal_time is None:
            return False
        return (timestamp - self.last_signal_time).total_seconds() < self.config.min_order_interval_seconds

    def _reject_reason(
        self,
        imbalance_long_ok: bool,
        imbalance_short_ok: bool,
        ofi_long_ok: bool,
        ofi_short_ok: bool,
        microprice_long_ok: bool,
        microprice_short_ok: bool,
        minute_long_ok: bool,
        minute_short_ok: bool,
        topix_long_ok: bool,
        topix_short_ok: bool,
    ) -> str:
        long_edge_ready = imbalance_long_ok and ofi_long_ok and microprice_long_ok
        short_edge_ready = imbalance_short_ok and ofi_short_ok and microprice_short_ok
        if long_edge_ready and not minute_long_ok:
            return "minute_bias_conflict_long"
        if short_edge_ready and not minute_short_ok:
            return "minute_bias_conflict_short"
        if long_edge_ready and not topix_long_ok:
            return "topix_bias_conflict_long"
        if short_edge_ready and not topix_short_ok:
            return "topix_bias_conflict_short"
        if not imbalance_long_ok and not imbalance_short_ok:
            return "imbalance_not_met"
        if not ofi_long_ok and not ofi_short_ok:
            return "ofi_not_met"
        if not microprice_long_ok and not microprice_short_ok:
            return "microprice_not_met"
        if (not minute_long_ok and imbalance_long_ok) or (not minute_short_ok and imbalance_short_ok):
            return "minute_bias_conflict"
        if (not topix_long_ok and imbalance_long_ok) or (not topix_short_ok and imbalance_short_ok):
            return "topix_bias_conflict"
        return "micro_no_directional_edge"


def _opposite(direction: Direction) -> Direction:
    if direction == "long":
        return "short"
    if direction == "short":
        return "long"
    return "flat"
