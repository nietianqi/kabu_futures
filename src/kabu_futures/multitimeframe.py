from __future__ import annotations

from collections import Counter

from .config import MultiTimeframeConfig
from .models import BookFeatures, Direction, MultiTimeframeScore, MultiTimeframeSnapshot, Signal


def _clamp_score(value: int, maximum: int) -> int:
    return max(0, min(maximum, value))


def _metadata_float(signal: Signal, key: str, default: float = 0.0) -> float:
    value = signal.metadata.get(key, default)
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _majority_direction(directions: list[Direction]) -> Direction:
    counts = Counter(direction for direction in directions if direction != "flat")
    if not counts:
        return "flat"
    most_common = counts.most_common()
    if len(most_common) > 1 and most_common[0][1] == most_common[1][1]:
        return "flat"
    return most_common[0][0]


def _opposite(left: Direction, right: Direction) -> bool:
    return (left == "long" and right == "short") or (left == "short" and right == "long")


class MultiTimeframeScorer:
    def __init__(self, config: MultiTimeframeConfig) -> None:
        self.config = config
        self.snapshot: MultiTimeframeSnapshot | None = None

    def update_snapshot(self, snapshot: MultiTimeframeSnapshot) -> None:
        self.snapshot = snapshot

    def score(
        self,
        signal_direction: Direction,
        minute_signal: Signal | None = None,
        book_features: BookFeatures | None = None,
    ) -> MultiTimeframeScore:
        if not self.config.enabled:
            return MultiTimeframeScore(30, 30, 25, 15, 100, signal_direction, None, "standard_minute_position")

        snapshot = self.snapshot
        if snapshot is None:
            # Neutral default keeps the engine usable before historical data is wired in,
            # while still requiring setup/execution quality for live signals.
            bias = signal_direction if signal_direction != "flat" else "flat"
            score = MultiTimeframeScore(22, 22, self._setup_score(minute_signal), self._execution_score(book_features), 0, bias, None, "none")
            return self._finalize(score)

        veto = self._veto_reason(snapshot, signal_direction, minute_signal, book_features)
        regime = self._regime_score(snapshot, signal_direction)
        bias_score, bias = self._bias_score(snapshot, signal_direction)
        setup = self._setup_score(minute_signal)
        execution = self._execution_score(book_features)
        return self._finalize(MultiTimeframeScore(regime, bias_score, setup, execution, 0, bias, veto, "none"))

    def _regime_score(self, snapshot: MultiTimeframeSnapshot, signal_direction: Direction) -> int:
        if snapshot.higher_timeframe_extreme_reversal or snapshot.daily_volatility_extreme:
            return 0
        long_terms = [snapshot.yearly_trend, snapshot.monthly_trend, snapshot.weekly_trend, snapshot.daily_trend]
        if signal_direction == "flat":
            return 15
        aligned = sum(1 for direction in long_terms if direction == signal_direction)
        opposite = sum(1 for direction in long_terms if _opposite(direction, signal_direction))
        neutral = sum(1 for direction in long_terms if direction == "flat")
        return _clamp_score(12 + aligned * 5 + neutral * 2 - opposite * 6, self.config.regime_score_max)

    def _bias_score(self, snapshot: MultiTimeframeSnapshot, signal_direction: Direction) -> tuple[int, Direction]:
        bias = _majority_direction([snapshot.daily_trend, snapshot.hourly_trend])
        if signal_direction == "flat":
            return 15, bias
        score = 15
        if snapshot.daily_trend == signal_direction:
            score += 8
        elif _opposite(snapshot.daily_trend, signal_direction):
            score -= 10
        if snapshot.hourly_trend == signal_direction:
            score += 10
        elif _opposite(snapshot.hourly_trend, signal_direction):
            score -= 12
        return _clamp_score(score, self.config.bias_score_max), bias

    def _setup_score(self, minute_signal: Signal | None) -> int:
        if minute_signal is None or not minute_signal.is_tradeable:
            return 12
        quality = max(minute_signal.confidence, _metadata_float(minute_signal, "setup_quality", minute_signal.confidence))
        close_location = _metadata_float(minute_signal, "close_location", 0.5)
        volume_ratio = _metadata_float(minute_signal, "volume_ratio", 1.0)
        if minute_signal.engine == "minute_orb":
            return min(self.config.setup_score_max, int(round(17 + quality * 8 + max(0.0, volume_ratio - 1.0) * 2)))
        if minute_signal.engine == "minute_vwap":
            return min(self.config.setup_score_max, int(round(15 + quality * 8 + abs(close_location - 0.5) * 3)))
        if minute_signal.engine == "directional_intraday":
            return min(self.config.setup_score_max, int(round(16 + quality * 8 + max(0.0, volume_ratio - 1.0) * 2)))
        return min(self.config.setup_score_max, int(round(12 + quality * 8)))

    def _execution_score(self, book_features: BookFeatures | None) -> int:
        if book_features is None:
            return 12
        if book_features.jump_detected or book_features.spread_ticks > 2:
            return 0
        score = 5
        if round(book_features.spread_ticks) == 1:
            score += 3
        if abs(book_features.imbalance) >= 0.30:
            score += 3
        if abs(book_features.ofi_ewma) > max(0.0, book_features.ofi_threshold):
            score += 2
        if abs(book_features.microprice_edge_ticks) >= 0.15:
            score += 2
        return _clamp_score(score, self.config.execution_score_max)

    def _veto_reason(
        self,
        snapshot: MultiTimeframeSnapshot,
        signal_direction: Direction,
        minute_signal: Signal | None,
        book_features: BookFeatures | None,
    ) -> str | None:
        if self.config.hard_veto_extreme_higher_timeframe_reversal and snapshot.higher_timeframe_extreme_reversal:
            return "extreme_higher_timeframe_reversal"
        if self.config.hard_veto_daily_extreme_volatility and snapshot.daily_volatility_extreme:
            return "daily_extreme_volatility"
        if (
            self.config.hard_veto_hourly_minute_strong_conflict
            and minute_signal is not None
            and snapshot.hourly_trend != "flat"
            and _opposite(snapshot.hourly_trend, minute_signal.direction)
            and minute_signal.confidence >= 0.65
        ):
            return "hourly_minute_strong_conflict"
        if (
            self.config.hard_veto_book_latency_or_depth_failure
            and book_features is not None
            and (book_features.jump_detected or book_features.spread_ticks > 2)
        ):
            return "book_latency_or_depth_failure"
        if signal_direction != "flat":
            long_term_bias = _majority_direction([snapshot.yearly_trend, snapshot.monthly_trend, snapshot.weekly_trend])
            if long_term_bias != "flat" and _opposite(long_term_bias, signal_direction):
                return "higher_timeframe_opposite_bias"
        return None

    def _finalize(self, score: MultiTimeframeScore) -> MultiTimeframeScore:
        total = score.regime_score + score.bias_score + score.setup_score + score.execution_score
        if score.veto_reason is not None or total < self.config.min_total_score_to_trade:
            scale = "none"
        elif total < 80:
            scale = "micro225_1"
        elif total < 90:
            scale = "standard_minute_position"
        else:
            scale = "extend_target_only"
        return MultiTimeframeScore(
            score.regime_score,
            score.bias_score,
            score.setup_score,
            score.execution_score,
            total,
            score.bias,
            score.veto_reason,
            scale,
        )
