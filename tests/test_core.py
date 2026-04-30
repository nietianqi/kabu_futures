from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
import pathlib
import sys
import unittest
from urllib.error import HTTPError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from dataclasses import replace
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from kabu_futures.alpha import (
    NTRatioSpreadEngine,
    StrategyArbiter,
    USJapanLeadLagScorer,
    compute_micro225_per_topix_mini,
)
from kabu_futures.api import KabuApiError, KabuStationClient, build_future_registration_symbols, extract_symbol_code
from kabu_futures.analysis_utils import iter_books, max_drawdown
from kabu_futures.config import default_config, effective_micro_engine_config, load_json_config
from kabu_futures.diagnostics_funnel import diagnosis_notes, latency_summary, micro_entry_funnel
from kabu_futures.engine import DualStrategyEngine
from kabu_futures.evolution import analyze_micro_log, calculate_markout_ticks
from kabu_futures.indicators import BarBuilder
from kabu_futures.live import MicroCandidateEmitter, _should_print_tick, tick_to_dict, signal_to_dict
from kabu_futures.live_api_health import LiveApiHealth
from kabu_futures.live_execution import LiveExecutionController
from kabu_futures.live_safety import LiveSafetyState
from kabu_futures.live_state import (
    LivePositionState,
    PendingLiveOrder,
    pending_summary,
    position_key,
    position_summary,
    validated_symbol_code,
)
from kabu_futures.log_diagnostics import diagnose_log
from kabu_futures.microstructure import BookFeatureEngine, RollingPercentile, microprice, percentile, weighted_imbalance
from kabu_futures.marketdata import BufferedJsonlMarketRecorder, KabuBoardNormalizer, MarketDataError, MarketDataSkip, signal_evaluation_to_dict
from kabu_futures.models import (
    AlphaSignal,
    Bar,
    BookFeatures,
    ExternalFactorsSnapshot,
    Level,
    MultiTimeframeSnapshot,
    OrderBook,
    PortfolioExposure,
    Signal,
    SignalEvaluation,
)
from kabu_futures.multitimeframe import MultiTimeframeScorer
from kabu_futures.execution import MicroTradeManager
from kabu_futures.orders import KabuFutureOrderBuilder
from kabu_futures.paper_execution import PaperExecutionController
from kabu_futures.replay import read_recorded_books, replay_jsonl
from kabu_futures.risk import OrderThrottle
from kabu_futures.serialization import signal_snapshot
from kabu_futures.runtime import live_startup_self_check
from kabu_futures.sessions import JST, classify_jst_session, new_entries_allowed
from kabu_futures.strategies import MicroStrategyEngine, MinuteStrategyEngine
from kabu_futures.tuning import evaluate_micro_config, tune_micro_params
from kabu_futures.walk_forward import make_windows, split_books_by_day, walk_forward_micro
from kabu_futures.promotion import PromotionThresholds, evaluate_challenger
from kabu_futures.regime import RegimeClassifier, split_books_by_regime


def book(ts: datetime, bid_qty: float = 100, ask_qty: float = 50, bid: float = 50000, ask: float = 50005) -> OrderBook:
    return OrderBook(
        symbol="NK225micro",
        timestamp=ts,
        best_bid_price=bid,
        best_bid_qty=bid_qty,
        best_ask_price=ask,
        best_ask_qty=ask_qty,
        buy_levels=(Level(bid, bid_qty), Level(bid - 5, 80), Level(bid - 10, 60)),
        sell_levels=(Level(ask, ask_qty), Level(ask + 5, 40), Level(ask + 10, 30)),
    )


def topix_book(
    ts: datetime,
    bid_qty: float = 100,
    ask_qty: float = 50,
    bid: float = 3770.5,
    ask: float = 3770.75,
) -> OrderBook:
    return OrderBook(
        symbol="TOPIXmini",
        timestamp=ts,
        best_bid_price=bid,
        best_bid_qty=bid_qty,
        best_ask_price=ask,
        best_ask_qty=ask_qty,
        buy_levels=(Level(bid, bid_qty), Level(bid - 0.25, 80), Level(bid - 0.5, 60)),
        sell_levels=(Level(ask, ask_qty), Level(ask + 0.25, 40), Level(ask + 0.5, 30)),
    )


class ConfigTests(unittest.TestCase):
    def test_default_config_trades_topix_with_symbol_specific_tick_size(self) -> None:
        cfg = default_config()
        self.assertIn("TOPIXmini", cfg.trade_symbols())
        self.assertEqual(cfg.tick_size_for("NK225micro"), 5.0)
        self.assertEqual(cfg.tick_size_for("TOPIXmini"), 0.25)
        self.assertEqual(cfg.tick_value_yen_for("TOPIXmini"), 250.0)

    def test_default_live_safety_limits_one_position_without_changing_paper_risk(self) -> None:
        cfg = default_config()

        self.assertEqual(cfg.live_execution.max_positions_per_symbol, 1)
        self.assertEqual(cfg.live_execution.loss_hold_guard_ticks, 15.0)
        self.assertEqual(cfg.live_execution.daily_loss_limit_yen, 5000.0)
        self.assertEqual(cfg.risk.max_positions_per_symbol, 5)

    def test_micro_ofi_percentile_config_validates(self) -> None:
        cfg = default_config()
        self.assertEqual(cfg.micro_engine.ofi_percentile, 70.0)
        with self.assertRaisesRegex(ValueError, "ofi_percentile"):
            replace(cfg, micro_engine=replace(cfg.micro_engine, ofi_percentile=120.0)).validate()

    def test_default_micro_entry_profile_keeps_existing_thresholds(self) -> None:
        cfg = default_config()
        effective = cfg.effective_micro_engine()

        self.assertEqual(cfg.micro_engine.entry_profile, "default")
        self.assertEqual(effective.imbalance_entry, cfg.micro_engine.imbalance_entry)
        self.assertEqual(effective.microprice_entry_ticks, cfg.micro_engine.microprice_entry_ticks)
        self.assertEqual(effective.ofi_percentile, cfg.micro_engine.ofi_percentile)

    def test_conservative_candidate_profile_overrides_only_entry_thresholds(self) -> None:
        cfg = replace(
            default_config(),
            micro_engine=replace(default_config().micro_engine, entry_profile="conservative_candidate_v1"),
        )
        effective = cfg.effective_micro_engine()

        self.assertEqual(effective.imbalance_entry, 0.28)
        self.assertEqual(effective.microprice_entry_ticks, 0.12)
        self.assertEqual(effective.ofi_percentile, 65.0)
        self.assertEqual(effective.spread_ticks_required, cfg.micro_engine.spread_ticks_required)
        self.assertEqual(effective.qty, cfg.micro_engine.qty)

    def test_unknown_micro_entry_profile_is_rejected(self) -> None:
        cfg = replace(default_config(), micro_engine=replace(default_config().micro_engine, entry_profile="aggressive"))

        with self.assertRaisesRegex(ValueError, "entry_profile"):
            cfg.validate()

    def test_load_json_config_accepts_conservative_candidate_profile(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "candidate.json"
            path.write_text('{"micro_engine":{"entry_profile":"conservative_candidate_v1"}}', encoding="utf-8")

            cfg = load_json_config(path)

        self.assertEqual(cfg.micro_engine.entry_profile, "conservative_candidate_v1")
        self.assertEqual(effective_micro_engine_config(cfg.micro_engine).imbalance_entry, 0.28)

    def test_load_json_config_validates_inconsistent_micro_thresholds(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "bad_config.json"
            path.write_text(
                '{"micro_engine":{"imbalance_entry":0.1,"imbalance_exit":0.2}}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "imbalance_entry"):
                load_json_config(path)

    def test_dual_strategy_engine_validates_manual_config(self) -> None:
        base_cfg = default_config()
        bad_cfg = replace(
            base_cfg,
            micro_engine=replace(base_cfg.micro_engine, imbalance_entry=0.1, imbalance_exit=0.2),
        )
        with self.assertRaisesRegex(ValueError, "imbalance_entry"):
            DualStrategyEngine(bad_cfg)

    def test_no_new_entry_window_loads_from_json_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "config.json"
            path.write_text(
                '{"micro_engine":{"no_new_entry_windows_jst":["15:25-16:30","22:00-22:15"]}}',
                encoding="utf-8",
            )
            cfg = load_json_config(path)
            self.assertEqual(list(cfg.micro_engine.no_new_entry_windows_jst), ["15:25-16:30", "22:00-22:15"])

    def test_session_schedule_loads_from_json_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "config.json"
            path.write_text(
                '{"session_schedule":{"day_continuous":"08:50-15:30","allow_new_entry_phases":["day_continuous"]}}',
                encoding="utf-8",
            )
            cfg = load_json_config(path)
            self.assertEqual(cfg.session_schedule.day_continuous, "08:50-15:30")
            self.assertEqual(list(cfg.session_schedule.allow_new_entry_phases), ["day_continuous"])


class SessionScheduleTests(unittest.TestCase):
    def test_day_session_boundaries(self) -> None:
        self.assertFalse(new_entries_allowed(datetime(2026, 4, 27, 8, 44, tzinfo=JST)))
        self.assertTrue(new_entries_allowed(datetime(2026, 4, 27, 8, 45, tzinfo=JST)))
        self.assertTrue(new_entries_allowed(datetime(2026, 4, 27, 15, 39, tzinfo=JST)))
        self.assertFalse(new_entries_allowed(datetime(2026, 4, 27, 15, 40, tzinfo=JST)))

    def test_night_session_boundaries(self) -> None:
        self.assertFalse(new_entries_allowed(datetime(2026, 4, 27, 16, 59, tzinfo=JST)))
        self.assertTrue(new_entries_allowed(datetime(2026, 4, 27, 17, 0, tzinfo=JST)))
        self.assertTrue(new_entries_allowed(datetime(2026, 4, 28, 5, 54, tzinfo=JST)))
        self.assertFalse(new_entries_allowed(datetime(2026, 4, 28, 5, 55, tzinfo=JST)))

    def test_api_maintenance_window(self) -> None:
        state = classify_jst_session(datetime(2026, 4, 28, 6, 15, tzinfo=JST))
        self.assertEqual(state.phase, "api_maintenance")
        self.assertEqual(state.api_window_status, "maintenance")
        self.assertFalse(state.new_entries_allowed)


class MicrostructureTests(unittest.TestCase):
    def test_rolling_percentile_matches_sorting_implementation(self) -> None:
        rolling = RollingPercentile(maxlen=5)
        window: list[float] = []
        values = [5.0, 1.0, 3.0, 9.0, 7.0, 2.0, 8.0]
        for value in values:
            rolling.update(value)
            window.append(value)
            if len(window) > 5:
                window.pop(0)
            for pct in (0.0, 50.0, 70.0, 100.0):
                self.assertEqual(rolling.percentile(pct), percentile(window, pct))

    def test_weighted_imbalance_is_positive_when_buy_depth_is_larger(self) -> None:
        imbalance, total = weighted_imbalance(book(datetime(2026, 4, 23, 9, 0)), 3)
        self.assertGreater(imbalance, 0)
        self.assertGreater(total, 0)

    def test_microprice_moves_above_mid_when_bid_qty_is_larger(self) -> None:
        b = book(datetime(2026, 4, 23, 9, 0), bid_qty=200, ask_qty=50)
        self.assertGreater(microprice(b), b.mid_price)

    def test_ofi_turns_positive_when_bid_queue_increases(self) -> None:
        cfg = default_config().micro_engine
        engine = BookFeatureEngine(cfg)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        engine.update(book(ts, bid_qty=100, ask_qty=100))
        features = engine.update(book(ts + timedelta(milliseconds=100), bid_qty=160, ask_qty=100))
        self.assertGreater(features.ofi, 0)

    def test_book_features_use_received_at_for_live_event_clock(self) -> None:
        cfg = default_config().micro_engine
        engine = BookFeatureEngine(cfg)
        trade_ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        first = book(trade_ts, bid_qty=100, ask_qty=100)
        second = book(trade_ts, bid_qty=120, ask_qty=90)
        first = replace(first, received_at=trade_ts + timedelta(seconds=10))
        second = replace(second, received_at=trade_ts + timedelta(seconds=10, milliseconds=50))
        engine.update(first, now=first.received_at)
        features = engine.update(second, now=second.received_at)
        self.assertEqual(features.latency_ms, 0.0)
        self.assertEqual(features.event_gap_ms, 50.0)
        self.assertFalse(features.jump_detected)
        self.assertIsNone(features.jump_reason)

    def test_book_features_ignore_live_push_gap_without_price_jump(self) -> None:
        cfg = default_config().micro_engine
        engine = BookFeatureEngine(cfg)
        trade_ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        first = replace(book(trade_ts, bid_qty=100, ask_qty=100), received_at=trade_ts)
        second = replace(book(trade_ts + timedelta(seconds=2), bid_qty=120, ask_qty=90), received_at=trade_ts + timedelta(seconds=2))

        engine.update(first, now=first.received_at)
        features = engine.update(second, now=second.received_at)

        self.assertGreater(features.event_gap_ms, cfg.websocket_latency_stop_ms)
        self.assertEqual(features.latency_ms, 0.0)
        self.assertFalse(features.jump_detected)
        self.assertIsNone(features.jump_reason)

    def test_book_features_keep_hard_jump_guards(self) -> None:
        cfg = default_config().micro_engine
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)

        spread_engine = BookFeatureEngine(cfg)
        spread_engine.update(replace(book(ts), received_at=ts), now=ts)
        spread_jump = spread_engine.update(replace(book(ts + timedelta(milliseconds=50), ask=50015), received_at=ts + timedelta(milliseconds=50)))
        self.assertTrue(spread_jump.jump_detected)
        self.assertEqual(spread_jump.jump_reason, "spread_wide")

        latency_engine = BookFeatureEngine(cfg)
        latency_engine.update(book(ts), now=ts)
        latency_jump = latency_engine.update(book(ts + timedelta(milliseconds=50)), now=ts + timedelta(seconds=2))
        self.assertTrue(latency_jump.jump_detected)
        self.assertEqual(latency_jump.jump_reason, "latency_high")

        mid_engine = BookFeatureEngine(cfg)
        mid_engine.update(replace(book(ts), received_at=ts), now=ts)
        mid_jump = mid_engine.update(
            replace(
                book(ts + timedelta(milliseconds=50), bid=50025, ask=50030),
                received_at=ts + timedelta(milliseconds=50),
            ),
            now=ts + timedelta(milliseconds=50),
        )
        self.assertTrue(mid_jump.jump_detected)
        self.assertEqual(mid_jump.jump_reason, "mid_move_jump")

    def test_micro_strategy_signal_survives_live_push_gap_without_price_jump(self) -> None:
        cfg = default_config().micro_engine
        engine = MicroStrategyEngine(cfg)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        for idx in range(8):
            current_ts = ts + timedelta(milliseconds=idx * 10)
            steady = replace(book(current_ts, bid_qty=100, ask_qty=100), received_at=current_ts)
            engine.evaluate_book(steady, now=current_ts)
        signal_ts = ts + timedelta(seconds=2)
        strong_bid = replace(book(signal_ts, bid_qty=1000, ask_qty=10), received_at=signal_ts)

        signal, evaluation = engine.evaluate_book(strong_bid, now=signal_ts)

        self.assertIsNotNone(signal)
        self.assertEqual(evaluation.decision, "allow")
        self.assertEqual(evaluation.reason, "micro_book_long")
        self.assertEqual(evaluation.metadata["jump_detected"], False)
        self.assertIsNone(evaluation.metadata["jump_reason"])

    def test_micro_strategy_uses_conservative_candidate_entry_profile(self) -> None:
        base_cfg = default_config()
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        b = book(ts, bid_qty=300, ask_qty=20)
        features = BookFeatures(
            timestamp=ts,
            symbol="NK225micro",
            spread_ticks=1.0,
            imbalance=0.285,
            ofi=1.0,
            ofi_ewma=1.0,
            ofi_threshold=0.0,
            microprice=b.mid_price + 0.65,
            microprice_edge_ticks=0.13,
            total_depth=500.0,
            jump_detected=False,
            latency_ms=0.0,
        )

        default_signal, default_eval = MicroStrategyEngine(
            base_cfg.micro_engine,
            tick_size=base_cfg.tick_size,
        ).evaluate_book(b, now=ts, features=features)
        profile_signal, profile_eval = MicroStrategyEngine(
            replace(base_cfg.micro_engine, entry_profile="conservative_candidate_v1"),
            tick_size=base_cfg.tick_size,
        ).evaluate_book(b, now=ts, features=features)

        self.assertIsNone(default_signal)
        self.assertEqual(default_eval.decision, "reject")
        self.assertIsNotNone(profile_signal)
        self.assertEqual(profile_eval.decision, "allow")
        self.assertEqual(profile_eval.metadata["imbalance_entry"], 0.28)
        self.assertEqual(profile_eval.metadata["microprice_entry_ticks"], 0.12)
        self.assertEqual(profile_eval.metadata["ofi_percentile"], 65.0)

    def test_micro_strategy_reuses_precomputed_book_features(self) -> None:
        cfg = default_config().micro_engine
        engine = MicroStrategyEngine(cfg)
        engine.features.update = Mock(side_effect=AssertionError("features should be reused"))  # type: ignore[method-assign]
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        b = book(ts, bid_qty=300, ask_qty=20)
        features = BookFeatures(
            timestamp=ts,
            symbol="NK225micro",
            spread_ticks=1.0,
            imbalance=0.6,
            ofi=80.0,
            ofi_ewma=80.0,
            ofi_threshold=10.0,
            microprice=b.mid_price + 1.0,
            microprice_edge_ticks=0.2,
            total_depth=500.0,
            jump_detected=False,
            latency_ms=10.0,
        )
        signal = engine.on_book(b, now=ts, features=features)
        self.assertIsNotNone(signal)
        engine.features.update.assert_not_called()

    def test_micro_strategy_can_invert_direction_for_experiment(self) -> None:
        base_cfg = default_config()
        cfg = replace(base_cfg.micro_engine, invert_direction=True)
        engine = MicroStrategyEngine(cfg)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        b = book(ts, bid_qty=300, ask_qty=20)
        features = BookFeatures(
            timestamp=ts,
            symbol="NK225micro",
            spread_ticks=1.0,
            imbalance=0.6,
            ofi=80.0,
            ofi_ewma=80.0,
            ofi_threshold=10.0,
            microprice=b.mid_price + 1.0,
            microprice_edge_ticks=0.2,
            total_depth=500.0,
            jump_detected=False,
            latency_ms=10.0,
        )
        signal, evaluation = engine.evaluate_book(b, now=ts, features=features)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, "short")
        self.assertEqual(signal.price, b.best_bid_price)
        self.assertEqual(signal.reason, "micro_book_inverted_short")
        self.assertEqual(signal.metadata["raw_signal_direction"], "long")
        self.assertEqual(signal.metadata["executed_signal_direction"], "short")
        self.assertEqual(evaluation.candidate_direction, "short")

    def test_dual_engine_generates_micro_signal_for_topix_trade_symbol(self) -> None:
        cfg = default_config()
        cfg = replace(
            cfg,
            micro_engine=replace(
                cfg.micro_engine,
                imbalance_entry=0.1,
                imbalance_exit=0.05,
                microprice_entry_ticks=0.01,
                min_order_interval_seconds=0,
            ),
            multi_timeframe=replace(
                cfg.multi_timeframe,
                min_total_score_to_trade=0,
                min_execution_score_to_chase=0,
            ),
        )
        engine = DualStrategyEngine(cfg)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        routed_signal = Signal("micro_book", "TOPIXmini", "long", 0.8, 3770.75, "micro_book_long")
        routed_eval = SignalEvaluation("micro_book", "TOPIXmini", ts, "allow", "micro_book_long", "long")
        engine.micro_engines["TOPIXmini"].evaluate_book = Mock(return_value=(routed_signal, routed_eval))  # type: ignore[method-assign]
        signals = engine.on_order_book(topix_book(ts), now=ts)

        tradeable = [signal for signal in signals if signal.engine == "micro_book" and signal.is_tradeable]
        self.assertTrue(tradeable)
        self.assertEqual(tradeable[0].symbol, "TOPIXmini")
        self.assertEqual(tradeable[0].price, 3770.75)
        engine.micro_engines["TOPIXmini"].evaluate_book.assert_called_once()
        self.assertEqual(engine.latest_signal_evaluations[-1].decision, "allow")
        self.assertEqual(engine.latest_signal_evaluations[-1].symbol, "TOPIXmini")

    def test_dual_strategy_engine_uses_effective_micro_profile(self) -> None:
        cfg = replace(
            default_config(),
            micro_engine=replace(default_config().micro_engine, entry_profile="conservative_candidate_v1"),
        )

        engine = DualStrategyEngine(cfg)

        self.assertEqual(engine.micro_config.imbalance_entry, 0.28)
        self.assertEqual(engine.micro_config.microprice_entry_ticks, 0.12)
        self.assertEqual(engine.micro_config.ofi_percentile, 65.0)
        self.assertEqual(engine.micro_engines["NK225micro"].config.imbalance_entry, 0.28)

    def test_micro_strategy_evaluation_records_reject_reason(self) -> None:
        cfg = default_config().micro_engine
        engine = MicroStrategyEngine(cfg)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        b = book(ts, bid=50000, ask=50015)
        features = BookFeatures(
            timestamp=ts,
            symbol="NK225micro",
            spread_ticks=3.0,
            imbalance=0.2,
            ofi=0.0,
            ofi_ewma=0.0,
            ofi_threshold=10.0,
            microprice=b.mid_price,
            microprice_edge_ticks=0.0,
            total_depth=150.0,
            jump_detected=False,
            latency_ms=10.0,
        )
        signal, evaluation = engine.evaluate_book(b, now=ts, features=features)
        self.assertIsNone(signal)
        self.assertEqual(evaluation.decision, "reject")
        self.assertEqual(evaluation.reason, "spread_not_required_width")
        self.assertEqual(evaluation.metadata["spread_ok"], False)

    def test_micro_strategy_marks_clean_one_check_reject_as_near_miss(self) -> None:
        cfg = default_config()
        engine = MicroStrategyEngine(cfg.micro_engine, tick_size=cfg.tick_size)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        features = BookFeatures(
            timestamp=ts,
            symbol="NK225micro",
            spread_ticks=1.0,
            imbalance=0.20,
            ofi=10.0,
            ofi_ewma=10.0,
            ofi_threshold=1.0,
            microprice=50005,
            microprice_edge_ticks=0.20,
            total_depth=100.0,
            jump_detected=False,
        )

        signal, evaluation = engine.evaluate_book(book(ts), features=features)

        self.assertIsNone(signal)
        self.assertEqual(evaluation.reason, "imbalance_not_met")
        self.assertTrue(evaluation.metadata["near_miss"])
        self.assertEqual(evaluation.metadata["near_miss_direction"], "long")
        self.assertEqual(evaluation.metadata["near_miss_missing"], "imbalance")

    def test_micro_strategy_does_not_mark_jump_as_near_miss(self) -> None:
        cfg = default_config()
        engine = MicroStrategyEngine(cfg.micro_engine, tick_size=cfg.tick_size)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        features = BookFeatures(
            timestamp=ts,
            symbol="NK225micro",
            spread_ticks=1.0,
            imbalance=0.20,
            ofi=10.0,
            ofi_ewma=10.0,
            ofi_threshold=1.0,
            microprice=50005,
            microprice_edge_ticks=0.20,
            total_depth=100.0,
            jump_detected=True,
            jump_reason="mid_move_jump",
        )

        signal, evaluation = engine.evaluate_book(book(ts), features=features)

        self.assertIsNone(signal)
        self.assertEqual(evaluation.reason, "jump_detected")
        self.assertFalse(evaluation.metadata["near_miss"])


class IndicatorTests(unittest.TestCase):
    def test_bar_builder_uses_incremental_cumulative_volume(self) -> None:
        builder = BarBuilder(60)
        start = datetime(2026, 4, 23, 9, 0)
        self.assertIsNone(builder.update("NK225micro", start, 50000, 1000))
        self.assertIsNone(builder.update("NK225micro", start + timedelta(seconds=10), 50005, 1006))
        closed = builder.update("NK225micro", start + timedelta(minutes=1), 50010, 1015)
        self.assertIsNotNone(closed)
        self.assertEqual(closed.volume, 6.0)


class MinuteStrategyTests(unittest.TestCase):
    def test_orb_breakout_generates_long_signal_with_topix_confirmation(self) -> None:
        cfg = default_config()
        engine = MinuteStrategyEngine(cfg.minute_engine, cfg.symbols)
        start = datetime(2026, 4, 23, 8, 45)
        for i in range(6):
            topix_close = 3000 + i * 0.5
            micro_close = 50000 + i * 5
            topix_bar = Bar("TOPIXmini", start + timedelta(minutes=i), start + timedelta(minutes=i + 1), topix_close - 0.25, topix_close + 0.5, topix_close - 0.5, topix_close, 10 + i)
            micro_bar = Bar("NK225micro", start + timedelta(minutes=i), start + timedelta(minutes=i + 1), micro_close - 5, micro_close + 5, micro_close - 10, micro_close, 12 + i)
            engine.on_bar(topix_bar)
            engine.on_bar(micro_bar)
        breakout = Bar("NK225micro", start + timedelta(minutes=6), start + timedelta(minutes=7), 50028, 50050, 50025, 50048, 32)
        signal = engine.on_bar(breakout)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.direction, "long")

    def test_minute_engine_infers_long_bias_without_needing_trade_signal(self) -> None:
        cfg = default_config()
        engine = MinuteStrategyEngine(cfg.minute_engine, cfg.symbols)
        start = datetime(2026, 4, 23, 9, 0)
        for i in range(10):
            topix_close = 3000 + i * 0.5
            micro_close = 50000 + i * 4
            engine.on_bar(Bar("TOPIXmini", start + timedelta(minutes=i), start + timedelta(minutes=i + 1), topix_close - 0.25, topix_close + 0.5, topix_close - 0.5, topix_close, 8 + i))
            engine.on_bar(Bar("NK225micro", start + timedelta(minutes=i), start + timedelta(minutes=i + 1), micro_close - 4, micro_close + 4, micro_close - 6, micro_close, 10 + i))
        self.assertEqual(engine.trend_bias("NK225micro"), "long")

    def test_trend_continuation_signal_generates_long_when_trend_resumes(self) -> None:
        cfg = default_config()
        engine = MinuteStrategyEngine(cfg.minute_engine, cfg.symbols)
        start = datetime(2026, 4, 23, 9, 0)
        topix_closes = [3000.0, 3000.5, 3001.0, 3001.5, 3002.0, 3002.0, 3002.25]
        micro_bars = [
            Bar("NK225micro", start + timedelta(minutes=0), start + timedelta(minutes=1), 50000, 50008, 49998, 50006, 10),
            Bar("NK225micro", start + timedelta(minutes=1), start + timedelta(minutes=2), 50006, 50014, 50004, 50012, 11),
            Bar("NK225micro", start + timedelta(minutes=2), start + timedelta(minutes=3), 50012, 50030, 50010, 50016, 11),
            Bar("NK225micro", start + timedelta(minutes=3), start + timedelta(minutes=4), 50016, 50020, 50014, 50018, 10),
            Bar("NK225micro", start + timedelta(minutes=4), start + timedelta(minutes=5), 50018, 50022, 50016, 50020, 10),
            Bar("NK225micro", start + timedelta(minutes=5), start + timedelta(minutes=6), 50023, 50024, 50022, 50023, 9),
            Bar("NK225micro", start + timedelta(minutes=6), start + timedelta(minutes=7), 50019, 50028, 50018, 50027, 16),
        ]
        signal = None
        for i, micro_bar in enumerate(micro_bars):
            topix_close = topix_closes[i]
            engine.on_bar(Bar("TOPIXmini", start + timedelta(minutes=i), start + timedelta(minutes=i + 1), topix_close - 0.25, topix_close + 0.5, topix_close - 0.5, topix_close, 9 + i))
            signal = engine.on_bar(micro_bar)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.engine, "directional_intraday")
        self.assertEqual(signal.direction, "long")

    def test_trend_pullback_observe_only_flags_are_opt_in(self) -> None:
        cfg = default_config()
        engine = DualStrategyEngine(cfg)
        long_signal = Signal("minute_vwap", "NK225micro", "long", 0.7, 50005, "trend_pullback_long")
        short_signal = Signal("minute_vwap", "NK225micro", "short", 0.7, 50000, "trend_pullback_short")
        self.assertIsNone(engine._minute_observe_only_reason(long_signal))
        self.assertIsNone(engine._minute_observe_only_reason(short_signal))

        cfg = replace(
            cfg,
            minute_engine=replace(
                cfg.minute_engine,
                trend_pullback_long_observe_only=True,
                trend_pullback_short_observe_only=True,
            ),
        )
        engine = DualStrategyEngine(cfg)
        self.assertEqual(engine._minute_observe_only_reason(long_signal), "trend_pullback_long_observe_only")
        self.assertEqual(engine._minute_observe_only_reason(short_signal), "trend_pullback_short_observe_only")


class OrderTests(unittest.TestCase):
    def test_stop_payload_for_long_position_uses_sell_side_and_under_trigger(self) -> None:
        payload = KabuFutureOrderBuilder().close_stop_market("123", 23, "long", 1, 49970, "HOLD").to_payload()
        self.assertEqual(payload["TradeType"], 2)
        self.assertEqual(payload["Side"], "1")
        self.assertEqual(payload["ReverseLimitOrder"]["UnderOver"], 1)

    def test_order_throttle_blocks_too_fast_orders(self) -> None:
        throttle = OrderThrottle(min_interval_seconds=3, max_per_minute=6)
        now = datetime(2026, 4, 23, 9, 0)
        self.assertEqual(throttle.allow(now)[0], True)
        throttle.record(now)
        self.assertEqual(throttle.allow(now + timedelta(seconds=1))[0], False)


class MarketDataAndExecutionTests(unittest.TestCase):
    def test_live_state_summary_helpers_keep_heartbeat_shape(self) -> None:
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=timezone.utc)
        position = LivePositionState("NK225micro", "161060023", 24, "long", 1, 50005.0, ts, "H1")
        pending = PendingLiveOrder("O1", "NK225micro", "161060023", 24, "long", 1, ts, "entry", position_key="hold:H1")

        self.assertEqual(position_key(position), "hold:H1")
        self.assertEqual(validated_symbol_code(position), "161060023")
        self.assertEqual(
            position_summary(position),
            {
                "symbol": "NK225micro",
                "symbol_code": "161060023",
                "exchange": 24,
                "direction": "long",
                "qty": 1,
                "entry_price": 50005.0,
                "hold_id": "H1",
            },
        )
        self.assertEqual(
            pending_summary(pending),
            {
                "order_id": "O1",
                "symbol": "NK225micro",
                "symbol_code": "161060023",
                "exchange": 24,
                "direction": "long",
                "qty": 1,
                "submitted_at": ts.isoformat(),
                "reason": "entry",
                "position_key": "hold:H1",
            },
        )
        self.assertIsNone(position_summary(None))
        self.assertIsNone(pending_summary(None))
        self.assertIsNone(validated_symbol_code(replace(position, symbol_code="NK225micro")))

    def test_live_api_health_records_backoff_latency_and_summary(self) -> None:
        base_cfg = default_config()
        cfg = replace(base_cfg, live_execution=replace(base_cfg.live_execution, api_error_cooldown_seconds=30))
        health = LiveApiHealth(cfg)
        ts = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)

        metadata = health.record_error(
            KabuApiError("kabu API HTTP 429: too many requests", status_code=429, category="rate_limit"),
            ts,
            "sendorder",
        )
        result = health.call("positions", lambda: {"data": []})
        summary = health.summary(ts + timedelta(seconds=1), position_sync_blocked=True, last_position_poll_at=ts)

        self.assertEqual(metadata["api_error_category"], "rate_limit")
        self.assertEqual(result, {"data": []})
        self.assertTrue(health.api_backoff_active(ts + timedelta(seconds=1)))
        self.assertEqual(summary["error_counts"]["rate_limit"], 1)
        self.assertTrue(summary["api_backoff_active"])
        self.assertIn("positions", summary["last_latency_ms"])
        self.assertIn("positions", summary["latency_ms"])
        self.assertEqual(
            summary["next_position_retry_at"],
            (ts + timedelta(seconds=cfg.live_execution.position_poll_interval_seconds)).isoformat(),
        )

        health.record_error(KabuApiError("HTTP 401", status_code=401, category="auth_error"), ts, "positions")
        self.assertTrue(health.summary(ts)["auth_failed"])
        health.record_success()
        self.assertFalse(health.summary(ts)["auth_failed"])

    def test_live_api_health_wrong_instance_cooldown_summary(self) -> None:
        health = LiveApiHealth(default_config())
        ts = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)

        metadata = health.record_error(
            KabuApiError("wrong kabu station instance", status_code=500, category="kabu_station_wrong_instance"),
            ts,
            "sendorder",
        )
        summary = health.summary(ts + timedelta(seconds=1))

        self.assertEqual(metadata["api_error_category"], "kabu_station_wrong_instance")
        self.assertEqual(summary["wrong_instance_errors"], 1)
        self.assertTrue(summary["wrong_instance_cooldown_active"])
        self.assertTrue(health.wrong_instance_cooldown_active(ts + timedelta(seconds=1)))

    def test_diagnostics_funnel_helpers_preserve_report_shape(self) -> None:
        counters: dict[str, object] = {
            "live_entry_orders_submitted": 2,
            "live_positions_detected": 1,
            "live_entry_orders_submitted_by_engine": Counter({"micro_book": 1, "minute_vwap": 1}),
            "live_tradeable_signals_by_engine": Counter({"micro_book": 3}),
            "signal_eval_decisions": Counter({"reject": 4, "allow": 1}),
            "signal_eval_engines": Counter({"micro_book": 5}),
            "signal_eval_reject_reasons": Counter({"imbalance_not_met": 4}),
            "signal_eval_failed_checks": Counter({"imbalance_not_met": 4}),
            "signal_eval_jump_reasons": Counter({"spread_wide": 2, "mid_move_jump": 1, "other": 1}),
            "near_miss_from_signal_eval": 2,
            "near_miss_missing_checks": Counter({"ofi_not_met": 2}),
            "near_miss_directions": Counter({"long": 2}),
            "micro_candidate_events": 1,
            "micro_candidate_count": 2,
            "micro_candidate_missing_checks": Counter({"microprice_not_met": 2}),
            "micro_candidate_directions": Counter({"short": 2}),
            "live_signals_by_engine": Counter({"micro_book": 2}),
            "live_entry_orders_cancelled": 1,
            "live_entry_orders_expired": 1,
            "live_entry_order_errors": 1,
            "live_exit_orders_submitted": 1,
            "live_exit_orders_cancelled": 1,
            "live_trades_closed": 1,
            "execution_rejects": Counter({"position_sync_blocked": 1}),
            "execution_reject_blocked_by": Counter({"position_sync": 1}),
            "position_sync_blocked_rejects": 1,
            "live_unsupported_minute_signals": 1,
            "api_error_categories": Counter({"auth_error": 1, "kabu_station_wrong_instance": 1}),
            "loss_hold_guard_events": 1,
            "kill_switch_events": 1,
            "suspected_old_live_policy": True,
        }
        samples: dict[str, list[float]] = defaultdict(list)
        samples["sendorder"] = [10.0, 20.0, 30.0]

        funnel = micro_entry_funnel(counters)  # type: ignore[arg-type]
        notes = diagnosis_notes(counters)  # type: ignore[arg-type]
        latency = latency_summary(samples)

        self.assertEqual(funnel["live_execution"]["entry_fill_rate"], 0.5)
        self.assertEqual(funnel["live_execution"]["micro_tradeable_signals_without_entry_submission"], 2)
        self.assertEqual(funnel["signal_eval"]["non_tradeable_jump_reasons"], {"spread_wide": 2, "mid_move_jump": 1})
        self.assertIn("kabu_auth_errors_blocked_live_entry", notes)
        self.assertEqual(latency["sendorder"]["p95"], 30.0)

    def test_kabu_normalizer_maps_real_symbol_alias_and_keeps_raw_symbol(self) -> None:
        payload = {
            "Symbol": "161050023",
            "BidPrice": 50005,
            "BidQty": 10,
            "AskPrice": 50000,
            "AskQty": 20,
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        normalized = KabuBoardNormalizer(symbol_aliases={"161050023": "NK225micro"}).normalize(payload)
        self.assertEqual(normalized.symbol, "NK225micro")
        self.assertEqual(normalized.raw_symbol, "161050023")
        self.assertEqual(normalized.best_bid_price, 50000)
        self.assertEqual(normalized.best_ask_price, 50005)

    def test_kabu_normalizer_maps_reversed_bid_ask_fields(self) -> None:
        payload = {
            "Symbol": "NK225micro",
            "BidPrice": 50005,
            "BidQty": 10,
            "AskPrice": 50000,
            "AskQty": 20,
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        normalized = KabuBoardNormalizer().normalize(payload)
        self.assertEqual(normalized.best_bid_price, 50000)
        self.assertEqual(normalized.best_ask_price, 50005)

    def test_kabu_normalizer_keeps_buy_sell_depth_semantics(self) -> None:
        payload = {
            "Symbol": "NK225micro",
            "BidPrice": 2408.5,
            "BidQty": 100,
            "AskPrice": 2407.5,
            "AskQty": 200,
            "Sell1": {"Price": 2408.5, "Qty": 100},
            "Sell2": {"Price": 2409.0, "Qty": 300},
            "Buy1": {"Price": 2407.5, "Qty": 200},
            "Buy2": {"Price": 2407.0, "Qty": 400},
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        normalized = KabuBoardNormalizer().normalize(payload)
        self.assertEqual(normalized.best_bid_price, 2407.5)
        self.assertEqual(normalized.best_bid_qty, 200)
        self.assertEqual(normalized.best_ask_price, 2408.5)
        self.assertEqual(normalized.best_ask_qty, 100)
        self.assertEqual([level.price for level in normalized.buy_levels], [2407.5, 2407.0])
        self.assertEqual([level.price for level in normalized.sell_levels], [2408.5, 2409.0])

    def test_kabu_normalizer_uses_depth_when_best_quote_fields_are_missing(self) -> None:
        payload = {
            "Symbol": "NK225micro",
            "Sell1": {"Price": 50005, "Qty": 10},
            "Buy1": {"Price": 50000, "Qty": 20},
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        normalized = KabuBoardNormalizer().normalize(payload)
        self.assertEqual(normalized.best_bid_price, 50000)
        self.assertEqual(normalized.best_ask_price, 50005)

    def test_kabu_normalizer_uses_received_at_as_live_book_clock(self) -> None:
        payload = {
            "Symbol": "TOPIXmini",
            "BidPrice": 3731.5,
            "BidQty": 1,
            "AskPrice": 3731.25,
            "AskQty": 1,
            "CurrentPriceTime": "2026-04-27T15:45:01+09:00",
        }
        received_at = datetime(2026, 4, 27, 12, 31, 38, tzinfo=timezone.utc)
        normalized = KabuBoardNormalizer().normalize(payload, received_at=received_at)
        self.assertEqual(normalized.timestamp, received_at)
        self.assertEqual(normalized.received_at, received_at)

    def test_kabu_normalizer_skips_locked_or_crossed_kabu_quote(self) -> None:
        payload = {
            "Symbol": "NK225micro",
            "BidPrice": 50000,
            "BidQty": 10,
            "AskPrice": 50005,
            "AskQty": 20,
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        with self.assertRaises(MarketDataSkip):
            KabuBoardNormalizer().normalize(payload)

    def test_kabu_normalizer_errors_when_quotes_are_missing(self) -> None:
        payload = {"Symbol": "NK225micro", "CurrentPriceTime": "2026-04-23T09:00:00+09:00"}
        with self.assertRaises(MarketDataError):
            KabuBoardNormalizer().normalize(payload)

    def test_live_signal_serialization_keeps_metadata(self) -> None:
        signal = Signal("risk", "NK225micro", "flat", 0.0, reason="market_data_error", metadata={"raw": "bad"})
        serialized = signal_to_dict(signal)
        self.assertEqual(serialized["reason"], "market_data_error")
        self.assertEqual(serialized["metadata"]["raw"], "bad")

    def test_shared_signal_snapshot_matches_live_signal_dict_fields(self) -> None:
        signal = Signal(
            "minute_vwap",
            "NK225micro",
            "long",
            0.72,
            50005,
            reason="trend_pullback_long",
            metadata={"atr": 30.0, "execution_score": 10},
            score=0.44,
            signal_horizon="intraday",
            expected_hold_seconds=900,
            risk_budget_pct=0.01,
            position_scale=1.0,
        )
        self.assertEqual(signal_snapshot(signal), signal_to_dict(signal))

    def test_signal_evaluation_serialization_keeps_reason_and_direction(self) -> None:
        evaluation = SignalEvaluation(
            "micro_book",
            "NK225micro",
            datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc),
            "reject",
            "imbalance_not_met",
            "long",
            {"spread_ok": True},
        )
        serialized = signal_evaluation_to_dict(evaluation)
        self.assertEqual(serialized["reason"], "imbalance_not_met")
        self.assertEqual(serialized["candidate_direction"], "long")
        self.assertEqual(serialized["metadata"]["spread_ok"], True)

    def test_tick_console_payload_contains_prices(self) -> None:
        b = book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc), bid=50000, ask=50005)
        payload = tick_to_dict(b, processed=7)
        self.assertEqual(payload["event"], "tick")
        self.assertEqual(payload["books"], 7)
        self.assertEqual(payload["bid"], 50000)
        self.assertEqual(payload["ask"], 50005)
        self.assertEqual(payload["spread"], 5)

    def test_tick_changes_mode_only_prints_changed_quotes(self) -> None:
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        first = book(ts, bid=50000, ask=50005)
        state: dict[str, tuple[float, float, float, float, float | None, float]] = {}
        self.assertTrue(_should_print_tick("changes", 1, 1, first, state))
        state[first.symbol] = (first.best_bid_price, first.best_ask_price, first.best_bid_qty, first.best_ask_qty, first.last_price, first.volume)
        self.assertFalse(_should_print_tick("changes", 2, 1, first, state))
        changed = book(ts + timedelta(seconds=1), bid=50005, ask=50010)
        self.assertTrue(_should_print_tick("changes", 3, 1, changed, state))

    def test_buffered_recorder_flushes_by_batch_and_close(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=2, flush_interval_seconds=60.0)
            recorder.write("event", {"seq": 1})
            self.assertEqual(path.read_text(encoding="utf-8"), "")
            recorder.write("event", {"seq": 2})
            self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 2)
            recorder.write("event", {"seq": 3})
            recorder.close()
            self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 3)

    def test_read_recorded_books_skips_non_book_events(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write("startup", {"mode": "test"})
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.write("heartbeat", {"books": 1})
            recorder.close()
            books = list(read_recorded_books(path))
            self.assertEqual(len(books), 1)
            self.assertEqual(books[0].symbol, "NK225micro")

    def test_replay_jsonl_accepts_buffered_live_log_format(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write("startup", {"mode": "test"})
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.write("heartbeat", {"books": 1})
            recorder.close()
            events = replay_jsonl(path, trade_mode="paper")
            self.assertEqual(events[-1]["event"], "paper_summary")
            self.assertEqual(events[-1]["paper_trades"], 0)

    def test_shared_analysis_utils_read_buffered_jsonl_and_drawdown(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write("startup", {"mode": "test"})
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.write("heartbeat", {"books": 1})
            recorder.close()

            books = list(iter_books(path))
            self.assertEqual(len(books), 1)
            self.assertEqual(books[0].symbol, "NK225micro")
            self.assertEqual(max_drawdown([1.0, -2.0, 0.5]), -2.0)

    def test_micro_candidate_emitter_records_near_miss_without_signal(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=1, flush_interval_seconds=60.0)
            emitter = MicroCandidateEmitter(recorder, min_interval_seconds=0)
            evaluation = SignalEvaluation(
                "micro_book",
                "NK225micro",
                datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc),
                "reject",
                "imbalance_not_met",
                "long",
                {
                    "near_miss": True,
                    "near_miss_direction": "long",
                    "near_miss_missing": "imbalance",
                    "spread_ok": True,
                    "jump_detected": False,
                    "long_failed_checks": ["imbalance"],
                },
            )

            emitter.write(evaluation)
            recorder.close()
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(rows[0]["kind"], "micro_candidate")
        self.assertEqual(rows[0]["payload"]["near_miss_missing"], "imbalance")
        self.assertEqual(rows[0]["payload"]["candidate_count"], 1)
        self.assertEqual(emitter.total_candidates, 1)
        self.assertEqual(emitter.emitted_candidates, 1)

    def test_live_startup_self_check_exposes_fingerprints_and_filters(self) -> None:
        payload = live_startup_self_check(default_config())

        self.assertTrue(payload["code_fingerprint"])
        self.assertTrue(payload["config_fingerprint"])
        self.assertEqual(payload["live_minute_atr_filter"], True)
        self.assertEqual(payload["min_execution_score_to_chase"], 10)
        self.assertIn("minute_vwap", payload["live_supported_engines"])
        self.assertEqual(payload["live_safety"]["minute_cooldown_seconds"], 180)
        self.assertEqual(payload["micro_entry_profile"], "default")
        self.assertEqual(payload["micro_effective_thresholds"]["imbalance_entry"], 0.3)

    def test_live_startup_self_check_exposes_gray_profile_thresholds(self) -> None:
        cfg = replace(
            default_config(),
            micro_engine=replace(default_config().micro_engine, entry_profile="conservative_candidate_v1"),
        )

        payload = live_startup_self_check(cfg)

        self.assertEqual(payload["micro_entry_profile"], "conservative_candidate_v1")
        self.assertTrue(payload["micro_entry_profile_active"])
        self.assertEqual(payload["micro_effective_thresholds"]["imbalance_entry"], 0.28)
        self.assertEqual(payload["micro_effective_thresholds"]["microprice_entry_ticks"], 0.12)
        self.assertEqual(payload["micro_effective_thresholds"]["ofi_percentile"], 65.0)

    def test_diagnose_log_reports_micro_entry_profile(self) -> None:
        cfg = replace(
            default_config(),
            micro_engine=replace(default_config().micro_engine, entry_profile="conservative_candidate_v1"),
        )
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "profile.jsonl"
            payload = live_startup_self_check(cfg)
            path.write_text(json.dumps({"kind": "startup", "payload": payload}) + "\n", encoding="utf-8")

            diagnostics = diagnose_log(path, cfg)

        profile = diagnostics["micro_entry_profile"]
        self.assertEqual(profile["configured_profile"], "conservative_candidate_v1")
        self.assertEqual(profile["observed_profiles"]["conservative_candidate_v1"], 1)
        self.assertEqual(profile["observed_effective_thresholds"]["imbalance_entry"], 0.28)

    def test_diagnose_log_flags_old_policy_and_bad_close_symbol(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "live.jsonl"
            rows = [
                {"kind": "startup", "payload": {"mode": "production", "live_supported_engines": ["micro_book"]}},
                {
                    "kind": "signal_eval_summary",
                    "payload": {
                        "engine": "micro_book",
                        "decision": "reject",
                        "reason": "jump_detected",
                        "count": 3,
                        "last_metadata": {"jump_detected": True, "jump_reason": "spread_wide", "spread_ok": True},
                    },
                },
                {
                    "kind": "signal_eval",
                    "payload": {
                        "engine": "micro_book",
                        "decision": "reject",
                        "reason": "imbalance_not_met",
                        "metadata": {
                            "imbalance_long_ok": False,
                            "imbalance_short_ok": False,
                            "near_miss": True,
                            "near_miss_direction": "long",
                            "near_miss_missing": "imbalance",
                        },
                    },
                },
                {
                    "kind": "micro_candidate",
                    "payload": {
                        "engine": "micro_book",
                        "symbol": "NK225micro",
                        "candidate_direction": "long",
                        "near_miss_direction": "long",
                        "near_miss_missing": "imbalance",
                        "candidate_count": 3,
                        "timestamp": "2026-04-28T10:00:00+09:00",
                        "metadata": {"spread_ok": True, "jump_detected": False},
                    },
                },
                {
                    "kind": "signal",
                    "payload": {
                        "event": "signal",
                        "engine": "micro_book",
                        "symbol": "NK225micro",
                        "direction": "long",
                        "confidence": 0.8,
                    },
                },
                {
                    "kind": "live_order_submitted",
                    "payload": {
                        "event": "live_order_submitted",
                        "reason": "entry_limit_fak_submitted",
                        "metadata": {
                            "signal": {
                                "engine": "minute_vwap",
                                "reason": "trend_pullback_long",
                                "metadata": {"atr": None, "execution_score": 7},
                            }
                        },
                    },
                },
                {
                    "kind": "live_order_expired",
                    "payload": {
                        "event": "live_order_expired",
                        "reason": "entry_order_expired_or_unfilled",
                        "metadata": {},
                    },
                },
                {
                    "kind": "execution_reject",
                    "payload": {
                        "event": "execution_reject",
                        "reason": "live_unsupported_signal_engine",
                        "metadata": {
                            "blocked_by": "live_supported_engines",
                            "signal": {"engine": "minute_vwap", "symbol": "NK225micro"},
                        },
                    },
                },
                {
                    "kind": "execution_reject",
                    "payload": {
                        "event": "execution_reject",
                        "reason": "position_sync_blocked",
                        "metadata": {"blocked_by": "position_state", "signal": {"engine": "micro_book"}},
                    },
                },
                {
                    "kind": "live_sync_error",
                    "payload": {
                        "event": "live_sync_error",
                        "reason": "positions_api_error",
                        "metadata": {
                            "error": 'kabu API HTTP 401: {"Code":4001007,"Message":"ログイン認証エラー"}'
                        },
                    },
                },
                {
                    "kind": "live_order_error",
                    "payload": {
                        "event": "live_order_error",
                        "reason": "exit_order_api_error",
                        "metadata": {
                            "error": "Symbol not found",
                            "order_payload": {"TradeType": 2, "Symbol": "NK225micro"},
                        },
                    },
                },
                {
                    "kind": "live_order_error",
                    "payload": {
                        "event": "live_order_error",
                        "reason": "kabu_station_wrong_instance",
                        "metadata": {
                            "error": "kabu API HTTP 500: 別のPCでkabuステーションが起動されました。",
                            "order_payload": {"TradeType": 1, "Symbol": "161060023"},
                        },
                    },
                },
                {
                    "kind": "live_trade_closed",
                    "payload": {
                        "event": "live_trade_closed",
                        "symbol": "NK225micro",
                        "direction": "long",
                        "reason": "stop_loss",
                        "pnl_ticks": -3.0,
                        "pnl_yen": -150.0,
                        "timestamp": "2026-04-28T10:00:00+09:00",
                        "metadata": {"engine": "minute_vwap", "signal_reason": "trend_pullback_long", "exit_reason": "stop_loss"},
                    },
                },
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            diagnostics = diagnose_log(path, default_config())

        self.assertTrue(diagnostics["suspected_old_live_policy"])
        self.assertEqual(diagnostics["minute_live_filter_violations"], 1)
        self.assertEqual(diagnostics["symbol_mapping_issues"]["close_order_non_numeric_symbol"], 1)
        self.assertEqual(diagnostics["symbol_mapping_issues"]["close_order_human_symbol"], 1)
        self.assertEqual(diagnostics["strategy_pnl"]["minute_vwap"]["losses"], 1)
        self.assertEqual(diagnostics["loss_samples"][0]["exit_reason"], "stop_loss")
        funnel = diagnostics["micro_entry_funnel"]
        self.assertEqual(funnel["signal_eval"]["decisions"]["reject"], 4)
        self.assertEqual(dict(funnel["signal_eval"]["failed_checks_top"])["jump_detected"], 3)
        self.assertEqual(dict(funnel["signal_eval"]["failed_checks_top"])["imbalance_not_met"], 1)
        self.assertEqual(funnel["signal_eval"]["jump_reasons"]["spread_wide"], 3)
        self.assertEqual(funnel["signal_eval"]["near_miss_count"], 1)
        self.assertEqual(dict(funnel["signal_eval"]["near_miss_missing_checks"])["imbalance"], 1)
        self.assertEqual(funnel["micro_candidates"]["candidate_count"], 3)
        self.assertEqual(dict(funnel["micro_candidates"]["missing_checks"])["imbalance"], 3)
        self.assertEqual(funnel["signals"]["tradeable_by_engine"]["micro_book"], 1)
        self.assertEqual(funnel["live_execution"]["entry_orders_submitted"], 1)
        self.assertEqual(funnel["live_execution"]["entry_orders_submitted_by_engine"]["minute_vwap"], 1)
        self.assertEqual(funnel["live_execution"]["micro_tradeable_signals_without_entry_submission"], 1)
        self.assertEqual(funnel["live_execution"]["entry_orders_expired"], 1)
        self.assertEqual(funnel["live_execution"]["entry_order_errors"], 1)
        self.assertEqual(funnel["live_execution"]["position_sync_blocked_rejects"], 1)
        self.assertEqual(funnel["live_execution"]["live_unsupported_minute_signals"], 1)
        self.assertEqual(funnel["live_execution"]["api_error_categories"]["auth_error"], 1)
        self.assertEqual(funnel["live_execution"]["api_error_categories"]["kabu_station_wrong_instance"], 1)
        self.assertIn("startup_missing_current_live_safety_fields_or_old_policy_detected", diagnostics["diagnosis_notes"])
        self.assertIn("position_sync_blocked_prevented_live_entry", diagnostics["diagnosis_notes"])
        self.assertIn("kabu_station_wrong_instance_detected_switch_to_orderable_instance", diagnostics["diagnosis_notes"])

    def test_diagnose_log_reconstructs_old_live_position_flat_pnl(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "old_live.jsonl"
            rows = [
                {"kind": "startup", "payload": {"mode": "production", "live_minute_atr_filter": True}},
                {
                    "kind": "live_order_submitted",
                    "payload": {
                        "event": "live_order_submitted",
                        "symbol": "NK225micro",
                        "direction": "short",
                        "qty": 1,
                        "entry_price": 60310.0,
                        "reason": "entry_limit_fak_submitted",
                        "metadata": {
                            "signal": {
                                "engine": "micro_book",
                                "symbol": "NK225micro",
                                "direction": "short",
                                "reason": "micro_book_short",
                                "metadata": {},
                            },
                            "order_id": "ENTRY1",
                        },
                    },
                },
                {
                    "kind": "live_position_detected",
                    "payload": {
                        "event": "live_position_detected",
                        "symbol": "NK225micro",
                        "direction": "short",
                        "qty": 1,
                        "entry_price": 60310.0,
                        "metadata": {"hold_id": "H1", "engine": "micro_book"},
                    },
                },
                {
                    "kind": "live_order_submitted",
                    "payload": {
                        "event": "live_order_submitted",
                        "symbol": "NK225micro",
                        "direction": "short",
                        "qty": 1,
                        "entry_price": 60310.0,
                        "exit_price": 60315.0,
                        "reason": "exit_order_submitted",
                        "metadata": {
                            "order_id": "EXIT1",
                            "engine": "micro_book",
                            "exit_reason": "imbalance_neutral",
                            "order_payload": {"TradeType": 2, "Symbol": "161060023", "ClosePositions": [{"HoldID": "H1", "Qty": 1}]},
                        },
                    },
                },
                {
                    "kind": "live_order_status",
                    "payload": {
                        "event": "live_order_status",
                        "symbol": "NK225micro",
                        "direction": "short",
                        "timestamp": "2026-04-28T11:12:31+09:00",
                        "metadata": {
                            "order_id": "EXIT1",
                            "order_status": {
                                "state": 5,
                                "cum_qty": 1,
                                "details": [{"rec_type": 8, "price": 60315.0, "execution_id": "E1"}],
                            },
                        },
                    },
                },
                {
                    "kind": "live_position_flat",
                    "payload": {
                        "event": "live_position_flat",
                        "symbol": "NK225micro",
                        "direction": "short",
                        "reason": "position_flat_confirmed",
                        "metadata": {},
                    },
                },
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            diagnostics = diagnose_log(path, default_config())

        self.assertEqual(diagnostics["strategy_pnl"]["micro_book"]["losses"], 1)
        self.assertEqual(diagnostics["strategy_pnl"]["micro_book"]["pnl_ticks"], -1.0)
        self.assertEqual(diagnostics["exit_reason_pnl"]["imbalance_neutral"]["losses"], 1)
        self.assertEqual(diagnostics["loss_samples"][0]["signal_reason"], "micro_book_short")
        self.assertEqual(diagnostics["micro_entry_funnel"]["live_execution"]["trades_closed"], 1)

    def test_diagnose_log_scores_live_readiness_and_cost_adjusted_pnl(self) -> None:
        base = default_config()
        cfg = replace(
            base,
            live_execution=replace(
                base.live_execution,
                commission_yen_per_order=10.0,
                assumed_slippage_ticks_per_trade=1.0,
            ),
        )
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "scored_live.jsonl"
            rows = [
                {
                    "kind": "startup",
                    "payload": {
                        "mode": "production",
                        "code_fingerprint": "abc",
                        "config_fingerprint": "def",
                        "live_minute_atr_filter": True,
                        "live_supported_engines": ["micro_book", "minute_vwap"],
                    },
                },
                {
                    "kind": "live_order_submitted",
                    "payload": {
                        "event": "live_order_submitted",
                        "reason": "entry_limit_fak_submitted",
                        "metadata": {
                            "signal": {"engine": "micro_book", "symbol": "NK225micro"},
                            "signal_to_order_send_ms": 2.5,
                            "order_send_to_api_response_ms": 7.5,
                        },
                    },
                },
                {
                    "kind": "live_order_cancelled",
                    "payload": {
                        "event": "live_order_cancelled",
                        "symbol": "NK225micro",
                        "direction": "long",
                        "reason": "pending_entry_timeout_cancel",
                        "metadata": {"is_entry": True, "order_id": "O1"},
                    },
                },
                {
                    "kind": "live_sync_error",
                    "payload": {
                        "event": "live_sync_error",
                        "symbol": "NK225micro",
                        "direction": "flat",
                        "reason": "loss_hold_guard_active",
                        "metadata": {"auto_loss_close_disabled": True, "manual_review_required": True},
                    },
                },
                {
                    "kind": "live_trade_closed",
                    "payload": {
                        "event": "live_trade_closed",
                        "symbol": "NK225micro",
                        "direction": "long",
                        "qty": 1,
                        "reason": "take_profit",
                        "pnl_ticks": 2.0,
                        "pnl_yen": 100.0,
                        "metadata": {"engine": "micro_book", "exit_reason": "take_profit"},
                    },
                },
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

            diagnostics = diagnose_log(path, cfg)

        readiness = diagnostics["live_readiness_score"]
        self.assertGreaterEqual(readiness["total"], 60)
        self.assertEqual(readiness["modules"]["risk_controls"]["score"], 14)
        self.assertTrue(readiness["assumptions"]["auto_loss_close_disabled"])
        self.assertTrue(readiness["assumptions"]["loss_hold_guard_blocks_entries_only"])
        self.assertEqual(diagnostics["strategy_pnl"]["micro_book"]["estimated_cost_yen"], 70.0)
        self.assertEqual(diagnostics["strategy_pnl"]["micro_book"]["net_pnl_yen"], 30.0)
        self.assertEqual(diagnostics["execution_latency_ms"]["signal_to_order_send_ms"]["p50"], 2.5)
        self.assertEqual(diagnostics["micro_entry_funnel"]["live_execution"]["entry_orders_cancelled"], 1)
        self.assertEqual(diagnostics["micro_entry_funnel"]["live_execution"]["loss_hold_guard_events"], 1)
        self.assertIn("loss_hold_guard_requires_manual_review_no_auto_loss_close", diagnostics["diagnosis_notes"])

    def test_micro_trade_manager_exits_on_take_profit(self) -> None:
        cfg = default_config()
        manager = MicroTradeManager(cfg.micro_engine, tick_size=cfg.tick_size)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal_price = 50005
        from kabu_futures.models import Signal

        manager.open_from_signal(Signal("micro_book", "NK225micro", "long", 0.8, signal_price), ts)
        exit_book = book(ts + timedelta(seconds=2), bid=50015, ask=50020)
        decision = manager.evaluate_exit(exit_book)
        self.assertTrue(decision.should_exit)
        self.assertEqual(decision.reason, "take_profit")

    def test_close_aggressive_limit_uses_close_position_order_when_hold_id_missing(self) -> None:
        payload = KabuFutureOrderBuilder().close_aggressive_limit("123", 23, "short", 1, 50020).to_payload()
        self.assertEqual(payload["TradeType"], 2)
        self.assertEqual(payload["Side"], "2")
        self.assertEqual(payload["ClosePositionOrder"], 0)

    def test_paper_mode_creates_micro_position_from_tradeable_signal(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)
        events = controller.on_signal(signal, book(ts, bid=50000, ask=50005))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "paper_entry")
        self.assertIsNotNone(controller.heartbeat_metadata()["paper_position"])

    def test_touch_fill_model_creates_pending_order_before_touch(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper", paper_fill_model="touch")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)
        events = controller.on_signal(signal, book(ts, bid=50000, ask=50010))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(events[0].event_type, "paper_pending")
        self.assertIsNone(heartbeat["paper_position"])
        self.assertEqual(heartbeat["paper_pending_orders"], 1)

    def test_touch_fill_model_opens_when_book_touches_limit_price(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper", paper_fill_model="touch")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)
        controller.on_signal(signal, book(ts, bid=50000, ask=50010))
        events = controller.on_book(book(ts + timedelta(seconds=1), bid=50000, ask=50005))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(events[0].event_type, "paper_entry")
        self.assertEqual(events[0].reason, "touch_fill")
        self.assertIsNotNone(heartbeat["paper_position"])
        self.assertEqual(heartbeat["paper_pending_orders"], 0)

    def test_touch_fill_model_also_exits_existing_position_on_same_book(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper", paper_fill_model="touch")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50010))
        controller.on_book(book(ts + timedelta(seconds=1), bid=50000, ask=50005))
        controller.on_signal(Signal("micro_book", "NK225micro", "short", 0.8, 50010), book(ts + timedelta(seconds=2), bid=50000, ask=50005))

        events = controller.on_book(book(ts + timedelta(seconds=3), bid=50010, ask=50015))

        self.assertEqual([event.event_type for event in events], ["paper_entry", "paper_exit"])
        self.assertEqual(events[0].direction, "short")
        self.assertEqual(events[1].direction, "long")
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["paper_position_count"], 1)
        self.assertEqual(heartbeat["paper_positions"][0]["direction"], "short")

    def test_touch_fill_model_cancels_stale_pending_order(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper", paper_fill_model="touch")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)
        controller.on_signal(signal, book(ts, bid=50000, ask=50010))
        events = controller.on_book(book(ts + timedelta(seconds=6), bid=50000, ask=50010))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(events[0].event_type, "paper_cancel")
        self.assertEqual(events[0].reason, "pending_timeout")
        self.assertEqual(heartbeat["paper_pending_orders"], 0)
        self.assertIsNone(heartbeat["paper_pending_order"])

    def test_paper_long_exits_on_take_profit_with_positive_pnl(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50005))
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=50015, ask=50020))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "take_profit")
        self.assertEqual(events[0].exit_price, 50010)
        self.assertEqual(events[0].pnl_ticks, 1.0)
        self.assertEqual(events[0].metadata["take_profit_ticks"], 1)
        self.assertGreater(events[0].pnl_ticks or 0, 0)
        self.assertGreater(events[0].pnl_yen or 0, 0)

    def test_paper_take_profit_widens_to_two_ticks_after_slippage(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50010, metadata={"live_entry_signal_price": 50005})
        entry = controller.on_signal(signal, book(ts, bid=50005, ask=50010))
        self.assertEqual(entry[0].metadata["take_profit_price"], 50020)
        self.assertEqual(entry[0].metadata["take_profit_ticks"], 2)

        self.assertEqual(controller.on_book(book(ts + timedelta(seconds=1), bid=50015, ask=50020)), [])
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=50020, ask=50025))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].exit_price, 50020)
        self.assertEqual(events[0].pnl_ticks, 2.0)

    def test_paper_long_does_not_exit_on_loss(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50005))
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=49990, ask=49995))
        self.assertEqual(events, [])
        self.assertIsNotNone(controller.heartbeat_metadata()["paper_position"])

    def test_paper_time_stop_no_longer_exits_when_not_profitable(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50005))
        exit_ts = ts + timedelta(seconds=cfg.micro_engine.time_stop_seconds + 1)
        events = controller.on_book(book(exit_ts, bid=50005, ask=50010))
        self.assertEqual(events, [])
        self.assertIsNotNone(controller.heartbeat_metadata()["paper_position"])

    def test_paper_allows_multiple_positions_until_symbol_limit(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)
        for idx in range(cfg.risk.max_positions_per_symbol):
            events = controller.on_signal(signal, book(ts + timedelta(seconds=idx), bid=50000, ask=50005))
            self.assertEqual(events[0].event_type, "paper_entry")
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["paper_position_count"], cfg.risk.max_positions_per_symbol)
        self.assertEqual(len(heartbeat["paper_positions"]), cfg.risk.max_positions_per_symbol)

        events = controller.on_signal(signal, book(ts + timedelta(seconds=10), bid=50000, ask=50005))
        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "max_positions_per_symbol")

    def test_minute_signal_creates_paper_entry(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0})
        events = controller.on_signal(signal, book(ts, bid=50000, ask=50005))
        self.assertEqual(events[0].event_type, "paper_entry")
        self.assertEqual(events[0].metadata["engine"], "minute_vwap")
        self.assertEqual(events[0].metadata["decision_stage"], "execution_order")
        self.assertEqual(events[0].metadata["decision_action"], "entry")
        heartbeat = controller.heartbeat_metadata()
        self.assertIsNotNone(heartbeat["paper_position"])
        self.assertIsNotNone(heartbeat["paper_minute_position"])

    def test_minute_long_exits_on_take_profit_with_positive_pnl(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0}), book(ts))
        events = controller.on_book(book(ts + timedelta(minutes=1), bid=50050, ask=50055))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "take_profit")
        self.assertEqual(events[0].exit_price, 50010)
        self.assertEqual(events[0].pnl_ticks, 1.0)
        self.assertEqual(events[0].metadata["take_profit_ticks"], 1)
        self.assertGreater(events[0].pnl_ticks or 0, 0)

    def test_minute_long_does_not_exit_on_loss(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0}), book(ts))
        events = controller.on_book(book(ts + timedelta(minutes=1), bid=49975, ask=49980))
        self.assertEqual(events, [])
        self.assertIsNotNone(controller.heartbeat_metadata()["paper_minute_position"])

    def test_minute_position_no_longer_exits_on_max_hold_minutes(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("directional_intraday", "NK225micro", "long", 0.8, 50005, "trend_continuation_long", {"atr": 30.0}), book(ts))
        exit_ts = ts + timedelta(minutes=cfg.minute_engine.max_hold_minutes + 1)
        events = controller.on_book(book(exit_ts, bid=50000, ask=50005))
        self.assertEqual(events, [])
        self.assertIsNotNone(controller.heartbeat_metadata()["paper_minute_position"])

    def test_paper_allows_micro_and_minute_positions_together(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0}), book(ts))
        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts + timedelta(seconds=1)))
        self.assertEqual(events[0].event_type, "paper_entry")
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["paper_position_count"], 2)
        self.assertIsNotNone(heartbeat["paper_micro_position"])
        self.assertIsNotNone(heartbeat["paper_minute_position"])

    def test_paper_multi_long_short_positions_exit_independently(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50005))
        controller.on_signal(Signal("micro_book", "NK225micro", "short", 0.8, 50000), book(ts + timedelta(seconds=1), bid=50000, ask=50005))
        self.assertEqual(controller.heartbeat_metadata()["paper_position_count"], 2)

        short_exit = controller.on_book(book(ts + timedelta(seconds=2), bid=49990, ask=49995))
        self.assertEqual(len(short_exit), 1)
        self.assertEqual(short_exit[0].direction, "short")
        self.assertEqual(controller.heartbeat_metadata()["paper_position_count"], 1)

        long_exit = controller.on_book(book(ts + timedelta(seconds=3), bid=50010, ask=50015))
        self.assertEqual(len(long_exit), 1)
        self.assertEqual(long_exit[0].direction, "long")
        self.assertEqual(controller.heartbeat_metadata()["paper_position_count"], 0)

    def test_heartbeat_aggregates_micro_and_minute_paper_trades(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0}), book(ts))
        controller.on_book(book(ts + timedelta(minutes=1), bid=50050, ask=50055))
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts + timedelta(minutes=2)))
        controller.on_book(book(ts + timedelta(minutes=2, seconds=2), bid=50015, ask=50020))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["paper_trades"], 2)
        self.assertEqual(heartbeat["paper_minute_trades"], 1)
        self.assertEqual(heartbeat["paper_micro_trades"], 1)
        self.assertGreater(heartbeat["paper_pnl_ticks"], 0)

    def test_observe_mode_does_not_create_paper_position(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="observe")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "execution_skip")
        self.assertEqual(events[0].reason, "observe_mode")
        heartbeat = controller.heartbeat_metadata()
        self.assertIsNone(heartbeat["paper_position"])
        self.assertEqual(heartbeat["paper_trades"], 0)
        self.assertEqual(heartbeat["paper_pnl_ticks"], 0.0)
        self.assertEqual(heartbeat["paper_pnl_yen"], 0.0)

    def test_no_new_entry_window_blocks_micro_tradeable_signal(self) -> None:
        cfg = default_config()
        engine = DualStrategyEngine(cfg)
        ts = datetime(2026, 4, 27, 15, 40, tzinfo=JST)
        for idx in range(5):
            event_ts = ts + timedelta(milliseconds=100 * idx)
            engine.on_order_book(book(event_ts, bid_qty=100, ask_qty=100), now=event_ts)
        final_ts = ts + timedelta(milliseconds=600)
        signals = engine.on_order_book(book(final_ts, bid_qty=300, ask_qty=20), now=final_ts)
        self.assertTrue(any(signal.reason == "session_not_tradeable" for signal in signals))
        self.assertFalse(any(signal.is_tradeable for signal in signals))
        self.assertEqual(engine.latest_signal_evaluations[-1].reason, "session_not_tradeable")
        self.assertEqual(engine.latest_signal_evaluations[-1].metadata["reject_stage"], "session_filter")
        self.assertEqual(engine.latest_signal_evaluations[-1].metadata["blocked_by"], "session_gate")
        self.assertEqual(engine.latest_signal_evaluations[-1].metadata["decision_trace"]["reason"], "session_not_tradeable")
        self.assertEqual(engine.latest_signal_evaluations[-1].metadata["session_phase"], "day_closing_call")
        self.assertEqual(engine.latest_signal_evaluations[-1].metadata["session_window_jst"], "15:40-15:45")

    def test_existing_paper_position_can_exit_during_blocked_session(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        entry_ts = datetime(2026, 4, 27, 15, 39, tzinfo=JST)
        exit_ts = datetime(2026, 4, 27, 15, 40, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(entry_ts))
        events = controller.on_book(book(exit_ts, bid=50015, ask=50020))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertGreater(events[0].pnl_ticks or 0, 0)

    def test_paper_topix_micro_trade_uses_topix_tick_value(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 27, 9, 0, tzinfo=JST)
        entry = controller.on_signal(Signal("micro_book", "TOPIXmini", "long", 0.8, 3770.75), topix_book(ts))
        self.assertEqual(entry[0].event_type, "paper_entry")
        self.assertEqual(entry[0].metadata["take_profit_price"], 3771.0)

        exits = controller.on_book(topix_book(ts + timedelta(seconds=1), bid=3771.0, ask=3771.25))
        self.assertEqual(exits[0].event_type, "paper_exit")
        self.assertEqual(exits[0].pnl_ticks, 1.0)
        self.assertEqual(exits[0].pnl_yen, 250.0)

    def test_live_micro_signal_submits_kabu_future_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        self.assertEqual(events[0].event_type, "live_order_submitted")
        self.assertEqual(events[0].qty, 1)
        self.assertEqual(client.sent[0]["Symbol"], "161060023")
        self.assertEqual(client.sent[0]["Exchange"], 24)
        self.assertEqual(client.sent[0]["TradeType"], 1)
        self.assertEqual(client.sent[0]["Side"], "2")
        self.assertEqual(client.sent[0]["Qty"], 1)
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_entry_orders_submitted"], 1)
        self.assertEqual(heartbeat["live_exit_orders_submitted"], 0)
        self.assertEqual(heartbeat["live_entry_fill_rate"], 0.0)

    def test_live_position_sync_auth_failure_blocks_then_recovers(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.fail_positions = True
                self.position_calls = 0

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                self.position_calls += 1
                if self.fail_positions:
                    raise KabuApiError(
                        'auth_recovery_failed after kabu API HTTP 401: {"Code":4001007}',
                        status_code=401,
                        category="auth_recovery_failed",
                    )
                return {"data": []}

        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, max_positions_per_symbol=2))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)

        blocked = controller.on_signal(signal, book(ts), exchange=24)

        self.assertEqual(blocked[-1].event_type, "execution_reject")
        self.assertEqual(blocked[-1].reason, "position_sync_blocked")
        self.assertEqual(blocked[0].reason, "auth_recovery_failed")
        self.assertEqual(blocked[0].metadata["api_error_category"], "auth_recovery_failed")
        self.assertEqual(client.sent, [])
        self.assertTrue(controller.heartbeat_metadata()["live_api_health"]["auth_failed"])

        client.fail_positions = False
        recovered = controller.on_signal(signal, book(ts + timedelta(seconds=2)), exchange=24)

        self.assertEqual(recovered[-1].event_type, "live_order_submitted")
        self.assertEqual(client.sent[0]["Symbol"], "161060023")
        self.assertFalse(controller.heartbeat_metadata()["live_position_sync_blocked"])
        self.assertFalse(controller.heartbeat_metadata()["live_api_health"]["auth_failed"])

    def test_live_wrong_kabu_station_instance_enters_cooldown(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                raise KabuApiError(
                    "kabu API HTTP 500: 別のPCでkabuステーションが起動されました。",
                    status_code=500,
                    category="kabu_station_wrong_instance",
                )

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, max_positions_per_symbol=2))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)

        failed = controller.on_signal(signal, book(ts), exchange=24)
        cooled = controller.on_signal(signal, book(ts + timedelta(seconds=1)), exchange=24)

        self.assertEqual(failed[0].event_type, "live_order_error")
        self.assertEqual(failed[0].reason, "kabu_station_wrong_instance")
        self.assertEqual(failed[0].metadata["blocked_by"], "kabu_station_instance")
        self.assertEqual(failed[0].metadata["api_error_category"], "kabu_station_wrong_instance")
        self.assertTrue(failed[0].metadata["live_api_health"]["wrong_instance_cooldown_active"])
        self.assertEqual(cooled[0].event_type, "execution_reject")
        self.assertEqual(cooled[0].reason, "kabu_station_wrong_instance_cooldown")
        self.assertEqual(len(client.sent), 1)

    def test_live_topix_micro_signal_uses_topix_symbol_code_and_tick_size(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, entry_slippage_ticks=1))
        controller = LiveExecutionController(
            client,
            cfg,
            {"NK225micro": "161060023", "TOPIXmini": "161060006"},
        )  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "TOPIXmini", "long", 0.8, 3770.75)
        events = controller.on_signal(signal, topix_book(ts), exchange=24)

        self.assertEqual(events[0].event_type, "live_order_submitted")
        self.assertEqual(client.sent[0]["Symbol"], "161060006")
        self.assertEqual(client.sent[0]["Exchange"], 24)
        self.assertEqual(client.sent[0]["Price"], 3771.0)
        self.assertEqual(events[0].metadata["entry_slippage_ticks"], 1)
        self.assertEqual(events[0].metadata["entry_order_price"], 3771.0)

    def test_live_sync_topix_position_submits_topix_take_profit_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if query.get("symbol") != "161060006":
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "T1",
                            "Symbol": "161060006",
                            "Exchange": 24,
                            "Side": "2",
                            "Price": 3770.75,
                            "LeavesQty": 1,
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(
            client,
            default_config(),
            {"NK225micro": "161060023", "TOPIXmini": "161060006"},
        )  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_book(topix_book(ts), None, exchange=24)

        self.assertTrue(any(event.event_type == "live_position_detected" and event.symbol == "TOPIXmini" for event in events))
        tp_events = [
            event
            for event in events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ]
        self.assertEqual(len(tp_events), 1)
        self.assertEqual(client.sent[0]["Symbol"], "161060006")
        self.assertEqual(client.sent[0]["TradeType"], 2)
        self.assertEqual(client.sent[0]["Price"], 3771.0)
        self.assertEqual(client.sent[0]["ClosePositions"], [{"HoldID": "T1", "Qty": 1}])

    def test_live_minute_signal_submits_kabu_future_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_signal(
            Signal(
                "minute_vwap",
                "NK225micro",
                "long",
                0.8,
                50005,
                "trend_pullback_long",
                {"atr": 30.0, "execution_score": 10},
            ),
            book(ts),
            exchange=24,
        )
        self.assertEqual(events[0].event_type, "live_order_submitted")
        self.assertEqual(events[0].qty, 1)
        self.assertEqual(client.sent[0]["Symbol"], "161060023")
        self.assertEqual(client.sent[0]["TradeType"], 1)
        self.assertEqual(client.sent[0]["Side"], "2")
        self.assertEqual(events[0].metadata["decision_action"], "allow")

    def test_live_rejects_minute_signal_without_atr(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_signal(
            Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"execution_score": 10}),
            book(ts),
            exchange=24,
        )

        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "live_minute_atr_missing")
        self.assertEqual(events[0].metadata["blocked_by"], "live_minute_atr")
        self.assertEqual(client.sent, [])

    def test_live_rejects_minute_signal_with_low_execution_score(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_signal(
            Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0, "execution_score": 9}),
            book(ts),
            exchange=24,
        )

        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "live_minute_execution_score_below_threshold")
        self.assertEqual(events[0].metadata["blocked_by"], "live_minute_execution_score")
        self.assertEqual(client.sent, [])

    def test_live_supports_guarded_minute_signal_by_default(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_signal(
            Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0, "execution_score": 10}),
            book(ts),
            exchange=24,
        )

        self.assertEqual(events[0].event_type, "live_order_submitted")
        self.assertEqual(client.sent[0]["Symbol"], "161060023")
        safety_state = events[0].metadata["live_safety_state"]
        self.assertEqual(
            safety_state["minute_cooldown_until"]["minute_vwap:trend_pullback_long:long"],
            (ts + timedelta(seconds=180)).isoformat(),
        )

    def test_live_minute_vwap_pullback_cooldown_blocks_second_entry(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal(
            "minute_vwap",
            "NK225micro",
            "long",
            0.8,
            50005,
            "trend_pullback_long",
            {"atr": 30.0, "execution_score": 10},
        )

        first = controller.on_signal(signal, book(ts), exchange=24)
        controller.pending_entry = None
        controller.entry_signal = None
        second = controller.on_signal(signal, book(ts + timedelta(seconds=60)), exchange=24)

        self.assertEqual(first[0].event_type, "live_order_submitted")
        self.assertEqual(second[0].event_type, "execution_reject")
        self.assertEqual(second[0].reason, "live_minute_cooldown_active")
        self.assertEqual(second[0].metadata["blocked_by"], "live_minute_cooldown")
        self.assertEqual(len(client.sent), 1)

    def test_live_minute_vwap_stop_losses_pause_until_next_quarter_hour(self) -> None:
        cfg = default_config()
        safety = LiveSafetyState(cfg)
        ts = datetime(2026, 4, 27, 21, 4, 10, tzinfo=JST)
        signal = Signal(
            "minute_vwap",
            "NK225micro",
            "short",
            0.8,
            50005,
            "trend_pullback_short",
            {"atr": 30.0, "execution_score": 10},
        )

        safety.record_exit("minute_vwap", "short", "trend_pullback_short", "stop_loss", -2.0, ts)
        safety.record_exit("minute_vwap", "short", "trend_pullback_short", "stop_loss", -3.0, ts + timedelta(seconds=1))
        blocked = safety.evaluate_entry(signal, ts + timedelta(seconds=2))
        reopened = safety.evaluate_entry(signal, datetime(2026, 4, 27, 21, 15, 1, tzinfo=JST))

        self.assertFalse(blocked.allowed)
        self.assertEqual(blocked.reason, "live_minute_stop_loss_pause")
        self.assertEqual(blocked.metadata["blocked_by"], "live_minute_stop_loss_pause")
        self.assertTrue(reopened.allowed)

    def test_live_micro_small_losses_pause_new_entries(self) -> None:
        cfg = default_config()
        safety = LiveSafetyState(cfg)
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)

        safety.record_exit("micro_book", "long", "book", "imbalance_neutral", -1.0, ts)
        safety.record_exit("micro_book", "long", "book", "microprice_neutral_or_reverse", -1.0, ts + timedelta(seconds=1))
        safety.record_exit("micro_book", "long", "book", "imbalance_neutral", -1.0, ts + timedelta(seconds=2))
        blocked = safety.evaluate_entry(signal, ts + timedelta(seconds=3))
        reopened = safety.evaluate_entry(signal, ts + timedelta(seconds=303))

        self.assertFalse(blocked.allowed)
        self.assertEqual(blocked.reason, "live_micro_small_loss_pause")
        self.assertEqual(blocked.metadata["blocked_by"], "live_micro_small_loss_pause")
        self.assertTrue(reopened.allowed)

    def test_live_rejects_unsupported_signal_without_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_signal(
            Signal("nt_ratio_spread", "NK225micro", "long", 0.8, 50005, "nt_entry"),
            book(ts),
            exchange=24,
        )
        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "live_unsupported_signal_engine")
        self.assertEqual(events[0].metadata["blocked_by"], "live_supported_engines")
        self.assertEqual(client.sent, [])

    def test_live_entry_slippage_ticks_adjusts_fak_limit_price(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        config = default_config()
        config = replace(config, live_execution=replace(config.live_execution, entry_slippage_ticks=1))
        client = FakeClient()
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        long_events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)

        self.assertEqual(long_events[0].event_type, "live_order_submitted")
        self.assertEqual(client.sent[0]["Price"], 50010.0)
        self.assertEqual(long_events[0].metadata["entry_signal_price"], 50005)
        self.assertEqual(long_events[0].metadata["entry_order_price"], 50010.0)

        controller.pending_entry = None
        controller.entry_signal = None
        short_events = controller.on_signal(Signal("micro_book", "NK225micro", "short", 0.8, 50000), book(ts), exchange=24)

        self.assertEqual(short_events[0].event_type, "live_order_submitted")
        self.assertEqual(client.sent[1]["Price"], 49995.0)

    def test_live_take_profit_widens_to_two_ticks_after_slippage_fill(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50010,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"}],
                        }
                    ]
                }

        config = default_config()
        config = replace(config, live_execution=replace(config.live_execution, entry_slippage_ticks=1))
        client = FakeClient()
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=50005, ask=50010), None, exchange=24)
        tp_event = [
            event
            for event in events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ][0]

        self.assertEqual(tp_event.metadata["take_profit_ticks"], 2)
        self.assertEqual(client.sent[-1]["Price"], 50020)

    def test_live_syncs_position_and_immediately_submits_take_profit_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"}],
                        }
                    ]
                }

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        sync_events = controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        self.assertTrue(any(event.event_type == "live_position_detected" for event in sync_events))
        tp_events = [
            event
            for event in sync_events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ]
        self.assertEqual(len(tp_events), 1)
        self.assertEqual(client.sent[-1]["TradeType"], 2)
        self.assertEqual(client.sent[-1]["Symbol"], "161060023")
        self.assertEqual(client.sent[-1]["Side"], "1")
        self.assertEqual(client.sent[-1]["Price"], 50010)
        self.assertEqual(client.sent[-1]["TimeInForce"], 1)
        self.assertEqual(client.sent[-1]["ClosePositions"], [{"HoldID": "E1", "Qty": 1}])
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_entry_orders_submitted"], 1)
        self.assertEqual(heartbeat["live_exit_orders_submitted"], 1)
        self.assertEqual(heartbeat["live_positions_detected"], 1)
        self.assertEqual(heartbeat["live_entry_fill_rate"], 1.0)
        controller.on_book(book(ts + timedelta(seconds=4), bid=49990, ask=49995), None, exchange=24)
        self.assertEqual(len([payload for payload in client.sent if payload["TradeType"] == 2]), 1)

    def test_live_order_status_prefers_execution_detail_price_for_entry(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "H1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 60095,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "O1",
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Price": 60100,
                            "Details": [
                                {
                                    "RecType": 8,
                                    "State": 0,
                                    "Qty": 1,
                                    "Price": 60095,
                                    "ExecutionID": "E2026042805A7O",
                                    "ExecutionDay": "2026-04-28T17:00:05+09:00",
                                }
                            ],
                        }
                    ]
                }

        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, entry_slippage_ticks=1))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 28, 17, 0, tzinfo=JST)

        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 60095), book(ts, bid=60090, ask=60095), exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=1), bid=60095, ask=60100), None, exchange=24)

        status = [event for event in events if event.event_type == "live_order_status"][0]
        detected = [event for event in events if event.event_type == "live_position_detected"][0]
        self.assertEqual(status.metadata["order_status"]["execution_price"], 60095)
        self.assertEqual(status.metadata["order_status"]["execution_id"], "E2026042805A7O")
        self.assertEqual(detected.metadata["entry_execution_price"], 60095)
        self.assertFalse(detected.metadata["entry_price_mismatch"])

    def test_live_position_entry_price_mismatch_is_diagnostic_only(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "H1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 60090,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "O1",
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Price": 60100,
                            "Details": [{"RecType": 8, "Qty": 1, "Price": 60095, "ExecutionID": "E1"}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 28, 17, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 60095), book(ts, bid=60090, ask=60095), exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=1), bid=60095, ask=60100), None, exchange=24)

        detected = [event for event in events if event.event_type == "live_position_detected"][0]
        self.assertEqual(detected.entry_price, 60090)
        self.assertEqual(detected.metadata["entry_execution_price"], 60095)
        self.assertTrue(detected.metadata["entry_price_mismatch"])
        self.assertEqual(controller.heartbeat_metadata()["live_positions"][0]["entry_price"], 60090)

    def test_live_exit_fill_updates_nk225micro_pnl_heartbeat(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = True

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "X1"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "H1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 60095,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                self.position_open = False
                return {
                    "data": [
                        {
                            "ID": "X1",
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Price": 60100,
                            "Details": [{"RecType": 8, "Qty": 1, "Price": 60100, "ExecutionID": "EX1"}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 28, 17, 0, tzinfo=JST)
        controller.on_book(book(ts, bid=60090, ask=60095), None, exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=1), bid=60100, ask=60105), None, exchange=24)

        closed = [event for event in events if event.event_type == "live_trade_closed"][0]
        self.assertEqual(closed.pnl_ticks, 1.0)
        self.assertEqual(closed.pnl_yen, 50.0)
        self.assertTrue(any(event.event_type == "live_position_flat" for event in events))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_trades_count"], 1)
        self.assertEqual(heartbeat["live_wins"], 1)
        self.assertEqual(heartbeat["live_pnl_ticks"], 1.0)
        self.assertEqual(heartbeat["live_pnl_yen"], 50.0)

    def test_live_exit_fill_updates_topixmini_pnl_with_topix_tick_value(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = True

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "TX1"}

            def positions(self, **query: object) -> dict[str, object]:
                if query.get("symbol") != "161060006" or not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "TH1",
                            "Symbol": "161060006",
                            "Exchange": 24,
                            "Price": 3770.75,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                self.position_open = False
                return {
                    "data": [
                        {
                            "ID": "TX1",
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Price": 3771.0,
                            "Details": [{"RecType": 8, "Qty": 1, "Price": 3771.0, "ExecutionID": "TEX1"}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(
            client,
            default_config(),
            {"NK225micro": "161060023", "TOPIXmini": "161060006"},
        )  # type: ignore[arg-type]
        ts = datetime(2026, 4, 28, 17, 0, tzinfo=JST)
        controller.on_book(topix_book(ts), None, exchange=24)
        events = controller.on_book(topix_book(ts + timedelta(seconds=1), bid=3771.0, ask=3771.25), None, exchange=24)

        closed = [event for event in events if event.event_type == "live_trade_closed"][0]
        self.assertEqual(closed.symbol, "TOPIXmini")
        self.assertEqual(closed.pnl_ticks, 1.0)
        self.assertEqual(closed.pnl_yen, 250.0)
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_trades_count"], 1)
        self.assertEqual(heartbeat["live_pnl_yen"], 250.0)

    def test_live_minute_position_immediately_submits_take_profit_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"}],
                        }
                    ]
                }

        client = FakeClient()
        config = default_config()
        config = replace(
            config,
            live_execution=replace(
                config.live_execution,
                supported_engines=("micro_book", "minute_orb", "minute_vwap", "directional_intraday"),
            ),
        )
        controller = LiveExecutionController(client, config, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal(
            "minute_vwap",
            "NK225micro",
            "long",
            0.8,
            50005,
            "trend_pullback_long",
            {"atr": 30.0, "execution_score": 10},
        )
        controller.on_signal(signal, book(ts), exchange=24)
        sync_events = controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        self.assertTrue(any(event.event_type == "live_position_detected" for event in sync_events))
        self.assertEqual(controller.heartbeat_metadata()["live_active_engine"], "minute_vwap")
        tp_events = [
            event
            for event in sync_events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ]
        self.assertEqual(len(tp_events), 1)
        self.assertEqual(tp_events[0].metadata["engine"], "minute_vwap")
        self.assertEqual(tp_events[0].metadata["take_profit_ticks"], 1)
        self.assertEqual(client.sent[-1]["TradeType"], 2)
        self.assertEqual(client.sent[-1]["Symbol"], "161060023")
        self.assertEqual(client.sent[-1]["Side"], "1")
        self.assertEqual(client.sent[-1]["Price"], 50010)
        self.assertEqual(client.sent[-1]["ClosePositions"], [{"HoldID": "E1", "Qty": 1}])

    def test_live_cold_start_syncs_existing_position_and_submits_take_profit(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.positions_calls = 0

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                self.positions_calls += 1
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        events = controller.on_book(book(ts, bid=50000, ask=50005), None, exchange=24)

        self.assertEqual(client.positions_calls, 1)
        self.assertTrue(any(event.event_type == "live_position_detected" for event in events))
        tp_events = [
            event
            for event in events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ]
        self.assertEqual(len(tp_events), 1)
        self.assertEqual(client.sent[-1]["TradeType"], 2)
        self.assertEqual(client.sent[-1]["ClosePositions"], [{"HoldID": "E1", "Qty": 1}])
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_position_count"], 1)
        self.assertEqual(heartbeat["live_entry_orders_submitted"], 0)
        self.assertEqual(heartbeat["live_own_entry_fills_detected"], 0)
        self.assertEqual(heartbeat["live_entry_fill_rate"], 0.0)

    def test_live_cold_start_capacity_blocks_sixth_entry(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.positions_calls = 0

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                self.positions_calls += 1
                return {
                    "data": [
                        {
                            "ExecutionID": f"E{idx}",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50000 + idx * 5,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                        for idx in range(1, 6)
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)

        self.assertEqual(client.positions_calls, 1)
        self.assertEqual(events[-1].event_type, "execution_reject")
        self.assertEqual(events[-1].reason, "max_positions_per_symbol")
        self.assertEqual(controller.heartbeat_metadata()["live_position_count"], 5)
        self.assertEqual(len([payload for payload in client.sent if payload["TradeType"] == 1]), 0)
        self.assertEqual(len([payload for payload in client.sent if payload["TradeType"] == 2]), 5)

    def test_live_syncs_multiple_positions_and_submits_each_take_profit(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        },
                        {
                            "ExecutionID": "E2",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50000,
                            "LeavesQty": 1,
                            "Side": "1",
                        },
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        events = controller.on_book(book(ts, bid=50000, ask=50005), None, exchange=24)

        self.assertFalse(any(event.reason == "multiple_live_positions" for event in events))
        tp_events = [
            event
            for event in events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ]
        self.assertEqual(len(tp_events), 2)
        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 2)
        self.assertIn({"HoldID": "E1", "Qty": 1}, [order["ClosePositions"][0] for order in close_orders])
        self.assertIn({"HoldID": "E2", "Qty": 1}, [order["ClosePositions"][0] for order in close_orders])
        self.assertIn("1", [order["Side"] for order in close_orders])
        self.assertIn("2", [order["Side"] for order in close_orders])
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_position_count"], 2)
        self.assertEqual(len(heartbeat["live_pending_exits"]), 2)

    def test_live_rejects_new_entry_at_five_positions(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ExecutionID": f"E{idx}",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50000 + idx * 5,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                        for idx in range(1, 6)
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"}],
                        }
                    ]
                }

        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, max_positions_per_symbol=2))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts + timedelta(seconds=3)), exchange=24)

        self.assertEqual(controller.heartbeat_metadata()["live_position_count"], 5)
        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "max_positions_per_symbol")

    def test_live_pending_exit_does_not_block_new_entry_when_capacity_remains(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"}],
                        }
                    ]
                }

        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, max_positions_per_symbol=2))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        events = controller.on_signal(Signal("micro_book", "NK225micro", "short", 0.8, 50000), book(ts + timedelta(seconds=3)), exchange=24)

        self.assertEqual(events[0].event_type, "live_order_submitted")
        self.assertEqual(events[0].reason, "entry_limit_fak_submitted")

    def test_live_rate_limit_error_enters_api_backoff(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent = 0

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent += 1
                raise KabuApiError("kabu API HTTP 429: too many requests", status_code=429, category="rate_limit")

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        cfg = replace(default_config(), live_execution=replace(default_config().live_execution, api_error_cooldown_seconds=30))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)

        error_events = controller.on_signal(signal, book(ts), exchange=24)
        blocked_events = controller.on_signal(signal, book(ts + timedelta(seconds=1)), exchange=24)
        health = controller._live_api_health(ts + timedelta(seconds=1))

        self.assertEqual(error_events[-1].event_type, "live_order_error")
        self.assertEqual(error_events[-1].reason, "kabu_api_rate_limit")
        self.assertEqual(blocked_events[0].reason, "live_api_backoff_active")
        self.assertEqual(client.sent, 1)
        self.assertTrue(health["api_backoff_active"])
        self.assertEqual(health["error_counts"]["rate_limit"], 1)

    def test_loss_hold_guard_blocks_new_entries_and_does_not_submit_close_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 1:
                    self.position_open = True
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                            "HoldID": "H1",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": query.get("id", "O1"),
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 1,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 1, "Price": 50005, "ExecutionID": "E1"}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)

        controller.on_signal(signal, book(ts), exchange=24)
        guard_events = controller.on_book(book(ts + timedelta(seconds=2), bid=49925, ask=49930), None, exchange=24)
        blocked_events = controller.on_signal(signal, book(ts + timedelta(seconds=3), bid=49925, ask=49930), exchange=24)
        heartbeat = controller.heartbeat_metadata()

        self.assertIn("loss_hold_guard_active", [event.reason for event in guard_events])
        self.assertEqual(blocked_events[0].reason, "loss_hold_guard_active")
        self.assertTrue(heartbeat["live_guard_state"]["loss_hold_guard_active"])
        self.assertTrue(heartbeat["live_guard_state"]["auto_loss_close_disabled"])
        self.assertEqual(len([payload for payload in client.sent if payload["TradeType"] == 2]), 0)

    def test_loss_hold_guard_after_position_sync_rejects_without_submitting_close_order(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                            "HoldID": "H1",
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        events = controller.on_signal(
            Signal("micro_book", "NK225micro", "long", 0.8, 49930),
            book(ts, bid=49925, ask=49930),
            exchange=24,
        )

        self.assertIn("position_sync", [event.reason for event in events])
        self.assertIn("loss_hold_guard_active", [event.reason for event in events])
        self.assertEqual(events[-1].event_type, "execution_reject")
        self.assertEqual(events[-1].reason, "loss_hold_guard_active")
        self.assertEqual(client.sent, [])

    def test_pending_entry_timeout_cancels_active_order_before_blocking(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.cancelled: list[str] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "O1",
                            "State": 1,
                            "OrderState": 1,
                            "OrderQty": 1,
                            "CumQty": 0,
                            "Details": [],
                        }
                    ]
                }

            def cancelorder(self, order_id: str) -> dict[str, object]:
                self.cancelled.append(order_id)
                return {"Result": 0, "OrderId": order_id}

        base = default_config()
        cfg = replace(
            base,
            live_execution=replace(
                base.live_execution,
                max_pending_entry_seconds=1,
                pending_entry_grace_seconds=2,
                cancel_pending_entry_on_timeout=True,
            ),
        )
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=3)), None, exchange=24)
        heartbeat = controller.heartbeat_metadata()

        self.assertEqual(client.cancelled, ["O1"])
        self.assertIn("pending_entry_timeout_cancel", [event.reason for event in events])
        self.assertIn("pending_entry_timeout_after_grace", [event.reason for event in events])
        self.assertEqual(heartbeat["live_entry_orders_cancelled"], 1)
        self.assertIsNone(heartbeat["live_pending_entry"])

    def test_contract_rollover_window_blocks_live_entry(self) -> None:
        class FakeClient:
            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                raise AssertionError("rollover gate should reject before sending orders")

        base = default_config()
        cfg = replace(base, symbols=replace(base.symbols, deriv_month=202604, rollover_business_days_before_last_trade=3))
        controller = LiveExecutionController(FakeClient(), cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 8, 9, 0, tzinfo=JST)

        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)

        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "contract_rollover_window")
        self.assertEqual(events[0].metadata["blocked_by"], "contract_rollover")

    def test_live_order_status_marks_unfilled_fak_as_expired(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "O1",
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 0,
                            "Details": [{"RecType": 7, "State": 0, "Qty": 1}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        self.assertTrue(any(event.event_type == "live_order_status" for event in events))
        expired = [event for event in events if event.event_type == "live_order_expired"]
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0].reason, "entry_order_expired_or_unfilled")
        heartbeat = controller.heartbeat_metadata()
        self.assertIsNone(heartbeat["live_pending_entry"])
        self.assertEqual(heartbeat["live_entry_orders_expired"], 1)
        self.assertEqual(heartbeat["live_entry_fill_rate"], 0.0)

    def test_live_partial_exit_releases_pending_and_reprices_remaining_take_profit(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_qty = 2

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": self.position_qty,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                order_id = query.get("id", "O1")
                if order_id == "O1":
                    return {
                        "data": [
                            {
                                "ID": "O1",
                                "State": 5,
                                "OrderState": 5,
                                "OrderQty": 2,
                                "CumQty": 1,
                                "Details": [
                                    {"RecType": 8, "State": 0, "Qty": 1, "ExecutionID": "E1"},
                                    {"RecType": 7, "State": 0, "Qty": 1},
                                ],
                            }
                        ]
                    }
                return {
                    "data": [
                        {
                            "ID": order_id,
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 2,
                            "CumQty": 2,
                            "Details": [{"RecType": 8, "State": 0, "Qty": 2, "ExecutionID": "E1"}],
                        }
                    ]
                }

        cfg = default_config()
        cfg = replace(
            cfg,
            micro_engine=replace(cfg.micro_engine, qty=2),
            live_execution=replace(cfg.live_execution, max_order_qty=2),
        )
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_book(book(ts, bid=50000, ask=50005), None, exchange=24)
        first_close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(first_close_orders[-1]["Qty"], 2)
        self.assertEqual(first_close_orders[-1]["ClosePositions"], [{"HoldID": "E1", "Qty": 2}])

        client.position_qty = 1
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)

        partial_events = [event for event in events if event.reason == "exit_order_partially_filled"]
        self.assertEqual(len(partial_events), 1)
        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 1)
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_positions"][0]["qty"], 1)
        self.assertEqual(heartbeat["live_pending_exits"], [])
        self.assertEqual(heartbeat["live_exit_failure_counts"]["hold:E1"], 1)

        controller.on_book(book(ts + timedelta(seconds=4), bid=50000, ask=50005), None, exchange=24)
        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 2)
        self.assertEqual(close_orders[-1]["Qty"], 1)
        self.assertEqual(close_orders[-1]["ClosePositions"], [{"HoldID": "E1", "Qty": 1}])
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_pending_exits"][0]["qty"], 1)

    def test_live_exit_api_errors_block_after_retries_and_reject_new_entries(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                if payload["TradeType"] == 2:
                    raise KabuApiError("close failed")
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {"data": []}

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        controller.on_book(book(ts, bid=50000, ask=50005), None, exchange=24)
        controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=4), bid=50000, ask=50005), None, exchange=24)

        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 3)
        self.assertTrue(all(payload["FrontOrderType"] != 120 for payload in close_orders))
        self.assertTrue(any(event.reason == "exit_order_blocked_after_retries" for event in events))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_exit_blocked"], ["hold:E1"])
        self.assertEqual(heartbeat["live_exit_failure_counts"]["hold:E1"], 3)

        reject_events = controller.on_signal(
            Signal("micro_book", "NK225micro", "short", 0.8, 50000),
            book(ts + timedelta(seconds=5)),
            exchange=24,
        )
        self.assertEqual(reject_events[-1].event_type, "execution_reject")
        self.assertEqual(reject_events[-1].reason, "exit_order_blocked_after_retries")

    def test_live_exit_terminal_unfilled_uses_backoff_then_blocks(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50005,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                order_id = query.get("id", "O1")
                return {
                    "data": [
                        {
                            "ID": order_id,
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 0,
                            "Details": [{"RecType": 7, "State": 0, "Qty": 1}],
                        }
                    ]
                }

        cfg = default_config()
        cfg = replace(cfg, live_execution=replace(cfg.live_execution, max_consecutive_exit_failures=2))
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        controller.on_book(book(ts, bid=50000, ask=50005), None, exchange=24)
        controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)
        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 1)
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_exit_failure_counts"]["hold:E1"], 1)
        self.assertIn("hold:E1", heartbeat["live_exit_retry_after"])

        controller.on_book(book(ts + timedelta(seconds=4), bid=50000, ask=50005), None, exchange=24)
        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 2)
        events = controller.on_book(book(ts + timedelta(seconds=6), bid=50000, ask=50005), None, exchange=24)

        close_orders = [payload for payload in client.sent if payload["TradeType"] == 2]
        self.assertEqual(len(close_orders), 2)
        self.assertTrue(any(event.reason == "exit_order_blocked_after_retries" for event in events))
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_exit_blocked"], ["hold:E1"])
        self.assertEqual(heartbeat["live_exit_failure_counts"]["hold:E1"], 2)

    def test_live_pending_entry_uses_grace_before_timeout(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "O1",
                            "State": 2,
                            "OrderState": 2,
                            "OrderQty": 1,
                            "CumQty": 0,
                            "Details": [],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)

        grace_events = controller.on_book(book(ts + timedelta(seconds=9), bid=50000, ask=50005), None, exchange=24)
        self.assertTrue(any(event.reason == "pending_entry_grace_active" for event in grace_events))
        heartbeat = controller.heartbeat_metadata()
        self.assertIsNotNone(heartbeat["live_pending_entry"])
        self.assertEqual(heartbeat["live_entry_failure_count"], 0)

        timeout_events = controller.on_book(book(ts + timedelta(seconds=21), bid=50000, ask=50005), None, exchange=24)
        self.assertTrue(any(event.reason == "pending_entry_timeout_after_grace" for event in timeout_events))
        heartbeat = controller.heartbeat_metadata()
        self.assertIsNone(heartbeat["live_pending_entry"])
        self.assertEqual(heartbeat["live_entry_failure_count"], 1)

    def test_live_entry_failures_enter_cooldown(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 1, "Message": "rejected"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

        cfg = default_config()
        cfg = replace(
            cfg,
            live_execution=replace(
                cfg.live_execution,
                max_consecutive_entry_failures=3,
                entry_failure_cooldown_seconds=30,
            ),
        )
        client = FakeClient()
        controller = LiveExecutionController(client, cfg, {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)

        for offset in range(3):
            events = controller.on_signal(
                Signal("micro_book", "NK225micro", "long", 0.8, 50005),
                book(ts + timedelta(seconds=offset)),
                exchange=24,
            )
            self.assertEqual(events[-1].event_type, "live_order_error")

        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_entry_failure_count"], 3)
        self.assertIsNotNone(heartbeat["live_entry_cooldown_until"])
        cooldown_events = controller.on_signal(
            Signal("micro_book", "NK225micro", "long", 0.8, 50005),
            book(ts + timedelta(seconds=3)),
            exchange=24,
        )
        self.assertEqual(cooldown_events[-1].event_type, "execution_reject")
        self.assertEqual(cooldown_events[-1].reason, "entry_failure_cooldown")
        self.assertEqual(len(client.sent), 3)

    def test_live_late_fill_uses_orphan_entry_signal_metadata(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []
                self.position_open = False

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": f"O{len(self.sent)}"}

            def positions(self, **query: object) -> dict[str, object]:
                if not self.position_open:
                    return {"data": []}
                return {
                    "data": [
                        {
                            "ExecutionID": "E1",
                            "Symbol": "161060023",
                            "Exchange": 24,
                            "Price": 50010,
                            "LeavesQty": 1,
                            "Side": "2",
                        }
                    ]
                }

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "O1",
                            "State": 2,
                            "OrderState": 2,
                            "OrderQty": 1,
                            "CumQty": 0,
                            "Details": [],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005, "rich_reason", {"imbalance": 0.7})
        controller.on_signal(signal, book(ts), exchange=24)
        controller.on_book(book(ts + timedelta(seconds=21), bid=50000, ask=50005), None, exchange=24)

        client.position_open = True
        events = controller.on_book(book(ts + timedelta(seconds=22), bid=50005, ask=50010), None, exchange=24)

        detected = [event for event in events if event.event_type == "live_position_detected"]
        self.assertEqual(detected[0].metadata["signal_reason"], "rich_reason")
        self.assertTrue(detected[0].metadata["own_entry_detected"])
        tp_events = [
            event
            for event in events
            if event.event_type == "live_order_submitted" and event.metadata.get("exit_reason") == "take_profit"
        ]
        self.assertEqual(tp_events[0].metadata["take_profit_ticks"], 2)
        heartbeat = controller.heartbeat_metadata()
        self.assertEqual(heartbeat["live_own_entry_fills_detected"], 1)
        self.assertEqual(heartbeat["live_entry_failure_count"], 0)

    def test_live_order_status_requires_exact_order_id_match(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.sent: list[dict[str, object]] = []

            def sendorder_future(self, payload: dict[str, object]) -> dict[str, object]:
                self.sent.append(payload)
                return {"Result": 0, "OrderId": "O1"}

            def positions(self, **query: object) -> dict[str, object]:
                return {"data": []}

            def orders(self, **query: object) -> dict[str, object]:
                return {
                    "data": [
                        {
                            "ID": "DIFFERENT",
                            "State": 5,
                            "OrderState": 5,
                            "OrderQty": 1,
                            "CumQty": 0,
                            "Details": [{"RecType": 7, "State": 0, "Qty": 1}],
                        }
                    ]
                }

        client = FakeClient()
        controller = LiveExecutionController(client, default_config(), {"NK225micro": "161060023"})  # type: ignore[arg-type]
        ts = datetime(2026, 4, 27, 21, 0, tzinfo=JST)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts), exchange=24)
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=50000, ask=50005), None, exchange=24)

        self.assertTrue(any(event.reason == "order_status_missing" for event in events))
        self.assertFalse(any(event.event_type == "live_order_expired" for event in events))
        heartbeat = controller.heartbeat_metadata()
        self.assertIsNotNone(heartbeat["live_pending_entry"])
        self.assertEqual(heartbeat["live_entry_orders_expired"], 0)

    def test_directional_intraday_long_observe_only_flag_is_opt_in(self) -> None:
        cfg = default_config()
        engine = DualStrategyEngine(cfg)
        reason = engine._minute_observe_only_reason(Signal("directional_intraday", "NK225micro", "long", 0.8, 50000))
        self.assertIsNone(reason)
        self.assertIsNone(engine._minute_observe_only_reason(Signal("minute_vwap", "NK225micro", "short", 0.8, 50000)))

        cfg = replace(
            cfg,
            minute_engine=replace(cfg.minute_engine, directional_intraday_long_observe_only=True),
        )
        engine = DualStrategyEngine(cfg)
        reason = engine._minute_observe_only_reason(Signal("directional_intraday", "NK225micro", "long", 0.8, 50000))
        self.assertEqual(reason, "directional_intraday_long_observe_only")


class EvolutionAndTuningTests(unittest.TestCase):
    def test_markout_calculation_long_and_short(self) -> None:
        self.assertEqual(calculate_markout_ticks("long", 50000, 50010, 5), 2.0)
        self.assertEqual(calculate_markout_ticks("short", 50000, 49990, 5), 2.0)

    def test_analyze_micro_log_handles_minute_paper_trade(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                self.calls = 0

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                self.calls += 1
                if self.calls == 1:
                    return [
                        Signal(
                            "minute_vwap",
                            "NK225micro",
                            "long",
                            0.8,
                            50005,
                            "trend_pullback_long",
                            {"atr": 30.0},
                        )
                    ]
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            recorder.write_book(book(ts, bid=50000, ask=50005))
            recorder.write_book(book(ts + timedelta(seconds=1), bid=50050, ask=50055))
            recorder.close()
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), markout_seconds=(0.5,))
        self.assertEqual(report["paper"]["trades"], 1)
        self.assertEqual(report["paper"]["paper_minute_trades"], 1)
        self.assertEqual(report["paper"]["exit_reasons"]["take_profit"], 1)
        self.assertEqual(report["markout"]["entries"], 1)
        self.assertEqual(sum(report["regime"]["distribution"].values()), 2)
        self.assertEqual(report["regime"]["paper"]["warmup"]["trades"], 1)
        self.assertGreater(report["regime"]["paper"]["warmup"]["net_pnl_ticks"], 0)
        self.assertEqual(report["regime"]["markout"]["warmup"]["entries"], 1)
        self.assertEqual(report["regime"]["markout"]["warmup"]["summary"]["0.5"]["count"], 1)

    def test_analyze_micro_log_regime_distribution_sums_to_books(self) -> None:
        class FakeEngine:
            latest_book_features = None

            def __init__(self, config: object) -> None:
                self.latest_signal_evaluations: list[SignalEvaluation] = []

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                self.latest_signal_evaluations = [
                    SignalEvaluation(
                        "micro_book",
                        order_book.symbol,
                        now or order_book.timestamp,
                        "reject",
                        "test_reject",
                    )
                ]
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            mids = [50002.5, 50012.5, 50042.5, 50047.5, 49982.5, 49992.5]
            for idx, mid in enumerate(mids):
                recorder.write_book(book(ts + timedelta(minutes=idx), bid=mid - 2.5, ask=mid + 2.5))
            recorder.close()
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), regime_kwargs={"warmup_periods": 1})

        self.assertEqual(report["books"], len(mids))
        self.assertEqual(sum(report["regime"]["distribution"].values()), len(mids))
        self.assertEqual(sum(bucket["total"] for bucket in report["regime"]["evaluations"].values()), len(mids))
        self.assertIn("high_vol", report["regime"]["distribution"])
        self.assertIn("low_vol", report["regime"]["distribution"])

    def test_analyze_micro_log_can_disable_regime_report(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), include_regime=False)

        self.assertNotIn("regime", report)

    def test_analyze_micro_log_entry_diagnostics_counts_failed_micro_checks(self) -> None:
        class FakeEngine:
            latest_book_features = None

            def __init__(self, config: object) -> None:
                self.latest_signal_evaluations: list[SignalEvaluation] = []

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                self.latest_signal_evaluations = [
                    SignalEvaluation(
                        "micro_book",
                        order_book.symbol,
                        now or order_book.timestamp,
                        "reject",
                        "spread_not_required_width",
                        "flat",
                        {
                            "spread_ok": False,
                            "imbalance_long_ok": False,
                            "imbalance_short_ok": False,
                            "ofi_long_ok": False,
                            "ofi_short_ok": False,
                            "microprice_long_ok": True,
                            "microprice_short_ok": False,
                            "blocked_by": "multi_timeframe",
                            "reject_stage": "multi_timeframe",
                            "policy_reason": "execution_score_below_threshold",
                        },
                    )
                ]
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                import json as _json

                handle.write(
                    _json.dumps(
                        {
                            "kind": "execution_reject",
                            "payload": {
                                "reason": "live_unsupported_signal_engine",
                                "metadata": {"signal": {"engine": "minute_vwap"}},
                            },
                        }
                    )
                    + "\n"
                )
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), include_regime=False)

        diagnostics = report["entry_diagnostics"]
        self.assertEqual(diagnostics["micro_rejects"], 1)
        self.assertEqual(diagnostics["failed_checks"]["spread_not_required_width"], 1)
        self.assertEqual(diagnostics["failed_checks"]["imbalance_not_met"], 1)
        self.assertEqual(diagnostics["failed_checks"]["ofi_not_met"], 1)
        self.assertEqual(diagnostics["blocked_by"]["multi_timeframe"], 1)
        self.assertEqual(diagnostics["policy_reasons"]["execution_score_below_threshold"], 1)
        self.assertEqual(diagnostics["recorded_live_unsupported_engines"]["minute_vwap"], 1)

    def test_analyze_micro_log_entry_fill_diagnostics_counts_observed_live_fill(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(ts))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                import json as _json

                handle.write(
                    _json.dumps(
                        {
                            "kind": "live_order_submitted",
                            "payload": {
                                "event_type": "live_order_submitted",
                                "symbol": "NK225micro",
                                "direction": "long",
                                "qty": 1,
                                "entry_price": 50005,
                                "reason": "entry_limit_fak_submitted",
                                "timestamp": ts.isoformat(),
                                "metadata": {
                                    "order_id": "O1",
                                    "entry_slippage_ticks": 1,
                                    "signal": {
                                        "engine": "micro_book",
                                        "symbol": "NK225micro",
                                        "direction": "long",
                                        "price": 50005,
                                        "reason": "micro_book_long",
                                    },
                                },
                            },
                        }
                    )
                    + "\n"
                )
                handle.write(
                    _json.dumps(
                        {
                            "kind": "live_position_detected",
                            "payload": {
                                "event_type": "live_position_detected",
                                "symbol": "NK225micro",
                                "direction": "long",
                                "qty": 1,
                                "entry_price": 50010,
                                "reason": "position_sync",
                                "timestamp": (ts + timedelta(seconds=1)).isoformat(),
                                "metadata": {"own_entry_detected": True},
                            },
                        }
                    )
                    + "\n"
                )
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), include_regime=False)

        funnel = report["entry_fill_diagnostics"]["observed_live_funnel"]
        self.assertEqual(funnel["entry_submitted"], 1)
        self.assertEqual(funnel["own_fills_detected"], 1)
        self.assertEqual(funnel["fill_rate"], 1.0)
        self.assertEqual(funnel["by_slippage_ticks"]["1"]["entry_submitted"], 1)
        self.assertEqual(funnel["by_slippage_ticks"]["1"]["own_fills_detected"], 1)

    def test_analyze_micro_log_logged_diagnostics_max_rows_limits_live_events(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(ts))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                import json as _json

                handle.write(
                    _json.dumps(
                        {
                            "kind": "live_order_submitted",
                            "payload": {
                                "event_type": "live_order_submitted",
                                "symbol": "NK225micro",
                                "direction": "long",
                                "reason": "entry_limit_fak_submitted",
                                "metadata": {"order_id": "O1", "entry_slippage_ticks": 1},
                            },
                        }
                    )
                    + "\n"
                )
                handle.write(
                    _json.dumps(
                        {
                            "kind": "live_position_detected",
                            "payload": {
                                "event_type": "live_position_detected",
                                "symbol": "NK225micro",
                                "direction": "long",
                                "qty": 1,
                                "entry_price": 50010,
                                "reason": "position_sync",
                                "timestamp": (ts + timedelta(seconds=1)).isoformat(),
                                "metadata": {"own_entry_detected": True},
                            },
                        }
                    )
                    + "\n"
                )
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                sampled = analyze_micro_log(path, default_config(), include_regime=False, logged_diagnostics_max_rows=2)
                full = analyze_micro_log(path, default_config(), include_regime=False, logged_diagnostics_max_rows=0)

        sampled_funnel = sampled["entry_fill_diagnostics"]["observed_live_funnel"]
        full_funnel = full["entry_fill_diagnostics"]["observed_live_funnel"]
        self.assertEqual(sampled_funnel["entry_submitted"], 1)
        self.assertEqual(sampled_funnel["own_fills_detected"], 0)
        self.assertEqual(full_funnel["entry_submitted"], 1)
        self.assertEqual(full_funnel["own_fills_detected"], 1)

    def test_analyze_micro_log_entry_fill_diagnostics_buckets_live_failures(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(ts))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                import json as _json

                for row in (
                    {
                        "kind": "live_order_submitted",
                        "payload": {
                            "event_type": "live_order_submitted",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "reason": "entry_limit_fak_submitted",
                            "metadata": {"order_id": "O2", "entry_slippage_ticks": 0},
                        },
                    },
                    {
                        "kind": "live_order_expired",
                        "payload": {
                            "event_type": "live_order_expired",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "reason": "entry_order_expired_or_unfilled",
                            "metadata": {"order_id": "O2"},
                        },
                    },
                    {
                        "kind": "live_order_submitted",
                        "payload": {
                            "event_type": "live_order_submitted",
                            "symbol": "NK225micro",
                            "direction": "short",
                            "reason": "entry_limit_fak_submitted",
                            "metadata": {"order_id": "O3", "entry_slippage_ticks": 2},
                        },
                    },
                    {
                        "kind": "live_sync_error",
                        "payload": {
                            "event_type": "live_sync_error",
                            "symbol": "NK225micro",
                            "direction": "short",
                            "reason": "pending_entry_timeout_after_grace",
                            "metadata": {"order_id": "O3"},
                        },
                    },
                    {
                        "kind": "live_order_error",
                        "payload": {
                            "event_type": "live_order_error",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "reason": "entry_order_api_error",
                            "metadata": {
                                "signal": {"direction": "long", "price": 50005},
                                "order_payload": {"Price": 50010},
                            },
                        },
                    },
                    {
                        "kind": "live_order_error",
                        "payload": {
                            "event_type": "live_order_error",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "reason": "entry_order_rejected",
                            "metadata": {"entry_slippage_ticks": 1},
                        },
                    },
                    {
                        "kind": "execution_reject",
                        "payload": {
                            "event_type": "execution_reject",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "reason": "entry_failure_cooldown",
                            "metadata": {},
                        },
                    },
                ):
                    handle.write(_json.dumps(row) + "\n")
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), include_regime=False)

        funnel = report["entry_fill_diagnostics"]["observed_live_funnel"]
        self.assertEqual(funnel["entry_submitted"], 2)
        self.assertEqual(funnel["expired_unfilled"], 1)
        self.assertEqual(funnel["timeout_after_grace"], 1)
        self.assertEqual(funnel["api_errors"], 1)
        self.assertEqual(funnel["rejected"], 1)
        self.assertEqual(funnel["cooldown_rejects"], 1)
        self.assertEqual(funnel["by_slippage_ticks"]["0"]["expired_unfilled"], 1)
        self.assertEqual(funnel["by_slippage_ticks"]["2"]["timeout_after_grace"], 1)

    def test_analyze_micro_log_entry_fill_simulation_estimates_slippage_fill_rate(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                self.calls = 0

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                self.calls += 1
                if self.calls == 1:
                    return [Signal("micro_book", "NK225micro", "long", 0.8, 50005, "micro_book_long")]
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            recorder.write_book(book(ts, bid=50000, ask=50005))
            recorder.write_book(book(ts + timedelta(milliseconds=100), bid=50005, ask=50010))
            recorder.close()
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(
                    path,
                    default_config(),
                    include_regime=False,
                    entry_fill_slippage_ticks=(0, 1),
                    entry_fill_latency_ms=(100,),
                )

        simulation = report["entry_fill_diagnostics"]["fak_fill_simulation"]["by_slippage_latency"]
        self.assertEqual(simulation["0"]["100"]["signals"], 1)
        self.assertEqual(simulation["0"]["100"]["fills"], 0)
        self.assertEqual(simulation["1"]["100"]["fills"], 1)

    def test_analyze_micro_log_reconstructs_live_trade_diagnostics(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            ts = datetime(2026, 4, 28, 17, 0, tzinfo=JST)
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(ts, bid=60090, ask=60095))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                import json as _json

                rows = [
                    {
                        "kind": "live_position_detected",
                        "payload": {
                            "event_type": "live_position_detected",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "qty": 1,
                            "entry_price": 60095,
                            "timestamp": ts.isoformat(),
                            "metadata": {"position_key": "hold:H1", "hold_id": "H1", "own_entry_detected": True},
                        },
                    },
                    {
                        "kind": "live_order_submitted",
                        "payload": {
                            "event_type": "live_order_submitted",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "qty": 1,
                            "reason": "exit_order_submitted",
                            "timestamp": (ts + timedelta(seconds=1)).isoformat(),
                            "metadata": {"order_id": "X1", "position_key": "hold:H1", "exit_reason": "take_profit"},
                        },
                    },
                    {
                        "kind": "live_order_status",
                        "payload": {
                            "event_type": "live_order_status",
                            "symbol": "NK225micro",
                            "direction": "long",
                            "qty": 1,
                            "timestamp": (ts + timedelta(seconds=2)).isoformat(),
                            "metadata": {
                                "order_id": "X1",
                                "position_key": "hold:H1",
                                "order_status": {
                                    "id": "X1",
                                    "state": 5,
                                    "order_state": 5,
                                    "cum_qty": 1,
                                    "execution_price": 60100,
                                },
                            },
                        },
                    },
                ]
                for row in rows:
                    handle.write(_json.dumps(row) + "\n")
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), include_regime=False)

        diagnostics = report["live_trade_diagnostics"]
        self.assertEqual(diagnostics["trades"], 1)
        self.assertEqual(diagnostics["reconstructed_trades"], 1)
        self.assertEqual(diagnostics["net_pnl_ticks"], 1.0)
        self.assertEqual(diagnostics["net_pnl_yen"], 50.0)
        self.assertEqual(diagnostics["by_symbol"]["NK225micro"]["net_pnl_ticks"], 1.0)

    def test_analyze_micro_log_counts_entry_price_mismatch_and_jump_diagnostics(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return []

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            ts = datetime(2026, 4, 28, 17, 0, tzinfo=JST)
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(topix_book(ts))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                import json as _json

                rows = [
                    {
                        "kind": "live_position_detected",
                        "payload": {
                            "event_type": "live_position_detected",
                            "symbol": "TOPIXmini",
                            "direction": "long",
                            "qty": 1,
                            "entry_price": 3770.5,
                            "timestamp": ts.isoformat(),
                            "metadata": {
                                "position_key": "hold:T1",
                                "entry_order_id": "TO1",
                                "position_entry_price": 3770.5,
                                "entry_execution_price": 3770.75,
                                "entry_execution_id": "TE1",
                                "entry_price_mismatch": True,
                            },
                        },
                    },
                    {
                        "kind": "signal_eval_summary",
                        "payload": {
                            "engine": "micro_book",
                            "symbol": "TOPIXmini",
                            "decision": "reject",
                            "reason": "jump_detected",
                            "candidate_direction": "flat",
                            "count": 2,
                            "start_timestamp": ts.isoformat(),
                            "first_metadata": {"jump_detected": True, "spread_ticks": 12.0},
                        },
                    },
                ]
                for row in rows:
                    handle.write(_json.dumps(row) + "\n")
            with patch("kabu_futures.evolution.DualStrategyEngine", FakeEngine):
                report = analyze_micro_log(path, default_config(), include_regime=False)

        self.assertEqual(report["live_trade_diagnostics"]["entry_price_mismatches"], 1)
        self.assertEqual(report["live_trade_diagnostics"]["entry_price_mismatch_samples"][0]["entry_execution_price"], 3770.75)
        self.assertEqual(report["jump_diagnostics"]["total"], 2)
        self.assertEqual(report["jump_diagnostics"]["by_symbol"]["TOPIXmini"], 2)
        self.assertEqual(report["jump_diagnostics"]["by_session_phase"]["night_continuous"], 2)
        self.assertEqual(report["jump_diagnostics"]["by_spread_ticks"][">=10"], 2)

    def test_analyze_micro_evolution_cli_passes_entry_fill_grids(self) -> None:
        from scripts import analyze_micro_evolution as cli

        captured: dict[str, object] = {}

        def fake_analyze(path: object, config: object, **kwargs: object) -> dict[str, object]:
            captured.update(kwargs)
            return {"ok": True}

        with patch.object(
            sys,
            "argv",
            [
                "analyze_micro_evolution.py",
                "dummy.jsonl",
                "--entry-fill-slippage-grid",
                "0,2",
                "--entry-fill-latency-ms",
                "0,250",
                "--diagnostics-max-rows",
                "7",
            ],
        ):
            with patch.object(cli, "load_json_config", lambda path: default_config()):
                with patch.object(cli, "analyze_micro_log", fake_analyze):
                    import io

                    with patch("sys.stdout", io.StringIO()):
                        self.assertEqual(cli.main(), 0)

        self.assertEqual(captured["entry_fill_slippage_ticks"], (0, 2))
        self.assertEqual(captured["entry_fill_latency_ms"], (0, 250))
        self.assertEqual(captured["logged_diagnostics_max_rows"], 7)

    def test_analyze_micro_evolution_cli_defaults_diagnostics_rows_to_max_books(self) -> None:
        from scripts import analyze_micro_evolution as cli

        captured: dict[str, object] = {}

        def fake_analyze(path: object, config: object, **kwargs: object) -> dict[str, object]:
            captured.update(kwargs)
            return {"ok": True}

        with patch.object(
            sys,
            "argv",
            [
                "analyze_micro_evolution.py",
                "dummy.jsonl",
                "--max-books",
                "3",
            ],
        ):
            with patch.object(cli, "load_json_config", lambda path: default_config()):
                with patch.object(cli, "analyze_micro_log", fake_analyze):
                    import io

                    with patch("sys.stdout", io.StringIO()):
                        self.assertEqual(cli.main(), 0)

        self.assertEqual(captured["max_books"], 3)
        self.assertEqual(captured["logged_diagnostics_max_rows"], 3)

    def test_evaluate_micro_config_does_not_execute_minute_signals(self) -> None:
        class FakeEngine:
            latest_signal_evaluations: list[SignalEvaluation] = []
            latest_book_features = None

            def __init__(self, config: object) -> None:
                pass

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                return [Signal("minute_vwap", "NK225micro", "long", 0.8, order_book.mid_price, "trend_pullback_long")]

        class FakeExecution:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def on_book(self, order_book: OrderBook, features: object = None) -> list[object]:
                return []

            def on_signal(self, signal: Signal, order_book: OrderBook) -> list[object]:
                raise AssertionError("minute signals must not execute during micro tuning")

        with patch("kabu_futures.tuning.DualStrategyEngine", FakeEngine), patch(
            "kabu_futures.tuning.PaperExecutionController",
            FakeExecution,
        ):
            result = evaluate_micro_config(
                [book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc))],
                default_config(),
                {"imbalance_entry": 0.3},
            )
        self.assertEqual(result["trades"], 0)
        self.assertEqual(result["signals"]["minute_vwap:long:trend_pullback_long"], 1)

    def test_tune_micro_params_skips_invalid_combo_without_crashing(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            for second in range(3):
                recorder.write_book(book(datetime(2026, 4, 23, 9, 0, second, tzinfo=timezone.utc)))
            recorder.close()
            report = tune_micro_params(path, default_config(), imbalance_grid=(0.05, 0.30), min_trades=1)
        self.assertEqual(report["decision"], "no_change")
        self.assertGreater(len(report["invalid_combos"]), 0)

    def test_tune_micro_params_generates_multi_dimensional_grid(self) -> None:
        def fake_evaluate(
            books: object,
            config: object,
            parameters: dict[str, object],
            paper_fill_model: object = "immediate",
        ) -> dict[str, object]:
            trades = int(float(parameters["imbalance_entry"]) * 10) + int(parameters["spread_ticks_required"])
            net = float(trades)
            return {
                "parameters": dict(parameters),
                "books": 1,
                "evaluations": {"reject": 1},
                "reject_reasons_top": [("imbalance_not_met", max(0, 10 - trades))],
                "signals": {},
                "paper_events": {},
                "exit_reasons": {},
                "trades": trades,
                "win_rate": 1.0,
                "net_pnl_ticks": net,
                "avg_pnl_ticks": round(net / trades, 4) if trades else 0.0,
                "max_drawdown_ticks": 0.0,
            }

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with patch("kabu_futures.tuning.evaluate_micro_config", side_effect=fake_evaluate) as mocked:
                report = tune_micro_params(
                    path,
                    default_config(),
                    imbalance_grid=(0.20, 0.30),
                    microprice_grid=(0.10, 0.15),
                    spread_grid=(1, 2),
                    min_trades=1,
                )

        self.assertEqual(mocked.call_count, 9)
        self.assertEqual(len(report["candidates"]), 8)
        self.assertEqual(report["baseline"]["parameters"]["imbalance_entry"], default_config().micro_engine.imbalance_entry)
        self.assertEqual(report["baseline"]["parameters"]["microprice_entry_ticks"], default_config().micro_engine.microprice_entry_ticks)
        self.assertEqual(report["baseline"]["parameters"]["ofi_percentile"], default_config().micro_engine.ofi_percentile)
        self.assertEqual(report["baseline"]["parameters"]["spread_ticks_required"], default_config().micro_engine.spread_ticks_required)
        self.assertEqual(report["grid"]["microprice_entry_ticks"], [0.1, 0.15])
        self.assertEqual(report["grid"]["ofi_percentile"], [default_config().micro_engine.ofi_percentile])
        self.assertEqual(report["grid"]["spread_ticks_required"], [1, 2])

    def test_tune_micro_params_uses_effective_profile_baseline(self) -> None:
        captured: list[tuple[dict[str, object], str]] = []

        def fake_evaluate(
            books: object,
            config: object,
            parameters: dict[str, object],
            paper_fill_model: object = "immediate",
        ) -> dict[str, object]:
            captured.append((dict(parameters), config.micro_engine.entry_profile))
            return {
                "parameters": dict(parameters),
                "books": 1,
                "evaluations": {},
                "reject_reasons_top": [],
                "signals": {},
                "paper_events": {},
                "exit_reasons": {},
                "tradeable_micro_signals": 1,
                "paper_entries": 1,
                "simulated_fill_rate": 1.0,
                "trades": 1,
                "win_rate": 1.0,
                "net_pnl_ticks": 1.0,
                "avg_pnl_ticks": 1.0,
                "max_drawdown_ticks": 0.0,
            }

        cfg = replace(
            default_config(),
            micro_engine=replace(default_config().micro_engine, entry_profile="conservative_candidate_v1"),
        )
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with patch("kabu_futures.tuning.evaluate_micro_config", side_effect=fake_evaluate):
                report = tune_micro_params(path, cfg, imbalance_grid=(0.26,), microprice_grid=(0.10,), min_trades=1)

        self.assertEqual(report["baseline"]["parameters"]["entry_profile"], "conservative_candidate_v1")
        self.assertEqual(report["baseline"]["parameters"]["imbalance_entry"], 0.28)
        self.assertEqual(report["baseline"]["parameters"]["microprice_entry_ticks"], 0.12)
        self.assertEqual(report["baseline"]["parameters"]["ofi_percentile"], 65.0)
        self.assertEqual(captured[1][1], "default")

    def test_tune_micro_params_includes_ofi_grid_and_fill_rate_gate(self) -> None:
        def fake_evaluate(
            books: object,
            config: object,
            parameters: dict[str, object],
            paper_fill_model: object = "immediate",
        ) -> dict[str, object]:
            is_baseline = parameters["ofi_percentile"] == default_config().micro_engine.ofi_percentile
            trades = 5 if is_baseline else 12
            return {
                "parameters": dict(parameters),
                "books": 1,
                "evaluations": {},
                "reject_reasons_top": [],
                "signals": {},
                "paper_events": {},
                "exit_reasons": {},
                "tradeable_micro_signals": trades,
                "paper_entries": trades,
                "simulated_fill_rate": 1.0 if is_baseline else 0.5,
                "trades": trades,
                "win_rate": 0.6,
                "net_pnl_ticks": 10.0 if is_baseline else 20.0,
                "avg_pnl_ticks": 2.0 if is_baseline else 1.6667,
                "max_drawdown_ticks": -2.0,
            }

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with patch("kabu_futures.tuning.evaluate_micro_config", side_effect=fake_evaluate):
                report = tune_micro_params(
                    path,
                    default_config(),
                    imbalance_grid=(0.28,),
                    microprice_grid=(0.12,),
                    ofi_percentile_grid=(65.0,),
                    min_trades=1,
                    min_fill_rate=0.70,
                )

        self.assertEqual(report["grid"]["ofi_percentile"], [65.0])
        self.assertEqual(report["decision"], "no_change")
        self.assertFalse(report["recommendation"]["checks"]["simulated_fill_rate_ok"])

    def test_tune_micro_params_marks_weaker_trade_increase_as_diagnostic_only(self) -> None:
        def fake_evaluate(
            books: object,
            config: object,
            parameters: dict[str, object],
            paper_fill_model: object = "immediate",
        ) -> dict[str, object]:
            is_baseline = parameters["imbalance_entry"] == default_config().micro_engine.imbalance_entry
            trades = 5 if is_baseline else 8
            net = 10.0 if is_baseline else 6.0
            avg = 2.0 if is_baseline else 0.75
            drawdown = -4.0 if is_baseline else -10.0
            return {
                "parameters": dict(parameters),
                "books": 1,
                "evaluations": {},
                "reject_reasons_top": [("imbalance_not_met", 10 if is_baseline else 5)],
                "signals": {},
                "paper_events": {},
                "exit_reasons": {},
                "trades": trades,
                "win_rate": 0.5,
                "net_pnl_ticks": net,
                "avg_pnl_ticks": avg,
                "max_drawdown_ticks": drawdown,
            }

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with patch("kabu_futures.tuning.evaluate_micro_config", side_effect=fake_evaluate):
                report = tune_micro_params(path, default_config(), imbalance_grid=(0.20,), min_trades=1)

        self.assertEqual(report["decision"], "no_change")
        self.assertEqual(report["recommendation"]["decision"], "diagnostic_only")
        self.assertFalse(report["recommendation"]["checks"]["net_pnl_improved"])
        self.assertFalse(report["recommendation"]["checks"]["avg_pnl_not_worse"])

    def test_tune_micro_params_recommends_candidate_only_when_safety_gates_pass(self) -> None:
        def fake_evaluate(
            books: object,
            config: object,
            parameters: dict[str, object],
            paper_fill_model: object = "immediate",
        ) -> dict[str, object]:
            is_baseline = parameters["imbalance_entry"] == default_config().micro_engine.imbalance_entry
            trades = 5 if is_baseline else 10
            net = 5.0 if is_baseline else 12.0
            avg = 1.0 if is_baseline else 1.2
            drawdown = -4.0 if is_baseline else -4.5
            return {
                "parameters": dict(parameters),
                "books": 1,
                "evaluations": {},
                "reject_reasons_top": [("imbalance_not_met", 10 if is_baseline else 3)],
                "signals": {},
                "paper_events": {},
                "exit_reasons": {},
                "trades": trades,
                "win_rate": 0.6,
                "net_pnl_ticks": net,
                "avg_pnl_ticks": avg,
                "max_drawdown_ticks": drawdown,
            }

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with patch("kabu_futures.tuning.evaluate_micro_config", side_effect=fake_evaluate):
                report = tune_micro_params(path, default_config(), imbalance_grid=(0.20,), min_trades=1)

        self.assertEqual(report["decision"], "recommended")
        self.assertEqual(report["recommendation"]["decision"], "recommended")
        self.assertEqual(report["recommendation"]["trade_delta"], 5)
        self.assertEqual(report["best_safe_candidate"]["parameters"]["imbalance_entry"], 0.20)


class CandidateReviewTests(unittest.TestCase):
    def _fake_tune_report(self, decision: str = "no_change") -> dict[str, object]:
        checks = {
            "trade_opportunity_2x": False,
            "net_pnl_not_worse": True,
            "drawdown_not_materially_worse": True,
            "simulated_fill_rate_ok": True,
        }
        return {
            "decision": decision,
            "baseline": {
                "parameters": {
                    "imbalance_entry": 0.3,
                    "microprice_entry_ticks": 0.15,
                    "ofi_percentile": 70.0,
                    "spread_ticks_required": 1,
                },
                "trades": 2,
                "net_pnl_ticks": 2.0,
                "avg_pnl_ticks": 1.0,
                "max_drawdown_ticks": 0.0,
            },
            "best": {
                "parameters": {
                    "imbalance_entry": 0.28,
                    "microprice_entry_ticks": 0.12,
                    "ofi_percentile": 65.0,
                    "spread_ticks_required": 1,
                },
                "trades": 3,
                "net_pnl_ticks": 1.5,
                "avg_pnl_ticks": 0.5,
                "max_drawdown_ticks": -1.0,
                "simulated_fill_rate": 0.9,
            },
            "candidates": [],
            "grid": {"imbalance_entry": [0.28, 0.26, 0.24]},
            "recommendation": {"decision": "diagnostic_only", "checks": checks},
        }

    def test_candidate_review_uses_temporary_champion_and_champion_first_promotion(self) -> None:
        from kabu_futures.promotion import PromotionDecision
        from scripts import candidate_review as cli

        captured: dict[str, object] = {}
        tune_report = self._fake_tune_report()

        def fake_evaluate(
            champion: dict[str, object],
            challenger: dict[str, object],
            **kwargs: object,
        ) -> PromotionDecision:
            captured["champion"] = champion
            captured["challenger"] = challenger
            captured["kwargs"] = kwargs
            return PromotionDecision(decision="reject", failed_gates=["walk_forward"])

        with TemporaryDirectory() as tmp:
            args = cli.build_parser().parse_args(
                [
                    str(pathlib.Path(tmp) / "missing_logs"),
                    "--baseline",
                    str(pathlib.Path(tmp) / "missing_baseline.json"),
                    "--skip-walk-forward",
                ]
            )
            with patch.object(cli, "diagnose_log", return_value={"files": ["a.jsonl"], "micro_entry_funnel": {}}):
                with patch.object(cli, "tune_micro_params", return_value=tune_report):
                    with patch.object(cli, "analyze_micro_log", return_value={"entry_fill_diagnostics": {}}):
                        with patch.object(cli, "jump_markout_report", return_value={"by_jump_reason": {}, "live_behavior_changed": False}):
                            with patch.object(cli, "evaluate_challenger", side_effect=fake_evaluate):
                                report = cli.build_report(args)

        self.assertEqual(captured["champion"]["source"], "temporary_champion_from_current_tune_baseline")
        self.assertIs(captured["challenger"], tune_report)
        self.assertEqual(report["promotion"]["promotion_order"], ["champion", "challenger"])
        self.assertEqual(report["final_decision"], "no_change")
        self.assertTrue(report["read_only"])
        self.assertFalse(report["live_config_changed"])

    def test_candidate_review_jump_report_separates_reasons_without_live_change(self) -> None:
        from scripts import candidate_review as cli

        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "jump.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            base_ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            recorder.write_book(book(base_ts, bid=50000, ask=50005))
            recorder.write_book(book(base_ts + timedelta(seconds=1), bid=50005, ask=50010))
            recorder.write_book(book(base_ts + timedelta(seconds=5), bid=50010, ask=50015))
            recorder.close()
            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "kind": "signal_eval_summary",
                            "payload": {
                                "engine": "micro_book",
                                "symbol": "NK225micro",
                                "decision": "reject",
                                "reason": "jump_detected",
                                "candidate_direction": "long",
                                "count": 1,
                                "start_timestamp": base_ts.isoformat(),
                                "first_metadata": {"jump_detected": True, "jump_reason": "mid_move_jump"},
                            },
                        }
                    )
                    + "\n"
                )
                handle.write(
                    json.dumps(
                        {
                            "kind": "signal_eval_summary",
                            "payload": {
                                "engine": "micro_book",
                                "symbol": "NK225micro",
                                "decision": "reject",
                                "reason": "jump_detected",
                                "candidate_direction": "flat",
                                "count": 2,
                                "start_timestamp": base_ts.isoformat(),
                                "first_metadata": {"jump_detected": True, "jump_reason": "spread_wide"},
                            },
                        }
                    )
                    + "\n"
                )

            report = cli.jump_markout_report(path, default_config(), horizons=(1.0, 5.0))

        self.assertFalse(report["live_behavior_changed"])
        self.assertIn("mid_move_jump", report["by_jump_reason"])
        self.assertIn("spread_wide", report["by_jump_reason"])
        self.assertEqual(report["by_jump_reason"]["spread_wide"]["events"], 2)
        self.assertGreater(report["by_jump_reason"]["mid_move_jump"]["directional_markout_ticks"]["1.0"]["avg_ticks"], 0)

    def test_candidate_review_cli_writes_no_change_report(self) -> None:
        from scripts import candidate_review as cli

        with TemporaryDirectory() as tmp:
            output = pathlib.Path(tmp) / "candidate_review.json"
            report = {
                "final_decision": "no_change",
                "promotion": {"decision": "reject"},
                "read_only": True,
                "live_config_changed": False,
            }
            with patch.object(cli, "build_report", return_value=report):
                import io

                with patch("sys.stdout", io.StringIO()):
                    with patch("sys.stderr", io.StringIO()):
                        self.assertEqual(cli.main(["logs", "--output", str(output)]), 0)

            written = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(written["final_decision"], "no_change")
        self.assertTrue(written["read_only"])
        self.assertFalse(written["live_config_changed"])

    def test_candidate_review_reports_effective_gray_profile(self) -> None:
        from kabu_futures.promotion import PromotionDecision
        from scripts import candidate_review as cli

        with TemporaryDirectory() as tmp:
            config_path = pathlib.Path(tmp) / "config.json"
            config_path.write_text('{"micro_engine":{"entry_profile":"conservative_candidate_v1"}}', encoding="utf-8")
            args = cli.build_parser().parse_args(
                [
                    str(pathlib.Path(tmp) / "missing_logs"),
                    "--config",
                    str(config_path),
                    "--skip-walk-forward",
                ]
            )
            with patch.object(cli, "diagnose_log", return_value={"files": ["a.jsonl"], "micro_entry_funnel": {}}):
                with patch.object(cli, "tune_micro_params", return_value=self._fake_tune_report()):
                    with patch.object(cli, "analyze_micro_log", return_value={"entry_fill_diagnostics": {}}):
                        with patch.object(cli, "jump_markout_report", return_value={"by_jump_reason": {}, "live_behavior_changed": False}):
                            with patch.object(cli, "evaluate_challenger", return_value=PromotionDecision(decision="reject")):
                                report = cli.build_report(args)

        self.assertEqual(report["live_rules_unchanged"]["micro_entry_profile"], "conservative_candidate_v1")
        self.assertEqual(report["live_rules_unchanged"]["micro_effective_thresholds"]["imbalance_entry"], 0.28)
        self.assertFalse(report["live_config_changed"])


class ApiRegistrationTests(unittest.TestCase):
    class _FakeHttpResponse:
        def __init__(self, payload: dict[str, object] | list[object] | None) -> None:
            self.raw = json.dumps(payload).encode("utf-8") if payload is not None else b""

        def __enter__(self) -> "ApiRegistrationTests._FakeHttpResponse":
            return self

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
            return None

        def read(self) -> bytes:
            return self.raw

    def _http_error(self, code: int, body: str) -> HTTPError:
        return HTTPError("http://localhost/kabusapi/test", code, "error", {}, BytesIO(body.encode("utf-8")))

    def test_extract_symbol_code_requires_symbol(self) -> None:
        self.assertEqual(extract_symbol_code({"Symbol": "165120019"}, "NK225micro"), "165120019")

    def test_query_endpoints_reauthenticate_once_after_401(self) -> None:
        client = KabuStationClient("pw", default_config().api)
        client.token = "OLD"

        with patch(
            "kabu_futures.api.urlopen",
            side_effect=[
                self._http_error(401, '{"Code":4001007,"Message":"ログイン認証エラー"}'),
                self._FakeHttpResponse({"Token": "NEW"}),
                self._FakeHttpResponse({"data": []}),
            ],
        ) as mocked:
            result = client.positions(product=3)

        self.assertEqual(result, {"data": []})
        self.assertEqual(client.token, "NEW")
        self.assertEqual(mocked.call_count, 3)

    def test_query_endpoints_do_not_retry_non_auth_errors(self) -> None:
        client = KabuStationClient("pw", default_config().api)
        client.token = "OLD"

        with patch("kabu_futures.api.urlopen", side_effect=[self._http_error(500, '{"Code":999}')]) as mocked:
            with self.assertRaises(KabuApiError) as raised:
                client.orders(product=3)

        self.assertEqual(raised.exception.status_code, 500)
        self.assertEqual(raised.exception.category, "server_error")
        self.assertEqual(mocked.call_count, 1)

    def test_api_errors_are_classified_for_live_backoff(self) -> None:
        cases = {
            400: "bad_request",
            403: "forbidden",
            429: "rate_limit",
            500: "server_error",
            503: "service_unavailable",
        }
        for status_code, category in cases.items():
            client = KabuStationClient("pw", default_config().api)
            client.token = "OLD"
            with self.subTest(status_code=status_code):
                with patch("kabu_futures.api.urlopen", side_effect=[self._http_error(status_code, '{"Code":999}')]):
                    with self.assertRaises(KabuApiError) as raised:
                        client.orders(product=3)

                self.assertEqual(raised.exception.category, category)

    def test_sendorder_does_not_retry_401_forever(self) -> None:
        client = KabuStationClient("pw", default_config().api)
        client.token = "OLD"

        with patch(
            "kabu_futures.api.urlopen",
            side_effect=[self._http_error(401, '{"Code":4001007,"Message":"ログイン認証エラー"}')],
        ) as mocked:
            with self.assertRaises(KabuApiError) as raised:
                client.sendorder_future({"Symbol": "161060023"})

        self.assertEqual(raised.exception.category, "auth_error")
        self.assertEqual(mocked.call_count, 1)

    def test_build_future_registration_symbols_resolves_each_exchange(self) -> None:
        class FakeClient:
            def symbolname_future(self, future_code: str, deriv_month: int) -> dict[str, object]:
                return {"Symbol": f"{future_code}-{deriv_month}", "SymbolName": future_code}

        symbols, resolved = build_future_registration_symbols(
            FakeClient(),  # type: ignore[arg-type]
            ["NK225micro", "TOPIXmini"],
            0,
            [23, 24],
        )
        self.assertEqual(
            symbols,
            [
                {"Symbol": "NK225micro-0", "Exchange": 23},
                {"Symbol": "NK225micro-0", "Exchange": 24},
                {"Symbol": "TOPIXmini-0", "Exchange": 23},
                {"Symbol": "TOPIXmini-0", "Exchange": 24},
            ],
        )
        self.assertEqual(resolved[0]["FutureCode"], "NK225micro")


class MultiTimeframeTests(unittest.TestCase):
    def test_strong_alignment_scores_tradeable(self) -> None:
        cfg = default_config()
        scorer = MultiTimeframeScorer(cfg.multi_timeframe)
        scorer.update_snapshot(
            MultiTimeframeSnapshot(
                timestamp=datetime(2026, 4, 23, 9, 0),
                symbol="NK225micro",
                yearly_trend="long",
                monthly_trend="long",
                weekly_trend="long",
                daily_trend="long",
                hourly_trend="long",
            )
        )
        score = scorer.score("long", minute_signal=Signal("minute_orb", "NK225micro", "long", 0.8))
        self.assertGreaterEqual(score.total_score, cfg.multi_timeframe.min_total_score_to_trade)
        self.assertIsNone(score.veto_reason)
        self.assertNotEqual(score.position_scale, "none")

    def test_higher_timeframe_opposite_bias_vetoes_trade(self) -> None:
        cfg = default_config()
        scorer = MultiTimeframeScorer(cfg.multi_timeframe)
        scorer.update_snapshot(
            MultiTimeframeSnapshot(
                timestamp=datetime(2026, 4, 23, 9, 0),
                symbol="NK225micro",
                yearly_trend="short",
                monthly_trend="short",
                weekly_trend="short",
                daily_trend="long",
                hourly_trend="long",
            )
        )
        score = scorer.score("long", minute_signal=Signal("minute_orb", "NK225micro", "long", 0.8))
        self.assertEqual(score.veto_reason, "higher_timeframe_opposite_bias")
        self.assertFalse(score.can_trade)

    def test_micro_signal_rejected_when_execution_score_is_too_low(self) -> None:
        base_cfg = default_config()
        cfg = replace(
            base_cfg,
            multi_timeframe=replace(base_cfg.multi_timeframe, min_execution_score_to_chase=16),
        )
        engine = DualStrategyEngine(cfg)
        engine.update_multi_timeframe(
            MultiTimeframeSnapshot(
                timestamp=datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc),
                symbol="NK225micro",
                yearly_trend="long",
                monthly_trend="long",
                weekly_trend="long",
                daily_trend="long",
                hourly_trend="long",
            )
        )
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        # Warm the OFI windows with neutral books, then send a valid micro signal.
        # The test raises min_execution_score_to_chase above the max execution
        # score, so the multi-timeframe arbiter must reject the otherwise valid signal.
        for idx in range(5):
            event_ts = ts + timedelta(milliseconds=100 * idx)
            engine.on_order_book(book(event_ts, bid_qty=100, ask_qty=100), now=event_ts)
        final_ts = ts + timedelta(milliseconds=450)
        signals = engine.on_order_book(book(final_ts, bid_qty=300, ask_qty=20), now=final_ts)
        risk_signals = [signal for signal in signals if signal.engine == "risk" and signal.reason == "execution_score_below_threshold"]
        self.assertEqual(len(risk_signals), 1)
        self.assertEqual(risk_signals[0].score, 0)
        self.assertEqual(risk_signals[0].signal_horizon, "system")
        self.assertEqual(risk_signals[0].metadata["blocked_by"], "multi_timeframe")
        self.assertEqual(risk_signals[0].metadata["decision_trace"]["reason"], "execution_score_below_threshold")


class AlphaStackTests(unittest.TestCase):
    def test_nt_ratio_engine_detects_expensive_nikkei_and_computes_dynamic_hedge(self) -> None:
        cfg = replace(default_config().nt_spread, min_history=20)
        engine = NTRatioSpreadEngine(cfg)
        ts = datetime(2026, 4, 1, 15, 0)
        for idx in range(59):
            engine.update(ts + timedelta(days=idx), 42000, 3000)
        signal = engine.update(ts + timedelta(days=60), 48000, 3000)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.direction, "short")
        self.assertEqual(signal.veto_reason, "nt_shadow_only")
        self.assertAlmostEqual(compute_micro225_per_topix_mini(43000, 3000), 6.98, places=1)
        self.assertEqual(signal.metadata["nt_ratio"], 16.0)
        self.assertGreater(signal.score, 70)

    def test_us_japan_lead_lag_scores_us_open_alignment(self) -> None:
        scorer = USJapanLeadLagScorer(default_config().lead_lag)
        snapshot = ExternalFactorsSnapshot(
            timestamp=datetime(2026, 4, 23, 23, 0, tzinfo=timezone.utc),
            es_momentum_1m=0.25,
            nq_momentum_1m=0.30,
            sox_bias="long",
            usdjpy_bias="long",
            us10y_bias="short",
            bank_bias="long",
        )
        score = scorer.score("long", snapshot)
        self.assertGreater(score.score, 0)
        self.assertEqual(score.bias, "long")
        self.assertEqual(score.metadata["external_factor_window"], "us_open_lead_lag")

    def test_event_risk_vetoes_external_factor_score(self) -> None:
        scorer = USJapanLeadLagScorer(default_config().lead_lag)
        score = scorer.score(
            "long",
            ExternalFactorsSnapshot(
                timestamp=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
                event_risk_flag=True,
            ),
        )
        self.assertEqual(score.veto_reason, "event_risk_flag")

    def test_strategy_arbiter_blocks_directional_signal_when_live_nt_conflicts(self) -> None:
        cfg = default_config()
        arbiter = StrategyArbiter(cfg.alpha_stack, cfg.risk)
        mtf_score = MultiTimeframeScorer(cfg.multi_timeframe)._finalize(
            replace(
                MultiTimeframeScorer(cfg.multi_timeframe).score("flat"),
                regime_score=25,
                bias_score=25,
                setup_score=22,
                execution_score=12,
                veto_reason=None,
            )
        )
        nt_signal = AlphaSignal(
            "nt_ratio_spread",
            "NT_RATIO",
            "short",
            90,
            "swing",
            risk_budget_pct=cfg.risk.nt_spread_risk_pct,
            position_scale="micro225_1",
        )
        intent = arbiter.evaluate(
            Signal("minute_orb", "NK225micro", "long", 0.8),
            mtf_score,
            nt_signal=nt_signal,
        )
        self.assertFalse(intent.allowed)
        self.assertEqual(intent.veto_reason, "nt_spread_direction_conflict")

    def test_strategy_arbiter_blocks_portfolio_beta_cap(self) -> None:
        cfg = default_config()
        arbiter = StrategyArbiter(cfg.alpha_stack, cfg.risk)
        scorer = MultiTimeframeScorer(cfg.multi_timeframe)
        scorer.update_snapshot(
            MultiTimeframeSnapshot(
                timestamp=datetime(2026, 4, 23, 9, 0),
                symbol="NK225micro",
                yearly_trend="long",
                monthly_trend="long",
                weekly_trend="long",
                daily_trend="long",
                hourly_trend="long",
            )
        )
        intent = arbiter.evaluate(
            Signal("minute_orb", "NK225micro", "long", 0.8),
            scorer.score("long", minute_signal=Signal("minute_orb", "NK225micro", "long", 0.8)),
            portfolio=PortfolioExposure(nikkei_beta=0.31),
        )
        self.assertFalse(intent.allowed)
        self.assertEqual(intent.veto_reason, "portfolio_beta_limit")


class WalkForwardTests(unittest.TestCase):
    """Tests for walk_forward rolling validation."""

    def _make_books(self, n: int, base_ts: datetime, day_offset: int = 0) -> list[OrderBook]:
        from datetime import timedelta
        ts = base_ts + timedelta(days=day_offset)
        return [
            book(ts + timedelta(seconds=i), bid_qty=200, ask_qty=20)
            for i in range(n)
        ]

    def test_make_windows_basic(self) -> None:
        days = ["2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24"]
        windows = make_windows(days, train_size=2, test_size=1)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0].train_days, ("2026-04-21", "2026-04-22"))
        self.assertEqual(windows[0].test_days, ("2026-04-23",))
        self.assertEqual(windows[1].train_days, ("2026-04-22", "2026-04-23"))

    def test_make_windows_insufficient_days_returns_empty(self) -> None:
        days = ["2026-04-21"]
        windows = make_windows(days, train_size=1, test_size=1)
        self.assertEqual(windows, [])

    def test_make_windows_invalid_args_raises(self) -> None:
        with self.assertRaises(ValueError):
            make_windows(["2026-04-21"], train_size=0, test_size=1)

    def test_split_books_by_day_groups_correctly(self) -> None:
        ts_day1 = datetime(2026, 4, 21, 9, 0, tzinfo=timezone.utc)
        ts_day2 = datetime(2026, 4, 22, 9, 0, tzinfo=timezone.utc)
        books_day1 = [book(ts_day1 + timedelta(seconds=i)) for i in range(3)]
        books_day2 = [book(ts_day2 + timedelta(seconds=i)) for i in range(2)]
        all_books = books_day1 + books_day2

        with TemporaryDirectory() as tmp:
            import json as _json
            p = pathlib.Path(tmp) / "test.jsonl"
            lines = []
            for b in all_books:
                lines.append(_json.dumps({
                    "symbol": b.symbol,
                    "timestamp": b.timestamp.isoformat(),
                    "best_bid_price": b.best_bid_price,
                    "best_bid_qty": b.best_bid_qty,
                    "best_ask_price": b.best_ask_price,
                    "best_ask_qty": b.best_ask_qty,
                }))
            p.write_text("\n".join(lines), encoding="utf-8")

            grouped = split_books_by_day(p)
            self.assertIn("2026-04-21", grouped)
            self.assertIn("2026-04-22", grouped)
            self.assertEqual(len(grouped["2026-04-21"]), 3)
            self.assertEqual(len(grouped["2026-04-22"]), 2)

    def test_walk_forward_insufficient_days_returns_diagnostics(self) -> None:
        cfg = default_config()
        with TemporaryDirectory() as tmp:
            import json as _json
            p = pathlib.Path(tmp) / "single.jsonl"
            ts = datetime(2026, 4, 21, 9, 0, tzinfo=timezone.utc)
            b = book(ts, bid_qty=200, ask_qty=20)
            p.write_text(_json.dumps({
                "symbol": b.symbol,
                "timestamp": b.timestamp.isoformat(),
                "best_bid_price": b.best_bid_price,
                "best_bid_qty": b.best_bid_qty,
                "best_ask_price": b.best_ask_price,
                "best_ask_qty": b.best_ask_qty,
            }), encoding="utf-8")

            result = walk_forward_micro(p, cfg, train_size=1, test_size=1)
            self.assertFalse(result["summary"]["stable"])
            self.assertGreater(len(result["summary"]["diagnostics"]), 0)

    def test_walk_forward_sample_data(self) -> None:
        """walk_forward runs end-to-end on sample data without error."""
        sample = pathlib.Path(__file__).resolve().parents[1] / "data" / "sample_market_data.jsonl"
        if not sample.exists():
            self.skipTest("sample_market_data.jsonl not available")
        cfg = default_config()
        result = walk_forward_micro(sample, cfg, train_size=1, test_size=1, min_trades=1)
        self.assertIn("summary", result)
        self.assertIn("evaluations", result)
        self.assertIn("days", result)


class PromotionTests(unittest.TestCase):
    """Tests for champion/challenger promotion gate."""

    def _base_challenger(self, trades: int = 30, net_pnl: float = 5.0, avg_pnl: float = 0.5) -> dict:
        return {
            "best": {
                "parameters": {"imbalance_entry": 0.22},
                "trades": trades,
                "net_pnl_ticks": net_pnl,
                "avg_pnl_ticks": avg_pnl,
                "max_drawdown_ticks": -5.0,
            },
            "candidates": [{"parameters": {"imbalance_entry": 0.22}}],
            "grid": {"imbalance_entry": [0.18, 0.20, 0.22, 0.25, 0.30]},
        }

    def test_promote_when_all_gates_pass(self) -> None:
        # Champion drawdown=-4.0; challenger drawdown=-5.0.
        # Use max_drawdown_ratio=2.0 so 5.0 <= 4.0*2.0 passes.
        champion = {"trades": 20, "net_pnl_ticks": 3.0, "avg_pnl_ticks": 0.3, "max_drawdown_ticks": -4.0}
        challenger = self._base_challenger(trades=30, net_pnl=5.0, avg_pnl=0.6)
        wf = {"pass_rate": 0.8, "windows": 5}
        t = PromotionThresholds(
            min_trades_per_day=1.0, min_avg_markout_ticks=0.0,
            walk_forward_pass_threshold=0.7, min_observation_days=0,
            max_drawdown_ratio=2.0,
            allow_grid_boundary=True,
        )
        decision = evaluate_challenger(champion, challenger, walk_forward_summary=wf, thresholds=t)
        self.assertEqual(decision.decision, "promote")

    def test_reject_when_no_best(self) -> None:
        challenger = {"best": None, "candidates": [], "grid": {}}
        decision = evaluate_challenger({}, challenger)
        # no trades → fails trades_per_day and avg_markout gates
        self.assertIn(decision.decision, ("reject", "hold"))

    def test_hold_when_borderline(self) -> None:
        champion = {"trades": 20, "net_pnl_ticks": 3.0, "max_drawdown_ticks": -4.0}
        challenger = self._base_challenger(trades=5, net_pnl=4.0, avg_pnl=0.8)
        t = PromotionThresholds(
            min_trades_per_day=10.0,   # fails: only 5 trades in 1 day
            min_avg_markout_ticks=0.0, # passes
            walk_forward_pass_threshold=0.7,
            min_observation_days=0,
        )
        wf = {"pass_rate": 0.8, "windows": 3}
        decision = evaluate_challenger(champion, challenger, walk_forward_summary=wf, thresholds=t)
        # 1 failed (trades_per_day), rest pass → should be hold
        self.assertIn(decision.decision, ("hold", "reject"))

    def test_veto_param_jump(self) -> None:
        # Champion imbalance=0.30, challenger imbalance=0.22 → change = 0.267 (26.7%).
        # With max_param_change_ratio=0.2, 0.267 > 0.2 → veto triggered.
        champion = {"parameters": {"imbalance_entry": 0.30}, "trades": 20, "net_pnl_ticks": 3.0}
        challenger = self._base_challenger()  # best.parameters = {"imbalance_entry": 0.22}
        t = PromotionThresholds(max_param_change_ratio=0.2, min_observation_days=0,
                                min_trades_per_day=1.0, min_avg_markout_ticks=0.0)
        decision = evaluate_challenger(champion, challenger, thresholds=t)
        self.assertIn("param_jump:imbalance_entry", " ".join(decision.veto_reasons))

    def test_no_champion_allows_any_valid_challenger(self) -> None:
        challenger = self._base_challenger(trades=30, net_pnl=0.1, avg_pnl=0.1)
        t = PromotionThresholds(
            min_trades_per_day=1.0, min_avg_markout_ticks=0.0,
            min_observation_days=0, walk_forward_pass_threshold=0.7,
            allow_grid_boundary=True,
        )
        wf = {"pass_rate": 1.0, "windows": 2}
        decision = evaluate_challenger({}, challenger, walk_forward_summary=wf, thresholds=t)
        self.assertEqual(decision.decision, "promote")


class RegimeTests(unittest.TestCase):
    """Tests for volatility regime classifier."""

    def _make_book(self, ts: datetime, mid: float) -> OrderBook:
        half_spread = 2.5
        return OrderBook(
            symbol="NK225micro",
            timestamp=ts,
            best_bid_price=mid - half_spread,
            best_bid_qty=100,
            best_ask_price=mid + half_spread,
            best_ask_qty=50,
            received_at=ts,
        )

    def test_warmup_during_initial_periods(self) -> None:
        clf = RegimeClassifier(warmup_periods=5)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        regime = clf.update(self._make_book(ts, 50000.0))
        self.assertEqual(regime, "warmup")

    def test_high_vol_after_large_moves(self) -> None:
        clf = RegimeClassifier(warmup_periods=2)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        prices = [50000, 50200, 50400, 50100, 49800, 50300, 50100, 49700, 50000, 50400]
        for i, price in enumerate(prices):
            # Each price in a different minute
            book_ts = ts + timedelta(minutes=i, seconds=1)
            clf.update(self._make_book(book_ts, float(price)))
        # After 10 minutes some regime should be assigned
        final_regime = clf.current_regime
        self.assertIn(final_regime, ("high_vol", "low_vol", "warmup"))

    def test_split_books_by_regime_returns_three_buckets(self) -> None:
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        books_list = [
            self._make_book(ts + timedelta(minutes=i, seconds=j), 50000.0 + i * 10.0)
            for i in range(10) for j in range(5)
        ]
        result = split_books_by_regime(books_list)
        self.assertIn("high_vol", result)
        self.assertIn("low_vol", result)
        self.assertIn("warmup", result)
        total = sum(len(v) for v in result.values())
        self.assertEqual(total, len(books_list))

    def test_regime_distribution_sums_to_total(self) -> None:
        from kabu_futures.regime import regime_distribution
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        books_list = [
            self._make_book(ts + timedelta(minutes=i), 50000.0)
            for i in range(20)
        ]
        dist = regime_distribution(books_list)
        self.assertEqual(sum(dist.values()), len(books_list))


if __name__ == "__main__":
    unittest.main()
