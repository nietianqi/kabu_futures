from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import pathlib
import sys
import unittest
import warnings

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
from kabu_futures.api import build_future_registration_symbols, extract_symbol_code
from kabu_futures.config import default_config, load_json_config
from kabu_futures.engine import DualStrategyEngine
from kabu_futures.indicators import BarBuilder
from kabu_futures.live import SignalEvaluationLogger, _should_print_tick, tick_to_dict, signal_to_dict
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
from kabu_futures.evolution import analyze_micro_log, calculate_markout_ticks
from kabu_futures.orders import KabuFutureOrderBuilder
from kabu_futures.paper_execution import PaperExecutionController
from kabu_futures.replay import read_recorded_books, replay_jsonl
from kabu_futures.risk import OrderThrottle
from kabu_futures.strategies import MicroStrategyEngine, MinuteStrategyEngine
from kabu_futures.tuning import evaluate_micro_config, evaluate_strategy_config, tune_micro_params, write_challenger_micro_engine
from kabu_futures.promotion import (
    PromotionThresholds,
    decision_to_dict,
    evaluate_challenger,
)
from kabu_futures.regime import RegimeClassifier
from kabu_futures.walk_forward import make_windows, walk_forward_micro


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


class ConfigTests(unittest.TestCase):
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

    def test_kabu_normalizer_rejects_crossed_kabu_quote(self) -> None:
        payload = {
            "Symbol": "NK225micro",
            "BidPrice": 50000,
            "BidQty": 10,
            "AskPrice": 50005,
            "AskQty": 20,
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        with self.assertRaises(MarketDataError):
            KabuBoardNormalizer().normalize(payload)

    def test_kabu_normalizer_skips_locked_quote_without_market_data_error(self) -> None:
        payload = {
            "Symbol": "NK225micro",
            "BidPrice": 50000,
            "BidQty": 10,
            "AskPrice": 50000,
            "AskQty": 20,
            "CurrentPriceTime": "2026-04-23T09:00:00+09:00",
        }
        with self.assertRaises(MarketDataSkip) as ctx:
            KabuBoardNormalizer().normalize(payload)
        self.assertEqual(ctx.exception.reason, "locked_quote")
        self.assertEqual(ctx.exception.metadata["raw_bid_price"], 50000)

    def test_live_signal_serialization_keeps_metadata(self) -> None:
        signal = Signal("risk", "NK225micro", "flat", 0.0, reason="market_data_error", metadata={"raw": "bad"})
        serialized = signal_to_dict(signal)
        self.assertEqual(serialized["reason"], "market_data_error")
        self.assertEqual(serialized["metadata"]["raw"], "bad")

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

    def test_signal_eval_summary_aggregates_consecutive_rejects(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            logger = SignalEvaluationLogger(recorder, "summary")
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            logger.write(SignalEvaluation("micro_book", "NK225micro", ts, "reject", "imbalance_not_met", "long", {"imbalance": 0.1}))
            logger.write(
                SignalEvaluation(
                    "micro_book",
                    "NK225micro",
                    ts + timedelta(seconds=1),
                    "reject",
                    "imbalance_not_met",
                    "long",
                    {"imbalance": 0.2},
                )
            )
            logger.flush()
            recorder.close()
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["kind"], "signal_eval_summary")
            self.assertEqual(rows[0]["payload"]["count"], 2)
            self.assertEqual(rows[0]["payload"]["reason"], "imbalance_not_met")
            self.assertEqual(rows[0]["payload"]["last_metadata"]["imbalance"], 0.2)

    def test_signal_eval_summary_preserves_allow_events(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            logger = SignalEvaluationLogger(recorder, "summary")
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            logger.write(SignalEvaluation("micro_book", "NK225micro", ts, "reject", "imbalance_not_met", "long", {}))
            logger.write(
                SignalEvaluation(
                    "micro_book",
                    "NK225micro",
                    ts + timedelta(seconds=1),
                    "allow",
                    "micro_book_long",
                    "long",
                    {"imbalance": 0.6},
                )
            )
            recorder.close()
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([row["kind"] for row in rows], ["signal_eval_summary", "signal_eval"])
            self.assertEqual(rows[1]["payload"]["decision"], "allow")
            self.assertEqual(rows[1]["payload"]["reason"], "micro_book_long")

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

    def test_read_recorded_books_closes_file_without_resource_warning(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", ResourceWarning)
                books = list(read_recorded_books(path))
            self.assertEqual(len(books), 1)
            self.assertFalse([warning for warning in caught if issubclass(warning.category, ResourceWarning)])

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

    def test_tune_micro_params_reports_no_change_when_sample_is_too_small(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            cfg = default_config()
            report = tune_micro_params(
                path,
                cfg,
                grid={
                    "imbalance_entry": (0.22,),
                    "microprice_entry_ticks": (cfg.micro_engine.microprice_entry_ticks,),
                    "take_profit_ticks": (cfg.micro_engine.take_profit_ticks,),
                    "stop_loss_ticks": (cfg.micro_engine.stop_loss_ticks,),
                },
                min_trades=1,
                top_n=3,
            )
            self.assertEqual(report["decision"], "no_change")
            self.assertEqual(report["books"], 1)
            self.assertEqual(report["parameter_trials"], 1)
            self.assertIn("baseline", report)

    def test_write_challenger_micro_engine_only_writes_recommended_payload(self) -> None:
        with TemporaryDirectory() as tmp:
            output = pathlib.Path(tmp) / "challenger.json"
            self.assertFalse(write_challenger_micro_engine({"decision": "no_change"}, output))
            self.assertFalse(output.exists())
            report = {
                "decision": "recommended",
                "recommended": {
                    "parameters": {
                        "imbalance_entry": 0.26,
                        "microprice_entry_ticks": 0.10,
                        "take_profit_ticks": 2,
                        "stop_loss_ticks": 3,
                    }
                },
            }
            self.assertTrue(write_challenger_micro_engine(report, output))
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(sorted(payload.keys()), ["micro_engine"])
            self.assertEqual(payload["micro_engine"]["imbalance_entry"], 0.26)

    def test_calculate_markout_ticks_handles_long_and_short(self) -> None:
        self.assertEqual(calculate_markout_ticks("long", 50005, 50015, 5), 2.0)
        self.assertEqual(calculate_markout_ticks("short", 50005, 49995, 5), 2.0)
        self.assertEqual(calculate_markout_ticks("flat", 50005, 50015, 5), 0.0)

    def test_analyze_micro_log_reports_trade_stats_and_markout(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=20, flush_interval_seconds=60.0)
            ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
            for idx in range(4):
                recorder.write_book(book(ts + timedelta(milliseconds=100 * idx), bid_qty=101, ask_qty=99))
            recorder.write_book(book(ts + timedelta(milliseconds=500), bid_qty=400, ask_qty=20))
            recorder.write_book(book(ts + timedelta(seconds=1), bid=50005, ask=50010, bid_qty=400, ask_qty=20))
            recorder.write_book(book(ts + timedelta(milliseconds=1500), bid=50010, ask=50015, bid_qty=400, ask_qty=20))
            recorder.close()

            base_cfg = default_config()
            cfg = replace(
                base_cfg,
                micro_engine=replace(
                    base_cfg.micro_engine,
                    imbalance_entry=0.20,
                    microprice_entry_ticks=0.05,
                    take_profit_ticks=1,
                    min_order_interval_seconds=0,
                ),
            )
            report = analyze_micro_log(path, cfg, markout_seconds=(0.5, 1.0))

            self.assertEqual(report["books"], 7)
            self.assertGreaterEqual(report["evaluations"]["decisions"].get("reject", 0), 1)
            self.assertEqual(report["paper"]["trades"], 1)
            self.assertEqual(report["paper"]["exit_reasons"]["take_profit"], 1)
            self.assertEqual(report["paper"]["by_engine"]["micro_book"]["trades"], 1)
            self.assertEqual(report["signals"]["by_reason"]["micro_book_long"], 1)
            self.assertEqual(report["markout"]["entries"], 1)
            self.assertEqual(report["markout"]["summary"]["0.5"]["count"], 1)
            self.assertEqual(report["markout"]["by_engine"]["micro_book"]["0.5"]["count"], 1)
            self.assertGreater(report["markout"]["summary"]["0.5"]["avg_ticks"], 0)
            self.assertEqual(report["metrics"]["trades"], 1)
            self.assertEqual(report["metrics"]["parameters"]["imbalance_entry"], 0.20)
            self.assertIn("2026-04-23T09:00:00+00:00", report["hourly"])

    def test_analyze_micro_log_handles_no_trades(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "market.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc), bid_qty=100, ask_qty=100))
            recorder.close()
            report = analyze_micro_log(path, default_config())
            self.assertEqual(report["books"], 1)
            self.assertEqual(report["paper"]["trades"], 0)
            self.assertEqual(report["markout"]["entries"], 0)
            self.assertEqual(report["markout"]["summary"], {})

    def test_analyze_micro_log_executes_minute_signal_without_non_micro_reject(self) -> None:
        class FakeEngine:
            def __init__(self, config: object) -> None:
                self.latest_signal_evaluations: list[SignalEvaluation] = []
                self.latest_book_features = None
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

        self.assertEqual(report["signals"]["by_reason"]["trend_pullback_long"], 1)
        self.assertEqual(report["paper"]["trades"], 1)
        self.assertEqual(report["paper"]["events"]["paper_entry"], 1)
        self.assertEqual(report["paper"]["events"]["paper_exit"], 1)
        self.assertEqual(report["paper"]["by_engine"]["minute_vwap"]["trades"], 1)
        self.assertEqual(report["markout"]["by_engine"]["minute_vwap"]["0.5"]["count"], 1)
        self.assertNotIn("execution_reject", report["paper"]["events"])
        self.assertEqual(report["paper"]["execution_reject_reasons"], {})

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
        self.assertGreater(events[0].pnl_ticks or 0, 0)
        self.assertGreater(events[0].pnl_yen or 0, 0)

    def test_paper_long_exits_on_stop_loss_with_negative_pnl(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50005))
        events = controller.on_book(book(ts + timedelta(seconds=2), bid=49990, ask=49995))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "stop_loss")
        self.assertLess(events[0].pnl_ticks or 0, 0)
        self.assertLess(events[0].pnl_yen or 0, 0)

    def test_paper_time_stop_exits_when_trade_is_not_profitable(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts, bid=50000, ask=50005))
        exit_ts = ts + timedelta(seconds=cfg.micro_engine.time_stop_seconds + 1)
        events = controller.on_book(book(exit_ts, bid=50005, ask=50010))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "time_stop")

    def test_minute_signal_creates_paper_entry(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0})
        events = controller.on_signal(signal, book(ts, bid=50000, ask=50005))
        self.assertEqual(events[0].event_type, "paper_entry")
        self.assertEqual(events[0].metadata["engine"], "minute_vwap")
        self.assertNotEqual(events[0].reason, "non_micro_signal")
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
        self.assertGreater(events[0].pnl_ticks or 0, 0)

    def test_minute_long_exits_on_stop_loss_with_negative_pnl(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0}), book(ts))
        events = controller.on_book(book(ts + timedelta(minutes=1), bid=49975, ask=49980))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "stop_loss")
        self.assertLess(events[0].pnl_ticks or 0, 0)

    def test_minute_position_exits_on_max_hold_minutes(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("directional_intraday", "NK225micro", "long", 0.8, 50005, "trend_continuation_long", {"atr": 30.0}), book(ts))
        exit_ts = ts + timedelta(minutes=cfg.minute_engine.max_hold_minutes + 1)
        events = controller.on_book(book(exit_ts, bid=50000, ask=50005))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "max_hold_minutes")

    def test_minute_breakeven_stop_after_profit_reaches_r(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_orb", "NK225micro", "long", 0.8, 50005, "orb_breakout_long", {"atr": 30.0}), book(ts))
        self.assertEqual(controller.on_book(book(ts + timedelta(minutes=1), bid=50035, ask=50040)), [])
        events = controller.on_book(book(ts + timedelta(minutes=2), bid=50005, ask=50010))
        self.assertEqual(events[0].event_type, "paper_exit")
        self.assertEqual(events[0].reason, "breakeven_stop")
        self.assertEqual(events[0].pnl_ticks, 0.0)

    def test_micro_signal_rejected_when_minute_position_exists(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0}), book(ts))
        events = controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts + timedelta(seconds=1)))
        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "already_has_position")

    def test_minute_signal_rejected_when_micro_position_exists(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        controller.on_signal(Signal("micro_book", "NK225micro", "long", 0.8, 50005), book(ts))
        signal = Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0})
        events = controller.on_signal(signal, book(ts + timedelta(seconds=1)))
        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "already_has_position")

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

    def test_paper_rejects_new_signal_when_position_exists(self) -> None:
        cfg = default_config()
        controller = PaperExecutionController(cfg, trade_mode="paper")
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        signal = Signal("micro_book", "NK225micro", "long", 0.8, 50005)
        controller.on_signal(signal, book(ts, bid=50000, ask=50005))
        events = controller.on_signal(signal, book(ts + timedelta(seconds=1), bid=50000, ask=50005))
        self.assertEqual(events[0].event_type, "execution_reject")
        self.assertEqual(events[0].reason, "already_has_position")

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


class ApiRegistrationTests(unittest.TestCase):
    def test_extract_symbol_code_requires_symbol(self) -> None:
        self.assertEqual(extract_symbol_code({"Symbol": "165120019"}, "NK225micro"), "165120019")

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


class TuningInvalidComboTests(unittest.TestCase):
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

            def heartbeat_metadata(self) -> dict[str, object]:
                return {"paper_pnl_yen": 0.0}

        cfg = default_config()
        with patch("kabu_futures.tuning.DualStrategyEngine", FakeEngine), patch(
            "kabu_futures.tuning.PaperExecutionController",
            FakeExecution,
        ):
            result = evaluate_micro_config(
                [book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc))],
                cfg,
                {"imbalance_entry": cfg.micro_engine.imbalance_entry},
            )
        self.assertEqual(result["trades"], 0)
        self.assertEqual(result["signals"]["minute_vwap:long:trend_pullback_long"], 1)

    def test_evaluate_strategy_config_executes_minute_signals(self) -> None:
        class FakeEngine:
            def __init__(self, config: object) -> None:
                self.latest_signal_evaluations: list[SignalEvaluation] = []
                self.latest_book_features = None
                self.calls = 0

            def on_order_book(self, order_book: OrderBook, now: datetime | None = None) -> list[Signal]:
                self.calls += 1
                if self.calls == 1:
                    return [Signal("minute_vwap", "NK225micro", "long", 0.8, 50005, "trend_pullback_long", {"atr": 30.0})]
                return []

        cfg = default_config()
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        with patch("kabu_futures.tuning.DualStrategyEngine", FakeEngine):
            result = evaluate_strategy_config(
                [
                    book(ts, bid=50000, ask=50005),
                    book(ts + timedelta(seconds=1), bid=50050, ask=50055),
                ],
                cfg,
                {"imbalance_entry": cfg.micro_engine.imbalance_entry},
            )
        self.assertEqual(result["trades"], 1)
        self.assertEqual(result["by_engine"]["minute_vwap"]["trades"], 1)
        self.assertEqual(result["paper_events"]["paper_entry"], 1)
        self.assertNotIn("non_micro_signal", result["execution_reject_reasons"])

    def test_tune_micro_params_skips_invalid_combo_without_crashing(self) -> None:
        # Invalid imbalance grid (some values <= imbalance_exit=0.10) should be skipped, not crash.
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "tiny.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            for second in range(3):
                recorder.write_book(book(datetime(2026, 4, 23, 9, 0, second, tzinfo=timezone.utc)))
            recorder.close()

            cfg = default_config()
            grid = {
                "imbalance_entry": (0.05, 0.30),  # 0.05 < imbalance_exit=0.10 -> invalid
                "microprice_entry_ticks": (0.15,),
                "take_profit_ticks": (2,),
                "stop_loss_ticks": (3,),
            }
            report = tune_micro_params(path, cfg, grid=grid, min_trades=1, max_books=10)
            self.assertIn("invalid_combos", report)
            self.assertGreater(len(report["invalid_combos"]), 0)
            self.assertEqual(report["decision"], "no_change")


class PromotionGateTests(unittest.TestCase):
    def test_promote_when_all_hard_gates_pass(self) -> None:
        champion = {
            "net_pnl_ticks": 10.0,
            "max_drawdown_ticks": 4.0,
            "trades": 200,
            "avg_pnl_ticks": 0.05,
            "avg_slippage_ticks": 0.5,
            "max_consecutive_losses": 3,
            "parameters": {"imbalance_entry": 0.30, "take_profit_ticks": 2},
        }
        challenger = {
            "net_pnl_ticks": 14.0,
            "max_drawdown_ticks": 4.5,
            "trades": 210,
            "avg_pnl_ticks": 0.067,
            "avg_markout_ticks_3s": 0.8,
            "avg_slippage_ticks": 0.5,
            "max_consecutive_losses": 3,
            "observation_days": 14,
            "parameters": {"imbalance_entry": 0.32, "take_profit_ticks": 2},
        }
        wf_summary = {"pass_rate": 0.75}
        result = evaluate_challenger(champion, challenger, walk_forward_summary=wf_summary)
        self.assertEqual(result.decision, "promote")
        self.assertIn("net_pnl_improvement", result.passed_gates)
        self.assertIn("walk_forward_stable", result.passed_gates)

    def test_reject_when_pnl_does_not_improve(self) -> None:
        champion = {"net_pnl_ticks": 10.0, "max_drawdown_ticks": 4.0, "trades": 200, "avg_pnl_ticks": 0.05}
        challenger = {
            "net_pnl_ticks": 8.0,  # worse
            "max_drawdown_ticks": 4.0,
            "trades": 210,
            "avg_pnl_ticks": 0.04,
            "observation_days": 14,
        }
        result = evaluate_challenger(champion, challenger)
        self.assertEqual(result.decision, "reject")
        self.assertTrue(any("net_pnl_delta" in failed for failed in result.failed_gates))

    def test_veto_when_parameter_jump_too_large(self) -> None:
        champion = {
            "net_pnl_ticks": 10.0,
            "max_drawdown_ticks": 4.0,
            "trades": 200,
            "avg_pnl_ticks": 0.05,
            "parameters": {"imbalance_entry": 0.30},
        }
        challenger = {
            "net_pnl_ticks": 14.0,
            "max_drawdown_ticks": 4.5,
            "trades": 210,
            "avg_pnl_ticks": 0.067,
            "observation_days": 14,
            "parameters": {"imbalance_entry": 0.10},  # 67% drop -> veto
        }
        result = evaluate_challenger(champion, challenger)
        self.assertEqual(result.decision, "reject")
        self.assertTrue(any("imbalance_entry" in v for v in result.veto_triggers))

    def test_hold_when_borderline(self) -> None:
        # Net PnL improves but a couple of secondary gates miss.
        champion = {"net_pnl_ticks": 10.0, "max_drawdown_ticks": 4.0, "trades": 300, "avg_pnl_ticks": 0.03}
        challenger = {
            "net_pnl_ticks": 11.0,
            "max_drawdown_ticks": 4.2,
            "trades": 50,  # below threshold given 14 days obs
            "avg_pnl_ticks": 0.04,
            "observation_days": 14,
            "avg_markout_ticks_3s": 0.2,  # below min markout threshold
            "parameters": {"imbalance_entry": 0.30},
        }
        result = evaluate_challenger(champion, challenger)
        self.assertIn(result.decision, ("hold", "reject"))
        self.assertGreater(len(result.failed_gates), 0)

    def test_decision_to_dict_is_json_friendly(self) -> None:
        result = evaluate_challenger(
            {"net_pnl_ticks": 10.0, "max_drawdown_ticks": 4.0, "trades": 100, "avg_pnl_ticks": 0.1},
            {"net_pnl_ticks": 12.0, "max_drawdown_ticks": 4.0, "trades": 150, "avg_pnl_ticks": 0.08, "observation_days": 14},
        )
        payload = decision_to_dict(result)
        json.dumps(payload)  # Should serialise without error
        self.assertIn("decision", payload)
        self.assertIsInstance(payload["passed_gates"], list)


class RegimeClassifierTests(unittest.TestCase):
    def test_warmup_returns_warmup_label(self) -> None:
        classifier = RegimeClassifier(window_seconds=60.0, warmup_samples=10, history_size=100)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        for i in range(5):
            label = classifier.update(book(ts + timedelta(seconds=i)))
            self.assertEqual(label, "warmup")

    def test_high_vol_detected_after_volatility_spike(self) -> None:
        classifier = RegimeClassifier(window_seconds=10.0, warmup_samples=5, history_size=100, high_vol_quantile=0.5)
        ts = datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)
        # Calm period: same prices
        for i in range(50):
            classifier.update(book(ts + timedelta(seconds=i), bid=50000, ask=50005))
        # Volatile spike: alternating prices
        labels: list[str] = []
        for i in range(50, 80):
            offset = 100 if i % 2 == 0 else -100
            labels.append(classifier.update(book(ts + timedelta(seconds=i), bid=50000 + offset, ask=50005 + offset)))
        self.assertIn("high_vol", labels)


class WalkForwardTests(unittest.TestCase):
    def test_make_windows_basic(self) -> None:
        days = ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25"]
        windows = make_windows(days, train_size=3, test_size=1, step=1)
        self.assertEqual(len(windows), 3)
        self.assertEqual(windows[0].train_days, ("2026-04-20", "2026-04-21", "2026-04-22"))
        self.assertEqual(windows[0].test_day, "2026-04-23")
        self.assertEqual(windows[0].test_days, ("2026-04-23",))
        self.assertEqual(windows[2].test_day, "2026-04-25")

    def test_make_windows_supports_multi_day_test_window(self) -> None:
        days = ["d1", "d2", "d3", "d4", "d5"]
        windows = make_windows(days, train_size=2, test_size=2, step=1)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0].train_days, ("d1", "d2"))
        self.assertEqual(windows[0].test_days, ("d3", "d4"))
        self.assertEqual(windows[1].test_days, ("d4", "d5"))

    def test_make_windows_step_two(self) -> None:
        days = ["d1", "d2", "d3", "d4", "d5", "d6", "d7"]
        windows = make_windows(days, train_size=3, step=2)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0].test_day, "d4")
        self.assertEqual(windows[1].test_day, "d6")

    def test_make_windows_validates_positive_args(self) -> None:
        with self.assertRaises(ValueError):
            make_windows(["d1", "d2"], train_size=0)

    def test_make_windows_returns_empty_when_not_enough_days(self) -> None:
        windows = make_windows(["d1", "d2"], train_size=5)
        self.assertEqual(windows, [])

    def test_walk_forward_accepts_split_logs_and_reports_insufficient_trades(self) -> None:
        with TemporaryDirectory() as tmp:
            first = pathlib.Path(tmp) / "live_20260423_090000.jsonl"
            second = pathlib.Path(tmp) / "live_20260424_090000.jsonl"
            recorder = BufferedJsonlMarketRecorder(first, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()
            recorder = BufferedJsonlMarketRecorder(second, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc)))
            recorder.close()

            report = walk_forward_micro([first, second], default_config(), train_size=1, test_size=1, min_trades=1)

        self.assertEqual(report["days"], ["2026-04-23", "2026-04-24"])
        self.assertEqual(report["summary"]["window_count"], 1)
        self.assertEqual(report["windows"][0]["train_days"], ["2026-04-23"])
        self.assertEqual(report["windows"][0]["test_days"], ["2026-04-24"])
        self.assertTrue(report["summary"]["insufficient_data"])
        self.assertIn("no_closed_test_trades", report["summary"]["diagnostics"])

    def test_walk_forward_reports_insufficient_days(self) -> None:
        with TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "live_20260423_090000.jsonl"
            recorder = BufferedJsonlMarketRecorder(path, batch_size=10, flush_interval_seconds=60.0)
            recorder.write_book(book(datetime(2026, 4, 23, 9, 0, tzinfo=timezone.utc)))
            recorder.close()

            report = walk_forward_micro(path, default_config(), train_size=1, test_size=1, min_trades=1)

        self.assertEqual(report["summary"]["window_count"], 0)
        self.assertTrue(report["summary"]["insufficient_data"])
        self.assertIn("insufficient_days_1_lt_required_2", report["summary"]["diagnostics"])


if __name__ == "__main__":
    unittest.main()
