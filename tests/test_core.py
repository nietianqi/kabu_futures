from __future__ import annotations

from datetime import datetime, timedelta, timezone
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from dataclasses import replace
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from kabu_futures.alpha import (
    NTRatioSpreadEngine,
    StrategyArbiter,
    USJapanLeadLagScorer,
    compute_micro225_per_topix_mini,
)
from kabu_futures.api import build_future_registration_symbols, extract_symbol_code
from kabu_futures.config import default_config
from kabu_futures.engine import DualStrategyEngine
from kabu_futures.indicators import BarBuilder
from kabu_futures.live import _should_print_tick, tick_to_dict, signal_to_dict
from kabu_futures.microstructure import BookFeatureEngine, RollingPercentile, microprice, percentile, weighted_imbalance
from kabu_futures.marketdata import BufferedJsonlMarketRecorder, KabuBoardNormalizer, MarketDataError, signal_evaluation_to_dict
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
from kabu_futures.strategies import MicroStrategyEngine, MinuteStrategyEngine


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
            books = read_recorded_books(path)
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
        self.assertEqual(events, [])
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
        self.assertTrue(any(signal.engine == "risk" and signal.reason == "execution_score_below_threshold" for signal in signals))


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


if __name__ == "__main__":
    unittest.main()
