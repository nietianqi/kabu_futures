from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Iterable, Iterator

from .models import Level, OrderBook, Signal, SignalEvaluation, TradeTick
from .serialization import signal_to_dict


class MarketDataError(ValueError):
    pass


class MarketDataSkip(ValueError):
    """Recoverable market-data condition that should be logged as a skip, not an error."""

    def __init__(self, reason: str, message: str | None = None, metadata: dict[str, Any] | None = None) -> None:
        super().__init__(message or reason)
        self.reason = reason
        self.metadata = metadata or {}

    def to_payload(self, raw: str, received_at: datetime) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "message": str(self),
            "metadata": self.metadata,
            "raw": raw,
            "received_at": received_at.isoformat(),
        }


def _parse_time(value: Any, fallback: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        text = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
    return fallback or datetime.now(timezone.utc)


def _num(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_level(value: Any) -> Level | None:
    if not isinstance(value, dict):
        return None
    price = _num(value.get("Price") or value.get("price") or value.get("BidPrice") or value.get("AskPrice"))
    qty = _num(value.get("Qty") or value.get("qty") or value.get("Quantity") or value.get("quantity"))
    if price <= 0:
        return None
    return Level(price, qty)


class KabuBoardNormalizer:
    """Normalize kabu board/PUSH payloads into explicit bid/ask semantics.

    For stocks, futures, and options, kabu's English Bid/Ask field names are
    reversed versus common market-data usage: BidPrice is best sell quote and
    AskPrice is best buy quote. Buy/Sell depth names are already semantically
    correct and must not be swapped.
    """

    def __init__(self, symbol: str | None = None, symbol_aliases: dict[str, str] | None = None) -> None:
        self.symbol = symbol
        self.symbol_aliases = symbol_aliases or {}

    def normalize(self, payload: dict[str, Any], received_at: datetime | None = None) -> OrderBook:
        raw_symbol = str(payload.get("Symbol") or payload.get("symbol") or self.symbol or "")
        if not raw_symbol:
            raise MarketDataError("Payload does not include a symbol")
        symbol = self.symbol_aliases.get(raw_symbol, raw_symbol)

        # kabu futures/stocks/options: raw Bid* is sell quote, raw Ask* is buy quote.
        raw_sell_price = _num(payload.get("BidPrice") or payload.get("bid_price") or payload.get("BestAskPrice"))
        raw_sell_qty = _num(payload.get("BidQty") or payload.get("bid_qty") or payload.get("BestAskQty"))
        raw_buy_price = _num(payload.get("AskPrice") or payload.get("ask_price") or payload.get("BestBidPrice"))
        raw_buy_qty = _num(payload.get("AskQty") or payload.get("ask_qty") or payload.get("BestBidQty"))

        buy_levels = self._side_levels(payload, "Buy")
        sell_levels = self._side_levels(payload, "Sell")

        if raw_buy_price <= 0 and buy_levels:
            raw_buy_price, raw_buy_qty = buy_levels[0].price, buy_levels[0].qty
        if raw_sell_price <= 0 and sell_levels:
            raw_sell_price, raw_sell_qty = sell_levels[0].price, sell_levels[0].qty
        if raw_buy_price <= 0 or raw_sell_price <= 0:
            raise MarketDataError("Payload does not include valid bid/ask prices")

        if raw_sell_price <= raw_buy_price:
            reason = "locked_quote" if raw_sell_price == raw_buy_price else "crossed_quote"
            raise MarketDataSkip(
                reason,
                f"Invalid kabu quote: best sell must be greater than best buy "
                f"(raw BidPrice={raw_sell_price}, raw AskPrice={raw_buy_price})",
                {
                    "symbol": symbol,
                    "raw_symbol": raw_symbol,
                    "raw_bid_price": raw_sell_price,
                    "raw_ask_price": raw_buy_price,
                },
            )

        buy_levels = buy_levels or (Level(raw_buy_price, raw_buy_qty),)
        sell_levels = sell_levels or (Level(raw_sell_price, raw_sell_qty),)
        timestamp = _book_event_time(payload, received_at)
        book = OrderBook(
            symbol=symbol,
            timestamp=timestamp,
            best_bid_price=raw_buy_price,
            best_bid_qty=raw_buy_qty,
            best_ask_price=raw_sell_price,
            best_ask_qty=raw_sell_qty,
            buy_levels=buy_levels,
            sell_levels=sell_levels,
            last_price=_num(payload.get("CurrentPrice") or payload.get("last_price"), default=0.0) or None,
            volume=_num(payload.get("TradingVolume") or payload.get("volume"), default=0.0),
            received_at=received_at or datetime.now(timezone.utc),
            raw_symbol=raw_symbol if raw_symbol != symbol else None,
        )
        book.validate()
        return book

    def normalize_raw(self, raw: str, received_at: datetime | None = None) -> OrderBook:
        payload = json.loads(raw)
        return self.normalize(payload, received_at=received_at)

    def _levels(self, payload: dict[str, Any], prefixes: Iterable[str]) -> tuple[Level, ...]:
        levels: list[Level] = []
        for prefix in prefixes:
            levels.extend(self._side_levels(payload, prefix))
        return tuple(levels)

    def _side_levels(self, payload: dict[str, Any], prefix: str) -> tuple[Level, ...]:
        levels: list[Level] = []
        lower_prefix = prefix.lower()
        for idx in range(1, 11):
            item = payload.get(f"{prefix}{idx}")
            if item is None:
                item = payload.get(f"{lower_prefix}{idx}")
            level = _extract_level(item)
            if level is not None:
                levels.append(level)
        collection = payload.get(f"{prefix}Levels")
        if collection is None:
            collection = payload.get(f"{lower_prefix}_levels")
        if isinstance(collection, list):
            for item in collection:
                level = _extract_level(item)
                if level is not None:
                    levels.append(level)
        return tuple(levels)


class JsonlMarketRecorder:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, kind: str, payload: Any, force_flush: bool = False) -> None:
        row = {"kind": kind, "payload": self._serialize(payload)}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def write_book(self, book: OrderBook, force_flush: bool = False) -> None:
        self.write("book", book, force_flush=force_flush)

    def write_trade(self, trade: TradeTick, force_flush: bool = False) -> None:
        self.write("trade", trade, force_flush=force_flush)

    def write_signal(self, signal: Signal, force_flush: bool = False) -> None:
        self.write("signal", signal, force_flush=force_flush)

    def _serialize(self, payload: Any) -> Any:
        if isinstance(payload, OrderBook):
            return order_book_to_dict(payload)
        if isinstance(payload, Signal):
            return signal_to_dict(payload)
        if isinstance(payload, SignalEvaluation):
            return signal_evaluation_to_dict(payload)
        if isinstance(payload, TradeTick):
            return trade_tick_to_dict(payload)
        if isinstance(payload, Level):
            return level_to_dict(payload)
        if is_dataclass(payload):
            return asdict(payload)
        return payload


class BufferedJsonlMarketRecorder(JsonlMarketRecorder):
    """JSONL recorder optimized for live hot paths.

    The file handle stays open and rows are written in batches. Signals,
    errors, and shutdown records can request a forced flush so audit-critical
    events are not left in memory for long.
    """

    def __init__(self, path: str | Path, batch_size: int = 256, flush_interval_seconds: float = 1.0) -> None:
        super().__init__(path)
        self.batch_size = max(1, int(batch_size))
        self.flush_interval_seconds = max(0.0, float(flush_interval_seconds))
        self._buffer: list[str] = []
        self._handle = self.path.open("a", encoding="utf-8")
        self._last_flush = time.perf_counter()
        self._closed = False

    def write(self, kind: str, payload: Any, force_flush: bool = False) -> None:
        if self._closed:
            raise ValueError("Recorder is already closed")
        row = {"kind": kind, "payload": self._serialize(payload)}
        self._buffer.append(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        now = time.perf_counter()
        interval_elapsed = self.flush_interval_seconds == 0 or now - self._last_flush >= self.flush_interval_seconds
        if force_flush or len(self._buffer) >= self.batch_size or interval_elapsed:
            self.flush()

    def flush(self) -> None:
        if self._closed:
            return
        if self._buffer:
            self._handle.writelines(self._buffer)
            self._buffer.clear()
        self._handle.flush()
        self._last_flush = time.perf_counter()

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        self._handle.close()
        self._closed = True

    def __enter__(self) -> "BufferedJsonlMarketRecorder":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _book_event_time(payload: dict[str, Any], received_at: datetime | None) -> datetime:
    if received_at is not None:
        return received_at
    return _parse_time(
        payload.get("timestamp")
        or payload.get("BidTime")
        or payload.get("AskTime")
        or payload.get("CurrentPriceTime"),
        fallback=received_at,
    )


def level_to_dict(level: Level) -> dict[str, float]:
    return {"price": level.price, "qty": level.qty}


def order_book_to_dict(book: OrderBook) -> dict[str, Any]:
    return {
        "symbol": book.symbol,
        "raw_symbol": book.raw_symbol,
        "timestamp": _dt(book.timestamp),
        "best_bid_price": book.best_bid_price,
        "best_bid_qty": book.best_bid_qty,
        "best_ask_price": book.best_ask_price,
        "best_ask_qty": book.best_ask_qty,
        "buy_levels": [level_to_dict(level) for level in book.buy_levels],
        "sell_levels": [level_to_dict(level) for level in book.sell_levels],
        "last_price": book.last_price,
        "volume": book.volume,
        "received_at": _dt(book.received_at),
    }


def trade_tick_to_dict(trade: TradeTick) -> dict[str, Any]:
    return {
        "symbol": trade.symbol,
        "timestamp": _dt(trade.timestamp),
        "price": trade.price,
        "qty": trade.qty,
        "side": trade.side,
        "received_at": _dt(trade.received_at),
    }


def signal_evaluation_to_dict(evaluation: SignalEvaluation) -> dict[str, Any]:
    return {
        "engine": evaluation.engine,
        "symbol": evaluation.symbol,
        "timestamp": _dt(evaluation.timestamp),
        "decision": evaluation.decision,
        "reason": evaluation.reason,
        "candidate_direction": evaluation.candidate_direction,
        "metadata": evaluation.metadata,
    }


class KabuWebSocketStream:
    """Optional kabu WebSocket reader.

    Install `websocket-client` to use this class. It is intentionally separate
    from strategy logic so tests and replays do not require live dependencies.
    """

    def __init__(
        self,
        websocket_url: str,
        normalizer: KabuBoardNormalizer | None = None,
        *,
        ping_interval_seconds: float = 20.0,
        ping_timeout_seconds: float = 10.0,
        recv_timeout_seconds: float = 30.0,
    ) -> None:
        self.websocket_url = websocket_url
        self.normalizer = normalizer or KabuBoardNormalizer()
        self.ping_interval_seconds = ping_interval_seconds
        self.ping_timeout_seconds = ping_timeout_seconds
        self.recv_timeout_seconds = recv_timeout_seconds

    def iter_books(self) -> Iterator[OrderBook]:
        for raw, received_at in self.iter_raw():
            yield self.normalizer.normalize_raw(raw, received_at=received_at)

    def iter_raw(self) -> Iterator[tuple[str, datetime]]:
        try:
            import websocket  # type: ignore
        except ImportError as exc:
            raise RuntimeError("Install websocket-client to use KabuWebSocketStream") from exc
        ws = websocket.create_connection(self.websocket_url, timeout=self.recv_timeout_seconds)
        last_ping = time.monotonic()
        try:
            while True:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    self._send_ping(ws)
                    last_ping = time.monotonic()
                    continue
                now = time.monotonic()
                if self.ping_interval_seconds > 0 and now - last_ping >= self.ping_interval_seconds:
                    self._send_ping(ws)
                    last_ping = now
                received_at = datetime.now(timezone.utc)
                yield raw, received_at
        finally:
            ws.close()

    def _send_ping(self, ws: Any) -> None:
        if not hasattr(ws, "ping"):
            return
        if hasattr(ws, "settimeout"):
            ws.settimeout(self.ping_timeout_seconds)
        try:
            ws.ping()
        finally:
            if hasattr(ws, "settimeout"):
                ws.settimeout(self.recv_timeout_seconds)
