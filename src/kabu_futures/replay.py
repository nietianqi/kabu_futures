from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .config import load_json_config, default_config
from .engine import DualStrategyEngine
from .models import Level, OrderBook
from .paper_execution import PaperExecutionController, TradeMode


def parse_book(row: dict[str, Any]) -> OrderBook:
    buy_levels = tuple(Level(float(item["price"]), float(item["qty"])) for item in row.get("buy_levels", []))
    sell_levels = tuple(Level(float(item["price"]), float(item["qty"])) for item in row.get("sell_levels", []))
    received_at = row.get("received_at")
    return OrderBook(
        symbol=str(row["symbol"]),
        timestamp=datetime.fromisoformat(str(row["timestamp"])),
        best_bid_price=float(row["best_bid_price"]),
        best_bid_qty=float(row["best_bid_qty"]),
        best_ask_price=float(row["best_ask_price"]),
        best_ask_qty=float(row["best_ask_qty"]),
        buy_levels=buy_levels,
        sell_levels=sell_levels,
        last_price=float(row["last_price"]) if row.get("last_price") is not None else None,
        volume=float(row.get("volume", 0.0)),
        received_at=datetime.fromisoformat(str(received_at)) if received_at else None,
        raw_symbol=str(row["raw_symbol"]) if row.get("raw_symbol") else None,
    )


def replay_jsonl(
    path: str | Path,
    config_path: str | Path | None = None,
    trade_mode: TradeMode = "observe",
) -> list[dict[str, Any]]:
    config = load_json_config(config_path) if config_path else default_config()
    engine = DualStrategyEngine(config)
    execution = PaperExecutionController(config, trade_mode=trade_mode)
    emitted: list[dict[str, Any]] = []
    for book in read_recorded_books(path):
        signals = engine.on_order_book(book)
        for event in execution.on_book(book, engine.latest_book_features):
            emitted.append(event.to_dict())
        for signal in signals:
            event = {
                "event": "signal",
                "timestamp": book.timestamp.isoformat(),
                "engine": signal.engine,
                "symbol": signal.symbol,
                "direction": signal.direction,
                "confidence": signal.confidence,
                "reason": signal.reason,
                "metadata": signal.metadata,
            }
            emitted.append(event)
            for execution_event in execution.on_signal(signal, book):
                emitted.append(execution_event.to_dict())
    if trade_mode == "paper":
        emitted.append({"event": "paper_summary", **execution.heartbeat_metadata()})
    return emitted


def read_recorded_books(path: str | Path) -> list[OrderBook]:
    books: list[OrderBook] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        kind = row.get("kind")
        if kind == "book":
            payload = row["payload"]
            payload["timestamp"] = payload["timestamp"]
            books.append(parse_book(payload))
        elif kind is None:
            books.append(parse_book(row))
    return books
