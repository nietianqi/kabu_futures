from __future__ import annotations

from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, Iterator

from .config import load_json_config, default_config
from .engine import DualStrategyEngine
from .models import Level, OrderBook
from .paper_execution import PaperExecutionController, TradeMode

_log = logging.getLogger(__name__)


def parse_book(row: dict[str, Any]) -> OrderBook:
    buy_levels = tuple(Level(float(item["price"]), float(item["qty"])) for item in row.get("buy_levels", []))
    sell_levels = tuple(Level(float(item["price"]), float(item["qty"])) for item in row.get("sell_levels", []))
    received_at = row.get("received_at")
    book = OrderBook(
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
    book.validate()
    return book


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


def read_recorded_books(path: str | Path) -> Iterator[OrderBook]:
    """Yield OrderBook objects from a JSONL file line by line (no full-file load)."""
    for line in Path(path).open(encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            _log.warning("Skipping malformed JSONL line: %s", exc)
            continue
        kind = row.get("kind")
        try:
            if kind == "book":
                yield parse_book(row["payload"])
            elif kind is None:
                yield parse_book(row)
        except (ValueError, KeyError) as exc:
            _log.warning("Skipping invalid book record: %s", exc)
