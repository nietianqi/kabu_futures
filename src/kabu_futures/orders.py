from __future__ import annotations

from dataclasses import dataclass

from .models import OrderIntent


class KabuConstants:
    TRADE_NEW = 1
    TRADE_CLOSE = 2
    TIF_FAS = 1
    TIF_FAK = 2
    TIF_FOK = 3
    SIDE_SELL = "1"
    SIDE_BUY = "2"
    FRONT_LIMIT = 20
    FRONT_STOP = 30
    FRONT_MARKET = 120
    AFTER_HIT_MARKET = 1
    AFTER_HIT_LIMIT = 2
    UNDER_OR_EQUAL = 1
    OVER_OR_EQUAL = 2


class KabuFutureOrderBuilder:
    def new_limit(self, symbol: str, exchange: int, direction: str, qty: int, price: float, tif: int = 2) -> OrderIntent:
        side = KabuConstants.SIDE_BUY if direction == "long" else KabuConstants.SIDE_SELL
        return OrderIntent(
            symbol=symbol,
            exchange=exchange,
            trade_type=KabuConstants.TRADE_NEW,
            side=side,
            qty=qty,
            front_order_type=KabuConstants.FRONT_LIMIT,
            price=price,
            time_in_force=tif,
        )

    def close_limit(
        self,
        symbol: str,
        exchange: int,
        position_direction: str,
        qty: int,
        price: float,
        hold_id: str,
        tif: int = 1,
    ) -> OrderIntent:
        side = KabuConstants.SIDE_SELL if position_direction == "long" else KabuConstants.SIDE_BUY
        return OrderIntent(
            symbol=symbol,
            exchange=exchange,
            trade_type=KabuConstants.TRADE_CLOSE,
            side=side,
            qty=qty,
            front_order_type=KabuConstants.FRONT_LIMIT,
            price=price,
            time_in_force=tif,
            close_positions=({"HoldID": hold_id, "Qty": qty},),
        )

    def close_aggressive_limit(
        self,
        symbol: str,
        exchange: int,
        position_direction: str,
        qty: int,
        price: float,
        hold_id: str | None = None,
        tif: int = 2,
    ) -> OrderIntent:
        side = KabuConstants.SIDE_SELL if position_direction == "long" else KabuConstants.SIDE_BUY
        close_positions = ({"HoldID": hold_id, "Qty": qty},) if hold_id else ()
        return OrderIntent(
            symbol=symbol,
            exchange=exchange,
            trade_type=KabuConstants.TRADE_CLOSE,
            side=side,
            qty=qty,
            front_order_type=KabuConstants.FRONT_LIMIT,
            price=price,
            time_in_force=tif,
            close_position_order=None if hold_id else 0,
            close_positions=close_positions,
        )

    def close_market(
        self,
        symbol: str,
        exchange: int,
        position_direction: str,
        qty: int,
        hold_id: str | None = None,
    ) -> OrderIntent:
        side = KabuConstants.SIDE_SELL if position_direction == "long" else KabuConstants.SIDE_BUY
        close_positions = ({"HoldID": hold_id, "Qty": qty},) if hold_id else ()
        return OrderIntent(
            symbol=symbol,
            exchange=exchange,
            trade_type=KabuConstants.TRADE_CLOSE,
            side=side,
            qty=qty,
            front_order_type=KabuConstants.FRONT_MARKET,
            price=0,
            time_in_force=KabuConstants.TIF_FAK,
            close_position_order=None if hold_id else 0,
            close_positions=close_positions,
        )

    def close_stop_market(
        self,
        symbol: str,
        exchange: int,
        position_direction: str,
        qty: int,
        trigger_price: float,
        hold_id: str,
    ) -> OrderIntent:
        side = KabuConstants.SIDE_SELL if position_direction == "long" else KabuConstants.SIDE_BUY
        under_over = KabuConstants.UNDER_OR_EQUAL if position_direction == "long" else KabuConstants.OVER_OR_EQUAL
        return OrderIntent(
            symbol=symbol,
            exchange=exchange,
            trade_type=KabuConstants.TRADE_CLOSE,
            side=side,
            qty=qty,
            front_order_type=KabuConstants.FRONT_STOP,
            price=0,
            time_in_force=KabuConstants.TIF_FAK,
            close_positions=({"HoldID": hold_id, "Qty": qty},),
            reverse_limit_order={
                "TriggerPrice": trigger_price,
                "UnderOver": under_over,
                "AfterHitOrderType": KabuConstants.AFTER_HIT_MARKET,
                "AfterHitPrice": 0,
            },
        )


@dataclass
class SyntheticOCOState:
    take_profit_order_id: str | None = None
    stop_order_id: str | None = None
    active: bool = False

    def mark_submitted(self, take_profit_order_id: str, stop_order_id: str) -> None:
        self.take_profit_order_id = take_profit_order_id
        self.stop_order_id = stop_order_id
        self.active = True

    def counterpart_to_cancel(self, filled_order_id: str) -> str | None:
        if filled_order_id == self.take_profit_order_id:
            return self.stop_order_id
        if filled_order_id == self.stop_order_id:
            return self.take_profit_order_id
        return None

    def mark_closed(self) -> None:
        self.take_profit_order_id = None
        self.stop_order_id = None
        self.active = False
