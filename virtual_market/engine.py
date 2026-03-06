from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Callable

from .types import EventLog, Order, OrderStatus, Side, Tick, Trade


class MatchingEngine:
    def __init__(self, symbol: str, max_fill_per_tick: int = 300) -> None:
        self.symbol = symbol
        self.max_fill_per_tick = max_fill_per_tick
        self.current_tick: Tick | None = None
        self.current_time: datetime | None = None
        self.order_seq = 0
        self.trade_seq = 0
        self.orders: dict[str, Order] = {}
        self.active_order_ids: list[str] = []
        self.events: list[EventLog] = []
        self.on_order: Callable[[Order], None] | None = None
        self.on_trade: Callable[[Trade], None] | None = None

    def bind_callbacks(
        self,
        on_order: Callable[[Order], None] | None,
        on_trade: Callable[[Trade], None] | None,
    ) -> None:
        self.on_order = on_order
        self.on_trade = on_trade

    def submit_limit_order(self, side: Side, price: float, volume: int) -> str:
        if self.current_tick is None or self.current_time is None:
            raise RuntimeError("engine_not_ready")
        self.order_seq += 1
        order_id = f"O{self.order_seq:08d}"
        queue_ahead = self._initial_queue_ahead(self.current_tick, side, price)
        order = Order(
            order_id=order_id,
            symbol=self.symbol,
            side=side,
            price=price,
            volume=volume,
            remaining=volume,
            created_at=self.current_time,
            updated_at=self.current_time,
            queue_ahead=queue_ahead,
        )
        self.orders[order_id] = order
        self.active_order_ids.append(order_id)
        self._push_event("ORDER_SUBMITTED", {"order_id": order_id, "side": side.value, "price": price, "volume": volume, "queue_ahead": queue_ahead})
        self._emit_order(order)
        return order_id

    def cancel_order(self, order_id: str, reason: str = "user_cancel") -> bool:
        order = self.orders.get(order_id)
        if order is None:
            return False
        if order.status in {OrderStatus.CANCELLED, OrderStatus.FILLED}:
            return False
        order.status = OrderStatus.CANCELLED
        if self.current_time is not None:
            order.updated_at = self.current_time
        if order_id in self.active_order_ids:
            self.active_order_ids.remove(order_id)
        self._push_event("ORDER_CANCELLED", {"order_id": order_id, "reason": reason, "remaining": order.remaining})
        self._emit_order(order)
        return True

    def process_tick(self, tick: Tick) -> None:
        self.current_tick = tick
        self.current_time = tick.timestamp
        self._push_event("TICK", {"last_price": tick.last_price, "bid1": tick.bid_price_1, "ask1": tick.ask_price_1, "volume": tick.volume})
        for order_id in list(self.active_order_ids):
            order = self.orders[order_id]
            if order.status in {OrderStatus.FILLED, OrderStatus.CANCELLED}:
                self.active_order_ids.remove(order_id)
                continue
            order.age_ticks += 1
            self._try_match(order, tick)
            if order.status in {OrderStatus.FILLED, OrderStatus.CANCELLED} and order_id in self.active_order_ids:
                self.active_order_ids.remove(order_id)

    def _try_match(self, order: Order, tick: Tick) -> None:
        if order.side == Side.BUY:
            crossing = tick.ask_price_1 > 0 and order.price >= tick.ask_price_1
            book_volume = max(float(tick.ask_volume_1), 1.0)
            match_price = tick.ask_price_1 if tick.ask_price_1 > 0 else order.price
        else:
            crossing = tick.bid_price_1 > 0 and order.price <= tick.bid_price_1
            book_volume = max(float(tick.bid_volume_1), 1.0)
            match_price = tick.bid_price_1 if tick.bid_price_1 > 0 else order.price
        if not crossing:
            return
        consume = max(float(tick.volume), 1.0)
        if order.queue_ahead > 0:
            order.queue_ahead = max(order.queue_ahead - consume, 0.0)
            self._push_event("QUEUE_PROGRESS", {"order_id": order.order_id, "queue_ahead": order.queue_ahead, "consume": consume})
            if order.queue_ahead > 0:
                return
        allowed = min(order.remaining, self.max_fill_per_tick, int(max(1.0, min(book_volume, consume))))
        if allowed <= 0:
            return
        self.trade_seq += 1
        trade = Trade(
            trade_id=f"T{self.trade_seq:08d}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            price=float(match_price),
            volume=int(allowed),
            timestamp=tick.timestamp,
        )
        order.remaining -= allowed
        order.updated_at = tick.timestamp
        order.fill_count += 1
        total_filled = order.volume - order.remaining
        if total_filled > 0:
            order.filled_avg_price = ((order.filled_avg_price * (total_filled - allowed)) + trade.price * allowed) / total_filled
        if order.remaining == 0:
            order.status = OrderStatus.FILLED
        else:
            order.status = OrderStatus.PARTIAL
        self._push_event("TRADE_FILLED", {"trade_id": trade.trade_id, "order_id": order.order_id, "price": trade.price, "volume": trade.volume, "remaining": order.remaining, "status": order.status.value})
        self._emit_trade(trade)
        self._emit_order(order)

    def _initial_queue_ahead(self, tick: Tick, side: Side, price: float) -> float:
        eps = 1e-6
        if side == Side.BUY and abs(price - tick.bid_price_1) <= eps:
            return max(float(tick.bid_volume_1), 0.0)
        if side == Side.SELL and abs(price - tick.ask_price_1) <= eps:
            return max(float(tick.ask_volume_1), 0.0)
        return 0.0

    def _emit_order(self, order: Order) -> None:
        if self.on_order:
            self.on_order(order)

    def _emit_trade(self, trade: Trade) -> None:
        if self.on_trade:
            self.on_trade(trade)

    def _push_event(self, event_type: str, payload: dict) -> None:
        if self.current_time is None:
            return
        self.events.append(EventLog(event_type=event_type, timestamp=self.current_time, payload=payload))

    def export_events(self) -> list[dict]:
        out: list[dict] = []
        for e in self.events:
            item = asdict(e)
            item["timestamp"] = e.timestamp.isoformat()
            out.append(item)
        return out
