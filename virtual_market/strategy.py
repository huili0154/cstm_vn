from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .engine import MatchingEngine
from .types import Order, OrderStatus, Side, Tick, Trade


@dataclass
class StrategyConfig:
    order_size: int = 1000
    lookback_ticks: int = 240
    buy_buffer: float = 0.0005
    sell_buffer: float = 0.0005
    cancel_after_ticks: int = 90
    price_tick: float = 0.001


class LowBuyHighSellStrategy:
    def __init__(self, engine: MatchingEngine, config: StrategyConfig | None = None) -> None:
        self.engine = engine
        self.config = config or StrategyConfig()
        self.window: deque[float] = deque(maxlen=self.config.lookback_ticks)
        self.position: int = 0
        self.active_order_id: str | None = None
        self.active_order_side: Side | None = None
        self.last_buy_price: float | None = None
        self.orders: dict[str, Order] = {}

    def on_tick(self, tick: Tick) -> None:
        self.window.append(float(tick.last_price))
        if len(self.window) < self.config.lookback_ticks:
            return
        if self.active_order_id:
            order = self.orders.get(self.active_order_id)
            if order and order.status in {OrderStatus.SUBMITTED, OrderStatus.PARTIAL} and order.age_ticks >= self.config.cancel_after_ticks:
                self.engine.cancel_order(order.order_id, reason="stale_order")
            return
        low = min(self.window)
        high = max(self.window)
        if self.position == 0:
            trigger = tick.last_price <= low * (1 + self.config.buy_buffer)
            if trigger:
                price = tick.bid_price_5 if tick.bid_price_5 > 0 else tick.bid_price_1 - self.config.price_tick
                price = max(price, self.config.price_tick)
                order_id = self.engine.submit_limit_order(Side.BUY, price, self.config.order_size)
                self.active_order_id = order_id
                self.active_order_side = Side.BUY
        else:
            trigger = tick.last_price >= high * (1 - self.config.sell_buffer)
            if trigger:
                price = tick.ask_price_5 if tick.ask_price_5 > 0 else tick.ask_price_1 + self.config.price_tick
                price = max(price, self.config.price_tick)
                order_id = self.engine.submit_limit_order(Side.SELL, price, self.position)
                self.active_order_id = order_id
                self.active_order_side = Side.SELL

    def on_order(self, order: Order) -> None:
        self.orders[order.order_id] = order
        if self.active_order_id != order.order_id:
            return
        if order.status in {OrderStatus.CANCELLED, OrderStatus.FILLED}:
            self.active_order_id = None
            self.active_order_side = None

    def on_trade(self, trade: Trade) -> None:
        if trade.side == Side.BUY:
            self.position += trade.volume
            self.last_buy_price = trade.price
        else:
            self.position -= trade.volume
