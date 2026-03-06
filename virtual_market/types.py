from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class Tick:
    symbol: str
    timestamp: datetime
    last_price: float
    volume: float
    bid_price_1: float
    bid_volume_1: float
    ask_price_1: float
    ask_volume_1: float
    bid_price_5: float
    ask_price_5: float


@dataclass
class Order:
    order_id: str
    symbol: str
    side: Side
    price: float
    volume: int
    remaining: int
    created_at: datetime
    updated_at: datetime
    queue_ahead: float = 0.0
    status: OrderStatus = OrderStatus.SUBMITTED
    filled_avg_price: float = 0.0
    fill_count: int = 0
    age_ticks: int = 0


@dataclass(frozen=True)
class Trade:
    trade_id: str
    order_id: str
    symbol: str
    side: Side
    price: float
    volume: int
    timestamp: datetime


@dataclass
class EventLog:
    event_type: str
    timestamp: datetime
    payload: dict = field(default_factory=dict)
