"""
核心数据类型定义。

所有模块（matching / strategy / backtest / live）共用的数据结构。
TickData 字段对齐 dataset/ticks/ 的 Parquet schema（58列）。
BarData 字段对齐 dataset/daily/ 的 Parquet schema（18列）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


# ════════════════════════════════════════════════════
#  枚举
# ════════════════════════════════════════════════════

class Direction(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderStatus(Enum):
    SUBMITTING = "SUBMITTING"          # 已提交, 尚未参与撮合
    ACTIVE = "ACTIVE"                  # 挂单中（未成交）
    PART_TRADED = "PART_TRADED"        # 部分成交, 剩余仍在挂单
    ALL_TRADED = "ALL_TRADED"          # 全部成交
    CANCELLED = "CANCELLED"            # 已撤销（未成交）
    PART_CANCELLED = "PART_CANCELLED"  # 部分成交后撤销剩余
    REJECTED = "REJECTED"              # 被拒绝


class MatchingMode(Enum):
    CLOSE_FILL = "close_fill"
    """日线收盘价模式。策略在 on_day_begin 中以收盘价下单，全量即时成交。"""

    TICK_FILL = "tick_fill"
    """Tick 即时成交模式。价格满足 bid/ask_1 即全量成交，不考虑盘口量。"""

    SMART_TICK_DELAY_FILL = "smart_tick_delay_fill"
    """Tick 延迟深度撮合模式。延迟≥1tick、10档逐档匹配、部分成交、被动排队。"""


# ════════════════════════════════════════════════════
#  交易数据结构
# ════════════════════════════════════════════════════

@dataclass
class Order:
    order_id: str
    symbol: str
    direction: Direction
    order_type: OrderType
    price: float
    volume: int                        # 委托量（ETF 以"份"为单位）
    traded: int = 0                    # 已成交量
    status: OrderStatus = OrderStatus.SUBMITTING
    create_time: datetime | None = None
    update_time: datetime | None = None

    @property
    def remaining(self) -> int:
        """未成交量。"""
        return self.volume - self.traded

    @property
    def is_active(self) -> bool:
        """订单是否仍在活跃状态（可撮合或可撤销）。"""
        return self.status in (
            OrderStatus.SUBMITTING,
            OrderStatus.ACTIVE,
            OrderStatus.PART_TRADED,
        )


@dataclass
class Trade:
    trade_id: str
    order_id: str
    symbol: str
    direction: Direction
    price: float
    volume: int
    commission: float = 0.0
    datetime: datetime | None = None


@dataclass
class Position:
    symbol: str
    volume: int = 0                    # 总持仓量 (>= 0)
    frozen: int = 0                    # 冻结量（卖出挂单锁定）
    cost_price: float = 0.0            # 加权平均成本价
    market_price: float = 0.0          # 最新市价
    today_bought: int = 0              # 当日买入量（T+1 限制用）
    enable_t0: bool = False             # T+0 模式标记

    @property
    def available(self) -> int:
        """可用量。T+0 模式不扣当日买入。"""
        base = self.volume - self.frozen
        if not self.enable_t0:
            base -= self.today_bought
        return max(0, base)

    @property
    def market_value(self) -> float:
        return self.volume * self.market_price

    @property
    def pnl(self) -> float:
        """浮动盈亏。"""
        if self.volume == 0:
            return 0.0
        return self.volume * (self.market_price - self.cost_price)


@dataclass
class Account:
    balance: float = 0.0               # 总资产（含冻结）
    frozen: float = 0.0                # 冻结资金（买单锁定）
    commission: float = 0.0            # 累计手续费

    @property
    def available(self) -> float:
        """可用资金。"""
        return self.balance - self.frozen


# ════════════════════════════════════════════════════
#  行情数据
# ════════════════════════════════════════════════════

@dataclass
class TickData:
    """
    Tick 快照数据，字段对齐 dataset/ticks/ Parquet schema（58列）。

    盘口为 10 档（bid/ask × price/volume × 10）。
    volume / turnover 为本快照增量；cum_volume / cum_turnover 为当日累计。
    """
    symbol: str
    datetime: datetime

    last_price: float = 0.0
    cum_volume: int = 0
    cum_turnover: float = 0.0
    volume: float = 0.0                # 本快照增量（对应 Parquet 的 volume 列）
    turnover: float = 0.0              # 本快照增量

    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    pre_close: float = 0.0

    trades_count: int = 0
    bs_flag: str = ""
    trade_flag: str = ""
    iopv: int = 0                      # ETF 参考净值（整数，需 ÷10000）

    weighted_avg_ask_price: float = 0.0
    weighted_avg_bid_price: float = 0.0
    total_ask_volume: int = 0
    total_bid_volume: int = 0

    # 10 档盘口
    bid_price_1: float = 0.0
    ask_price_1: float = 0.0
    bid_volume_1: int = 0
    ask_volume_1: int = 0

    bid_price_2: float = 0.0
    ask_price_2: float = 0.0
    bid_volume_2: int = 0
    ask_volume_2: int = 0

    bid_price_3: float = 0.0
    ask_price_3: float = 0.0
    bid_volume_3: int = 0
    ask_volume_3: int = 0

    bid_price_4: float = 0.0
    ask_price_4: float = 0.0
    bid_volume_4: int = 0
    ask_volume_4: int = 0

    bid_price_5: float = 0.0
    ask_price_5: float = 0.0
    bid_volume_5: int = 0
    ask_volume_5: int = 0

    bid_price_6: float = 0.0
    ask_price_6: float = 0.0
    bid_volume_6: int = 0
    ask_volume_6: int = 0

    bid_price_7: float = 0.0
    ask_price_7: float = 0.0
    bid_volume_7: int = 0
    ask_volume_7: int = 0

    bid_price_8: float = 0.0
    ask_price_8: float = 0.0
    bid_volume_8: int = 0
    ask_volume_8: int = 0

    bid_price_9: float = 0.0
    ask_price_9: float = 0.0
    bid_volume_9: int = 0
    ask_volume_9: int = 0

    bid_price_10: float = 0.0
    ask_price_10: float = 0.0
    bid_volume_10: int = 0
    ask_volume_10: int = 0

    def ask_prices(self) -> list[float]:
        """返回卖盘 10 档价格列表（索引 0 = 卖一）。"""
        return [
            self.ask_price_1, self.ask_price_2, self.ask_price_3,
            self.ask_price_4, self.ask_price_5, self.ask_price_6,
            self.ask_price_7, self.ask_price_8, self.ask_price_9,
            self.ask_price_10,
        ]

    def ask_volumes(self) -> list[int]:
        """返回卖盘 10 档挂单量列表。"""
        return [
            self.ask_volume_1, self.ask_volume_2, self.ask_volume_3,
            self.ask_volume_4, self.ask_volume_5, self.ask_volume_6,
            self.ask_volume_7, self.ask_volume_8, self.ask_volume_9,
            self.ask_volume_10,
        ]

    def bid_prices(self) -> list[float]:
        """返回买盘 10 档价格列表（索引 0 = 买一）。"""
        return [
            self.bid_price_1, self.bid_price_2, self.bid_price_3,
            self.bid_price_4, self.bid_price_5, self.bid_price_6,
            self.bid_price_7, self.bid_price_8, self.bid_price_9,
            self.bid_price_10,
        ]

    def bid_volumes(self) -> list[int]:
        """返回买盘 10 档挂单量列表。"""
        return [
            self.bid_volume_1, self.bid_volume_2, self.bid_volume_3,
            self.bid_volume_4, self.bid_volume_5, self.bid_volume_6,
            self.bid_volume_7, self.bid_volume_8, self.bid_volume_9,
            self.bid_volume_10,
        ]


@dataclass
class BarData:
    """
    日线数据，字段对齐 dataset/daily/ Parquet schema。
    价格使用后复权值（open_bwd 等）以便直接计算收益。
    """
    symbol: str
    datetime: datetime                 # 交易日（时间部分为 00:00:00）

    open_price: float = 0.0            # 后复权开盘价（open_bwd）
    high_price: float = 0.0            # 后复权最高价
    low_price: float = 0.0             # 后复权最低价
    close_price: float = 0.0           # 后复权收盘价
    pre_close: float = 0.0             # 后复权前收盘价

    volume: float = 0.0                # 成交量
    turnover: float = 0.0              # 成交额

    # 未复权价格（可选，用于展示）
    raw_open: float = 0.0
    raw_high: float = 0.0
    raw_low: float = 0.0
    raw_close: float = 0.0

    adj_factor: float = 1.0            # 复权因子
    pct_chg: float = 0.0               # 涨跌幅 (%)
    name: str = ""                     # 品种名称
