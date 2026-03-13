# 策略接口设计文档 (core/strategy.py)

## 1. 概述

策略接口是回测和实盘共用的统一 API。一份策略代码, 不做任何修改, 可以同时运行在两种环境:
- **回测**: `backtest/engine.py` 驱动, 从 Parquet 回放数据, `core/matching.py` 撮合
- **实盘**: `live/engine.py` 驱动, 从 mini-qmt 接收行情, 通过 `qmt_gateway.py` 下单

**三种撮合模式均使用同一个策略接口**:
- `close_fill`: 只调 `on_day_begin(bar)`
- `tick_fill` / `smart_tick_delay_fill`: 每日开始调 `on_day_begin(bar)` + 每个 tick 调 `on_tick(tick)`
- **实盘**: 与 tick 模式相同的回调, 策略代码零修改

## 2. vnpy 策略架构分析

### 2.1 vnpy 的策略基类 CtaTemplate

```python
class CtaTemplate(ABC):
    # 类级别声明
    author: str = ""
    parameters: list = []    # 可配置参数名列表
    variables: list = []     # 可监控变量名列表

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        self.cta_engine = cta_engine   # 引擎引用 (backtest 或 live)
        self.strategy_name = strategy_name
        self.vt_symbol = vt_symbol
        self.inited: bool = False
        self.trading: bool = False
        self.pos: float = 0            # 净持仓

    # 生命周期回调 (策略需要实现)
    def on_init(self) -> None: ...     # @abstractmethod
    def on_start(self) -> None: ...
    def on_stop(self) -> None: ...

    # 行情回调 (策略选择实现)
    def on_tick(self, tick: TickData) -> None: ...
    def on_bar(self, bar: BarData) -> None: ...

    # 成交回调
    def on_trade(self, trade: TradeData) -> None: ...
    def on_order(self, order: OrderData) -> None: ...

    # 下单接口 (已实现，策略直接调用)
    def buy(price, volume, stop=False) -> list[str]:  # 买开
    def sell(price, volume, stop=False) -> list[str]:  # 卖平
    def short(price, volume, stop=False) -> list[str]: # 卖开
    def cover(price, volume, stop=False) -> list[str]: # 买平
    def cancel_order(vt_orderid: str) -> None:
    def cancel_all() -> None:

    # 辅助接口
    def write_log(msg: str) -> None:
    def get_engine_type() -> EngineType:
    def get_pricetick() -> float:
    def load_bar(days, interval, callback) -> None:
    def load_tick(days) -> None:
```

### 2.2 vnpy 如何实现"一份策略两处运行"

核心机制：**策略不直接操作引擎，所有操作通过 `self.cta_engine` 代理**。

```
策略调用 self.buy(price, volume)
  → CtaTemplate.send_order(direction=LONG, offset=OPEN, price, volume)
    → self.cta_engine.send_order(self, direction, offset, price, volume, stop, lock, net)
      ├── 如果 engine 是 BacktestingEngine → 创建本地 OrderData, 等待 cross_limit_order 撮合
      └── 如果 engine 是 LiveCtaEngine    → 通过 gateway 发送真实订单
```

**关键设计**：`cta_engine` 是一个"鸭子类型"接口 — BacktestingEngine 和 LiveCtaEngine 实现相同的方法签名：
- `send_order(strategy, direction, offset, price, volume, stop, lock, net) -> list`
- `cancel_order(strategy, vt_orderid) -> None`
- `cancel_all(strategy) -> None`
- `write_log(msg, strategy) -> None`
- `get_engine_type() -> EngineType`
- `get_pricetick(strategy) -> float`
- `load_bar(vt_symbol, days, interval, callback, use_database) -> list`
- `load_tick(vt_symbol, days, callback) -> list`

### 2.3 vnpy 调用序列

**回测 (BacktestingEngine.run_backtesting):**
```
for data in history_data:
    engine.cross_limit_order()    # 先撮合
    engine.cross_stop_order()
    strategy.on_bar(bar)          # 再推送行情

    # 策略在 on_bar 中可以调用 buy/sell/cancel
    # 新提交的订单会在下一个 bar 时被撮合
```

**实盘 (LiveCtaEngine):**
```
行情推送 → EventEngine 触发 → strategy.on_tick(tick) → 策略调用 buy/sell
  → gateway.send_order() → 券商执行
  → gateway 收到回报 → EventEngine → strategy.on_order/on_trade
```

## 3. 我们的策略接口设计

### 3.1 设计原则

1. **ETF 交易简化**：ETF 无开平仓概念, 统一用 BUY/SELL, 去掉 Offset
2. **去掉 stop order**：停止单用策略层逻辑实现, 撮合引擎不处理
3. **保持 vnpy 的 engine 代理模式**：策略通过 `self._engine` 间接操作
4. **显式接口**：用 ABC 抽象基类, 不用鸭子类型
5. **净持仓模型**：ETF 只有多头, pos >= 0 (可以为 0)

### 3.2 核心数据类型 (core/datatypes.py)

```python
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Direction(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderStatus(Enum):
    SUBMITTING = "SUBMITTING"    # 已提交, 等待撮合
    ACTIVE = "ACTIVE"            # 挂单中（未成交或部分成交）
    PART_TRADED = "PART_TRADED"  # 部分成交，剩余仍在挂单
    ALL_TRADED = "ALL_TRADED"    # 全部成交
    CANCELLED = "CANCELLED"      # 已撤销（未成交）
    PART_CANCELLED = "PART_CANCELLED"  # 部分成交后撤销剩余
    REJECTED = "REJECTED"        # 被拒绝


@dataclass
class Order:
    order_id: str
    symbol: str
    direction: Direction
    order_type: OrderType
    price: float
    volume: int                  # ETF 以"份"为单位, 整数
    traded: int = 0              # 已成交量
    status: OrderStatus = OrderStatus.SUBMITTING
    create_time: datetime | None = None
    update_time: datetime | None = None


@dataclass
class Trade:
    trade_id: str
    order_id: str
    symbol: str
    direction: Direction
    price: float
    volume: int
    commission: float = 0.0      # 本笔手续费
    datetime: datetime | None = None


@dataclass
class Position:
    symbol: str
    volume: int = 0              # 持仓量 (>= 0)
    frozen: int = 0              # 冻结量 (卖出挂单锁定)
    cost_price: float = 0.0      # 持仓成本价
    market_price: float = 0.0    # 最新市价
    pnl: float = 0.0             # 浮动盈亏

    @property
    def available(self) -> int:
        return self.volume - self.frozen


@dataclass
class Account:
    balance: float = 0.0         # 总资产
    frozen: float = 0.0          # 冻结资金 (买单锁定)
    commission: float = 0.0      # 累计手续费

    @property
    def available(self) -> float:
        return self.balance - self.frozen


@dataclass
class TickData:
    """
    对齐现有 Parquet schema (52 列)。
    """
    symbol: str
    datetime: datetime
    last_price: float = 0.0
    volume: int = 0
    turnover: float = 0.0
    cum_volume: int = 0
    cum_turnover: float = 0.0

    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    pre_close: float = 0.0

    iopv: float = 0.0            # ETF 参考净值

    # 10 档盘口 (我们的数据优势, vnpy 只有 5 档)
    bid_price_1: float = 0.0
    bid_price_2: float = 0.0
    bid_price_3: float = 0.0
    bid_price_4: float = 0.0
    bid_price_5: float = 0.0
    bid_price_6: float = 0.0
    bid_price_7: float = 0.0
    bid_price_8: float = 0.0
    bid_price_9: float = 0.0
    bid_price_10: float = 0.0

    ask_price_1: float = 0.0
    ask_price_2: float = 0.0
    ask_price_3: float = 0.0
    ask_price_4: float = 0.0
    ask_price_5: float = 0.0
    ask_price_6: float = 0.0
    ask_price_7: float = 0.0
    ask_price_8: float = 0.0
    ask_price_9: float = 0.0
    ask_price_10: float = 0.0

    bid_volume_1: int = 0
    bid_volume_2: int = 0
    bid_volume_3: int = 0
    bid_volume_4: int = 0
    bid_volume_5: int = 0
    bid_volume_6: int = 0
    bid_volume_7: int = 0
    bid_volume_8: int = 0
    bid_volume_9: int = 0
    bid_volume_10: int = 0

    ask_volume_1: int = 0
    ask_volume_2: int = 0
    ask_volume_3: int = 0
    ask_volume_4: int = 0
    ask_volume_5: int = 0
    ask_volume_6: int = 0
    ask_volume_7: int = 0
    ask_volume_8: int = 0
    ask_volume_9: int = 0
    ask_volume_10: int = 0


@dataclass
class BarData:
    """日线数据。"""
    symbol: str
    datetime: datetime
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    volume: int = 0
    turnover: float = 0.0
```

### 3.3 引擎接口 (Engine Protocol)

回测引擎和实盘引擎必须实现以下接口:

```python
from abc import ABC, abstractmethod

class EngineBase(ABC):
    """回测引擎和实盘引擎的公共接口。"""

    @abstractmethod
    def send_order(
        self,
        strategy: "StrategyBase",
        symbol: str,
        direction: Direction,
        order_type: OrderType,
        price: float,
        volume: int,
    ) -> str:
        """提交订单, 返回 order_id。"""
        ...

    @abstractmethod
    def cancel_order(self, strategy: "StrategyBase", order_id: str) -> None:
        """撤销订单。"""
        ...

    @abstractmethod
    def cancel_all(self, strategy: "StrategyBase") -> None:
        """撤销策略的所有活跃订单。"""
        ...

    @abstractmethod
    def get_pending_orders(
        self, strategy: "StrategyBase", symbol: str = ""
    ) -> list:
        """查询未完成委托列表。可选按 symbol 过滤。"""
        ...

    @abstractmethod
    def get_position(self, strategy: "StrategyBase", symbol: str) -> Position:
        """查询持仓。"""
        ...

    @abstractmethod
    def get_account(self, strategy: "StrategyBase") -> Account:
        """查询账户。"""
        ...

    @abstractmethod
    def write_log(self, msg: str, strategy: "StrategyBase") -> None:
        """记录日志。"""
        ...

    @abstractmethod
    def get_pricetick(self, strategy: "StrategyBase", symbol: str) -> float:
        """查询最小价格变动。"""
        ...

    @abstractmethod
    def get_latest_tick(self, strategy: "StrategyBase", symbol: str) -> TickData | None:
        """
        查询指定品种的最新 tick 快照。

        - 只能查询 strategy.symbols 中声明的品种。
        - 如果 symbol 不在关注列表中 → 抛出 ValueError。
        - 如果该品种当天尚无 tick 数据 → 返回 None。
        """
        ...
```

### 3.4 策略基类 (core/strategy.py)

```python
from abc import ABC, abstractmethod

class StrategyBase(ABC):
    """
    策略基类。回测和实盘共用，一份代码两处运行。

    用法:
        class MyStrategy(StrategyBase):
            fast_period: int = 10   # 声明为类变量 = 可配置参数
            slow_period: int = 20

            parameters = ["fast_period", "slow_period"]
            variables = ["fast_ma", "slow_ma"]

            def on_init(self):
                self.fast_ma = 0.0
                self.slow_ma = 0.0

            def on_tick(self, tick: TickData):
                ...  # 策略逻辑
    """

    # 子类声明
    author: str = ""
    parameters: list[str] = []    # 可配置参数名列表
    variables: list[str] = []     # 可监控变量名列表

    def __init__(
        self,
        engine: EngineBase,
        strategy_name: str,
        symbols: list[str],       # 关注品种列表, symbols[0] 为主驱动品种
        setting: dict | None = None,
    ) -> None:
        self._engine = engine
        self.strategy_name = strategy_name
        self.symbols = symbols     # 引擎据此预加载数据和跟踪 latest_tick
        self.inited: bool = False
        self.trading: bool = False

        if setting:
            self.update_setting(setting)

    def update_setting(self, setting: dict) -> None:
        """
        从 dict 更新 parameters 中声明的可配置参数。

        支持的参数类型: int, float, str, bool, list, dict 等任意可序列化值。
        参数名必须先在子类中声明为类变量(含默认值), 并列入 parameters 列表,
        才能通过 setting 覆盖。symbols 也可通过 setting 传入以覆盖构造函数的值。

        示例::
            strategy = MyStrategy(engine, "test", ["510300.SH"],
                                  setting={"fast_period": 3, "threshold": 0.02})
        """
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    # ==================== 生命周期回调 ====================

    @abstractmethod
    def on_init(self) -> None:
        """策略初始化。加载历史数据、计算指标等。"""
        ...

    def on_start(self) -> None:
        """策略启动。"""
        pass

    def on_stop(self) -> None:
        """策略停止。"""
        pass

    # ==================== 行情回调 ====================

    def on_day_begin(self, bar: BarData) -> None:
        """
        每日开始时回调。三种模式均调用。
        - close_fill: bar 包含当日 OHLCV（策略在此下单，以收盘价成交）
        - tick_fill / smart_tick_delay_fill: bar 包含前一交易日 OHLCV
        - 实盘: bar 包含前一交易日 OHLCV
        策略可在此做日线级别决策、更新指标、调整仓位。
        """
        pass

    def on_tick(self, tick: TickData) -> None:
        """
        收到新的 Tick 数据。
        close_fill 模式下不会调用此方法。
        """
        pass

    def on_bar(self, bar: BarData) -> None:
        """
        收到新的 Bar 数据（预留，用于分钟级 bar 合成）。
        """
        pass

    # ==================== 成交回调 ====================

    def on_order(self, order: Order) -> None:
        """订单状态更新。"""
        pass

    def on_trade(self, trade: Trade) -> None:
        """成交回报。"""
        pass

    # ==================== 下单接口 ====================

    def buy(self, symbol: str, price: float, volume: int) -> str:
        """
        买入。返回 order_id。
        相当于 vnpy 的 buy() (买开多)。
        ETF 无开平概念, 直接买入。
        """
        if not self.trading:
            return ""
        return self._engine.send_order(
            self, symbol, Direction.BUY, OrderType.LIMIT, price, volume
        )

    def sell(self, symbol: str, price: float, volume: int) -> str:
        """
        卖出。返回 order_id。
        相当于 vnpy 的 sell() (卖平多)。
        """
        if not self.trading:
            return ""
        return self._engine.send_order(
            self, symbol, Direction.SELL, OrderType.LIMIT, price, volume
        )

    def buy_market(self, symbol: str, volume: int) -> str:
        """市价买入。"""
        if not self.trading:
            return ""
        return self._engine.send_order(
            self, symbol, Direction.BUY, OrderType.MARKET, 0, volume
        )

    def sell_market(self, symbol: str, volume: int) -> str:
        """市价卖出。"""
        if not self.trading:
            return ""
        return self._engine.send_order(
            self, symbol, Direction.SELL, OrderType.MARKET, 0, volume
        )

    def cancel_order(self, order_id: str) -> None:
        """撤销指定订单。"""
        if self.trading:
            self._engine.cancel_order(self, order_id)

    def cancel_all(self) -> None:
        """撤销所有活跃订单。"""
        if self.trading:
            self._engine.cancel_all(self)

    # ==================== 查询接口 ====================

    def get_position(self, symbol: str) -> Position:
        """查询指定品种持仓。"""
        return self._engine.get_position(self, symbol)

    def get_account(self) -> Account:
        """查询账户。"""
        return self._engine.get_account(self)

    def get_pending_orders(self, symbol: str = "") -> list:
        """
        查询未完成委托列表。
        返回状态为 SUBMITTING/ACTIVE/PART_TRADED 的 Order 列表。
        可选按 symbol 过滤。
        """
        return self._engine.get_pending_orders(self, symbol)

    def write_log(self, msg: str) -> None:
        """记录日志。"""
        self._engine.write_log(msg, self)

    def get_pricetick(self, symbol: str) -> float:
        """查询最小价格变动。"""
        return self._engine.get_pricetick(self, symbol)

    def get_latest_tick(self, symbol: str) -> TickData | None:
        """
        查询指定品种的最新 tick 快照。
        symbol 必须在 self.symbols 中, 否则引擎抛出 ValueError。
        返回 None 表示当天尚无该品种的 tick 数据。
        """
        return self._engine.get_latest_tick(self, symbol)
```

### 3.5 与 vnpy 的关键差异

| 对比项 | vnpy CtaTemplate | 我们的 StrategyBase |
|--------|-----------------|-------------------|
| 品种数量 | 单品种 `vt_symbol` | 多品种 `symbols: list[str]` |
| 下单方向 | buy/sell/short/cover (含 Offset) | buy/sell (纯买卖, 无开平) |
| 停止单 | 内置 `stop=True` | 不内置, 策略自行实现 |
| 下单返回 | `list[str]` 多个 order_id | `str` 单个 order_id |
| 持仓查询 | `self.pos` 属性 | `self.get_position(symbol)` 方法 |
| 账户查询 | 无 | `self.get_account()` 方法 |
| 未完成委托查询 | 无 | `self.get_pending_orders()` 方法 |
| 每日回调 | 无 | `on_day_begin(bar)` 所有模式均调用 |
| 引擎类型 | 鸭子类型 | ABC 抽象类 EngineBase |
| symbol 参数 | 在 __init__ 绑定 | buy/sell 每次传入 |
| 行情快照查询 | 无 | `get_latest_tick(symbol)` 查任意关注品种 |
| 关注列表 | 无 (单品种) | `symbols` 列表, 引擎据此预加载+边界校验 |

### 3.6 示例策略

```python
class DemoMaCross(StrategyBase):
    """双均线交叉策略 — 演示策略接口用法 (单品种日线级)。"""

    author = "cstm"
    fast_period: int = 5
    slow_period: int = 20
    lot_size: int = 10000   # 每次交易份数

    parameters = ["fast_period", "slow_period", "lot_size"]
    variables = ["fast_ma", "slow_ma", "pos"]

    def on_init(self):
        self.fast_ma = 0.0
        self.slow_ma = 0.0
        self.pos = 0
        self.close_history: list[float] = []
        self.write_log("策略初始化完成")

    def on_day_begin(self, bar: BarData):
        self.close_history.append(bar.close_price)
        if len(self.close_history) < self.slow_period:
            return

        self.fast_ma = sum(self.close_history[-self.fast_period:]) / self.fast_period
        self.slow_ma = sum(self.close_history[-self.slow_period:]) / self.slow_period

        position = self.get_position(bar.symbol)

        if self.fast_ma > self.slow_ma and position.volume == 0:
            self.buy(bar.symbol, bar.close_price, self.lot_size)
        elif self.fast_ma < self.slow_ma and position.volume > 0:
            self.sell(bar.symbol, bar.close_price, position.volume)

    def on_trade(self, trade: Trade):
        self.write_log(f"成交: {trade.direction.value} {trade.volume}份 @ {trade.price}")

    def on_order(self, order: Order):
        if order.status == OrderStatus.REJECTED:
            self.write_log(f"委托被拒: {order.order_id}")
        elif order.status == OrderStatus.PART_CANCELLED:
            self.write_log(f"部分撤单: {order.order_id} 已成{order.traded}/{order.volume}")


class DemoSpread(StrategyBase):
    """
    演示多品种套利策略 — 展示 symbols 关注列表和 get_latest_tick 用法。

    实例化::
        strategy = DemoSpread(
            engine, "spread_1",
            symbols=["510300.SH", "159300.SZ"],    # [0] 是主驱动品种
            setting={"spread_threshold": 0.005, "lot_size": 10000}
        )
    """

    author = "cstm"
    spread_threshold: float = 0.005
    lot_size: int = 10000

    parameters = ["spread_threshold", "lot_size"]
    variables = ["current_spread"]

    def on_init(self):
        self.current_spread = 0.0
        self.write_log(f"套利策略初始化, 跟踪品种: {self.symbols}")

    def on_tick(self, tick: TickData):
        # tick 是 symbols[0] (510300.SH) 的主 tick
        other = self.get_latest_tick(self.symbols[1])  # 159300.SZ
        if other is None:
            return   # 另一品种当天尚无数据

        # 策略自行判断另一品种数据的时效性
        self.current_spread = tick.last_price - other.last_price

        if self.current_spread > self.spread_threshold:
            self.sell(self.symbols[0], tick.bid_price_1, self.lot_size)
            self.buy(self.symbols[1], other.ask_price_1, self.lot_size)

    def on_trade(self, trade: Trade):
        self.write_log(f"{trade.symbol} 成交: {trade.direction.value} {trade.volume}@{trade.price}")
```

## 4. 策略生命周期

### 4.1 回测时序 — close_fill 模式

```
BacktestEngine (mode=CLOSE_FILL):
  1. strategy = MyStrategy(engine, "test", ["510300.SH"], {"fast_period": 5})
  2. strategy.on_init()              # 加载历史数据
  3. strategy.inited = True
  4. strategy.on_start()
  5. strategy.trading = True
  6. for bar in daily_bars:
        matching.match_bar(bar)      # 撮合昨日挂单（以收盘价）→ on_order/on_trade
        strategy.on_day_begin(bar)   # 策略决策 → 新订单在下一日 match_bar 中撮合
  7. strategy.on_stop()
  8. 输出绩效报告
```

### 4.2 回测时序 — tick_fill / smart_tick_delay_fill 模式

```
BacktestEngine (mode=TICK_FILL or SMART_TICK_DELAY_FILL):
  1. strategy = MyStrategy(engine, "spread_1",
         symbols=["510300.SH", "159300.SZ"],
         setting={"spread_threshold": 0.005})
  2. strategy.on_init()
  3. strategy.inited = True
  4. strategy.on_start()
  5. strategy.trading = True
  6. 预加载: 根据 strategy.symbols 加载所有关注品种的 tick 数据
     初始化: 为每个 symbol 建立 cursor 和 latest_tick 字典
  7. for each trading_day:
        strategy.on_day_begin(prev_day_bar)   # 推送 primary symbol 的前日日线

        primary_symbol = strategy.symbols[0]  # "510300.SH"
        other_symbols = strategy.symbols[1:]  # ["159300.SZ"]

        for primary_tick in primary_ticks_of_day:
            T = primary_tick.datetime

            # 7a. 推进所有非主品种到 ≤ T
            for sym in other_symbols:
                while cursor[sym] 未越界 and sym_ticks[sym][cursor[sym]].datetime ≤ T:
                    tick_s = sym_ticks[sym][cursor[sym]]
                    latest_tick[sym] = tick_s
                    if sym 有挂单:
                        matching.match_tick(tick_s)  → 可能触发 on_order/on_trade
                    cursor[sym] += 1

            # 7b. 撮合主品种自己的挂单
            latest_tick[primary_symbol] = primary_tick
            if primary 有挂单:
                matching.match_tick(primary_tick)  → 可能触发 on_order/on_trade

            # 7c. 推给策略
            strategy.on_tick(primary_tick)

  8. strategy.on_stop()
  9. 输出绩效报告
```

**多品种 tick 驱动要点**:
- `on_tick()` 只收到 primary symbol 的 tick，策略通过 `get_latest_tick(other)` 查询非主品种
- 非主品种逐条推进并撮合（不跳过中间 tick，避免漏掉限价单成交）
- 非主品种无挂单时只更新 `latest_tick`，不调用 matching
- `get_latest_tick(symbol)` 对不在 `strategy.symbols` 中的品种抛 `ValueError`
- 各品种 tick 时间戳天然不对齐（SH ~3s 从 :01 起，SZ ~3s 从 :00 起），归并推进是最真实的仿真

### 4.3 实盘时序

```
LiveEngine:
  1. strategy = MyStrategy(engine, "live_1",
         symbols=["510300.SH", "159300.SZ"],
         setting={"spread_threshold": 0.005})
  2. strategy.on_init()
  3. strategy.inited = True
  4. strategy.on_start()
  5. strategy.trading = True
  6. 开盘前:
        strategy.on_day_begin(prev_day_bar)   # 推送 primary symbol 昨日日线
     行情线程:
        # 实盘中所有 symbols 的 tick 都通过 qmt 订阅
        # 引擎内部维护 latest_tick 字典
        # 只有 primary symbol 的 tick 触发 strategy.on_tick()
        qmt.get_full_tick(strategy.symbols) 推送 tick
          → tick.symbol == primary → strategy.on_tick(tick)
          → tick.symbol != primary → 仅更新 latest_tick[symbol]
          → 策略调用 buy/sell
          → qmt_gateway.send_order()
     回报线程:
        XtQuantTraderCallback.on_stock_order(xt_order)
          → 转换为 Order
          → strategy.on_order(order)
        XtQuantTraderCallback.on_stock_trade(xt_trade)
          → 转换为 Trade
          → strategy.on_trade(trade)
  7. 用户点击"停止" → strategy.on_stop()
```

### 4.4 回测与实盘的差异屏蔽

| 方面 | 回测 (BacktestEngine) | 实盘 (LiveEngine via mini-qmt) |
|------|----------------------|-------------------------------|
| 行情来源 | Parquet 文件回放 | xtdata.get_full_tick() 实时推送 |
| on_day_begin | 从日线 Parquet 加载 | 从 xtdata 查昨日日线 |
| send_order | → MatchingEngine.submit_order() | → xt_trader.order_stock() |
| 撤单 | → MatchingEngine.cancel_order() | → xt_trader.cancel_order_stock() |
| 撮合 | MatchingEngine 本地撮合 | 券商服务器撮合 |
| on_order 触发 | matching 撮合后同步回调 | XtQuantTraderCallback.on_stock_order() 异步推送 |
| on_trade 触发 | matching 生成 Trade 后同步回调 | XtQuantTraderCallback.on_stock_trade() 异步推送 |
| Position 状态 | 引擎维护的虚拟持仓 | xt_trader.query_stock_position() 真实持仓 |
| Account 状态 | 虚拟资金 | xt_trader.query_stock_asset() 真实资金 |
| get_pending_orders | 从 MatchingEngine 查 active_orders | xt_trader.query_stock_orders(cancelable_only=True) |

**策略不感知这些差异** — 所有交互都通过 EngineBase 接口。

## 5. mini-qmt (xtquant) 兼容映射

### 5.1 xtquant 架构概述

mini-qmt 通过 `xtquant` Python 包提供外部交易接口，包含两个核心模块:
- **xtdata** — 行情数据（get_full_tick、get_instrument_detail 等）
- **xttrader** — 交易操作（XtQuantTrader + XtQuantTraderCallback）

mini-qmt 运行模式: Python 程序通过 `XtQuantTrader(userdata_mini_path, session_id)` 连接本地运行的 MiniQMT 客户端。

### 5.2 策略接口 → xtquant API 映射表

#### 下单接口映射

| 我们的接口 | xtquant 对应 | 说明 |
|-----------|-------------|------|
| `strategy.buy(symbol, price, volume)` | `xt_trader.order_stock(acc, symbol, STOCK_BUY, volume, FIX_PRICE, price, strategy_name, remark)` | 返回 order_id (int > 0, 失败=-1) |
| `strategy.sell(symbol, price, volume)` | `xt_trader.order_stock(acc, symbol, STOCK_SELL, volume, FIX_PRICE, price, strategy_name, remark)` | 同上 |
| `strategy.buy_market(symbol, volume)` | `xt_trader.order_stock(acc, symbol, STOCK_BUY, volume, LATEST_PRICE, 0, ...)` | 用最新价 |
| `strategy.sell_market(symbol, volume)` | `xt_trader.order_stock(acc, symbol, STOCK_SELL, volume, LATEST_PRICE, 0, ...)` | 同上 |
| `strategy.cancel_order(order_id)` | `xt_trader.cancel_order_stock(acc, order_id)` | 返回 0=成功 |
| `strategy.cancel_all()` | 查询 `query_stock_orders(cancelable_only=True)` → 逐个 `cancel_order_stock` | 需循环处理 |

#### 回调映射

| 我们的回调 | xtquant 回调 | 数据转换 |
|-----------|-------------|----------|
| `on_order(order: Order)` | `XtQuantTraderCallback.on_stock_order(xt_order: XtOrder)` | 见下方字段映射 |
| `on_trade(trade: Trade)` | `XtQuantTraderCallback.on_stock_trade(xt_trade: XtTrade)` | 见下方字段映射 |
| (引擎处理) | `on_order_error(xt_error: XtOrderError)` | 转为 Order(status=REJECTED) |
| (引擎处理) | `on_cancel_error(xt_error: XtCancelError)` | 记录日志 |
| (引擎处理) | `on_disconnected()` | 触发重连或停止策略 |

#### 查询接口映射

| 我们的接口 | xtquant 对应 |
|-----------|-------------|
| `get_position(symbol)` | `xt_trader.query_stock_position(acc, symbol)` → XtPosition |
| `get_account()` | `xt_trader.query_stock_asset(acc)` → XtAsset |
| `get_pending_orders()` | `xt_trader.query_stock_orders(acc, cancelable_only=True)` → [XtOrder] |

### 5.3 数据结构字段映射

#### Order ↔ XtOrder

| 我们的 Order | XtOrder | 说明 |
|-------------|---------|------|
| order_id: str | order_id: int | 我们用 str 抽象, gateway 转换 |
| symbol: str | stock_code: str | 格式一致 "510300.SH" |
| direction: Direction | order_type: int | BUY→STOCK_BUY, SELL→STOCK_SELL |
| order_type: OrderType | price_type: int | LIMIT→FIX_PRICE, MARKET→LATEST_PRICE |
| price: float | price: float | |
| volume: int | order_volume: int | |
| traded: int | traded_volume: int | |
| status: OrderStatus | order_status: int | 见状态码映射表 |
| create_time | order_time: int | 时间戳转换 |

#### OrderStatus ↔ xtconstant 状态码

| 我们的 OrderStatus | xtconstant | 值 | 说明 |
|-------------------|------------|---|------|
| SUBMITTING | ORDER_UNREPORTED | 48 | 报单已提交，尚未到交易所 |
| ACTIVE | ORDER_REPORTED | 50 | 已到交易所，等待成交 |
| ACTIVE | ORDER_WAIT_REPORTING | 49 | 待报 |
| PART_TRADED | ORDER_PART_SUCC | 55 | 部分成交，剩余待成交 |
| ALL_TRADED | ORDER_SUCCEEDED | 56 | 全部成交 |
| CANCELLED | ORDER_CANCELED | 54 | 已撤 |
| PART_CANCELLED | ORDER_PART_CANCEL | 53 | 部分成交后撤剩余 |
| REJECTED | ORDER_JUNK | 57 | 废单 |

注: 中间状态 ORDER_REPORTED_CANCEL(51), ORDER_PARTSUCC_CANCEL(52) 表示"待撤中"，
在我们系统中仍映射为 ACTIVE / PART_TRADED（因为撤单尚未确认）。

#### Trade ↔ XtTrade

| 我们的 Trade | XtTrade | 说明 |
|-------------|---------|------|
| trade_id: str | traded_id: str | |
| order_id: str | order_id: int | |
| symbol: str | stock_code: str | |
| direction: Direction | order_type: int | |
| price: float | traded_price: float | |
| volume: int | traded_volume: int | |
| commission: float | (需自行计算) | |
| datetime | traded_time: int | |

#### Position ↔ XtPosition

| 我们的 Position | XtPosition | 说明 |
|----------------|------------|------|
| symbol: str | stock_code: str | |
| volume: int | volume: int | 总持仓 |
| frozen: int | frozen_volume: int | 冻结数量 |
| cost_price: float | avg_price: float | 成本价 |
| market_price: float | (需 get_full_tick 获取) | |
| available (property) | can_use_volume: int | 可用量 |

#### Account ↔ XtAsset

| 我们的 Account | XtAsset | 说明 |
|---------------|---------|------|
| balance: float | total_asset: float | 总资产 |
| frozen: float | frozen_cash: float | 冻结资金 |
| available (property) | cash: float | 可用资金 |
| commission: float | (需自行累计) | |

### 5.4 QmtGateway 设计要点

```
live/
├── qmt_gateway.py    # xtquant 封装层
└── engine.py          # LiveEngine (实现 EngineBase)

QmtGateway 职责:
  1. 管理 XtQuantTrader 连接生命周期 (connect/start/subscribe)
  2. 实现 XtQuantTraderCallback，将 xtquant 回调转为我们的 Order/Trade
  3. 提供 send_order / cancel_order 方法，转换参数后调用 xt_trader
  4. 处理异步回调线程安全（xtquant 回调在独立线程）
  5. 处理 on_order_error / on_cancel_error 异常情况
```

### 5.5 行情接入

```
实盘行情通过 xtdata 获取:
  - xtdata.get_full_tick(stock_list) → dict，包含 lastPrice、bid/ask 等
  - 需要轮询或订阅机制，将 xtdata 返回值转为我们的 TickData 结构
  - 注意: xtdata 的 tick 字段命名与我们的 Parquet schema 不完全一致，需要 gateway 做映射

实盘日线通过 xtdata 获取:
  - xtdata.get_market_data_ex(...) 获取历史日线
  - 用于 on_day_begin(bar) 回调
```

## 6. 策略代码跨模式兼容设计

### 6.1 兼容原则

一份策略代码, 在 close_fill / tick_fill / smart_tick_delay_fill / 实盘 四种场景下, **零修改**运行。

关键设计:
- `on_day_begin(bar)` 所有模式均调用 → 日线级决策放这里
- `on_tick(tick)` 只在 tick 模式和实盘调用 → tick 级决策放这里
- 策略通过 EngineBase 接口下单/查询 → 引擎屏蔽底层差异
- 回调 on_order/on_trade 语义一致 → 回测生成 vs 券商推送, 策略无感知

### 6.2 策略编写模板

```python
class MyStrategy(StrategyBase):
    """跨 close_fill/tick/实盘 兼容的策略模板。"""

    def on_day_begin(self, bar: BarData):
        # ✅ 所有模式均调用
        # 更新日线级指标 (均线、ATR、布林等)
        # close_fill 模式: 可在此直接下单
        pass

    def on_tick(self, tick: TickData):
        # ✅ tick_fill / smart_tick_delay_fill / 实盘 调用
        # ❌ close_fill 模式不调用
        # tick 级决策 (盘口信号、微观结构等)
        pass

    def on_order(self, order: Order):
        # ✅ 所有模式均调用
        # 统一处理: 成交/部分成交/撤单/拒绝
        pass

    def on_trade(self, trade: Trade):
        # ✅ 所有模式均调用
        pass
```

## 7. 文件规划

```
core/
├── __init__.py
├── datatypes.py       # Direction, OrderType, OrderStatus, Order, Trade,
│                      # Position, Account, TickData, BarData
├── matching.py        # MatchingEngine (详见 design_matching_engine.md)
├── strategy.py        # EngineBase (ABC), StrategyBase (ABC)
├── event.py           # EventBus (on/emit, 同步模式)
└── data_feed.py       # ParquetTickFeed, ParquetBarFeed

backtest/
├── __init__.py
└── engine.py          # BacktestEngine (实现 EngineBase)
                       # 支持三种 MatchingMode

live/
├── __init__.py
├── engine.py          # LiveEngine (实现 EngineBase)
└── qmt_gateway.py     # XtQuantTrader 封装
                       # XtQuantTraderCallback 实现
                       # xtquant ↔ 我们的数据类型转换
```
