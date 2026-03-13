"""
策略接口 — 回测与实盘共用。

EngineBase:  回测引擎和实盘引擎的公共抽象接口。
StrategyBase: 策略基类，一份策略代码，回测/实盘零修改运行。
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from core.datatypes import (
    Account,
    BarData,
    Direction,
    Order,
    OrderType,
    Position,
    TickData,
    Trade,
)


# ════════════════════════════════════════════════
#  EngineBase — 引擎公共接口
# ════════════════════════════════════════════════


class EngineBase(ABC):
    """
    回测引擎 (BacktestEngine) 和实盘引擎 (LiveEngine) 必须实现的统一接口。

    策略通过 ``self._engine`` 调用这些方法，从而屏蔽回测/实盘差异。
    """

    @abstractmethod
    def send_order(
        self,
        strategy: StrategyBase,
        symbol: str,
        direction: Direction,
        order_type: OrderType,
        price: float,
        volume: int,
    ) -> str:
        """提交订单，返回 order_id。"""
        ...

    @abstractmethod
    def cancel_order(self, strategy: StrategyBase, order_id: str) -> None:
        """撤销订单。"""
        ...

    @abstractmethod
    def cancel_all(self, strategy: StrategyBase) -> None:
        """撤销策略的所有活跃订单。"""
        ...

    @abstractmethod
    def get_pending_orders(
        self, strategy: StrategyBase, symbol: str = ""
    ) -> list[Order]:
        """查询未完成委托列表。可选按 symbol 过滤。"""
        ...

    @abstractmethod
    def get_position(self, strategy: StrategyBase, symbol: str) -> Position:
        """查询持仓。"""
        ...

    @abstractmethod
    def get_account(self, strategy: StrategyBase) -> Account:
        """查询账户。"""
        ...

    @abstractmethod
    def write_log(self, msg: str, strategy: StrategyBase) -> None:
        """记录日志。"""
        ...

    @abstractmethod
    def get_pricetick(self, strategy: StrategyBase, symbol: str) -> float:
        """查询最小价格变动。"""
        ...

    @abstractmethod
    def get_latest_tick(
        self, strategy: StrategyBase, symbol: str
    ) -> TickData | None:
        """查询指定品种最新 tick 快照。symbol 必须在 strategy.symbols 中。"""
        ...


# ════════════════════════════════════════════════
#  StrategyBase — 策略基类
# ════════════════════════════════════════════════


class StrategyBase(ABC):
    """
    策略基类。回测和实盘共用, 一份代码两处运行。

    - ETF 无开平仓概念, 统一 BUY / SELL。
    - 支持多品种 (``symbols: list[str]``)。
    - 通过 ``self._engine`` 间接操作; 回测时连 MatchingEngine, 实盘时连 QmtGateway。

    用法::

        class MyStrategy(StrategyBase):
            fast_period: int = 10
            slow_period: int = 20

            parameters = ["fast_period", "slow_period"]
            variables = ["fast_ma", "slow_ma"]

            def on_init(self):
                self.fast_ma = 0.0
                self.slow_ma = 0.0

            def on_tick(self, tick: TickData):
                ...
    """

    # 子类声明
    author: str = ""
    parameters: list[str] = []
    variables: list[str] = []

    def __init__(
        self,
        engine: EngineBase,
        strategy_name: str,
        symbols: list[str],
        setting: dict | None = None,
    ) -> None:
        self._engine = engine
        self.strategy_name = strategy_name
        self.symbols = symbols
        self.inited: bool = False
        self.trading: bool = False

        if setting:
            self.update_setting(setting)

    def update_setting(self, setting: dict) -> None:
        """从 dict 更新 parameters 中声明的可配置参数。"""
        for name in self.parameters:
            if name in setting:
                setattr(self, name, setting[name])

    # ──────────────── 生命周期回调 ────────────────

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

    # ──────────────── 行情回调 ────────────────

    def on_day_begin(self, bar: BarData) -> None:
        """
        每日开始时回调。所有模式均调用。

        - close_fill: bar 包含当日 OHLCV, 策略在此下单以收盘价成交。
        - tick 模式: bar 包含前一交易日 OHLCV。
        - 实盘: bar 包含前一交易日 OHLCV。
        """
        pass

    def on_tick(self, tick: TickData) -> None:
        """
        Tick 推送。close_fill 模式不调用。
        """
        pass

    def on_bar(self, bar: BarData) -> None:
        """
        合成 Bar 推送（预留, 用于分钟级 bar 合成）。
        """
        pass

    # ──────────────── 成交回调 ────────────────

    def on_order(self, order: Order) -> None:
        """订单状态更新。"""
        pass

    def on_trade(self, trade: Trade) -> None:
        """成交回报。"""
        pass

    # ──────────────── 下单接口 ────────────────

    def buy(self, symbol: str, price: float, volume: int) -> str:
        """限价买入。返回 order_id。"""
        if not self.trading:
            return ""
        return self._engine.send_order(
            self, symbol, Direction.BUY, OrderType.LIMIT, price, volume
        )

    def sell(self, symbol: str, price: float, volume: int) -> str:
        """限价卖出。返回 order_id。"""
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

    # ──────────────── 查询接口 ────────────────

    def get_position(self, symbol: str) -> Position:
        """查询指定品种持仓。"""
        return self._engine.get_position(self, symbol)

    def get_account(self) -> Account:
        """查询账户。"""
        return self._engine.get_account(self)

    def get_pending_orders(self, symbol: str = "") -> list[Order]:
        """查询未完成委托 (SUBMITTING / ACTIVE / PART_TRADED)。"""
        return self._engine.get_pending_orders(self, symbol)

    def write_log(self, msg: str) -> None:
        """记录日志。"""
        self._engine.write_log(msg, self)

    def get_pricetick(self, symbol: str) -> float:
        """查询最小价格变动。"""
        return self._engine.get_pricetick(self, symbol)

    def get_latest_tick(self, symbol: str) -> TickData | None:
        """查询指定品种最新 tick 快照。symbol 必须在 self.symbols 中。"""
        return self._engine.get_latest_tick(self, symbol)
