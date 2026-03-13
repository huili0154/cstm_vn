"""
BacktestEngine 测试。

测试回测引擎的三种撮合模式、持仓/资金管理、多品种 tick 驱动。
使用真实 Parquet 数据。
"""

import pytest
from pathlib import Path

from backtest.engine import BacktestEngine, BacktestResult
from core.datatypes import (
    Account,
    BarData,
    Direction,
    MatchingMode,
    Order,
    OrderStatus,
    OrderType,
    Position,
    TickData,
    Trade,
)
from core.strategy import EngineBase, StrategyBase


DATASET_DIR = Path(__file__).parent.parent / "dataset"


# ════════════════════════════════════════════════
#  测试用策略
# ════════════════════════════════════════════════


class BuyAndHoldStrategy(StrategyBase):
    """最简策略: 第一天买入，一直持有。"""

    lot_size: int = 10000
    parameters = ["lot_size"]

    def on_init(self):
        self.bought = False

    def on_day_begin(self, bar: BarData):
        if not self.bought:
            self.buy(bar.symbol, bar.close_price, self.lot_size)
            self.bought = True


class BuyOnTickStrategy(StrategyBase):
    """Tick 模式: 第一个有效 tick 时买入。"""

    lot_size: int = 1000
    parameters = ["lot_size"]

    def on_init(self):
        self.bought = False
        self.order_count = 0
        self.trade_count = 0
        self.tick_count = 0

    def on_day_begin(self, bar: BarData):
        pass

    def on_tick(self, tick: TickData):
        self.tick_count += 1
        if not self.bought and tick.ask_price_1 > 0:
            oid = self.buy(tick.symbol, tick.ask_price_1, self.lot_size)
            if oid:
                self.bought = True

    def on_order(self, order: Order):
        self.order_count += 1

    def on_trade(self, trade: Trade):
        self.trade_count += 1


class BuyThenSellStrategy(StrategyBase):
    """买入后第二天卖出 (验证 T+1)。"""

    lot_size: int = 1000
    parameters = ["lot_size"]

    def on_init(self):
        self.day_count = 0
        self.bought = False
        self.sold = False

    def on_day_begin(self, bar: BarData):
        self.day_count += 1

    def on_tick(self, tick: TickData):
        if not self.bought and tick.ask_price_1 > 0:
            oid = self.buy(tick.symbol, tick.ask_price_1, self.lot_size)
            if oid:
                self.bought = True
        elif self.bought and not self.sold and self.day_count >= 2:
            pos = self.get_position(tick.symbol)
            if pos.available > 0 and tick.bid_price_1 > 0:
                self.sell(tick.symbol, tick.bid_price_1, pos.available)
                self.sold = True


class MultiSymbolStrategy(StrategyBase):
    """多品种策略: 第一个有效 tick 时查询非主品种的 latest_tick。"""

    parameters = []

    def on_init(self):
        self.primary_ticks = 0
        self.other_latest_seen = 0
        self.got_other_tick = False
        self.other_symbol_name = ""

    def on_tick(self, tick: TickData):
        self.primary_ticks += 1
        if len(self.symbols) > 1:
            self.other_symbol_name = self.symbols[1]
            other = self.get_latest_tick(self.symbols[1])
            if other is not None:
                self.other_latest_seen += 1
                self.got_other_tick = True


class InvalidSymbolStrategy(StrategyBase):
    """尝试查询不在关注列表的品种。"""

    parameters = []

    def on_init(self):
        self.error_raised = False

    def on_tick(self, tick: TickData):
        try:
            self.get_latest_tick("999999.SH")
        except ValueError:
            self.error_raised = True


class OrderUnsubscribedSymbolStrategy(StrategyBase):
    """尝试下单不在 symbols 列表中的品种。"""

    parameters = []

    def on_init(self):
        self.rejected_buy = False
        self.rejected_sell = False

    def on_day_begin(self, bar: BarData):
        if not self.rejected_buy:
            oid = self.buy("NONEXIST.SZ", 1.0, 1000)
            if oid == "":
                self.rejected_buy = True
        if not self.rejected_sell:
            oid = self.sell("NONEXIST.SZ", 1.0, 1000)
            if oid == "":
                self.rejected_sell = True


class InsufficientFundsStrategy(StrategyBase):
    """尝试买入超过资金的量。"""

    parameters = []

    def on_init(self):
        self.rejected = False

    def on_tick(self, tick: TickData):
        if not self.rejected and tick.ask_price_1 > 0:
            # 尝试买入极大量
            oid = self.buy(tick.symbol, tick.ask_price_1, 999_999_999)
            if oid == "":
                self.rejected = True


# ════════════════════════════════════════════════
#  CLOSE_FILL 模式测试
# ════════════════════════════════════════════════


class TestCloseFill:
    def test_buy_and_hold(self):
        """买入持有，回测完后应有持仓和净值变化。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.CLOSE_FILL,
            initial_capital=1_000_000,
            rate=0.00005,
        )
        strategy = BuyAndHoldStrategy(
            engine, "test_bh", ["510300.SH"],
            setting={"lot_size": 10000},
        )
        result = engine.run(strategy, "20260105", "20260115")

        assert len(result.trades) > 0
        assert result.start_balance == 1_000_000
        assert result.end_balance > 0
        assert len(result.daily_nav) > 0

        # 应该只有一笔买入
        assert len(result.trades) == 1
        trade = result.trades[0]
        assert trade.direction == Direction.BUY
        assert trade.volume == 10000

        # 持仓
        pos = engine.get_position(strategy, "510300.SH")
        assert pos.volume == 10000
        assert pos.cost_price > 0

    def test_nav_tracking(self):
        """每日净值应该被记录。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.CLOSE_FILL,
            initial_capital=500_000,
        )
        strategy = BuyAndHoldStrategy(
            engine, "test_nav", ["510300.SH"],
            setting={"lot_size": 5000},
        )
        result = engine.run(strategy, "20260105", "20260120")

        # 应有多天净值记录
        assert len(result.daily_nav) >= 5
        # 所有净值 > 0
        for date, nav in result.daily_nav:
            assert nav > 0
            assert len(date) == 8

    def test_no_data(self):
        """日期范围无数据时应正常返回空结果。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.CLOSE_FILL,
        )
        strategy = BuyAndHoldStrategy(
            engine, "test_empty", ["510300.SH"],
        )
        result = engine.run(strategy, "20200101", "20200110")
        assert len(result.trades) == 0
        assert len(result.daily_nav) == 0


# ════════════════════════════════════════════════
#  TICK_FILL 模式测试
# ════════════════════════════════════════════════


class TestTickFill:
    def test_buy_on_tick(self):
        """Tick 模式买入，应有成交。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
            rate=0.00005,
        )
        strategy = BuyOnTickStrategy(
            engine, "test_tick_buy", ["510300.SH"],
            setting={"lot_size": 1000},
        )
        result = engine.run(strategy, "20250102", "20250106")

        assert strategy.tick_count > 0
        assert strategy.bought
        assert len(result.trades) >= 1

        trade = result.trades[0]
        assert trade.direction == Direction.BUY
        assert trade.volume == 1000

    def test_tick_callbacks(self):
        """on_order 和 on_trade 应被正确调用。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
        )
        strategy = BuyOnTickStrategy(
            engine, "test_cb", ["510300.SH"],
            setting={"lot_size": 1000},
        )
        engine.run(strategy, "20250102", "20250106")

        # on_order 至少被调用一次 (SUBMITTING→ALL_TRADED)
        assert strategy.order_count >= 1
        assert strategy.trade_count >= 1

    def test_t_plus_1(self):
        """T+1: 当天买入不能当天卖出。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
        )
        strategy = BuyThenSellStrategy(
            engine, "test_t1", ["510300.SH"],
            setting={"lot_size": 1000},
        )
        result = engine.run(strategy, "20250102", "20250108")

        # 应买入成功
        assert strategy.bought

        if strategy.sold:
            # 卖出应在第2天或之后
            buy_trade = [t for t in result.trades if t.direction == Direction.BUY][0]
            sell_trade = [t for t in result.trades if t.direction == Direction.SELL][0]
            assert sell_trade.datetime > buy_trade.datetime


# ════════════════════════════════════════════════
#  SMART_TICK_DELAY_FILL 模式测试
# ════════════════════════════════════════════════


class TestSmartTickDelayFill:
    def test_buy_smart(self):
        """Smart 模式买入。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            initial_capital=1_000_000,
        )
        strategy = BuyOnTickStrategy(
            engine, "test_smart", ["510300.SH"],
            setting={"lot_size": 1000},
        )
        result = engine.run(strategy, "20250102", "20250106")

        assert strategy.tick_count > 0
        assert strategy.bought
        # smart 模式延迟成交，可能在下一个tick才成交
        assert len(result.trades) >= 1


# ════════════════════════════════════════════════
#  多品种测试
# ════════════════════════════════════════════════


class TestMultiSymbol:
    def test_latest_tick_available(self):
        """非主品种的 latest_tick 应可查询。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
        )
        strategy = MultiSymbolStrategy(
            engine, "test_multi", ["510300.SH", "159300.SZ"],
        )
        engine.run(strategy, "20250102", "20250103")

        assert strategy.primary_ticks > 0
        assert strategy.got_other_tick
        assert strategy.other_latest_seen > 0

    def test_invalid_symbol_raises(self):
        """查询不在关注列表的品种应报错。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
        )
        strategy = InvalidSymbolStrategy(
            engine, "test_invalid", ["510300.SH"],
        )
        engine.run(strategy, "20250102", "20250103")
        assert strategy.error_raised


# ════════════════════════════════════════════════
#  资金/持仓边界测试
# ════════════════════════════════════════════════


class TestRiskControl:
    def test_insufficient_funds(self):
        """资金不足时应拒绝订单。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=100,  # 极小资金
        )
        strategy = InsufficientFundsStrategy(
            engine, "test_funds", ["510300.SH"],
        )
        engine.run(strategy, "20250102", "20250103")
        assert strategy.rejected

    def test_sell_without_position(self):
        """无持仓卖出应被拒绝。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
        )

        class SellWithoutPosStrategy(StrategyBase):
            parameters = []

            def on_init(self):
                self.rejected = False

            def on_tick(self, tick: TickData):
                if not self.rejected and tick.bid_price_1 > 0:
                    oid = self.sell(tick.symbol, tick.bid_price_1, 1000)
                    if oid == "":
                        self.rejected = True

        strategy = SellWithoutPosStrategy(
            engine, "test_no_pos", ["510300.SH"],
        )
        engine.run(strategy, "20250102", "20250103")
        assert strategy.rejected

    def test_order_unsubscribed_symbol(self):
        """下单不在 symbols 列表中的品种应被拒绝。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.CLOSE_FILL,
            initial_capital=1_000_000,
        )
        strategy = OrderUnsubscribedSymbolStrategy(
            engine, "test_unsub", ["510300.SH"],
        )
        engine.run(strategy, "20250102", "20250110")
        assert strategy.rejected_buy, "Buy on unsubscribed symbol should be rejected"
        assert strategy.rejected_sell, "Sell on unsubscribed symbol should be rejected"

    def test_account_balance_consistency(self):
        """买入后资金应减少，卖出后应回笼。"""
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.TICK_FILL,
            initial_capital=1_000_000,
            rate=0.0,  # 无佣金简化验证
        )
        strategy = BuyThenSellStrategy(
            engine, "test_balance", ["510300.SH"],
            setting={"lot_size": 1000},
        )
        result = engine.run(strategy, "20250102", "20250108")

        # 最终净值应大于 0
        assert result.end_balance > 0

        # 如果买卖都成功了，净值应接近初始资金
        if strategy.sold:
            assert abs(result.end_balance - 1_000_000) < 50_000  # 允许价格波动


class TestSettingParameters:
    """验证策略参数传递。"""

    def test_setting_override(self):
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.CLOSE_FILL,
        )
        strategy = BuyAndHoldStrategy(
            engine, "test_param", ["510300.SH"],
            setting={"lot_size": 5000},
        )
        assert strategy.lot_size == 5000

    def test_default_parameters(self):
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.CLOSE_FILL,
        )
        strategy = BuyAndHoldStrategy(
            engine, "test_default", ["510300.SH"],
        )
        assert strategy.lot_size == 10000  # 默认值
