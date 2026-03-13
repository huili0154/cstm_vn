"""
MatchingEngine 单元测试 — 覆盖三种撮合模式。
"""

import unittest
from datetime import datetime

from core.datatypes import (
    BarData,
    Direction,
    MatchingMode,
    Order,
    OrderStatus,
    OrderType,
    TickData,
    Trade,
)
from core.matching import MatchingEngine


def make_tick(
    symbol: str = "510300.SH",
    last_price: float = 4.0,
    ask1: float = 4.001,
    bid1: float = 3.999,
    ask_vol1: int = 100000,
    bid_vol1: int = 100000,
    cum_volume: int = 1000000,
    dt: datetime | None = None,
) -> TickData:
    """快速生成 TickData 用于测试。"""
    return TickData(
        symbol=symbol,
        datetime=dt or datetime(2025, 1, 2, 9, 30, 0),
        last_price=last_price,
        cum_volume=cum_volume,
        ask_price_1=ask1,
        bid_price_1=bid1,
        ask_volume_1=ask_vol1,
        bid_volume_1=bid_vol1,
        volume=500.0,
    )


def make_bar(
    symbol: str = "510300.SH",
    close_price: float = 4.0,
    dt: datetime | None = None,
) -> BarData:
    return BarData(
        symbol=symbol,
        datetime=dt or datetime(2025, 1, 2),
        open_price=3.95,
        high_price=4.05,
        low_price=3.90,
        close_price=close_price,
        volume=10000000.0,
        turnover=4e9,
    )


# ════════════════════════════════════════════════
#  CLOSE_FILL 模式
# ════════════════════════════════════════════════


class TestCloseFill(unittest.TestCase):
    def setUp(self):
        self.engine = MatchingEngine(
            mode=MatchingMode.CLOSE_FILL,
            rate=0.0003,
            slippage=0.0,
            min_commission=5.0,
        )
        self.order_callbacks: list[Order] = []
        self.trade_callbacks: list[Trade] = []
        self.engine.set_on_order(self.order_callbacks.append)
        self.engine.set_on_trade(self.trade_callbacks.append)

    def test_buy_fills_at_close(self):
        order = self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.0, 10000
        )
        self.assertEqual(order.status, OrderStatus.SUBMITTING)

        bar = make_bar(close_price=4.05)
        trades = self.engine.match_bar(bar)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 4.05)
        self.assertEqual(trades[0].volume, 10000)
        self.assertEqual(order.status, OrderStatus.ALL_TRADED)
        self.assertGreater(trades[0].commission, 0)

    def test_sell_fills_at_close(self):
        order = self.engine.submit_order(
            "510300.SH", Direction.SELL, OrderType.LIMIT, 4.0, 5000
        )
        bar = make_bar(close_price=3.95)
        trades = self.engine.match_bar(bar)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 3.95)
        self.assertEqual(trades[0].direction, Direction.SELL)

    def test_commission_min(self):
        """手续费不低于 min_commission。"""
        order = self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 1.0, 100
        )
        bar = make_bar(close_price=1.0)
        trades = self.engine.match_bar(bar)
        # turnover = 100 * 1.0 = 100, fee = 0.03, but min=5
        self.assertEqual(trades[0].commission, 5.0)

    def test_callbacks_fired(self):
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.0, 1000
        )
        self.engine.match_bar(make_bar(close_price=4.0))

        # 应该有: ACTIVE 通知 + ALL_TRADED 通知
        self.assertEqual(len(self.order_callbacks), 2)
        self.assertEqual(len(self.trade_callbacks), 1)

    def test_no_match_wrong_symbol(self):
        self.engine.submit_order(
            "159919.SZ", Direction.BUY, OrderType.LIMIT, 4.0, 1000
        )
        bar = make_bar(symbol="510300.SH", close_price=4.0)
        trades = self.engine.match_bar(bar)
        self.assertEqual(len(trades), 0)

    def test_slippage(self):
        engine = MatchingEngine(
            mode=MatchingMode.CLOSE_FILL,
            slippage=0.002,
        )
        engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.0, 1000
        )
        trades = engine.match_bar(make_bar(close_price=4.0))
        self.assertAlmostEqual(trades[0].price, 4.002)  # 买单 + slippage


# ════════════════════════════════════════════════
#  TICK_FILL 模式
# ════════════════════════════════════════════════


class TestTickFill(unittest.TestCase):
    def setUp(self):
        self.engine = MatchingEngine(
            mode=MatchingMode.TICK_FILL,
            rate=0.0003,
        )

    def test_limit_buy_fills_when_price_ge_ask(self):
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.001, 5000
        )
        tick = make_tick(ask1=4.001, bid1=3.999)
        trades = self.engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 4.001)
        self.assertEqual(trades[0].volume, 5000)

    def test_limit_buy_no_fill_when_price_lt_ask(self):
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 3.998, 5000
        )
        tick = make_tick(ask1=4.001)
        trades = self.engine.match_tick(tick)
        self.assertEqual(len(trades), 0)

    def test_limit_sell_fills_when_price_le_bid(self):
        self.engine.submit_order(
            "510300.SH", Direction.SELL, OrderType.LIMIT, 3.999, 5000
        )
        tick = make_tick(bid1=3.999)
        trades = self.engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 3.999)

    def test_market_buy_fills_at_ask1(self):
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.MARKET, 0, 5000
        )
        tick = make_tick(ask1=4.001)
        trades = self.engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 4.001)

    def test_market_sell_fills_at_bid1(self):
        self.engine.submit_order(
            "510300.SH", Direction.SELL, OrderType.MARKET, 0, 5000
        )
        tick = make_tick(bid1=3.999)
        trades = self.engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 3.999)

    def test_full_fill_regardless_of_volume(self):
        """tick_fill 不考虑盘口量限制，全量成交。"""
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.001, 999999
        )
        tick = make_tick(ask1=4.001, ask_vol1=100)  # 盘口只有 100
        trades = self.engine.match_tick(tick)

        self.assertEqual(trades[0].volume, 999999)  # 仍全量成交


# ════════════════════════════════════════════════
#  SMART_TICK_DELAY_FILL 模式
# ════════════════════════════════════════════════


class TestSmartTickDelayFill(unittest.TestCase):
    def setUp(self):
        self.engine = MatchingEngine(
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            rate=0.0003,
            volume_limit_ratio=0.5,
        )

    def test_delay_one_tick(self):
        """提交时的 tick 不能成交，需要延迟至少 1 tick。"""
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.001, 5000
        )
        # 第一个 tick（提交所在 tick）不应成交
        tick1 = make_tick(ask1=4.001, cum_volume=100000)
        trades1 = self.engine.match_tick(tick1)
        self.assertEqual(len(trades1), 0)

        # 第二个 tick 才能成交
        tick2 = make_tick(ask1=4.001, cum_volume=200000)
        trades2 = self.engine.match_tick(tick2)
        self.assertGreater(len(trades2), 0)

    def test_market_buy_only_level1(self):
        """市价买单只吃卖一，成交量受 volume_limit_ratio 限制。"""
        self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000
        )
        # tick1: 提交 tick，不成交
        tick1 = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=100000)
        self.engine.match_tick(tick1)

        # tick2: volume_delta = 200000-100000 = 100000
        # max_fill = min(50000, 100000) * 0.5 = 25000
        tick2 = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=200000)
        trades = self.engine.match_tick(tick2)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 4.001)
        self.assertEqual(trades[0].volume, 25000)

    def test_market_sell_only_level1(self):
        """市价卖单只吃买一。"""
        self.engine.submit_order(
            "510300.SH", Direction.SELL, OrderType.MARKET, 0, 100000
        )
        tick1 = make_tick(bid1=3.999, bid_vol1=60000, cum_volume=100000)
        self.engine.match_tick(tick1)

        tick2 = make_tick(bid1=3.999, bid_vol1=60000, cum_volume=200000)
        trades = self.engine.match_tick(tick2)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, 3.999)
        self.assertEqual(trades[0].volume, 30000)  # min(60000,100000)*0.5

    def test_limit_buy_aggressive_multi_level(self):
        """限价买单价格穿越卖盘，逐档扫描成交。"""
        order = self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.005, 100000
        )
        tick1 = make_tick(cum_volume=100000)
        self.engine.match_tick(tick1)

        # tick2: ask1=4.001 vol=20000, ask2=4.002 vol=30000
        tick2 = TickData(
            symbol="510300.SH",
            datetime=datetime(2025, 1, 2, 9, 30, 6),
            last_price=4.002,
            cum_volume=200000,
            ask_price_1=4.001,
            ask_volume_1=20000,
            ask_price_2=4.002,
            ask_volume_2=30000,
            ask_price_3=4.003,
            ask_volume_3=50000,
            bid_price_1=4.000,
            bid_volume_1=40000,
        )
        trades = self.engine.match_tick(tick2)

        self.assertGreater(len(trades), 0)
        # 应该在多档成交（4.001, 4.002, 4.003 都 <= 4.005）
        total_filled = sum(t.volume for t in trades)
        self.assertGreater(total_filled, 0)
        self.assertLessEqual(total_filled, 100000)

    def test_limit_sell_aggressive(self):
        """限价卖单价格穿越买盘。"""
        self.engine.submit_order(
            "510300.SH", Direction.SELL, OrderType.LIMIT, 3.995, 50000
        )
        tick1 = make_tick(cum_volume=100000)
        self.engine.match_tick(tick1)

        tick2 = TickData(
            symbol="510300.SH",
            datetime=datetime(2025, 1, 2, 9, 30, 6),
            last_price=3.998,
            cum_volume=200000,
            bid_price_1=3.999,
            bid_volume_1=30000,
            bid_price_2=3.998,
            bid_volume_2=20000,
            ask_price_1=4.001,
            ask_volume_1=10000,
        )
        trades = self.engine.match_tick(tick2)

        self.assertGreater(len(trades), 0)
        total_filled = sum(t.volume for t in trades)
        self.assertGreater(total_filled, 0)

    def test_partial_fill_across_ticks(self):
        """订单跨多个 tick 部分成交。"""
        order = self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.MARKET, 0, 200000
        )
        # tick1: 提交 tick
        tick1 = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=100000)
        self.engine.match_tick(tick1)

        # tick2: 部分成交
        tick2 = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=200000)
        trades2 = self.engine.match_tick(tick2)
        self.assertEqual(len(trades2), 1)
        filled_2 = trades2[0].volume

        # tick3: 继续部分成交
        tick3 = make_tick(ask1=4.002, ask_vol1=50000, cum_volume=300000)
        trades3 = self.engine.match_tick(tick3)

        total = filled_2 + sum(t.volume for t in trades3)
        self.assertLess(total, 200000)  # 仍未全部成交
        self.assertEqual(order.status, OrderStatus.PART_TRADED)

    def test_cancel_partial(self):
        """部分成交后撤单，状态应为 PART_CANCELLED。"""
        order = self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.MARKET, 0, 200000
        )
        tick1 = make_tick(cum_volume=100000)
        self.engine.match_tick(tick1)

        tick2 = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=200000)
        self.engine.match_tick(tick2)

        self.assertGreater(order.traded, 0)
        self.engine.cancel_order(order.order_id)
        self.assertEqual(order.status, OrderStatus.PART_CANCELLED)

    def test_cancel_unfilled(self):
        """未成交直接撤单，状态应为 CANCELLED。"""
        order = self.engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 3.0, 5000
        )
        # tick1: 提交 tick, 不成交
        tick1 = make_tick(cum_volume=100000)
        self.engine.match_tick(tick1)

        # 撤单前不应有成交
        self.assertEqual(order.traded, 0)
        self.engine.cancel_order(order.order_id)
        self.assertEqual(order.status, OrderStatus.CANCELLED)


# ════════════════════════════════════════════════
#  通用功能
# ════════════════════════════════════════════════


class TestGeneral(unittest.TestCase):
    def test_order_id_sequential(self):
        engine = MatchingEngine()
        o1 = engine.submit_order("A", Direction.BUY, OrderType.LIMIT, 1.0, 100)
        o2 = engine.submit_order("B", Direction.BUY, OrderType.LIMIT, 1.0, 100)
        self.assertEqual(o1.order_id, "O000001")
        self.assertEqual(o2.order_id, "O000002")

    def test_reset(self):
        engine = MatchingEngine(mode=MatchingMode.CLOSE_FILL)
        engine.submit_order("A", Direction.BUY, OrderType.LIMIT, 1.0, 100)
        engine.reset()
        self.assertEqual(len(engine.get_active_orders()), 0)
        self.assertEqual(len(engine.get_all_trades()), 0)

    def test_cancel_all(self):
        engine = MatchingEngine(mode=MatchingMode.TICK_FILL)
        engine.submit_order("A", Direction.BUY, OrderType.LIMIT, 1.0, 100)
        engine.submit_order("B", Direction.SELL, OrderType.LIMIT, 2.0, 200)
        cancelled = engine.cancel_all()
        self.assertEqual(len(cancelled), 2)
        self.assertEqual(len(engine.get_active_orders()), 0)

    def test_get_pending_by_symbol(self):
        engine = MatchingEngine()
        engine.submit_order("A", Direction.BUY, OrderType.LIMIT, 1.0, 100)
        engine.submit_order("B", Direction.BUY, OrderType.LIMIT, 1.0, 100)
        engine.submit_order("A", Direction.SELL, OrderType.LIMIT, 2.0, 50)

        pending_a = engine.get_pending_orders("A")
        self.assertEqual(len(pending_a), 2)
        pending_all = engine.get_pending_orders()
        self.assertEqual(len(pending_all), 3)


if __name__ == "__main__":
    unittest.main()
