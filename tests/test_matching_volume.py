"""
成交量计算精确性测试 — 验证 SMART_TICK_DELAY_FILL 模式的核心公式。

覆盖:
  1. 市价单精确成交量（含 _round_lot 效果）
  2. 主动吃盘口逐档精确成交量（含跨档 delta 消耗）
  3. 被动排队精确估算
  4. 跨订单 delta 消耗
  5. delta 耗尽后停止成交
  6. _round_lot 边界
"""

import unittest
from datetime import datetime

from core.datatypes import (
    Direction,
    MatchingMode,
    OrderStatus,
    OrderType,
    TickData,
)
from core.matching import MatchingEngine


# ────────────────────────────────────────────────
#  工具函数
# ────────────────────────────────────────────────


def make_tick(
    symbol: str = "510300.SH",
    last_price: float = 4.0,
    ask1: float = 4.001,
    bid1: float = 3.999,
    ask_vol1: int = 100000,
    bid_vol1: int = 100000,
    cum_volume: int = 1000000,
    dt: datetime | None = None,
    **kwargs,
) -> TickData:
    """快速构造 TickData，支持通过 kwargs 设置多档盘口。"""
    fields = dict(
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
    fields.update(kwargs)
    return TickData(**fields)


def make_engine(ratio: float = 0.5, rate: float = 0.0) -> MatchingEngine:
    return MatchingEngine(
        mode=MatchingMode.SMART_TICK_DELAY_FILL,
        rate=rate,
        volume_limit_ratio=ratio,
    )


def skip_delay(engine: MatchingEngine, cum_volume: int = 100000) -> None:
    """喂一个 tick 跳过延迟期。"""
    engine.match_tick(make_tick(cum_volume=cum_volume))


# ════════════════════════════════════════════════
#  1. 市价单精确成交量
# ════════════════════════════════════════════════


class TestMarketOrderVolumePrecise(unittest.TestCase):
    """验证市价单公式: _round_lot(int(min(level_vol, remaining_delta) * ratio))"""

    def test_basic_formula(self):
        """ask_vol=50000, delta=100000, ratio=0.5 → 25000"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        tick = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=200000)
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].volume, 25000)
        self.assertEqual(trades[0].price, 4.001)

    def test_delta_smaller_than_level_vol(self):
        """delta < level_vol 时, delta 是瓶颈"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta=30000, ask_vol=50000 → min(50000,30000)*0.5=15000
        tick = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=130000)
        trades = engine.match_tick(tick)

        self.assertEqual(trades[0].volume, 15000)

    def test_round_lot_truncation(self):
        """非100整数倍时 round_lot 向下截断"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta=100000, ask_vol=333 → int(min(333,100000)*0.5)=166 → round_lot=100
        tick = make_tick(ask1=4.001, ask_vol1=333, cum_volume=200000)
        trades = engine.match_tick(tick)

        self.assertEqual(trades[0].volume, 100)

    def test_round_lot_to_zero(self):
        """计算结果不足一手时不成交"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta=100000, ask_vol=99 → int(99*0.5)=49 → round_lot=0
        tick = make_tick(ask1=4.001, ask_vol1=99, cum_volume=200000)
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 0)

    def test_order_remaining_caps_fill(self):
        """order.remaining 小于 max_fill 时, remaining 是上限"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 1000)
        skip_delay(engine, cum_volume=100000)

        # max_fill = round_lot(int(min(50000,100000)*0.5)) = 25000
        # 但 order.remaining = 1000, 所以 fill = 1000
        tick = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=200000)
        trades = engine.match_tick(tick)

        self.assertEqual(trades[0].volume, 1000)

    def test_sell_market_precise(self):
        """卖市价单对称验证"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.SELL, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta=80000, bid_vol=40000 → min(40000,80000)*0.5=20000
        tick = make_tick(bid1=3.999, bid_vol1=40000, cum_volume=180000)
        trades = engine.match_tick(tick)

        self.assertEqual(trades[0].volume, 20000)
        self.assertEqual(trades[0].price, 3.999)

    def test_ratio_variation(self):
        """不同 ratio 值验证"""
        for ratio, expected in [(1.0, 50000), (0.1, 5000), (0.25, 12500)]:
            engine = make_engine(ratio=ratio)
            engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 200000)
            skip_delay(engine, cum_volume=100000)

            tick = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=200000)
            trades = engine.match_tick(tick)

            self.assertEqual(
                trades[0].volume, expected,
                f"ratio={ratio}: expected {expected}, got {trades[0].volume}"
            )


# ════════════════════════════════════════════════
#  2. 主动吃盘口 — 逐档精确成交量 + delta 消耗
# ════════════════════════════════════════════════


class TestAggressiveFillPrecise(unittest.TestCase):

    def test_multi_level_buy_exact_volumes(self):
        """3档精确成交量，delta 充裕时各档独立计算（但会被消耗）。"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 4.005, 200000)
        skip_delay(engine, cum_volume=100000)

        # delta = 200000-100000 = 100000
        # ask1=4.001 vol=20000: round_lot(int(min(20000,100000)*0.5)) = 10000 → rem=90000
        # ask2=4.002 vol=30000: round_lot(int(min(30000,90000)*0.5))  = 15000 → rem=75000
        # ask3=4.003 vol=50000: round_lot(int(min(50000,75000)*0.5))  = 25000 → rem=50000
        tick = make_tick(
            ask1=4.001, ask_vol1=20000,
            ask_price_2=4.002, ask_volume_2=30000,
            ask_price_3=4.003, ask_volume_3=50000,
            bid1=4.000, bid_vol1=40000,
            cum_volume=200000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 3)
        self.assertEqual(trades[0].price, 4.001)
        self.assertEqual(trades[0].volume, 10000)
        self.assertEqual(trades[1].price, 4.002)
        self.assertEqual(trades[1].volume, 15000)
        self.assertEqual(trades[2].price, 4.003)
        self.assertEqual(trades[2].volume, 25000)
        self.assertEqual(sum(t.volume for t in trades), 50000)

    def test_multi_level_sell_exact_volumes(self):
        """卖单3档精确验证（设计文档中的示例, 但带 delta 消耗）。"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.SELL, OrderType.LIMIT, 3.995, 200000)
        skip_delay(engine, cum_volume=100000)

        # delta = 100000
        # bid1=3.999 vol=30000: round_lot(int(min(30000,100000)*0.5)) = 15000 → rem=85000
        # bid2=3.998 vol=20000: round_lot(int(min(20000,85000)*0.5))  = 10000 → rem=75000
        # bid3=3.997 vol=10000: round_lot(int(min(10000,75000)*0.5))  = 5000  → rem=70000
        tick = make_tick(
            ask1=4.001, ask_vol1=10000,
            bid1=3.999, bid_vol1=30000,
            bid_price_2=3.998, bid_volume_2=20000,
            bid_price_3=3.997, bid_volume_3=10000,
            cum_volume=200000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 3)
        self.assertEqual(trades[0].price, 3.999)
        self.assertEqual(trades[0].volume, 15000)
        self.assertEqual(trades[1].price, 3.998)
        self.assertEqual(trades[1].volume, 10000)
        self.assertEqual(trades[2].price, 3.997)
        self.assertEqual(trades[2].volume, 5000)

    def test_delta_consumed_across_levels(self):
        """delta 较小时，后续档位可用量显著减少。"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 4.005, 200000)
        skip_delay(engine, cum_volume=100000)

        # delta = 20000 (很小)
        # ask1=4.001 vol=50000: round_lot(int(min(50000,20000)*0.5)) = 10000 → rem=10000
        # ask2=4.002 vol=50000: round_lot(int(min(50000,10000)*0.5)) = 5000  → rem=5000
        # ask3=4.003 vol=50000: round_lot(int(min(50000,5000)*0.5))  = 2500→round=2500 → rem=2500
        tick = make_tick(
            ask1=4.001, ask_vol1=50000,
            ask_price_2=4.002, ask_volume_2=50000,
            ask_price_3=4.003, ask_volume_3=50000,
            bid1=4.000, bid_vol1=40000,
            cum_volume=120000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(trades[0].volume, 10000)
        self.assertEqual(trades[1].volume, 5000)
        self.assertEqual(trades[2].volume, 2500)
        total = sum(t.volume for t in trades)
        self.assertEqual(total, 17500)
        # 关键: 总成交量 17500 < delta 20000 ✓ (旧实现会是 30000, 超过 delta)

    def test_delta_exhausted_mid_scan(self):
        """delta 在扫描中途耗尽，后续档位不成交。"""
        engine = make_engine(ratio=1.0)  # ratio=1.0 消耗更快
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 4.005, 200000)
        skip_delay(engine, cum_volume=100000)

        # delta = 10000
        # ask1 vol=8000: round_lot(int(min(8000,10000)*1.0)) = 8000 → rem=2000
        # ask2 vol=50000: round_lot(int(min(50000,2000)*1.0)) = 2000 → rem=0
        # ask3: rem=0 → break
        tick = make_tick(
            ask1=4.001, ask_vol1=8000,
            ask_price_2=4.002, ask_volume_2=50000,
            ask_price_3=4.003, ask_volume_3=50000,
            bid1=4.000, bid_vol1=40000,
            cum_volume=110000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].volume, 8000)
        self.assertEqual(trades[1].volume, 2000)
        self.assertEqual(sum(t.volume for t in trades), 10000)  # 恰好 == delta

    def test_order_price_stops_scan(self):
        """委托价不够吃到某档时停止，不继续扫后续档位。"""
        engine = make_engine(ratio=0.5)
        # 委托价 4.002，只能吃到 ask1=4.001 和 ask2=4.002
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 4.002, 200000)
        skip_delay(engine, cum_volume=100000)

        tick = make_tick(
            ask1=4.001, ask_vol1=20000,
            ask_price_2=4.002, ask_volume_2=30000,
            ask_price_3=4.003, ask_volume_3=50000,  # 吃不到
            bid1=4.000, bid_vol1=40000,
            cum_volume=200000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].price, 4.001)
        self.assertEqual(trades[1].price, 4.002)
        # 不应有 4.003 的成交


# ════════════════════════════════════════════════
#  3. 被动排队精确估算
# ════════════════════════════════════════════════


class TestPassiveQueuePrecise(unittest.TestCase):
    """验证被动排队公式:
    _round_lot(int(remaining_delta * (remaining / max(level_vol, remaining)) * 0.5))
    """

    def test_buy_passive_small_order(self):
        """小单(remaining < level_volume): 比例缩小"""
        engine = make_engine(ratio=0.5)
        # 买限价 3.998 < ask1=4.001 → 不穿越，走被动排队
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 3.998, 5000)
        skip_delay(engine, cum_volume=100000)

        # last_price=3.998 <= order.price=3.998 → 触发
        # level_volume = bid_vol1 = 200000
        # estimated = round_lot(int(50000 * (5000/max(200000,5000)) * 0.5))
        #           = round_lot(int(50000 * (5000/200000) * 0.5))
        #           = round_lot(int(50000 * 0.025 * 0.5))
        #           = round_lot(int(625)) = 600
        tick = make_tick(
            last_price=3.998,
            ask1=4.001, ask_vol1=10000,
            bid1=3.998, bid_vol1=200000,
            cum_volume=150000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].volume, 600)
        self.assertEqual(trades[0].price, 3.998)  # 被动成交用自己的委托价

    def test_buy_passive_large_order(self):
        """大单(remaining >= level_volume): 比例=1"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 3.998, 500000)
        skip_delay(engine, cum_volume=100000)

        # remaining=500000 >= level_volume=200000 → ratio_part = 1.0
        # estimated = round_lot(int(50000 * 1.0 * 0.5)) = 25000
        tick = make_tick(
            last_price=3.998,
            ask1=4.001, ask_vol1=10000,
            bid1=3.998, bid_vol1=200000,
            cum_volume=150000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].volume, 25000)

    def test_sell_passive(self):
        """卖被动排队对称验证"""
        engine = make_engine(ratio=0.5)
        # 卖限价 4.002 > bid1=3.999 → 不穿越，走被动排队
        engine.submit_order("510300.SH", Direction.SELL, OrderType.LIMIT, 4.002, 10000)
        skip_delay(engine, cum_volume=100000)

        # last_price=4.003 >= order.price=4.002 → 触发
        # level_volume = ask_vol1 = 80000
        # estimated = round_lot(int(60000 * (10000/max(80000,10000)) * 0.5))
        #           = round_lot(int(60000 * 0.125 * 0.5))
        #           = round_lot(int(3750)) = 3700
        tick = make_tick(
            last_price=4.003,
            ask1=4.001, ask_vol1=80000,
            bid1=3.999, bid_vol1=50000,
            cum_volume=160000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].volume, 3700)
        self.assertEqual(trades[0].price, 4.002)

    def test_passive_no_trigger_when_price_not_reached(self):
        """last_price 未到委托价时不触发"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 3.990, 10000)
        skip_delay(engine, cum_volume=100000)

        # last_price=3.995 > order.price=3.990 → 不触发
        tick = make_tick(last_price=3.995, cum_volume=150000)
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 0)

    def test_passive_zero_delta(self):
        """delta=0 时不成交"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 3.998, 10000)
        skip_delay(engine, cum_volume=150000)

        # cum_volume 没变 → delta=0
        tick = make_tick(last_price=3.998, cum_volume=150000)
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 0)


# ════════════════════════════════════════════════
#  4. 跨订单 delta 消耗
# ════════════════════════════════════════════════


class TestCrossOrderDeltaConsumption(unittest.TestCase):

    def test_two_market_buy_orders_share_delta(self):
        """两笔市价买单共享同一 delta，第二笔可用量减少。"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta = 100000
        # order1: round_lot(int(min(80000, 100000)*0.5)) = 40000, rem_delta=60000
        # order2: round_lot(int(min(80000, 60000)*0.5))  = 30000, rem_delta=30000
        tick = make_tick(ask1=4.001, ask_vol1=80000, cum_volume=200000)
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].volume, 40000)
        self.assertEqual(trades[1].volume, 30000)
        self.assertEqual(sum(t.volume for t in trades), 70000)

    def test_buy_and_sell_orders_share_delta(self):
        """买卖单同时挂在同一品种，共享 delta。"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        engine.submit_order("510300.SH", Direction.SELL, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta = 60000
        # order1 (buy): round_lot(int(min(50000,60000)*0.5)) = 25000, rem=35000
        # order2 (sell): round_lot(int(min(50000,35000)*0.5)) = 17500→17500, rem=17500
        tick = make_tick(
            ask1=4.001, ask_vol1=50000,
            bid1=3.999, bid_vol1=50000,
            cum_volume=160000,
        )
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].volume, 25000)  # buy
        self.assertEqual(trades[1].volume, 17500)  # sell
        total = sum(t.volume for t in trades)
        self.assertLessEqual(total, 60000)  # 不超过 delta

    def test_delta_exhausted_second_order_gets_nothing(self):
        """第一笔单耗尽 delta，第二笔无法成交。"""
        engine = make_engine(ratio=1.0)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 5000)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta = 5000, ratio=1.0
        # order1: round_lot(int(min(50000,5000)*1.0)) = 5000, rem_delta=0
        # order2: rem_delta=0 → skip
        tick = make_tick(ask1=4.001, ask_vol1=50000, cum_volume=105000)
        trades = engine.match_tick(tick)

        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].volume, 5000)

    def test_different_symbols_independent_delta(self):
        """不同品种有各自独立的 delta，互不干扰。"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        engine.submit_order("159919.SZ", Direction.BUY, OrderType.MARKET, 0, 100000)

        # 两个品种各自的 delay tick
        engine.match_tick(make_tick(symbol="510300.SH", cum_volume=100000))
        engine.match_tick(make_tick(symbol="159919.SZ", cum_volume=100000))

        # 510300 tick: delta=50000
        tick_sh = make_tick(
            symbol="510300.SH", ask1=4.001, ask_vol1=80000, cum_volume=150000
        )
        trades_sh = engine.match_tick(tick_sh)

        # 159919 tick: delta=30000
        tick_sz = make_tick(
            symbol="159919.SZ", ask1=2.001, ask_vol1=80000, cum_volume=130000
        )
        trades_sz = engine.match_tick(tick_sz)

        # 510300: min(80000,50000)*0.5 = 25000
        self.assertEqual(trades_sh[0].volume, 25000)
        # 159919: min(80000,30000)*0.5 = 15000
        self.assertEqual(trades_sz[0].volume, 15000)

    def test_aggressive_then_market_share_delta(self):
        """一笔主动限价单 + 一笔市价单，共享 delta。"""
        engine = make_engine(ratio=0.5)
        # 先提交限价单（主动穿越）
        engine.submit_order("510300.SH", Direction.BUY, OrderType.LIMIT, 4.005, 200000)
        # 再提交市价单
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 100000)
        skip_delay(engine, cum_volume=100000)

        # delta=50000
        # 限价单: ask1 vol=40000, round_lot(int(min(40000,50000)*0.5))=20000 → rem=30000
        #         ask2 vol=30000, round_lot(int(min(30000,30000)*0.5))=15000 → rem=15000
        # 市价单: round_lot(int(min(40000,15000)*0.5))=7500→7500 → rem=7500
        tick = make_tick(
            ask1=4.001, ask_vol1=40000,
            ask_price_2=4.002, ask_volume_2=30000,
            bid1=4.000, bid_vol1=40000,
            cum_volume=150000,
        )
        trades = engine.match_tick(tick)

        # 限价单 2 笔 + 市价单 1 笔
        self.assertEqual(len(trades), 3)
        self.assertEqual(trades[0].volume, 20000)  # 限价 ask1
        self.assertEqual(trades[1].volume, 15000)  # 限价 ask2
        self.assertEqual(trades[2].volume, 7500)   # 市价 ask1
        self.assertEqual(sum(t.volume for t in trades), 42500)


# ════════════════════════════════════════════════
#  5. _round_lot 边界
# ════════════════════════════════════════════════


class TestRoundLot(unittest.TestCase):
    def test_round_lot_values(self):
        engine = MatchingEngine()
        self.assertEqual(engine._round_lot(0), 0)
        self.assertEqual(engine._round_lot(99), 0)
        self.assertEqual(engine._round_lot(100), 100)
        self.assertEqual(engine._round_lot(150), 100)
        self.assertEqual(engine._round_lot(199), 100)
        self.assertEqual(engine._round_lot(200), 200)
        self.assertEqual(engine._round_lot(12345), 12300)


# ════════════════════════════════════════════════
#  6. 涨停/跌停 单边盘口
# ════════════════════════════════════════════════


class TestLimitUpDown(unittest.TestCase):

    def test_no_ask_no_buy_fill(self):
        """涨停封板 ask_price_1=0，买单不应成交"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 10000)
        skip_delay(engine, cum_volume=100000)

        tick = make_tick(ask1=0, ask_vol1=0, bid1=3.999, bid_vol1=500000,
                         cum_volume=200000)
        trades = engine.match_tick(tick)
        self.assertEqual(len(trades), 0)

    def test_no_bid_no_sell_fill(self):
        """跌停封板 bid_price_1=0，卖单不应成交"""
        engine = make_engine(ratio=0.5)
        engine.submit_order("510300.SH", Direction.SELL, OrderType.MARKET, 0, 10000)
        skip_delay(engine, cum_volume=100000)

        tick = make_tick(ask1=4.001, ask_vol1=500000, bid1=0, bid_vol1=0,
                         cum_volume=200000)
        trades = engine.match_tick(tick)
        self.assertEqual(len(trades), 0)


# ════════════════════════════════════════════════
#  7. 延迟机制精确验证
# ════════════════════════════════════════════════


class TestDelayMechanism(unittest.TestCase):

    def test_submit_at_tick1_match_at_tick3(self):
        """tick1 提交, tick2 不匹配, tick3 匹配"""
        engine = make_engine(ratio=0.5)

        # tick1: seq=1
        engine.match_tick(make_tick(cum_volume=100000))
        # 在 tick1 之后提交
        order = engine.submit_order(
            "510300.SH", Direction.BUY, OrderType.LIMIT, 4.001, 10000
        )

        # tick2: seq=2, 2-1=1 ≤ 1 → 不匹配
        trades2 = engine.match_tick(make_tick(ask1=4.001, cum_volume=200000))
        self.assertEqual(len(trades2), 0)
        self.assertEqual(order.status, OrderStatus.ACTIVE)  # 已转 ACTIVE

        # tick3: seq=3, 3-1=2 > 1 → 匹配
        trades3 = engine.match_tick(make_tick(ask1=4.001, cum_volume=300000))
        self.assertGreater(len(trades3), 0)

    def test_submit_between_ticks(self):
        """两笔单在不同 tick 提交，各自遵守延迟"""
        engine = make_engine(ratio=0.5)

        # tick1
        engine.match_tick(make_tick(cum_volume=100000))
        o1 = engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 5000)

        # tick2: o1 不匹配
        engine.match_tick(make_tick(cum_volume=200000))
        o2 = engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 5000)

        # tick3: o1 可匹配, o2 不可
        trades3 = engine.match_tick(make_tick(ask1=4.001, ask_vol1=50000, cum_volume=300000))
        self.assertEqual(len(trades3), 1)  # 只有 o1

        # tick4: o2 可匹配
        trades4 = engine.match_tick(make_tick(ask1=4.001, ask_vol1=50000, cum_volume=400000))
        self.assertGreater(len(trades4), 0)


# ════════════════════════════════════════════════
#  8. 手续费精确验证
# ════════════════════════════════════════════════


class TestCommissionPrecise(unittest.TestCase):

    def test_commission_formula(self):
        engine = MatchingEngine(rate=0.0003, min_commission=5.0)
        # turnover = 4.0 * 10000 = 40000, fee = 40000*0.0003 = 12.0
        self.assertAlmostEqual(engine.calc_commission(4.0, 10000), 12.0)

    def test_commission_min_applies(self):
        engine = MatchingEngine(rate=0.0003, min_commission=5.0)
        # turnover = 1.0 * 100 = 100, fee = 0.03, min=5.0
        self.assertAlmostEqual(engine.calc_commission(1.0, 100), 5.0)

    def test_commission_at_boundary(self):
        """刚好在临界点：turnover * rate == min_commission"""
        engine = MatchingEngine(rate=0.0003, min_commission=5.0)
        # need turnover = 5.0 / 0.0003 = 16666.67
        # price=1.666667 vol=10000 → turnover=16666.67 → fee=5.0
        self.assertAlmostEqual(engine.calc_commission(1.666667, 10000), 5.0, places=2)

    def test_commission_in_smart_trade(self):
        """SMART 模式成交时佣金正确附加在 Trade 上"""
        engine = make_engine(ratio=0.5, rate=0.0003)
        engine.submit_order("510300.SH", Direction.BUY, OrderType.MARKET, 0, 10000)
        skip_delay(engine, cum_volume=100000)

        tick = make_tick(ask1=4.0, ask_vol1=50000, cum_volume=200000)
        trades = engine.match_tick(tick)

        # fill = round_lot(int(min(50000,100000)*0.5)) = 25000
        # 但 remaining=10000 → fill=10000
        self.assertEqual(trades[0].volume, 10000)
        # commission = max(4.0*10000*0.0003, 0) = 12.0
        self.assertAlmostEqual(trades[0].commission, 12.0)


if __name__ == "__main__":
    unittest.main()
