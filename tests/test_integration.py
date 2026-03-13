"""
集成测试 — 用真实 Parquet 数据验证 data_feed + matching 联动。

依赖 dataset/ 目录下的数据文件:
  - dataset/daily/510300.SH/
  - dataset/ticks/510300.SH/2025-01/20250102.parquet
"""

import unittest
from pathlib import Path

from core.data_feed import ParquetBarFeed, ParquetTickFeed
from core.datatypes import Direction, MatchingMode, OrderType
from core.matching import MatchingEngine

DATASET_DIR = Path(__file__).resolve().parents[1] / "dataset"
SYMBOL = "510300.SH"
TICK_DATE = "20250102"


def _has_data() -> bool:
    return (DATASET_DIR / "ticks" / SYMBOL / "2025-01" / f"{TICK_DATE}.parquet").exists()


@unittest.skipUnless(_has_data(), "需要 dataset/ 下的真实数据")
class TestDataFeedLoad(unittest.TestCase):
    """验证 ParquetBarFeed / ParquetTickFeed 读取正确。"""

    def test_bar_feed_loads(self):
        feed = ParquetBarFeed(DATASET_DIR)
        bars = feed.load(SYMBOL)
        self.assertGreater(len(bars), 0)

        bar = bars[0]
        self.assertEqual(bar.symbol, SYMBOL)
        self.assertGreater(bar.close_price, 0)
        self.assertGreater(bar.volume, 0)

    def test_bar_feed_date_range(self):
        feed = ParquetBarFeed(DATASET_DIR)
        dates = feed.get_trading_dates(SYMBOL)
        self.assertGreater(len(dates), 0)
        # 日期应为 YYYYMMDD 格式，8 位字符串
        self.assertEqual(len(dates[0]), 8)

    def test_tick_feed_loads(self):
        feed = ParquetTickFeed(DATASET_DIR)
        ticks = feed.load_day(SYMBOL, TICK_DATE)
        self.assertGreater(len(ticks), 0)

        # 前几个 tick 可能是盘前（last_price=0），找一个交易时段的
        trading_tick = None
        for t in ticks:
            if t.last_price > 0 and t.cum_volume > 0:
                trading_tick = t
                break
        self.assertIsNotNone(trading_tick, "找不到交易时段的 tick")
        self.assertEqual(trading_tick.symbol, SYMBOL)
        self.assertGreater(trading_tick.last_price, 0)
        self.assertGreater(trading_tick.cum_volume, 0)

    def test_tick_data_has_depth(self):
        """验证 10 档盘口加载正确。"""
        feed = ParquetTickFeed(DATASET_DIR)
        ticks = feed.load_day(SYMBOL, TICK_DATE)

        # 找一个有盘口的 tick（一般盘中都有）
        tick_with_depth = None
        for t in ticks:
            if t.ask_price_1 > 0 and t.bid_price_1 > 0:
                tick_with_depth = t
                break
        self.assertIsNotNone(tick_with_depth, "找不到有盘口的 tick")
        self.assertGreater(tick_with_depth.ask_volume_1, 0)
        self.assertGreater(tick_with_depth.bid_volume_1, 0)

    def test_tick_available_dates(self):
        feed = ParquetTickFeed(DATASET_DIR)
        dates = feed.get_available_dates(SYMBOL)
        self.assertIn(TICK_DATE, dates)


@unittest.skipUnless(_has_data(), "需要 dataset/ 下的真实数据")
class TestIntegrationCloseFill(unittest.TestCase):
    """日线 close_fill 模式端到端验证。"""

    def test_bar_close_fill_round_trip(self):
        feed = ParquetBarFeed(DATASET_DIR)
        bars = feed.load(SYMBOL)
        self.assertGreater(len(bars), 2)

        engine = MatchingEngine(
            mode=MatchingMode.CLOSE_FILL,
            rate=0.0003,
            min_commission=0.0,
        )

        # Day 0: 提交买单
        order = engine.submit_order(
            SYMBOL, Direction.BUY, OrderType.LIMIT,
            bars[0].close_price, 10000,
        )

        # Day 1: 撮合（以 day1 收盘价成交）
        trades = engine.match_bar(bars[1])
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].price, bars[1].close_price)
        self.assertEqual(trades[0].volume, 10000)

        # 账面验证
        all_trades = engine.get_all_trades()
        self.assertEqual(len(all_trades), 1)


@unittest.skipUnless(_has_data(), "需要 dataset/ 下的真实数据")
class TestIntegrationTickFill(unittest.TestCase):
    """Tick 即时成交模式端到端验证。"""

    def test_tick_fill_buy_sell_cycle(self):
        feed = ParquetTickFeed(DATASET_DIR)
        ticks = feed.load_day(SYMBOL, TICK_DATE)
        self.assertGreater(len(ticks), 100)

        engine = MatchingEngine(
            mode=MatchingMode.TICK_FILL,
            rate=0.0003,
        )

        # 在盘口出现后提交买单
        buy_tick = None
        for t in ticks:
            if t.ask_price_1 > 0:
                buy_tick = t
                break

        order = engine.submit_order(
            SYMBOL, Direction.BUY, OrderType.LIMIT,
            buy_tick.ask_price_1, 5000,
        )

        # 下一个 tick 撮合
        for t in ticks:
            trades = engine.match_tick(t)
            if trades:
                self.assertEqual(trades[0].volume, 5000)
                break

        self.assertEqual(len(engine.get_all_trades()), 1)


@unittest.skipUnless(_has_data(), "需要 dataset/ 下的真实数据")
class TestIntegrationSmartFill(unittest.TestCase):
    """Smart tick delay fill 端到端验证。"""

    def test_smart_delay_and_partial(self):
        feed = ParquetTickFeed(DATASET_DIR)
        ticks = feed.load_day(SYMBOL, TICK_DATE)
        self.assertGreater(len(ticks), 100)

        engine = MatchingEngine(
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            rate=0.0003,
            volume_limit_ratio=0.5,
        )

        # 找到盘中实际交易时段（有盘口 + cum_volume > 0）
        start_idx = 0
        for i, t in enumerate(ticks):
            if t.ask_price_1 > 0 and t.bid_price_1 > 0 and t.cum_volume > 0:
                start_idx = i
                break

        # 先推送一个 tick 建立 cum_volume 基准
        engine.match_tick(ticks[start_idx])

        # 提交一个大额市价买单（很可能需要多个 tick 才能全部成交）
        engine.submit_order(
            SYMBOL, Direction.BUY, OrderType.MARKET, 0, 500000,
        )

        # 继续推送后续 ticks
        total_filled = 0
        trade_count = 0
        for t in ticks[start_idx + 1:start_idx + 50]:
            trades = engine.match_tick(t)
            for tr in trades:
                total_filled += tr.volume
                trade_count += 1

        # 至少应该有部分成交（延迟 1 tick 后开始成交）
        self.assertGreater(trade_count, 0, "应该产生至少 1 笔成交")
        self.assertGreater(total_filled, 0, "总成交量应 > 0")


if __name__ == "__main__":
    unittest.main()
