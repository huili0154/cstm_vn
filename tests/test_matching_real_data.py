"""
真实 Tick 数据精确验证测试 — 159655.SZ / 2025-06-03

完全通过 BacktestEngine.run() 驱动, 与实际回测使用方式一模一样。
每个测试编写一个继承 StrategyBase 的测试策略, 在 on_tick() 中下单,
在 on_trade() 中记录成交, 回测结束后对账。

固定品种: 159655.SZ
固定日期: 2025-06-03
固定起始时刻: 09:30:03 (策略在此之后才下单, 跳过盘前)
"""

import unittest
from datetime import time as dtime
from pathlib import Path

from backtest.engine import BacktestEngine
from core.datatypes import (
    BarData,
    Direction,
    MatchingMode,
    Order,
    OrderType,
    TickData,
    Trade,
)
from core.strategy import StrategyBase

# ────────────────────────────────────────────────
#  常量
# ────────────────────────────────────────────────

SYMBOL = "159655.SZ"
DATE = "20250603"
DATASET_DIR = str(Path(__file__).resolve().parent.parent / "dataset")
RATIO = 0.5
LOT_SIZE = 100
RATE = 0.00005
INITIAL_CAPITAL = 1_000_000.0
MARKET_OPEN = dtime(9, 30, 3)  # 跳过集合竞价


def round_lot(v: int) -> int:
    return (v // LOT_SIZE) * LOT_SIZE


def is_market_open(tick: TickData) -> bool:
    return tick.datetime.time() >= MARKET_OPEN


# ════════════════════════════════════════════════
#  测试 1: 市价买单 — 逐 tick 成交量审计
# ════════════════════════════════════════════════


class MarketBuyAuditStrategy(StrategyBase):
    """09:30:03 后下一笔大市价买单, 记录所有 tick 和成交。"""

    def on_init(self):
        self.order_vol = 500000
        self.ordered = False
        self.trade_log: list[Trade] = []
        self.all_ticks: list[TickData] = []
        self.order_tick_idx: int = -1  # 下单时的 tick 索引

    def on_tick(self, tick: TickData):
        self.all_ticks.append(tick)
        if not self.ordered and is_market_open(tick):
            self.buy_market(SYMBOL, self.order_vol)
            self.ordered = True
            self.order_tick_idx = len(self.all_ticks) - 1

    def on_trade(self, trade: Trade):
        self.trade_log.append(trade)


class TestRealDataMarketBuyAudit(unittest.TestCase):

    def test_market_buy_per_tick_audit(self):
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            initial_capital=INITIAL_CAPITAL,
            rate=RATE,
            volume_limit_ratio=RATIO,
        )
        strategy = MarketBuyAuditStrategy(engine, "audit_market_buy", [SYMBOL])
        result = engine.run(strategy, DATE, DATE)

        self.assertGreater(len(result.trades), 0, "应有成交")

        ticks = strategy.all_ticks
        oi = strategy.order_tick_idx
        self.assertGreater(oi, -1, "策略未下单")

        # 引擎内部 delta 追踪:
        #   match_tick 仅在有挂单后才调用。下单后:
        #   tick[oi+1]: match_tick 首次调用, _prev_cum=0 → delta=cum_vol(巨大), 但延迟跳过
        #   tick[oi+2]: match_tick 第二次, delta=正常, 延迟满足, 可撮合
        # 所以审计从 tick[oi+2] 开始, prev_cum 用 tick[oi+1].cum_volume
        start = oi + 2
        self.assertLess(start, len(ticks), "tick 数据不足以跳过延迟")

        prev_cum = ticks[oi + 1].cum_volume
        remaining = strategy.order_vol
        trade_idx = 0

        for i in range(start, min(len(ticks), oi + 40)):
            tick = ticks[i]
            delta = max(0, tick.cum_volume - prev_cum)
            prev_cum = tick.cum_volume

            # 手算期望
            if remaining > 0 and tick.ask_price_1 > 0 and delta > 0:
                expected_fill = min(
                    round_lot(int(min(tick.ask_volume_1, delta) * RATIO)),
                    remaining,
                )
            else:
                expected_fill = 0

            # 通过 trade.datetime 匹配成交
            actual_fill = 0
            while (trade_idx < len(strategy.trade_log)
                   and strategy.trade_log[trade_idx].datetime == tick.datetime):
                actual_fill += strategy.trade_log[trade_idx].volume
                trade_idx += 1

            self.assertEqual(
                actual_fill, expected_fill,
                f"tick[{i}] {tick.datetime}: "
                f"delta={delta}, ask_vol1={tick.ask_volume_1}, "
                f"expected={expected_fill}, actual={actual_fill}, remaining={remaining}"
            )
            remaining -= actual_fill

            if remaining <= 0:
                break


# ════════════════════════════════════════════════
#  测试 2: 主动限价买单 — 多档吃盘口逐档审计
# ════════════════════════════════════════════════


class AggressiveLimitAuditStrategy(StrategyBase):
    """09:30:03 后下一笔高价限价买单穿越多档。"""

    def on_init(self):
        self.order_price = 1.560  # 远高于市价
        self.order_vol = 1000000
        self.ordered = False
        self.trade_log: list[Trade] = []
        self.all_ticks: list[TickData] = []
        self.order_tick_idx: int = -1

    def on_tick(self, tick: TickData):
        self.all_ticks.append(tick)
        if not self.ordered and is_market_open(tick):
            self.buy(SYMBOL, self.order_price, self.order_vol)
            self.ordered = True
            self.order_tick_idx = len(self.all_ticks) - 1

    def on_trade(self, trade: Trade):
        self.trade_log.append(trade)


class TestRealDataAggressiveLimitAudit(unittest.TestCase):

    def test_aggressive_limit_buy_level_audit(self):
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            initial_capital=5_000_000.0,
            rate=RATE,
            volume_limit_ratio=RATIO,
        )
        strategy = AggressiveLimitAuditStrategy(engine, "audit_aggressive", [SYMBOL])
        result = engine.run(strategy, DATE, DATE)

        self.assertGreater(len(result.trades), 0, "应有成交")

        ticks = strategy.all_ticks
        oi = strategy.order_tick_idx

        # 首批成交在 tick[oi+2]
        first_match_tick = ticks[oi + 2]
        prev_tick = ticks[oi + 1]
        delta = max(0, first_match_tick.cum_volume - prev_tick.cum_volume)

        first_dt = first_match_tick.datetime
        first_tick_trades = [t for t in result.trades if t.datetime == first_dt]
        self.assertGreater(len(first_tick_trades), 0)

        # 逐档手算
        ask_prices = first_match_tick.ask_prices()
        ask_volumes = first_match_tick.ask_volumes()
        rem_delta = delta
        expected_trades = []
        order_remaining = strategy.order_vol

        for level_idx in range(10):
            ap = ask_prices[level_idx]
            av = ask_volumes[level_idx]
            if ap <= 0 or av <= 0:
                continue
            if strategy.order_price < ap:
                break
            if rem_delta <= 0:
                break
            max_fill = round_lot(int(min(av, rem_delta) * RATIO))
            fill = min(max_fill, order_remaining)
            if fill > 0:
                expected_trades.append((ap, fill))
                rem_delta -= fill
                order_remaining -= fill

        # 断言笔数和每笔精确值
        self.assertEqual(
            len(first_tick_trades), len(expected_trades),
            f"成交笔数: engine={len(first_tick_trades)}, expected={len(expected_trades)}\n"
            f"delta={delta}, asks={list(zip(ask_prices[:5], ask_volumes[:5]))}"
        )

        for j, (trade, (exp_price, exp_vol)) in enumerate(
            zip(first_tick_trades, expected_trades)
        ):
            self.assertAlmostEqual(
                trade.price, exp_price, places=3,
                msg=f"Level {j+1} 价格: got {trade.price}, expected {exp_price}"
            )
            self.assertEqual(
                trade.volume, exp_vol,
                f"Level {j+1} 成交量: got {trade.volume}, expected {exp_vol}"
            )

        total = sum(t.volume for t in first_tick_trades)
        self.assertLessEqual(total, delta, "总成交量超过 volume_delta")


# ════════════════════════════════════════════════
#  测试 3: 多笔同向市价买单 — 跨订单 delta 共享审计
# ════════════════════════════════════════════════


class MultiOrderStrategy(StrategyBase):
    """09:30:03 后同时下 3 笔市价买单。"""

    def on_init(self):
        self.order_vols = [200000, 150000, 100000]
        self.ordered = False
        self.trade_log: list[Trade] = []
        self.all_ticks: list[TickData] = []
        self.order_tick_idx: int = -1

    def on_tick(self, tick: TickData):
        self.all_ticks.append(tick)
        if not self.ordered and is_market_open(tick):
            for v in self.order_vols:
                self.buy_market(SYMBOL, v)
            self.ordered = True
            self.order_tick_idx = len(self.all_ticks) - 1

    def on_trade(self, trade: Trade):
        self.trade_log.append(trade)


class TestRealDataMultiOrderDeltaSharing(unittest.TestCase):

    def test_three_market_buys_share_delta(self):
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            initial_capital=5_000_000.0,
            rate=RATE,
            volume_limit_ratio=RATIO,
        )
        strategy = MultiOrderStrategy(engine, "audit_multi", [SYMBOL])
        result = engine.run(strategy, DATE, DATE)

        self.assertGreater(len(result.trades), 0)

        ticks = strategy.all_ticks
        oi = strategy.order_tick_idx

        # 首次撮合 tick
        match_tick = ticks[oi + 2]
        prev_tick = ticks[oi + 1]
        delta = max(0, match_tick.cum_volume - prev_tick.cum_volume)

        first_dt = match_tick.datetime
        first_tick_trades = [t for t in result.trades if t.datetime == first_dt]

        # 手算: 3 笔单共享 remaining_delta, 每笔只吃 ask1
        rem_delta = delta
        expected_fills = []
        for v in strategy.order_vols:
            if rem_delta <= 0 or match_tick.ask_price_1 <= 0:
                expected_fills.append(0)
                continue
            max_fill = round_lot(
                int(min(match_tick.ask_volume_1, rem_delta) * RATIO)
            )
            fill = min(max_fill, v)
            expected_fills.append(fill)
            rem_delta -= fill

        actual_fills = [t.volume for t in first_tick_trades]
        expected_nonzero = [f for f in expected_fills if f > 0]

        self.assertEqual(
            actual_fills, expected_nonzero,
            f"delta={delta}, ask_vol1={match_tick.ask_volume_1}, "
            f"expected={expected_fills}, actual={actual_fills}"
        )

        self.assertLessEqual(sum(actual_fills), delta)


# ════════════════════════════════════════════════
#  测试 4: 被动排队买单 — 公式精确验证
# ════════════════════════════════════════════════


class PassiveQueueStrategy(StrategyBase):
    """09:30:03 后挂一个低于 ask1 的被动限价买单。"""

    def on_init(self):
        self.passive_price = 0.0
        self.order_vol = 100000
        self.ordered = False
        self.trade_log: list[Trade] = []
        self.all_ticks: list[TickData] = []
        self.order_tick_idx: int = -1

    def on_tick(self, tick: TickData):
        self.all_ticks.append(tick)
        if not self.ordered and is_market_open(tick):
            # 挂在 bid1 价位, 不穿越 ask
            self.passive_price = tick.bid_price_1
            if self.passive_price > 0:
                self.buy(SYMBOL, self.passive_price, self.order_vol)
                self.ordered = True
                self.order_tick_idx = len(self.all_ticks) - 1

    def on_trade(self, trade: Trade):
        self.trade_log.append(trade)


class TestRealDataPassiveQueueAudit(unittest.TestCase):

    def test_passive_buy_audit(self):
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            initial_capital=INITIAL_CAPITAL,
            rate=RATE,
            volume_limit_ratio=RATIO,
        )
        strategy = PassiveQueueStrategy(engine, "audit_passive", [SYMBOL])
        result = engine.run(strategy, DATE, DATE)

        self.assertGreater(len(result.trades), 0, "整天无被动成交")

        # 验证首笔: 成交价 == 委托价 (被动成交特征)
        first_trade = result.trades[0]
        self.assertAlmostEqual(
            first_trade.price, strategy.passive_price, places=3,
            msg="被动成交价应等于委托价"
        )

        # 找到首笔成交对应的 tick, 手算被动排队公式
        first_dt = first_trade.datetime
        ticks = strategy.all_ticks
        tick_for_trade = None
        tick_before = None
        for i, tick in enumerate(ticks):
            if tick.datetime == first_dt:
                tick_for_trade = tick
                tick_before = ticks[i - 1] if i > 0 else None
                break

        self.assertIsNotNone(tick_for_trade)
        self.assertIsNotNone(tick_before)
        delta = max(0, tick_for_trade.cum_volume - tick_before.cum_volume)

        # 触发条件: last_price <= 委托价
        self.assertLessEqual(tick_for_trade.last_price, strategy.passive_price + 0.0001)

        # 手算被动排队公式
        remaining = strategy.order_vol
        level_volume = (tick_for_trade.bid_volume_1
                        if tick_for_trade.bid_volume_1 > 0 else 1)
        estimated = round_lot(int(
            delta * (remaining / max(level_volume, remaining)) * 0.5
        ))
        expected_fill = min(max(estimated, 0), remaining)

        self.assertEqual(
            first_trade.volume, expected_fill,
            f"delta={delta}, bid_vol1={tick_for_trade.bid_volume_1}, "
            f"remaining={remaining}, expected={expected_fill}, got={first_trade.volume}"
        )


# ════════════════════════════════════════════════
#  测试 5: 全链路 — 资金/持仓/盈亏对账
# ════════════════════════════════════════════════


class BuyHoldStrategy(StrategyBase):
    """09:30:03 后买入, 一直持有到收盘。"""

    def on_init(self):
        self.order_vol = 100000
        self.ordered = False
        self.trade_log: list[Trade] = []

    def on_tick(self, tick: TickData):
        if not self.ordered and is_market_open(tick):
            self.buy_market(SYMBOL, self.order_vol)
            self.ordered = True

    def on_trade(self, trade: Trade):
        self.trade_log.append(trade)


class TestRealDataFullCycleReconciliation(unittest.TestCase):
    """
    通过 BacktestEngine 跑完整一天, 验证:
    1. NAV = balance + 持仓市值
    2. 总手续费 = Σ(trade.commission)
    3. 资金守恒: 初始资金 = 余额 + 买入花费 + 总佣金 (无卖出)
    4. 持仓量 = Σ(买入成交量)
    """

    def test_buy_hold_reconcile(self):
        rate = 0.0003
        engine = BacktestEngine(
            dataset_dir=DATASET_DIR,
            mode=MatchingMode.SMART_TICK_DELAY_FILL,
            initial_capital=INITIAL_CAPITAL,
            rate=rate,
            volume_limit_ratio=RATIO,
        )
        strategy = BuyHoldStrategy(engine, "audit_full", [SYMBOL])
        result = engine.run(strategy, DATE, DATE)

        self.assertGreater(len(result.trades), 0, "应有成交")

        # 1. 总手续费一致
        sum_trade_commission = sum(t.commission for t in result.trades)
        self.assertAlmostEqual(
            result.total_commission, sum_trade_commission, places=4,
            msg="total_commission 与 Σ(trade.commission) 不一致"
        )

        # 2. 每笔佣金公式正确
        for trade in result.trades:
            expected_comm = max(trade.price * trade.volume * rate, 0)
            self.assertAlmostEqual(
                trade.commission, expected_comm, places=6,
                msg=f"Trade {trade.trade_id} 佣金不匹配"
            )

        # 3. 持仓量 = 总买入量
        total_filled = sum(t.volume for t in result.trades)
        pos = engine._positions.get(SYMBOL)
        self.assertIsNotNone(pos)
        self.assertEqual(pos.volume, total_filled)
        self.assertLessEqual(total_filled, strategy.order_vol)

        # 4. 资金守恒 (只买无卖): 初始资金 = 余额 + 买入花费 + 总佣金
        total_buy_cost = sum(t.price * t.volume for t in result.trades)
        self.assertAlmostEqual(
            INITIAL_CAPITAL,
            engine._account.balance + total_buy_cost + sum_trade_commission,
            places=2,
            msg="资金不守恒"
        )

        # 5. NAV = balance + 持仓市值
        expected_nav = engine._account.balance + pos.volume * pos.market_price
        self.assertAlmostEqual(
            result.end_balance, expected_nav, places=2,
            msg="NAV 与 balance + 持仓市值不一致"
        )

        # 6. end_balance 应大于 0
        self.assertGreater(result.end_balance, 0)


if __name__ == "__main__":
    unittest.main()
