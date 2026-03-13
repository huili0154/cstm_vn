"""
观测测试: 159655.SZ 100万股市价卖出在 SMART_TICK_DELAY_FILL 模式下的逐笔成交。

运行方式: python tests/test_smart_sell_observation.py
"""
import sys
from pathlib import Path

# 确保项目根目录在 sys.path 中
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.datatypes import MatchingMode, Trade
from core.strategy import StrategyBase
from backtest.engine import BacktestEngine


class SellObserverStrategy(StrategyBase):
    """在指定时间挂出市价大单卖出，收集所有成交记录。"""

    symbols = ["159655.SZ"]
    parameters = ["sell_volume", "sell_hour", "sell_minute"]

    def __init__(self, engine, strategy_name: str = "SellObserver"):
        super().__init__(engine, strategy_name, self.symbols)
        self.sell_volume: int = 200_000
        self.sell_hour: int = 10
        self.sell_minute: int = 30

        self._order_sent: bool = False
        self.collected_trades: list[tuple[Trade, int, int]] = []  # (trade, vol_delta, bid_vol_1)
        self._prev_cum_volume: int = 0

    # ── 生命周期 ──
    def on_init(self) -> None:
        pass

    def on_start(self) -> None:
        pass

    def on_stop(self) -> None:
        pass

    # ── 行情 ──
    def on_tick(self, tick) -> None:
        if self._order_sent:
            return
        t = tick.datetime
        if t.hour > self.sell_hour or (
            t.hour == self.sell_hour and t.minute >= self.sell_minute
        ):
            self.sell_market(tick.symbol, self.sell_volume)
            self._order_sent = True

    def on_day_begin(self, bar) -> None:
        pass

    # ── 交易 ──
    def on_order(self, order) -> None:
        pass

    def on_trade(self, trade: Trade) -> None:
        tick = self.get_latest_tick(trade.symbol)
        if tick:
            vol_delta = tick.cum_volume - self._prev_cum_volume
            if vol_delta < 0:
                vol_delta = 0
            # 同一tick多笔成交时delta只算第一笔
            if vol_delta > 0:
                self._prev_cum_volume = tick.cum_volume
            bid_vol_1 = tick.bid_volume_1
        else:
            vol_delta = 0
            bid_vol_1 = 0
        self.collected_trades.append((trade, vol_delta, bid_vol_1))


def main():
    engine = BacktestEngine(
        dataset_dir="dataset",
        mode=MatchingMode.SMART_TICK_DELAY_FILL,
        initial_capital=0.0,
        rate=0.00005,
        slippage=0.0,
        min_commission=0.0,
        pricetick=0.001,
        volume_limit_ratio=0.2,
    )

    strategy = SellObserverStrategy(engine)
    strategy.update_setting({
        "sell_volume": 200_000,
        "sell_hour": 10,
        "sell_minute": 30,
    })

    result = engine.run(
        strategy,
        start_date="20250106",
        end_date="20250106",
        initial_positions={"159655.SZ": (200_000, 1.711)},
    )

    # ── 打印逐笔成交 ──
    trades = strategy.collected_trades
    print()
    print("=" * 80)
    print(f"  159655.SZ 市价卖出观测  |  "
          f"总委托量={strategy.sell_volume:,}  ratio=0.2")
    print("=" * 80)
    print(f"{'序号':>5}  {'成交时间':<24}  {'成交价':>8}  {'成交量':>10}  "
          f"{'累计成交':>10}  {'tick量':>10}  {'bid_vol_1':>10}")
    print("-" * 100)

    cum_vol = 0
    total_turnover = 0.0
    for i, (t, vd, bv1) in enumerate(trades, 1):
        cum_vol += t.volume
        total_turnover += t.price * t.volume
        vd_str = f"{vd:>10,}" if vd > 0 else f"{'↑':>10}"
        print(f"{i:>5}  {str(t.datetime):<24}  {t.price:>8.3f}  {t.volume:>10,}  "
              f"{cum_vol:>10,}  {vd_str}  {bv1:>10,}")

    print("-" * 100)

    # ── 汇总 ──
    vwap = total_turnover / cum_vol if cum_vol > 0 else 0.0
    filled = cum_vol >= strategy.sell_volume
    remaining = strategy.sell_volume - cum_vol

    print(f"  总成交笔数:  {len(trades)}")
    print(f"  总成交量:    {cum_vol:,} 股")
    print(f"  委托量:      {strategy.sell_volume:,} 股")
    print(f"  剩余未成交:  {remaining:,} 股")
    print(f"  加权均价:    {vwap:.4f}")
    print(f"  总佣金:      {result.total_commission:.2f}")
    print(f"  全部成交:    {'✓ 是' if filled else '✗ 否'}")
    print(f"  期末现金:    {result.end_balance:,.2f}")
    print("=" * 80)


if __name__ == "__main__":
    main()
