"""
网格策略回测脚本 — 运行后查看绩效。

用法:
    python run_grid_backtest.py
"""

import matplotlib
matplotlib.use("TkAgg")

from core.datatypes import MatchingMode
from backtest.engine import BacktestEngine
from backtest.report import BacktestReport
from strategies.grid_strategy import GridStrategy


def main() -> None:
    # ── 回测配置 ──
    SYMBOL = "510300.SH"
    START = "20250101"
    END = "20250228"
    INITIAL_CAPITAL = 500_000.0

    # ── 策略参数 ──
    setting = {
        "grid_step": 0.015,     # 1.5% 网格间距
        "grid_lots": 5000,      # 每格 5000 份 (约 2.3万元 @ 4.6)
        "max_grids": 5,         # 最多向下买 5 格
        "base_price": 0,        # 0 = 自动取首个价格
    }

    # ── 创建引擎 (TICK_FILL 模式) ──
    engine = BacktestEngine(
        dataset_dir="dataset",
        mode=MatchingMode.TICK_FILL,
        initial_capital=INITIAL_CAPITAL,
        rate=0.00005,
        slippage=0.001,
        pricetick=0.001,
    )

    # ── 创建策略 ──
    strategy = GridStrategy(
        engine=engine,
        strategy_name="Grid510300",
        symbols=[SYMBOL],
        setting=setting,
    )

    # ── 运行回测 ──
    print(f"回测: {SYMBOL}  {START} ~ {END}")
    print(f"参数: {setting}")
    print(f"初始资金: {INITIAL_CAPITAL:,.0f}")
    print("-" * 50)

    result = engine.run(strategy, START, END)

    # ── 输出结果 ──
    report = BacktestReport(result)
    report.print_summary()
    report.show_charts()


if __name__ == "__main__":
    main()
