"""
DS_DMTR 策略回测脚本 — 运行后查看绩效 & block 详情。

用法:
    python run_ds_dmtr_backtest.py
"""

import json
import matplotlib
matplotlib.use("TkAgg")

from pathlib import Path
from core.datatypes import MatchingMode
from backtest.engine import BacktestEngine
from backtest.report import BacktestReport
from strategies.ds_dmtr_strategy import DsDmtrStrategy


PARAMS_FILE = Path("ds_dmtr_params.json")

MATCHING_MODE_MAP = {
    "close_fill": MatchingMode.CLOSE_FILL,
    "tick_fill": MatchingMode.TICK_FILL,
    "smart_tick_delay_fill": MatchingMode.SMART_TICK_DELAY_FILL,
}


def load_params() -> dict:
    with open(PARAMS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def print_block_details(strategy: DsDmtrStrategy) -> None:
    blocks = strategy._block_logs
    print(f"\n{'='*80}")
    print(f"  Block 详情  (共 {len(blocks)} 个)")
    print(f"{'='*80}")

    for b in blocks:
        flag = ""
        if b.sell_filled > b.sell_order_volume and b.sell_order_volume >= 100:
            flag += " [!卖成交>发单]"
        if b.buy_filled > b.buy_order_volume and b.buy_order_volume >= 100:
            flag += " [!买成交>发单]"
        cash_delta = b.end_cash - b.signal_cash
        if abs(cash_delta) > b.signal_nav * 0.01:
            flag += f" [!现金变动={cash_delta:+,.0f}]"

        print(f"\n  {b.block_id}  {b.direction}  state={b.state.name}{flag}")
        print(f"    信号时间: {b.signal_time}  结束: {b.end_time}")
        print(f"    ab_ratio={b.ab_ratio:.6f}  dsm={b.delta_sigma_minutes:.3f}  dsd={b.delta_sigma_days:.3f}")
        print(f"    trade_pct={b.trade_pct:.2%}  high={b.is_high_pct}")
        print(f"    卖: {b.sell_symbol}  期望={b.desired_sell_volume}  发单={b.sell_order_volume}  成交={b.sell_filled}  均价={b.sell_avg_price:.4f}  信号价={b.sell_signal_price:.4f}")
        print(f"    买: {b.buy_symbol}  期望={b.desired_buy_volume}  发单={b.buy_order_volume}  成交={b.buy_filled}  均价={b.buy_avg_price:.4f}  信号价={b.buy_signal_price:.4f}")
        print(f"    佣金: 卖={b.sell_commission:.2f}  买={b.buy_commission:.2f}")
        print(f"    现金: {b.signal_cash:,.0f} → {b.end_cash:,.0f}  (Δ={cash_delta:+,.0f})")
        print(f"    净值: {b.signal_nav:,.0f} → {b.end_nav:,.0f}  (Δ={b.end_nav - b.signal_nav:+,.0f})")
        print(f"    耗时: {b.total_duration:.0f}s  chase_round={b.chase_round}")

    non_done = [b for b in blocks if b.state.name not in ("DONE", "BUY_ONLY")]
    if non_done:
        print(f"\n{'='*80}")
        print(f"  非 DONE 状态汇总 ({len(non_done)} 个)")
        print(f"{'='*80}")
        from collections import Counter
        state_counts = Counter(b.state.name for b in non_done)
        for state, cnt in state_counts.most_common():
            print(f"    {state}: {cnt}")

    overflow = [b for b in blocks
                if (b.sell_filled > b.sell_order_volume and b.sell_order_volume >= 100)
                or (b.buy_filled > b.buy_order_volume and b.buy_order_volume >= 100)]
    if overflow:
        print(f"\n{'='*80}")
        print(f"  成交量 > 发单量 的 block ({len(overflow)} 个)")
        print(f"{'='*80}")
        for b in overflow:
            print(f"    {b.block_id}: 卖 {b.sell_filled}/{b.sell_order_volume}  买 {b.buy_filled}/{b.buy_order_volume}  state={b.state.name}")


def main() -> None:
    params = load_params()
    eng_cfg = params["engine"]
    strat_cfg = params["strategy"]

    mode = MATCHING_MODE_MAP.get(eng_cfg["matching_mode"], MatchingMode.TICK_FILL)

    engine = BacktestEngine(
        dataset_dir=eng_cfg.get("dataset_dir", "dataset"),
        mode=mode,
        initial_capital=eng_cfg.get("initial_capital", 100_000.0),
        rate=eng_cfg.get("rate", 0.0001),
        slippage=eng_cfg.get("slippage", 0.0),
        pricetick=eng_cfg.get("pricetick", 0.001),
        volume_limit_ratio=eng_cfg.get("volume_limit_ratio", 0.5),
        enable_t0=eng_cfg.get("enable_t0", False),
        credit_ratio=eng_cfg.get("credit_ratio", 0.0),
    )

    symbols = [strat_cfg["symbol_a"], strat_cfg["symbol_b"]]
    strategy = DsDmtrStrategy(
        engine=engine,
        strategy_name="DsDmtr",
        symbols=symbols,
        setting=strat_cfg,
    )

    start = strat_cfg.get("start_date", "20250101")
    end = strat_cfg.get("end_date", "20251231")
    initial_positions = params.get("initial_positions", {})
    pos_dict = {}
    for sym, info in initial_positions.items():
        if isinstance(info, (list, tuple)) and len(info) == 2:
            pos_dict[sym] = (int(info[0]), float(info[1]))

    print(f"回测: {symbols}  {start} ~ {end}")
    print(f"撮合模式: {mode.name}")
    print(f"初始资金: {eng_cfg.get('initial_capital', 100_000.0):,.0f}")
    print(f"初始持仓: {pos_dict if pos_dict else '无'}")
    print("-" * 50)

    result = engine.run(strategy, start, end, initial_positions=pos_dict or None)

    report = BacktestReport(result)
    report.print_summary()

    print_block_details(strategy)

    # report.show_charts()


if __name__ == "__main__":
    main()
