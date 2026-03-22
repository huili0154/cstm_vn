from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
import sys

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backtest.engine import BacktestEngine
from backtest.mstr_fast_simple import MstrFastSimpleRunner
from core.datatypes import MatchingMode
from strategies.mstr_strategy import MstrStrategy


def _load_config(path: Path) -> tuple[dict, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["engine"], data["strategy"]


def _run_engine(engine_params: dict, strategy_params: dict):
    mode_str = str(engine_params.get("mode", "smart_tick_delay_fill")).lower()
    mode = (
        MatchingMode.CLOSE_FILL
        if mode_str == "close_fill"
        else MatchingMode.TICK_FILL
        if mode_str == "tick_fill"
        else MatchingMode.SMART_TICK_DELAY_FILL
    )
    engine = BacktestEngine(
        dataset_dir=engine_params.get("dataset_dir", "dataset"),
        mode=mode,
        initial_capital=float(engine_params.get("initial_capital", 1_000_000.0)),
        rate=float(engine_params.get("rate", 0.00005)),
        slippage=float(engine_params.get("slippage", 0.0)),
        pricetick=float(engine_params.get("pricetick", 0.001)),
        volume_limit_ratio=float(engine_params.get("volume_limit_ratio", 1.0)),
        credit_ratio=float(engine_params.get("credit_ratio", 0.0)),
        enable_t0=bool(engine_params.get("enable_t0", False)),
    )
    setting = {k: v for k, v in strategy_params.items() if k not in ("symbols", "start_date", "end_date")}
    setting["dataset_dir"] = engine_params.get("dataset_dir", "dataset")
    strategy = MstrStrategy(engine, "MSTR_ATTR", list(strategy_params["symbols"]), setting)
    result = engine.run(strategy, strategy_params["start_date"], strategy_params["end_date"])
    return result, list(getattr(strategy, "_block_logs", []))


def _run_fast(engine_params: dict, strategy_params: dict):
    runner = MstrFastSimpleRunner(
        dataset_dir=engine_params.get("dataset_dir", "dataset"),
        strategy_params=strategy_params,
        engine_params=engine_params,
    )
    return runner.run()


def _nav_map(daily_nav: list[tuple[str, float]]) -> dict[str, float]:
    return {d: float(v) for d, v in daily_nav}


def _trade_stats(trades: list) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "trade_count": 0.0,
            "buy_notional": 0.0,
            "sell_notional": 0.0,
            "commission": 0.0,
            "buy_qty": 0.0,
            "sell_qty": 0.0,
        }
    )
    for t in trades:
        d = t.datetime.strftime("%Y%m%d")
        side = getattr(t.direction, "value", "")
        notional = float(t.price) * float(t.volume)
        row = stats[d]
        row["trade_count"] += 1.0
        row["commission"] += float(t.commission)
        if side == "BUY":
            row["buy_notional"] += notional
            row["buy_qty"] += float(t.volume)
        elif side == "SELL":
            row["sell_notional"] += notional
            row["sell_qty"] += float(t.volume)
    return stats


def _signal_stats_engine(block_logs: list) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for b in block_logs:
        st = getattr(b, "signal_time", None)
        if st:
            out[st.strftime("%Y%m%d")] += 1
    return out


def _signal_stats_fast(blocks: list) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for b in blocks:
        out[b.ts.strftime("%Y%m%d")] += 1
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--output", default="")
    args = p.parse_args()

    cfg = Path(args.config)
    engine_params, strategy_params = _load_config(cfg)
    eng_res, eng_blocks = _run_engine(engine_params, strategy_params)
    fast_res = _run_fast(engine_params, strategy_params)

    eng_nav = _nav_map(eng_res.daily_nav)
    fast_nav = _nav_map(fast_res.daily_nav)
    eng_trade = _trade_stats(eng_res.trades)
    fast_trade = _trade_stats(fast_res.trades)
    eng_sig = _signal_stats_engine(eng_blocks)
    fast_sig = _signal_stats_fast(fast_res.blocks)

    dates = sorted(set(eng_nav) | set(fast_nav))
    if not args.output:
        out_path = cfg.with_name(f"{cfg.stem}_daily_attribution.csv")
    else:
        out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, float | str]] = []
    prev_diff = 0.0
    for d in dates:
        e_nav = float(eng_nav.get(d, 0.0))
        f_nav = float(fast_nav.get(d, 0.0))
        diff = f_nav - e_nav
        delta = diff - prev_diff
        prev_diff = diff

        et = eng_trade.get(d, {})
        ft = fast_trade.get(d, {})
        e_buy = float(et.get("buy_notional", 0.0))
        e_sell = float(et.get("sell_notional", 0.0))
        f_buy = float(ft.get("buy_notional", 0.0))
        f_sell = float(ft.get("sell_notional", 0.0))
        e_turn = e_buy + e_sell
        f_turn = f_buy + f_sell

        rows.append(
            {
                "date": d,
                "engine_nav": e_nav,
                "fast_nav": f_nav,
                "nav_diff": diff,
                "nav_diff_delta": delta,
                "engine_trade_count": float(et.get("trade_count", 0.0)),
                "fast_trade_count": float(ft.get("trade_count", 0.0)),
                "engine_signal_count": float(eng_sig.get(d, 0)),
                "fast_signal_count": float(fast_sig.get(d, 0)),
                "engine_buy_notional": e_buy,
                "fast_buy_notional": f_buy,
                "engine_sell_notional": e_sell,
                "fast_sell_notional": f_sell,
                "engine_turnover": e_turn,
                "fast_turnover": f_turn,
                "turnover_diff": f_turn - e_turn,
                "engine_commission": float(et.get("commission", 0.0)),
                "fast_commission": float(ft.get("commission", 0.0)),
                "commission_diff": float(ft.get("commission", 0.0)) - float(et.get("commission", 0.0)),
                "engine_buy_qty": float(et.get("buy_qty", 0.0)),
                "fast_buy_qty": float(ft.get("buy_qty", 0.0)),
                "engine_sell_qty": float(et.get("sell_qty", 0.0)),
                "fast_sell_qty": float(ft.get("sell_qty", 0.0)),
            }
        )

    headers = list(rows[0].keys()) if rows else []
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    top_up = sorted(rows, key=lambda r: float(r["nav_diff_delta"]), reverse=True)[:10]
    top_down = sorted(rows, key=lambda r: float(r["nav_diff_delta"]))[:10]
    print(f"CSV写出: {out_path}")
    print(f"区间最终差值 FAST-ENGINE: {rows[-1]['nav_diff']:+.2f}")
    print("差值扩大Top10(日增量):")
    for r in top_up:
        print(
            f"  {r['date']} delta={float(r['nav_diff_delta']):+.2f}, "
            f"diff={float(r['nav_diff']):+.2f}, turnover_diff={float(r['turnover_diff']):+.2f}, "
            f"signal(E/F)={int(r['engine_signal_count'])}/{int(r['fast_signal_count'])}"
        )
    print("差值缩小Top10(日增量):")
    for r in top_down:
        print(
            f"  {r['date']} delta={float(r['nav_diff_delta']):+.2f}, "
            f"diff={float(r['nav_diff']):+.2f}, turnover_diff={float(r['turnover_diff']):+.2f}, "
            f"signal(E/F)={int(r['engine_signal_count'])}/{int(r['fast_signal_count'])}"
        )


if __name__ == "__main__":
    main()
