from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import sys

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backtest.engine import BacktestEngine
from backtest.mstr_fast_simple import MstrFastSimpleRunner
from core.datatypes import MatchingMode
from strategies.mstr_strategy import MstrStrategy


def _load(path: Path) -> tuple[dict, dict]:
    d = json.loads(path.read_text(encoding="utf-8"))
    return d["engine"], d["strategy"]


def _run_engine(engine_params: dict, strategy_params: dict):
    engine = BacktestEngine(
        dataset_dir=engine_params.get("dataset_dir", "dataset"),
        mode=MatchingMode.SMART_TICK_DELAY_FILL,
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
    strategy = MstrStrategy(engine, "MSTR_BIAS", list(strategy_params["symbols"]), setting)
    result = engine.run(strategy, strategy_params["start_date"], strategy_params["end_date"])
    return result, list(getattr(strategy, "_block_logs", []))


def _run_fast(engine_params: dict, strategy_params: dict):
    r = MstrFastSimpleRunner(
        dataset_dir=engine_params.get("dataset_dir", "dataset"),
        strategy_params=strategy_params,
        engine_params=engine_params,
    )
    result = r.run()
    return result


def _trade_agg(trades: list) -> dict[tuple[str, str, str], dict[str, float]]:
    out: dict[tuple[str, str, str], dict[str, float]] = {}
    for t in trades:
        d = t.datetime.strftime("%Y%m%d")
        side = getattr(getattr(t, "direction", None), "value", "")
        key = (d, t.symbol, side)
        if key not in out:
            out[key] = {"qty": 0.0, "notional": 0.0, "comm": 0.0, "count": 0.0}
        out[key]["qty"] += float(t.volume)
        out[key]["notional"] += float(t.volume) * float(t.price)
        out[key]["comm"] += float(t.commission)
        out[key]["count"] += 1.0
    return out


def _daily_nav_map(nav: list[tuple[str, float]]) -> dict[str, float]:
    return {d: v for d, v in nav}


def _print_top_trade_diffs(engine_trades: list, fast_trades: list, topn: int = 10) -> None:
    a = _trade_agg(engine_trades)
    b = _trade_agg(fast_trades)
    keys = sorted(set(a) | set(b))
    rows = []
    for k in keys:
        ea = a.get(k, {"qty": 0.0, "notional": 0.0, "comm": 0.0, "count": 0.0})
        fb = b.get(k, {"qty": 0.0, "notional": 0.0, "comm": 0.0, "count": 0.0})
        rows.append(
            (
                abs(ea["notional"] - fb["notional"]),
                k,
                ea["qty"],
                fb["qty"],
                ea["notional"],
                fb["notional"],
                ea["count"],
                fb["count"],
            )
        )
    rows.sort(reverse=True, key=lambda x: x[0])
    print("交易差异Top:")
    for _, k, eq, fq, en, fn, ec, fc in rows[:topn]:
        d, s, side = k
        print(
            f"  {d} {s} {side}: qty {eq:.0f}/{fq:.0f}, notional {en:.2f}/{fn:.2f}, count {ec:.0f}/{fc:.0f}"
        )


def _print_nav_diffs(engine_nav: list[tuple[str, float]], fast_nav: list[tuple[str, float]], topn: int = 10) -> None:
    a = _daily_nav_map(engine_nav)
    b = _daily_nav_map(fast_nav)
    rows = []
    for d in sorted(set(a) | set(b)):
        ea = a.get(d, 0.0)
        fb = b.get(d, 0.0)
        rows.append((abs(fb - ea), d, ea, fb, fb - ea))
    rows.sort(reverse=True, key=lambda x: x[0])
    print("日净值差异Top:")
    for _, d, ea, fb, diff in rows[:topn]:
        print(f"  {d}: ENGINE={ea:.2f}, FAST={fb:.2f}, diff={diff:+.2f}")


def _engine_block_summary(block_logs: list) -> None:
    print("ENGINE block摘要:")
    for b in block_logs:
        st = b.signal_time.strftime("%Y-%m-%d %H:%M:%S") if b.signal_time else ""
        print(
            f"  {st} {b.mode} {b.sell_symbol}->{b.buy_symbol} "
            f"buy_filled={b.buy_filled} sell_filled={b.sell_filled} "
            f"buy_avg={b.buy_avg_price:.6f} sell_avg={b.sell_avg_price:.6f}"
        )


def _fast_block_summary(fast_res) -> None:
    grouped: dict[datetime, list] = defaultdict(list)
    for t in fast_res.trades:
        grouped[t.datetime].append(t)
    print("FAST block摘要:")
    for b in fast_res.blocks:
        ts = b.ts
        ts_trades = grouped.get(ts, [])
        buy_qty = sum(int(t.volume) for t in ts_trades if getattr(t.direction, "value", "") == "BUY")
        sell_qty = sum(int(t.volume) for t in ts_trades if getattr(t.direction, "value", "") == "SELL")
        buy_notional = sum(float(t.volume) * float(t.price) for t in ts_trades if getattr(t.direction, "value", "") == "BUY")
        sell_notional = sum(float(t.volume) * float(t.price) for t in ts_trades if getattr(t.direction, "value", "") == "SELL")
        buy_avg = (buy_notional / buy_qty) if buy_qty > 0 else 0.0
        sell_avg = (sell_notional / sell_qty) if sell_qty > 0 else 0.0
        print(
            f"  {ts.strftime('%Y-%m-%d %H:%M:%S')} {b.mode} {b.sell_symbol}->{b.buy_symbol} "
            f"buy_qty={buy_qty} sell_qty={sell_qty} buy_avg={buy_avg:.6f} sell_avg={sell_avg:.6f}"
        )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "mstr_params_1m.json"))
    args = p.parse_args()

    engine_params, strategy_params = _load(Path(args.config))
    eng_res, eng_blocks = _run_engine(engine_params, strategy_params)
    fast_res = _run_fast(engine_params, strategy_params)

    print(f"END_BALANCE ENGINE={eng_res.end_balance:.2f}, FAST={fast_res.end_balance:.2f}, DIFF={fast_res.end_balance - eng_res.end_balance:+.2f}")
    print(f"COMMISSION ENGINE={eng_res.total_commission:.2f}, FAST={fast_res.total_commission:.2f}, DIFF={fast_res.total_commission - eng_res.total_commission:+.2f}")
    print(f"TRADES ENGINE={len(eng_res.trades)}, FAST={len(fast_res.trades)}")
    print(f"BLOCKS ENGINE={len(eng_blocks)}, FAST={len(fast_res.blocks)}")
    print(f"MODES ENGINE={dict(Counter(getattr(b, 'mode', '') for b in eng_blocks))}, FAST={dict(Counter(getattr(b, 'mode', '') for b in fast_res.blocks))}")
    _engine_block_summary(eng_blocks)
    _fast_block_summary(fast_res)
    _print_top_trade_diffs(eng_res.trades, fast_res.trades, topn=12)
    _print_nav_diffs(eng_res.daily_nav, fast_res.daily_nav, topn=12)


if __name__ == "__main__":
    main()
