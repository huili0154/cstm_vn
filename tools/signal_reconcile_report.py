from __future__ import annotations

import argparse
import bisect
import json
from datetime import UTC, datetime
from pathlib import Path
import sys

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backtest.engine import BacktestEngine
from backtest.mstr_fast_simple import MstrFastSimpleRunner
from core.data_feed import ParquetBarFeed
from core.datatypes import MatchingMode
from strategies.mstr_strategy import MstrStrategy


def _load_config(path: Path) -> tuple[dict, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("engine", {}), data.get("strategy", {})


def _to_ns(dt: datetime) -> int:
    return int((dt - datetime(1970, 1, 1)).total_seconds() * 1_000_000_000)


def _from_ns(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=UTC).replace(tzinfo=None)


def _latest_last_at_or_before(dt_arr, last_arr, ts_ns: int) -> tuple[int | None, float]:
    idx = bisect.bisect_right(dt_arr, ts_ns) - 1
    if idx < 0:
        return None, 0.0
    return int(dt_arr[idx]), float(last_arr[idx])


def _pair_stat(feed: ParquetBarFeed, symbols: list[str], date_str: str, window: int, h: str, si: str) -> tuple[float, float]:
    all_daily: dict[str, dict[str, float]] = {}
    for sym in symbols:
        daily: dict[str, float] = {}
        for bar in feed.load(sym):
            d = bar.datetime.strftime("%Y%m%d")
            daily[d] = bar.close_price
        all_daily[sym] = daily
    n = window - 1
    hist_h = [all_daily[h][d] for d in sorted(k for k in all_daily[h] if k < date_str)]
    hist_si = [all_daily[si][d] for d in sorted(k for k in all_daily[si] if k < date_str)]
    if len(hist_h) < n or len(hist_si) < n:
        return 0.0, 0.0
    hh = hist_h[-n:]
    hs = hist_si[-n:]
    ratios = [a / b for a, b in zip(hh, hs) if b > 0]
    mu = sum(ratios) / len(ratios)
    max_dev = max(ratios) - mu
    return mu, max_dev


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "mstr_params_1m.json"))
    args = parser.parse_args()
    engine_params, strategy_params = _load_config(Path(args.config))
    symbols = list(strategy_params["symbols"])
    primary = symbols[0]

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
    strategy = MstrStrategy(engine, "MSTR_DIFF", symbols, setting)
    engine.run(strategy, strategy_params["start_date"], strategy_params["end_date"])
    engine_blocks = [b for b in getattr(strategy, "_block_logs", []) if getattr(b, "signal_time", None)]
    engine_rotation_blocks = [b for b in engine_blocks if getattr(b, "mode", "") == "rotation"]

    runner = MstrFastSimpleRunner(
        dataset_dir=engine_params.get("dataset_dir", "dataset"),
        strategy_params=strategy_params,
        engine_params=engine_params,
    )
    fast_res = runner.run()
    fast_signal_times = [b.ts for b in fast_res.blocks]
    fast_rotation_times = [b.ts for b in fast_res.blocks if getattr(b, "mode", "") == "rotation"]

    print("ENGINE blocks:")
    for b in engine_blocks:
        st = b.signal_time.strftime("%Y-%m-%d %H:%M:%S") if b.signal_time else ""
        print(
            f"  {st} {b.mode} {b.sell_symbol}->{b.buy_symbol} "
            f"score={b.trigger_score:.6f} dev={b.trigger_dev:.6f}"
        )
    print("FAST blocks:")
    for b in fast_res.blocks:
        print(
            f"  {b.ts.strftime('%Y-%m-%d %H:%M:%S')} {b.mode} "
            f"{b.sell_symbol}->{b.buy_symbol} score={b.score:.6f} dev={b.dev:.6f}"
        )

    missing = []
    tol_sec = 120
    for b in engine_rotation_blocks:
        st = b.signal_time
        matched = any(
            (ft.strftime("%Y%m%d") == st.strftime("%Y%m%d")) and (abs((ft - st).total_seconds()) <= tol_sec)
            for ft in fast_rotation_times
        )
        if not matched:
            missing.append(b)

    print(
        f"ENGINE信号总数={len(engine_blocks)}, FAST信号总数={len(fast_signal_times)}; "
        f"ENGINE rotation={len(engine_rotation_blocks)}, FAST rotation={len(fast_rotation_times)}, "
        f"缺失rotation={len(missing)}"
    )
    if not missing:
        return

    tgt = missing[0]
    st = tgt.signal_time
    assert st is not None
    date_str = st.strftime("%Y%m%d")
    month = f"{date_str[:4]}-{date_str[4:6]}"
    h = tgt.sell_symbol
    si = tgt.buy_symbol
    print(f"分析缺失信号: {st.strftime('%Y-%m-%d %H:%M:%S')} {h}->{si}, engine_score={tgt.trigger_score:.6f}, engine_dev={tgt.trigger_dev:.6f}")

    dataset_dir = Path(engine_params.get("dataset_dir", "dataset"))
    fp_h = dataset_dir / "ticks" / h / month / f"{date_str}.parquet"
    fp_si = dataset_dir / "ticks" / si / month / f"{date_str}.parquet"
    fp_p = dataset_dir / "ticks" / primary / month / f"{date_str}.parquet"

    data_h = runner._load_tick_minimal(fp_h)
    data_si = runner._load_tick_minimal(fp_si)
    data_p = runner._load_tick_minimal(fp_p)
    ts_ns = _to_ns(st)
    h_ts, h_px = _latest_last_at_or_before(data_h["dt"], data_h["last"], ts_ns)
    si_ts, si_px = _latest_last_at_or_before(data_si["dt"], data_si["last"], ts_ns)

    feed = ParquetBarFeed(dataset_dir)
    mu, max_dev = _pair_stat(feed, symbols, date_str, int(strategy_params["window"]), h, si)
    if h_px > 0 and si_px > 0 and max_dev > 0:
        ratio = h_px / si_px
        dev = ratio - mu
        score = dev / max_dev
        print(
            f"ENGINE信号时刻重算 FAST口径: ratio={ratio:.6f}, mu={mu:.6f}, dev={dev:.6f}, max_dev={max_dev:.6f}, score={score:.6f}"
        )
    else:
        print("ENGINE信号时刻重算失败: 价格或max_dev不可用")

    p_dt = data_p["dt"]
    i = bisect.bisect_right(p_dt, ts_ns) - 1
    idx_prev = max(i, 0)
    idx_next = min(i + 1, len(p_dt) - 1)
    probe = [("prev_primary", int(p_dt[idx_prev])), ("next_primary", int(p_dt[idx_next]))]
    for tag, t_ns in probe:
        hh_ts, hh_px = _latest_last_at_or_before(data_h["dt"], data_h["last"], t_ns)
        ss_ts, ss_px = _latest_last_at_or_before(data_si["dt"], data_si["last"], t_ns)
        if hh_px <= 0 or ss_px <= 0 or max_dev <= 0:
            print(f"{tag}: 数据不足")
            continue
        ratio = hh_px / ss_px
        dev = ratio - mu
        score = dev / max_dev
        print(
            f"{tag}@{_from_ns(t_ns).strftime('%H:%M:%S')} score={score:.6f}, dev={dev:.6f}, "
            f"h_lag={(t_ns - (hh_ts or t_ns))/1e9:.3f}s, si_lag={(t_ns - (ss_ts or t_ns))/1e9:.3f}s"
        )


if __name__ == "__main__":
    main()
