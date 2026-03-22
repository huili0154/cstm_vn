from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
import sys
from collections import Counter
from datetime import datetime, timedelta

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from backtest.engine import BacktestEngine
from backtest.mstr_fast_simple import MstrFastSimpleRunner
from core.datatypes import MatchingMode
from strategies.mstr_strategy import MstrStrategy


def _load_config(path: Path) -> tuple[dict, dict, dict[str, tuple[int, float]] | None]:
    data = json.loads(path.read_text(encoding="utf-8"))
    engine = data.get("engine", {})
    strategy = data.get("strategy", {})
    ipos_data = data.get("initial_positions", {}) or {}
    initial_positions: dict[str, tuple[int, float]] = {}
    for sym, value in ipos_data.items():
        if isinstance(value, dict):
            vol = int(value.get("volume", 0))
            cost = float(value.get("cost_price", 0.0))
        elif isinstance(value, (list, tuple)) and len(value) >= 2:
            vol = int(value[0])
            cost = float(value[1])
        else:
            continue
        if vol > 0:
            initial_positions[sym] = (vol, cost)
    return engine, strategy, (initial_positions or None)


def _calc_max_drawdown(daily_nav: list[tuple[str, float]]) -> float:
    if not daily_nav:
        return 0.0
    peak = daily_nav[0][1]
    max_dd = 0.0
    for _, nav in daily_nav:
        if nav > peak:
            peak = nav
        if peak > 0:
            dd = (peak - nav) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _summarize(start_balance: float, end_balance: float, daily_nav: list[tuple[str, float]], trade_count: int, total_commission: float) -> dict:
    total_return = 0.0
    if start_balance > 0:
        total_return = (end_balance - start_balance) / start_balance
    return {
        "start_balance": start_balance,
        "end_balance": end_balance,
        "total_return": total_return,
        "max_drawdown": _calc_max_drawdown(daily_nav),
        "trade_count": trade_count,
        "commission": total_commission,
    }


def _trade_profile(trades: list) -> dict:
    by_symbol_count: Counter = Counter()
    by_symbol_volume: Counter = Counter()
    by_side_count: Counter = Counter()
    notional_total = 0.0
    for t in trades:
        sym = getattr(t, "symbol", "")
        vol = int(getattr(t, "volume", 0))
        px = float(getattr(t, "price", 0.0))
        direction = getattr(getattr(t, "direction", None), "value", "")
        by_symbol_count[sym] += 1
        by_symbol_volume[sym] += vol
        by_side_count[direction] += 1
        notional_total += vol * px
    return {
        "by_symbol_count": dict(by_symbol_count),
        "by_symbol_volume": dict(by_symbol_volume),
        "by_side_count": dict(by_side_count),
        "notional_total": notional_total,
    }


def _state_name(block) -> str:
    state = getattr(block, "state", "")
    return getattr(state, "value", str(state))


def _dt_date_str(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.strftime("%Y%m%d")


def _collect_engine_signal_times(block_logs: list) -> list[datetime]:
    out: list[datetime] = []
    for b in block_logs:
        sig_t = getattr(b, "signal_time", None)
        if isinstance(sig_t, datetime):
            out.append(sig_t)
            continue
        for e in getattr(b, "events", []):
            if getattr(e, "event_type", "") != "SIGNAL":
                continue
            et = getattr(e, "time", None)
            if isinstance(et, datetime):
                out.append(et)
                break
    out.sort()
    return out


def _collect_fast_signal_times(blocks: list) -> list[datetime]:
    out: list[datetime] = []
    for b in blocks:
        ts = getattr(b, "ts", None)
        if isinstance(ts, datetime):
            out.append(ts)
    out.sort()
    return out


def _count_by_day(datetimes: list[datetime]) -> dict[str, int]:
    c: Counter = Counter()
    for dt in datetimes:
        c[dt.strftime("%Y%m%d")] += 1
    return dict(c)


def _trade_times(trades: list) -> list[datetime]:
    out: list[datetime] = []
    for t in trades:
        dt = getattr(t, "datetime", None)
        if isinstance(dt, datetime):
            out.append(dt)
    out.sort()
    return out


def _window_trade_count(trade_times: list[datetime], start: datetime, end: datetime) -> int:
    return sum(1 for t in trade_times if start <= t <= end)


def _signal_alignment(engine_signals: list[datetime], fast_signals: list[datetime], tolerance_sec: int = 120) -> dict:
    if not engine_signals:
        return {
            "engine_signals": 0,
            "fast_signals": len(fast_signals),
            "matched": 0,
            "match_ratio": 0.0,
            "median_abs_sec": 0.0,
            "p90_abs_sec": 0.0,
        }
    fast_by_day: dict[str, list[datetime]] = {}
    for dt in fast_signals:
        fast_by_day.setdefault(dt.strftime("%Y%m%d"), []).append(dt)
    abs_diffs: list[float] = []
    matched = 0
    for e in engine_signals:
        candidates = fast_by_day.get(e.strftime("%Y%m%d"), [])
        if not candidates:
            continue
        best = min(abs((f - e).total_seconds()) for f in candidates)
        if best <= tolerance_sec:
            matched += 1
            abs_diffs.append(best)
    abs_diffs.sort()
    median_abs = abs_diffs[len(abs_diffs) // 2] if abs_diffs else 0.0
    p90_idx = int(len(abs_diffs) * 0.9) - 1
    if p90_idx < 0:
        p90_idx = 0
    p90_abs = abs_diffs[p90_idx] if abs_diffs else 0.0
    return {
        "engine_signals": len(engine_signals),
        "fast_signals": len(fast_signals),
        "matched": matched,
        "match_ratio": matched / len(engine_signals) if engine_signals else 0.0,
        "median_abs_sec": median_abs,
        "p90_abs_sec": p90_abs,
    }


def _same_window_behavior(engine_signals: list[datetime], engine_trades: list[datetime], fast_trades: list[datetime], seconds: int = 60) -> dict:
    if not engine_signals:
        return {
            "windows": 0,
            "avg_engine_trades": 0.0,
            "avg_fast_trades": 0.0,
            "fast_zero_windows": 0,
        }
    eng_counts: list[int] = []
    fast_counts: list[int] = []
    fast_zero = 0
    for st in engine_signals:
        ed = st + timedelta(seconds=seconds)
        ec = _window_trade_count(engine_trades, st, ed)
        fc = _window_trade_count(fast_trades, st, ed)
        eng_counts.append(ec)
        fast_counts.append(fc)
        if fc == 0:
            fast_zero += 1
    return {
        "windows": len(engine_signals),
        "avg_engine_trades": sum(eng_counts) / len(eng_counts),
        "avg_fast_trades": sum(fast_counts) / len(fast_counts),
        "fast_zero_windows": fast_zero,
    }


def _run_engine(engine_params: dict, strategy_params: dict, initial_positions: dict[str, tuple[int, float]] | None) -> tuple[dict, float, int]:
    dataset_dir = engine_params.get("dataset_dir", "dataset")
    engine = BacktestEngine(
        dataset_dir=dataset_dir,
        mode=MatchingMode.SMART_TICK_DELAY_FILL,
        initial_capital=float(engine_params.get("initial_capital", 1_000_000.0)),
        rate=float(engine_params.get("rate", 0.00005)),
        slippage=float(engine_params.get("slippage", 0.0)),
        pricetick=float(engine_params.get("pricetick", 0.001)),
        volume_limit_ratio=float(engine_params.get("volume_limit_ratio", 0.5)),
        credit_ratio=float(engine_params.get("credit_ratio", 0.0)),
        enable_t0=bool(engine_params.get("enable_t0", False)),
    )
    setting = {k: v for k, v in strategy_params.items() if k not in ("symbols", "start_date", "end_date")}
    setting["dataset_dir"] = dataset_dir
    strategy = MstrStrategy(
        engine=engine,
        strategy_name="MSTR_ENGINE_BASELINE",
        symbols=list(strategy_params["symbols"]),
        setting=setting,
    )
    t0 = time.perf_counter()
    result = engine.run(
        strategy,
        strategy_params["start_date"],
        strategy_params["end_date"],
        initial_positions=initial_positions,
    )
    elapsed = time.perf_counter() - t0
    summary = _summarize(
        start_balance=result.start_balance,
        end_balance=result.end_balance,
        daily_nav=result.daily_nav,
        trade_count=len(result.trades),
        total_commission=result.total_commission,
    )
    block_logs = getattr(strategy, "_block_logs", [])
    summary["blocks"] = len(block_logs)
    summary["profile"] = _trade_profile(result.trades)
    summary["block_states"] = dict(Counter(_state_name(b) for b in block_logs))
    summary["block_modes"] = dict(Counter(getattr(b, "mode", "") for b in block_logs))
    engine_signal_times = _collect_engine_signal_times(block_logs)
    engine_trade_times = _trade_times(result.trades)
    summary["signal_times"] = engine_signal_times
    summary["signal_by_day"] = _count_by_day(engine_signal_times)
    summary["trade_times"] = engine_trade_times
    summary["trade_by_day"] = _count_by_day(engine_trade_times)
    summary["signal_events"] = sum(
        1
        for b in block_logs
        for e in getattr(b, "events", [])
        if getattr(e, "event_type", "") == "SIGNAL"
    )
    return summary, elapsed, len(result.trades)


def _run_fast(engine_params: dict, strategy_params: dict, initial_positions: dict[str, tuple[int, float]] | None) -> tuple[dict, float, int]:
    runner = MstrFastSimpleRunner(
        dataset_dir=engine_params.get("dataset_dir", "dataset"),
        strategy_params=strategy_params,
        engine_params=engine_params,
        initial_positions=initial_positions,
    )
    t0 = time.perf_counter()
    result = runner.run()
    elapsed = time.perf_counter() - t0
    summary = _summarize(
        start_balance=result.start_balance,
        end_balance=result.end_balance,
        daily_nav=result.daily_nav,
        trade_count=len(result.trades),
        total_commission=result.total_commission,
    )
    summary["blocks"] = len(result.blocks)
    summary["signals"] = result.signal_count
    summary["tick_count"] = result.tick_count
    summary["profile"] = _trade_profile(result.trades)
    summary["block_modes"] = dict(Counter(getattr(b, "mode", "") for b in result.blocks))
    summary["daily_prepare_seconds"] = result.daily_prepare_seconds
    summary["tick_preload_seconds"] = result.tick_preload_seconds
    summary["replay_seconds"] = result.replay_seconds
    summary["total_seconds_reported"] = result.total_seconds
    summary["preload_workers"] = result.preload_workers
    summary["preload_tasks"] = result.preload_tasks
    summary["cache_hit"] = result.cache_hit
    summary["cache_load_seconds"] = result.cache_load_seconds
    summary["cache_save_seconds"] = result.cache_save_seconds
    summary["skip_no_quote"] = result.skip_no_quote
    summary["skip_stale_quote"] = result.skip_stale_quote
    summary["skip_warmup"] = result.skip_warmup
    fast_signal_times = _collect_fast_signal_times(result.blocks)
    fast_trade_times = _trade_times(result.trades)
    summary["signal_times"] = fast_signal_times
    summary["signal_by_day"] = _count_by_day(fast_signal_times)
    summary["trade_times"] = fast_trade_times
    summary["trade_by_day"] = _count_by_day(fast_trade_times)
    return summary, elapsed, len(result.trades)


def _fmt_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def _print_compare(engine_summary: dict, fast_summary: dict, engine_sec: float, fast_sec: float) -> None:
    speedup = engine_sec / fast_sec if fast_sec > 0 else 0.0
    print("=== MSTR ENGINE vs FAST_SIMPLE 对比 ===")
    print(f"ENGINE 耗时: {engine_sec:.2f}s")
    print(f"FAST   耗时: {fast_sec:.2f}s")
    print(f"速度提升: {speedup:.2f}x")
    print("")
    print(f"ENGINE 期末净值: {engine_summary['end_balance']:.2f}")
    print(f"FAST   期末净值: {fast_summary['end_balance']:.2f}")
    print(f"ENGINE 总收益: {_fmt_pct(engine_summary['total_return'])}")
    print(f"FAST   总收益: {_fmt_pct(fast_summary['total_return'])}")
    print(f"ENGINE 最大回撤: {_fmt_pct(engine_summary['max_drawdown'])}")
    print(f"FAST   最大回撤: {_fmt_pct(fast_summary['max_drawdown'])}")
    print(f"ENGINE 成交笔数: {engine_summary['trade_count']}")
    print(f"FAST   成交笔数: {fast_summary['trade_count']}")
    print(f"ENGINE 手续费: {engine_summary['commission']:.2f}")
    print(f"FAST   手续费: {fast_summary['commission']:.2f}")
    print(f"ENGINE Block数: {engine_summary.get('blocks', 0)}")
    print(f"FAST   Block数: {fast_summary.get('blocks', 0)}")
    print(f"ENGINE SIGNAL事件: {engine_summary.get('signal_events', 0)}")
    print(f"FAST   SIGNAL次数: {fast_summary.get('signals', 0)}")
    print(f"FAST   扫描Tick数: {fast_summary.get('tick_count', 0)}")
    print(
        "FAST   分段耗时: "
        f"daily={fast_summary.get('daily_prepare_seconds', 0.0):.2f}s, "
        f"preload={fast_summary.get('tick_preload_seconds', 0.0):.2f}s, "
        f"replay={fast_summary.get('replay_seconds', 0.0):.2f}s, "
        f"total={fast_summary.get('total_seconds_reported', 0.0):.2f}s"
    )
    print(
        "FAST   预加载并发: "
        f"workers={fast_summary.get('preload_workers', 0)}, "
        f"tasks={fast_summary.get('preload_tasks', 0)}"
    )
    print(
        "FAST   Cache: "
        f"hit={fast_summary.get('cache_hit', False)}, "
        f"load={fast_summary.get('cache_load_seconds', 0.0):.2f}s, "
        f"save={fast_summary.get('cache_save_seconds', 0.0):.2f}s"
    )
    print(
        "FAST   跳过统计: "
        f"warmup={fast_summary.get('skip_warmup', 0)}, "
        f"no_quote={fast_summary.get('skip_no_quote', 0)}, "
        f"cross_symbol_delay={fast_summary.get('skip_stale_quote', 0)}"
    )
    if fast_summary.get("total_seconds_reported", 0.0) > 0:
        preload_ratio = (
            fast_summary.get("tick_preload_seconds", 0.0)
            / fast_summary["total_seconds_reported"]
        )
        print(f"FAST   预加载占比: {_fmt_pct(preload_ratio)}")
    nav_base = max(1.0, abs(engine_summary["end_balance"]))
    nav_gap = abs(fast_summary["end_balance"] - engine_summary["end_balance"]) / nav_base
    print("")
    print(f"期末净值偏差: {_fmt_pct(nav_gap)}")
    print("")
    print("=== 决策/执行差异诊断 ===")
    eng_modes = engine_summary.get("block_modes", {})
    fast_modes = fast_summary.get("block_modes", {})
    print(f"ENGINE block模式: {eng_modes}")
    print(f"FAST   block模式: {fast_modes}")
    print(f"ENGINE block状态: {engine_summary.get('block_states', {})}")
    eng_profile = engine_summary.get("profile", {})
    fast_profile = fast_summary.get("profile", {})
    print(f"ENGINE 方向笔数: {eng_profile.get('by_side_count', {})}")
    print(f"FAST   方向笔数: {fast_profile.get('by_side_count', {})}")
    print(f"ENGINE 各标的成交笔数: {eng_profile.get('by_symbol_count', {})}")
    print(f"FAST   各标的成交笔数: {fast_profile.get('by_symbol_count', {})}")
    print(f"ENGINE 各标的成交量: {eng_profile.get('by_symbol_volume', {})}")
    print(f"FAST   各标的成交量: {fast_profile.get('by_symbol_volume', {})}")
    print("")
    print("=== 1个月信号时段行为对比 ===")
    eng_sig = engine_summary.get("signal_times", [])
    fast_sig = fast_summary.get("signal_times", [])
    align = _signal_alignment(eng_sig, fast_sig, tolerance_sec=120)
    print(
        "信号对齐(同日±120秒): "
        f"匹配 {align['matched']}/{align['engine_signals']} "
        f"({_fmt_pct(align['match_ratio'])}), "
        f"中位偏差 {align['median_abs_sec']:.1f}s, "
        f"P90偏差 {align['p90_abs_sec']:.1f}s"
    )
    win = _same_window_behavior(
        eng_sig,
        engine_summary.get("trade_times", []),
        fast_summary.get("trade_times", []),
        seconds=60,
    )
    print(
        "ENGINE信号后60秒成交: "
        f"ENGINE均值 {win['avg_engine_trades']:.2f} 笔, "
        f"FAST均值 {win['avg_fast_trades']:.2f} 笔, "
        f"FAST零成交窗口 {win['fast_zero_windows']}/{win['windows']}"
    )
    eng_sig_day = engine_summary.get("signal_by_day", {})
    fast_sig_day = fast_summary.get("signal_by_day", {})
    all_days = sorted(set(eng_sig_day) | set(fast_sig_day))
    print("按日信号次数(ENGINE/FAST):")
    for d in all_days[:31]:
        print(f"  {d}: {eng_sig_day.get(d, 0)}/{fast_sig_day.get(d, 0)}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="mstr_fast_compare")
    parser.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "mstr_params.json"))
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        raise SystemExit(f"配置文件不存在: {cfg_path}")

    engine_params, strategy_params, initial_positions = _load_config(cfg_path)
    engine_summary, engine_sec, _ = _run_engine(engine_params, strategy_params, initial_positions)
    fast_summary, fast_sec, _ = _run_fast(engine_params, strategy_params, initial_positions)
    _print_compare(engine_summary, fast_summary, engine_sec, fast_sec)


if __name__ == "__main__":
    main()
