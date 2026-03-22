from __future__ import annotations

import math
import json
import struct
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from pathlib import Path
import time as _time
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from core.data_feed import ParquetBarFeed
from core.datatypes import Direction, Position, Trade


@dataclass
class FastBlock:
    ts: datetime
    mode: str
    sell_symbol: str
    buy_symbol: str
    sell_qty: int
    buy_qty: int
    sell_price: float
    buy_price: float
    score: float
    dev: float


@dataclass
class FastSimpleResult:
    trades: list[Trade] = field(default_factory=list)
    daily_nav: list[tuple[str, float]] = field(default_factory=list)
    blocks: list[FastBlock] = field(default_factory=list)
    start_balance: float = 0.0
    end_balance: float = 0.0
    total_commission: float = 0.0
    tick_count: int = 0
    signal_count: int = 0
    mode: str = "FAST_SIMPLE"
    daily_prepare_seconds: float = 0.0
    tick_preload_seconds: float = 0.0
    replay_seconds: float = 0.0
    total_seconds: float = 0.0
    preload_workers: int = 0
    preload_tasks: int = 0
    cache_hit: bool = False
    cache_load_seconds: float = 0.0
    cache_save_seconds: float = 0.0
    skip_no_quote: int = 0
    skip_stale_quote: int = 0
    skip_warmup: int = 0


class MstrFastSimpleRunner:
    _CACHE_MAGIC = b"FSMC2\n"

    def __init__(
        self,
        dataset_dir: str | Path,
        strategy_params: dict,
        engine_params: dict,
        initial_positions: dict[str, tuple[int, float]] | None = None,
    ) -> None:
        self.dataset_dir = Path(dataset_dir)
        self.initial_positions = initial_positions or {}

        self.symbols: list[str] = list(strategy_params["symbols"])
        self.start_date: str = strategy_params["start_date"]
        self.end_date: str = strategy_params["end_date"]
        self.window: int = int(strategy_params.get("window", 20))
        self.k_threshold: float = float(strategy_params.get("k_threshold", 0.8))
        self.least_bias: float = float(strategy_params.get("least_bias", 0.01))
        self.num_blocks: int = int(strategy_params.get("num_blocks", 5))
        self.cooldown_1: int = int(strategy_params.get("cooldown_1", 10))
        self.cooldown_2: int = int(strategy_params.get("cooldown_2", 15))
        self.block_timeout: int = int(strategy_params.get("block_timeout", 20))
        self.max_positions: int = int(strategy_params.get("max_positions", 3))
        self.max_single_weight: float = float(strategy_params.get("max_single_weight", 0.5))
        self.min_cash_reserve: float = float(strategy_params.get("min_cash_reserve", 100.0))
        self.enable_t0: bool = bool(strategy_params.get("enable_t0", False))

        trading_cutoff = strategy_params.get("trading_cutoff_str", "14:55:00")
        h, m, s = (int(x) for x in trading_cutoff.split(":"))
        self.trading_cutoff = time(h, m, s)

        self.initial_capital: float = float(engine_params.get("initial_capital", 1_000_000.0))
        self.rate: float = float(engine_params.get("rate", 0.00005))
        self.slippage: float = float(engine_params.get("slippage", 0.0))
        self.pricetick: float = float(engine_params.get("pricetick", 0.001))
        self.volume_limit_ratio: float = float(engine_params.get("volume_limit_ratio", 1.0))
        self.credit_ratio: float = float(engine_params.get("credit_ratio", 0.0))
        self.max_block_per_symbol: int = max(
            1, math.floor(self.num_blocks * self.max_single_weight)
        )
        self._cache_file = (
            self.dataset_dir / "meta" / "fast_simple_tick_cache.bin"
        )

    @staticmethod
    def _empty_tick_arrays() -> dict[str, list]:
        return {"dt": [], "last": [], "bid1": [], "ask1": []}

    @staticmethod
    def _load_tick_minimal(fp: Path) -> dict[str, list]:
        if not fp.exists():
            return MstrFastSimpleRunner._empty_tick_arrays()
        table = pq.read_table(
            fp,
            columns=["datetime", "last_price", "bid_price_1", "ask_price_1"],
        )
        if table.num_rows <= 0:
            return MstrFastSimpleRunner._empty_tick_arrays()
        dt_raw = table.column("datetime").to_pylist()
        last_raw = table.column("last_price").to_pylist()
        bid_raw = table.column("bid_price_1").to_pylist()
        ask_raw = table.column("ask_price_1").to_pylist()
        dt_list: list = []
        last_list: list[float] = []
        bid_list: list[float] = []
        ask_list: list[float] = []
        for i in range(len(dt_raw)):
            dtv = dt_raw[i]
            if dtv is None:
                continue
            dt_list.append(MstrFastSimpleRunner._dt_to_ns(dtv))
            last_list.append(float(last_raw[i] or 0.0))
            bid_list.append(float(bid_raw[i] or 0.0))
            ask_list.append(float(ask_raw[i] or 0.0))
        if not dt_list:
            return MstrFastSimpleRunner._empty_tick_arrays()
        ordered = True
        prev = dt_list[0]
        for i in range(1, len(dt_list)):
            if dt_list[i] < prev:
                ordered = False
                break
            prev = dt_list[i]
        if not ordered:
            idx = sorted(range(len(dt_list)), key=lambda k: dt_list[k])
            dt_list = [dt_list[k] for k in idx]
            last_list = [last_list[k] for k in idx]
            bid_list = [bid_list[k] for k in idx]
            ask_list = [ask_list[k] for k in idx]
        return {"dt": dt_list, "last": last_list, "bid1": bid_list, "ask1": ask_list}

    def _build_cache_key(
        self,
        trading_dates: list[str],
        jobs: list[tuple[str, str, Path]],
    ) -> dict:
        existing = [fp for _, _, fp in jobs if fp.exists()]
        total_size = sum(fp.stat().st_size for fp in existing)
        max_mtime_ns = max((fp.stat().st_mtime_ns for fp in existing), default=0)
        return {
            "version": 3,
            "dataset_dir": str(self.dataset_dir.resolve()),
            "symbols": list(self.symbols),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "trading_dates": list(trading_dates),
            "file_count": len(existing),
            "total_size": total_size,
            "max_mtime_ns": max_mtime_ns,
        }

    @staticmethod
    def _dt_to_ns(v) -> int:
        if isinstance(v, datetime):
            epoch = datetime(1970, 1, 1)
            return int((v - epoch).total_seconds() * 1_000_000_000)
        if isinstance(v, np.datetime64):
            return int(np.asarray(v, dtype="datetime64[ns]").astype("int64"))
        return int(v)

    def _try_load_cache(
        self,
        cache_key: dict,
        trading_dates: list[str],
        jobs: list[tuple[str, str, Path]],
    ) -> tuple[dict[str, dict[str, dict[str, Any]]] | None, float]:
        t0 = _time.perf_counter()
        if not self._cache_file.exists():
            return None, _time.perf_counter() - t0
        try:
            with self._cache_file.open("rb") as f:
                magic = f.read(len(self._CACHE_MAGIC))
                if magic != self._CACHE_MAGIC:
                    return None, _time.perf_counter() - t0
                header_size_raw = f.read(4)
                if len(header_size_raw) != 4:
                    return None, _time.perf_counter() - t0
                header_size = struct.unpack("<I", header_size_raw)[0]
                header_bytes = f.read(header_size)
                header = json.loads(header_bytes.decode("utf-8"))
        except Exception:
            return None, _time.perf_counter() - t0
        if not isinstance(header, dict):
            return None, _time.perf_counter() - t0
        if header.get("key") != cache_key:
            return None, _time.perf_counter() - t0
        job_meta = header.get("jobs")
        if not isinstance(job_meta, list):
            return None, _time.perf_counter() - t0
        if len(job_meta) != len(jobs):
            return None, _time.perf_counter() - t0
        expected_pairs = [(d, s) for d, s, _ in jobs]
        got_pairs = [(str(x[0]), str(x[1])) for x in job_meta]
        if got_pairs != expected_pairs:
            return None, _time.perf_counter() - t0
        counts = [int(x[2]) for x in job_meta]
        total_count = int(sum(counts))
        offset0 = len(self._CACHE_MAGIC) + 4 + header_size
        dt: Any = np.memmap(
            self._cache_file,
            mode="r",
            dtype=np.int64,
            offset=offset0,
            shape=(total_count,),
        )
        offset1 = offset0 + total_count * np.dtype(np.int64).itemsize
        last: Any = np.memmap(
            self._cache_file,
            mode="r",
            dtype=np.float64,
            offset=offset1,
            shape=(total_count,),
        )
        offset2 = offset1 + total_count * np.dtype(np.float64).itemsize
        bid1: Any = np.memmap(
            self._cache_file,
            mode="r",
            dtype=np.float64,
            offset=offset2,
            shape=(total_count,),
        )
        offset3 = offset2 + total_count * np.dtype(np.float64).itemsize
        ask1: Any = np.memmap(
            self._cache_file,
            mode="r",
            dtype=np.float64,
            offset=offset3,
            shape=(total_count,),
        )
        tick_cache: dict[str, dict[str, dict[str, Any]]] = {d: {} for d in trading_dates}
        base = 0
        for date_str, symbol, count in job_meta:
            c = int(count)
            end = base + c
            tick_cache[date_str][symbol] = {
                "dt": dt[base:end],
                "last": last[base:end],
                "bid1": bid1[base:end],
                "ask1": ask1[base:end],
            }
            base = end
        return tick_cache, _time.perf_counter() - t0

    def _save_cache(
        self,
        cache_key: dict,
        tick_cache: dict[str, dict[str, dict[str, Any]]],
        jobs: list[tuple[str, str, Path]],
    ) -> float:
        t0 = _time.perf_counter()
        self._cache_file.parent.mkdir(parents=True, exist_ok=True)
        counts: list[int] = []
        total_count = 0
        for date_str, symbol, _ in jobs:
            arr = tick_cache.get(date_str, {}).get(symbol, self._empty_tick_arrays())["dt"]
            c = len(arr)
            counts.append(c)
            total_count += c
        dt_all: Any = np.empty(total_count, dtype=np.int64)
        last_all: Any = np.empty(total_count, dtype=np.float64)
        bid1_all: Any = np.empty(total_count, dtype=np.float64)
        ask1_all: Any = np.empty(total_count, dtype=np.float64)
        base = 0
        for i, (date_str, symbol, _) in enumerate(jobs):
            data = tick_cache.get(date_str, {}).get(symbol, self._empty_tick_arrays())
            dt_arr = data["dt"]
            last_arr = data["last"]
            bid_arr = data["bid1"]
            ask_arr = data["ask1"]
            c = counts[i]
            if c <= 0:
                continue
            end = base + c
            dt_all[base:end] = np.asarray(
                [self._dt_to_ns(v) for v in dt_arr],
                dtype=np.int64,
            )
            last_all[base:end] = np.asarray(last_arr, dtype=np.float64)
            bid1_all[base:end] = np.asarray(bid_arr, dtype=np.float64)
            ask1_all[base:end] = np.asarray(ask_arr, dtype=np.float64)
            base = end
        header = {
            "key": cache_key,
            "jobs": [
                [date_str, symbol, int(counts[i])]
                for i, (date_str, symbol, _) in enumerate(jobs)
            ],
        }
        header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
        with self._cache_file.open("wb") as f:
            f.write(self._CACHE_MAGIC)
            f.write(struct.pack("<I", len(header_bytes)))
            f.write(header_bytes)
            dt_all.tofile(f)
            last_all.tofile(f)
            bid1_all.tofile(f)
            ask1_all.tofile(f)
        return _time.perf_counter() - t0

    def run(self) -> FastSimpleResult:
        t_all0 = _time.perf_counter()
        t_daily0 = _time.perf_counter()
        bar_feed = ParquetBarFeed(self.dataset_dir)
        all_daily: dict[str, dict[str, tuple[float, float]]] = {}
        for symbol in self.symbols:
            bars = bar_feed.load(symbol)
            daily: dict[str, tuple[float, float]] = {}
            for bar in bars:
                daily[bar.datetime.strftime("%Y%m%d")] = (
                    bar.close_price,
                    bar.adj_factor,
                )
            all_daily[symbol] = daily

        positions: dict[str, Position] = {
            s: Position(symbol=s, enable_t0=self.enable_t0) for s in self.symbols
        }
        balance = self.initial_capital
        commission_total = 0.0
        trade_seq = 0
        trades: list[Trade] = []
        blocks: list[FastBlock] = []
        daily_nav: list[tuple[str, float]] = []
        signal_count = 0
        block_count: dict[str, int] = {s: 0 for s in self.symbols}
        cash_blocks = self.num_blocks

        if self.initial_positions:
            per_block_budget = max(1.0, self.initial_capital / max(1, self.num_blocks))
            used_blocks = 0
            for symbol, (volume, cost_price) in self.initial_positions.items():
                if symbol not in positions or volume <= 0 or cost_price <= 0:
                    continue
                pos = positions[symbol]
                pos.volume = int(volume)
                pos.cost_price = float(cost_price)
                pos.market_price = float(cost_price)
                block_guess = max(
                    1, int(round((volume * cost_price) / per_block_budget))
                )
                block_guess = min(block_guess, self.max_block_per_symbol)
                block_count[symbol] = block_guess
                used_blocks += block_guess
            cash_blocks = max(0, self.num_blocks - used_blocks)

        primary = self.symbols[0]
        primary_tick_root = self.dataset_dir / "ticks" / primary
        trading_dates: list[str] = []
        if primary_tick_root.is_dir():
            for month_dir in sorted(primary_tick_root.iterdir()):
                if not month_dir.is_dir():
                    continue
                for fp in sorted(month_dir.glob("*.parquet")):
                    trading_dates.append(fp.stem)
        trading_dates = [d for d in trading_dates if self.start_date <= d <= self.end_date]
        bars_map = {b.datetime.strftime("%Y%m%d"): b for b in bar_feed.load(primary)}
        daily_prepare_seconds = _time.perf_counter() - t_daily0

        t_tick_preload0 = _time.perf_counter()
        tick_cache: dict[str, dict[str, dict[str, list]]] = {}
        jobs: list[tuple[str, str, Path]] = []
        for date_str in trading_dates:
            tick_cache[date_str] = {}
            month = f"{date_str[:4]}-{date_str[4:6]}"
            for symbol in self.symbols:
                fp = (
                    self.dataset_dir
                    / "ticks"
                    / symbol
                    / month
                    / f"{date_str}.parquet"
                )
                jobs.append((date_str, symbol, fp))
        cache_key = self._build_cache_key(trading_dates, jobs)
        cache_hit = False
        cache_save_seconds = 0.0
        tick_cache_loaded, cache_load_seconds = self._try_load_cache(
            cache_key,
            trading_dates,
            jobs,
        )
        workers = 1
        if tick_cache_loaded is not None:
            tick_cache = tick_cache_loaded
            cache_hit = True
        else:
            for date_str, symbol, fp in jobs:
                tick_cache[date_str][symbol] = self._load_tick_minimal(fp)
            cache_save_seconds = self._save_cache(cache_key, tick_cache, jobs)
        tick_preload_seconds = _time.perf_counter() - t_tick_preload0

        t_replay0 = _time.perf_counter()
        prev_date: str | None = None
        stats_ready = False
        tick_count = 0
        skip_no_quote = 0
        skip_stale_quote = 0
        skip_warmup = 0
        stale_limit_ns = 5 * 60 * 1_000_000_000
        session_start_sec = 9 * 3600 + 30 * 60
        session_end_sec = 15 * 3600
        cutoff_sec = (
            self.trading_cutoff.hour * 3600
            + self.trading_cutoff.minute * 60
            + self.trading_cutoff.second
        )
        daily_closes: dict[str, list[float]] = {}
        adj_factors: dict[str, float] = {}
        mu_map: dict[tuple[str, str], float] = {}
        max_dev_map: dict[tuple[str, str], float] = {}
        current_prices: dict[str, float] = {}

        for date_str in trading_dates:
            day_block_count = 0
            cooldown_end_ns = 0
            if prev_date:
                for symbol in self.symbols:
                    entry = all_daily.get(symbol, {}).get(prev_date)
                    if not entry:
                        continue
                    close_adj, adj = entry
                    daily_closes.setdefault(symbol, []).append(close_adj)
                    adj_factors[symbol] = adj
                max_history = self.window * 3
                for symbol in self.symbols:
                    closes = daily_closes.get(symbol, [])
                    if len(closes) > max_history:
                        daily_closes[symbol] = closes[-max_history:]
                mu_map.clear()
                max_dev_map.clear()
                n = self.window - 1
                for i, sym_a in enumerate(self.symbols):
                    closes_a = daily_closes.get(sym_a, [])
                    if len(closes_a) < n:
                        continue
                    hist_a = closes_a[-n:]
                    for j, sym_b in enumerate(self.symbols):
                        if i == j:
                            continue
                        closes_b = daily_closes.get(sym_b, [])
                        if len(closes_b) < n:
                            continue
                        hist_b = closes_b[-n:]
                        ratios = []
                        for ca, cb in zip(hist_a, hist_b):
                            if cb > 0:
                                ratios.append(ca / cb)
                        if len(ratios) < n:
                            continue
                        pair_mu = sum(ratios) / len(ratios)
                        pair_max_dev = max(ratios) - pair_mu
                        mu_map[(sym_a, sym_b)] = pair_mu
                        max_dev_map[(sym_a, sym_b)] = pair_max_dev
                if not self.enable_t0:
                    for pos in positions.values():
                        pos.today_bought = 0
            if not stats_ready:
                for symbol in self.symbols:
                    daily = all_daily.get(symbol, {})
                    sorted_dates = sorted(d for d in daily if d < date_str)
                    daily_closes[symbol] = [daily[d][0] for d in sorted_dates]
                    if sorted_dates:
                        adj_factors[symbol] = daily[sorted_dates[-1]][1]
                    else:
                        adj_factors[symbol] = 1.0
                mu_map.clear()
                max_dev_map.clear()
                n = self.window - 1
                for i, sym_a in enumerate(self.symbols):
                    closes_a = daily_closes.get(sym_a, [])
                    if len(closes_a) < n:
                        continue
                    hist_a = closes_a[-n:]
                    for j, sym_b in enumerate(self.symbols):
                        if i == j:
                            continue
                        closes_b = daily_closes.get(sym_b, [])
                        if len(closes_b) < n:
                            continue
                        hist_b = closes_b[-n:]
                        ratios = []
                        for ca, cb in zip(hist_a, hist_b):
                            if cb > 0:
                                ratios.append(ca / cb)
                        if len(ratios) < n:
                            continue
                        pair_mu = sum(ratios) / len(ratios)
                        pair_max_dev = max(ratios) - pair_mu
                        mu_map[(sym_a, sym_b)] = pair_mu
                        max_dev_map[(sym_a, sym_b)] = pair_max_dev
                stats_ready = True

            day_ticks = tick_cache[date_str]
            primary_data = day_ticks.get(
                primary, {"dt": [], "last": [], "bid1": [], "ask1": []}
            )
            primary_dt = primary_data["dt"]
            primary_last = primary_data["last"]
            primary_bid1 = primary_data["bid1"]
            primary_ask1 = primary_data["ask1"]
            other_symbols = self.symbols[1:]
            cursors = {s: 0 for s in other_symbols}
            latest_quotes: dict[str, tuple[int, float, float, float]] = {}

            for i in range(len(primary_dt)):
                now_ns = int(primary_dt[i])
                last_price = primary_last[i]
                bid1 = primary_bid1[i]
                ask1 = primary_ask1[i]
                if last_price <= 0:
                    continue
                sec_of_day = (now_ns // 1_000_000_000) % 86400
                if sec_of_day < session_start_sec or sec_of_day >= session_end_sec:
                    continue
                if sec_of_day >= cutoff_sec:
                    continue
                remaining_min = (cutoff_sec - sec_of_day) / 60.0
                if remaining_min < self.block_timeout:
                    continue

                tick_count += 1
                if cooldown_end_ns > 0 and now_ns < cooldown_end_ns:
                    continue

                for sym in other_symbols:
                    data = day_ticks.get(
                        sym, {"dt": [], "last": [], "bid1": [], "ask1": []}
                    )
                    dt_arr = data["dt"]
                    last_arr = data["last"]
                    bid_arr = data["bid1"]
                    ask_arr = data["ask1"]
                    idx = cursors[sym]
                    while idx < len(dt_arr):
                        ts = int(dt_arr[idx])
                        if ts > now_ns:
                            break
                        if last_arr[idx] > 0:
                            latest_quotes[sym] = (
                                ts,
                                float(last_arr[idx]),
                                float(bid_arr[idx]),
                                float(ask_arr[idx]),
                            )
                        idx += 1
                    cursors[sym] = idx

                latest_quotes[primary] = (
                    now_ns,
                    float(last_price),
                    float(bid1),
                    float(ask1),
                )

                min_len = min(
                    (len(daily_closes.get(s, [])) for s in self.symbols),
                    default=0,
                )
                if min_len < self.window - 1:
                    skip_warmup += 1
                    continue

                valid_ticks = True
                for sym in self.symbols:
                    lt = latest_quotes.get(sym)
                    if not lt:
                        skip_no_quote += 1
                        valid_ticks = False
                        break
                    lt_dt, lt_last, _, _ = lt
                    if now_ns - lt_dt > stale_limit_ns:
                        skip_stale_quote += 1
                        valid_ticks = False
                        break
                    current_prices[sym] = lt_last
                if not valid_ticks:
                    continue
                now_dt = datetime(1970, 1, 1) + timedelta(microseconds=now_ns // 1000)

                held_symbols = [s for s, c in block_count.items() if c > 0]
                signals: list[dict] = []
                for h in held_symbols:
                    for si in self.symbols:
                        if si == h:
                            continue
                        pmu = mu_map.get((h, si))
                        pmax = max_dev_map.get((h, si))
                        px_h = current_prices.get(h, 0.0)
                        px_si = current_prices.get(si, 0.0)
                        if (
                            pmu is None
                            or pmax is None
                            or pmax <= 0
                            or px_h <= 0
                            or px_si <= 0
                        ):
                            continue
                        ratio = px_h / px_si
                        dev = ratio - pmu
                        score = dev / pmax
                        if (
                            dev > 0
                            and score > self.k_threshold
                            and dev > self.least_bias
                        ):
                            signals.append(
                                {
                                    "h": h,
                                    "si": si,
                                    "score": score,
                                    "dev": dev,
                                }
                            )
                signals.sort(key=lambda x: x["dev"], reverse=True)
                if not signals:
                    if cash_blocks <= 0:
                        continue
                    safe: list[tuple[str, float, float]] = []
                    for c in self.symbols:
                        is_safe = True
                        total_dev = 0.0
                        min_score = float("inf")
                        for si in self.symbols:
                            if si == c:
                                continue
                            pmu = mu_map.get((c, si))
                            pmax = max_dev_map.get((c, si))
                            px_c = current_prices.get(c, 0.0)
                            px_si = current_prices.get(si, 0.0)
                            if (
                                pmu is None
                                or pmax is None
                                or pmax <= 0
                                or px_c <= 0
                                or px_si <= 0
                            ):
                                is_safe = False
                                break
                            dev = (px_c / px_si) - pmu
                            score = dev / pmax
                            total_dev += dev
                            if (
                                dev > 0
                                and score > self.k_threshold
                                and dev > self.least_bias
                            ):
                                is_safe = False
                                break
                            if score < min_score:
                                min_score = score
                        if is_safe:
                            safe.append((c, total_dev, min_score))
                    if safe:
                        safe.sort(key=lambda x: x[1])
                        target, _, min_score = safe[0]
                        if min_score <= -self.k_threshold:
                            signal_count += 1
                            lt = latest_quotes.get(target)
                            if lt:
                                _, _, _, lt_ask1 = lt
                                idle_cash = max(0.0, balance - self.min_cash_reserve)
                                if idle_cash > 0:
                                    per_block = idle_cash / max(1, cash_blocks)
                                    ask = lt_ask1
                                    buy_px = 0.0
                                    if ask > 0:
                                        buy_px = round(
                                            math.ceil((ask + self.slippage) / self.pricetick)
                                            * self.pricetick,
                                            6,
                                        )
                                    if buy_px > 0:
                                        target_qty = (int(per_block / buy_px) // 100) * 100
                                        eff_cash = max(0.0, balance - self.min_cash_reserve)
                                        by_cash = (int(eff_cash / buy_px) // 100) * 100
                                        qty = min(target_qty, by_cash)
                                        qty = (int(qty) // 100) * 100
                                        if qty > 0:
                                            trade_seq += 1
                                            gross = buy_px * qty
                                            trade_comm = gross * self.rate
                                            balance -= gross + trade_comm
                                            commission_total += trade_comm
                                            pos = positions[target]
                                            total_cost = pos.cost_price * pos.volume + gross
                                            pos.volume += qty
                                            pos.cost_price = (
                                                total_cost / pos.volume if pos.volume > 0 else 0.0
                                            )
                                            pos.market_price = buy_px
                                            pos.today_bought += qty
                                            trades.append(
                                                Trade(
                                                    trade_id=f"F{trade_seq:08d}",
                                                    order_id=f"F{trade_seq:08d}",
                                                    symbol=target,
                                                    direction=Direction.BUY,
                                                    price=buy_px,
                                                    volume=qty,
                                                    commission=trade_comm,
                                                    datetime=now_dt,
                                                )
                                            )
                                            cash_blocks = max(0, cash_blocks - 1)
                                            block_count[target] = block_count.get(target, 0) + 1
                                            blocks.append(
                                                FastBlock(
                                                    ts=now_dt,
                                                    mode="pure_buy",
                                                    sell_symbol="",
                                                    buy_symbol=target,
                                                    sell_qty=0,
                                                    buy_qty=qty,
                                                    sell_price=0.0,
                                                    buy_price=buy_px,
                                                    score=0.0,
                                                    dev=0.0,
                                                )
                                            )
                                            day_block_count += 1
                                            cool_minutes = (
                                                self.cooldown_1
                                                if day_block_count == 1
                                                else self.cooldown_2
                                            )
                                            cooldown_end_ns = (
                                                now_ns + int(cool_minutes * 60 * 1_000_000_000)
                                            )
                    continue
                best_dev = signals[0]["dev"]
                chosen: dict | None = None
                for sig in signals:
                    si = sig["si"]
                    h = sig["h"]
                    si_count = block_count.get(si, 0)
                    if si_count >= self.max_block_per_symbol:
                        continue
                    if si_count == 0:
                        num_held = len([s for s, c in block_count.items() if c > 0])
                        h_after = block_count.get(h, 0) - 1
                        effective_held = num_held - (1 if h_after <= 0 else 0)
                        if effective_held >= self.max_positions:
                            continue
                    if sig["dev"] < best_dev * 0.9:
                        break
                    chosen = sig
                    break
                if not chosen:
                    continue
                signal_count += 1
                h = chosen["h"]
                si = chosen["si"]
                h_count = block_count.get(h, 0)
                if h_count <= 0:
                    continue
                pos_h = positions[h]
                avail = pos_h.available
                if avail <= 0:
                    continue
                sell_target = (int(avail // h_count) // 100) * 100
                remainder = avail - sell_target * h_count
                if 0 < remainder < 100:
                    sell_target += remainder
                if sell_target <= 0:
                    continue
                lt_sell = latest_quotes.get(h)
                if not lt_sell:
                    continue
                _, _, lt_sell_bid1, _ = lt_sell
                sell_qty = min(sell_target, pos_h.available)
                sell_qty = (int(sell_qty) // 100) * 100
                if sell_qty <= 0:
                    continue
                sell_base = lt_sell_bid1
                sell_px = 0.0
                if sell_base > 0:
                    sell_px = round(
                        math.floor((sell_base - self.slippage) / self.pricetick)
                        * self.pricetick,
                        6,
                    )
                if sell_px <= 0:
                    continue
                trade_seq += 1
                sell_gross = sell_px * sell_qty
                sell_comm = sell_gross * self.rate
                balance += sell_gross - sell_comm
                commission_total += sell_comm
                pos_h.volume -= sell_qty
                pos_h.market_price = sell_px
                if pos_h.volume <= 0:
                    pos_h.volume = 0
                    pos_h.cost_price = 0.0
                    pos_h.today_bought = 0
                trades.append(
                    Trade(
                        trade_id=f"F{trade_seq:08d}",
                        order_id=f"F{trade_seq:08d}",
                        symbol=h,
                        direction=Direction.SELL,
                        price=sell_px,
                        volume=sell_qty,
                        commission=sell_comm,
                        datetime=now_dt,
                    )
                )
                block_count[h] = max(0, block_count.get(h, 0) - 1)

                lt_buy = latest_quotes.get(si)
                if not lt_buy:
                    cash_blocks = min(self.num_blocks, cash_blocks + 1)
                    continue
                _, _, _, lt_buy_ask1 = lt_buy
                idle_cash = max(0.0, balance - self.min_cash_reserve)
                buy_budget = sell_gross + idle_cash / max(1, self.num_blocks)
                buy_base = lt_buy_ask1
                buy_px = 0.0
                if buy_base > 0:
                    buy_px = round(
                        math.ceil((buy_base + self.slippage) / self.pricetick)
                        * self.pricetick,
                        6,
                    )
                if buy_px <= 0:
                    cash_blocks = min(self.num_blocks, cash_blocks + 1)
                    continue
                buy_target = (int(buy_budget / buy_px) // 100) * 100
                eff_cash = max(0.0, balance - self.min_cash_reserve)
                buy_by_cash = (int(eff_cash / buy_px) // 100) * 100
                buy_qty = min(buy_target, buy_by_cash)
                buy_qty = (int(buy_qty) // 100) * 100
                if buy_qty <= 0:
                    cash_blocks = min(self.num_blocks, cash_blocks + 1)
                    continue
                trade_seq += 1
                buy_gross = buy_px * buy_qty
                buy_comm = buy_gross * self.rate
                balance -= buy_gross + buy_comm
                commission_total += buy_comm
                pos_si = positions[si]
                total_cost = pos_si.cost_price * pos_si.volume + buy_gross
                pos_si.volume += buy_qty
                pos_si.cost_price = total_cost / pos_si.volume if pos_si.volume > 0 else 0.0
                pos_si.market_price = buy_px
                pos_si.today_bought += buy_qty
                trades.append(
                    Trade(
                        trade_id=f"F{trade_seq:08d}",
                        order_id=f"F{trade_seq:08d}",
                        symbol=si,
                        direction=Direction.BUY,
                        price=buy_px,
                        volume=buy_qty,
                        commission=buy_comm,
                        datetime=now_dt,
                    )
                )
                block_count[si] = block_count.get(si, 0) + 1
                blocks.append(
                    FastBlock(
                        ts=now_dt,
                        mode="rotation",
                        sell_symbol=h,
                        buy_symbol=si,
                        sell_qty=sell_qty,
                        buy_qty=buy_qty,
                        sell_price=sell_px,
                        buy_price=buy_px,
                        score=float(chosen["score"]),
                        dev=float(chosen["dev"]),
                    )
                )
                day_block_count += 1
                cool_minutes = (
                    self.cooldown_1
                    if day_block_count == 1
                    else self.cooldown_2
                )
                cooldown_end_ns = now_ns + int(cool_minutes * 60 * 1_000_000_000)

            bar_today = bars_map.get(date_str)
            if bar_today:
                for sym, pos in positions.items():
                    if sym == primary and pos.volume > 0:
                        pos.market_price = bar_today.close_price
            for sym, pos in positions.items():
                if pos.volume <= 0:
                    continue
                if sym == primary:
                    continue
                lt = latest_quotes.get(sym)
                if lt and lt[1] > 0:
                    _, lt_last, _, _ = lt
                    pos.market_price = lt_last
            nav_mv = sum(
                p.volume * p.market_price for p in positions.values() if p.volume > 0
            )
            daily_nav.append((date_str, balance + nav_mv))
            prev_date = date_str
        replay_seconds = _time.perf_counter() - t_replay0
        total_seconds = _time.perf_counter() - t_all0

        final_nav_mv = sum(
            p.volume * p.market_price for p in positions.values() if p.volume > 0
        )
        return FastSimpleResult(
            trades=trades,
            daily_nav=daily_nav,
            blocks=blocks,
            start_balance=self.initial_capital,
            end_balance=balance + final_nav_mv,
            total_commission=commission_total,
            tick_count=tick_count,
            signal_count=signal_count,
            daily_prepare_seconds=daily_prepare_seconds,
            tick_preload_seconds=tick_preload_seconds,
            replay_seconds=replay_seconds,
            total_seconds=total_seconds,
            preload_workers=workers,
            preload_tasks=len(jobs),
            cache_hit=cache_hit,
            cache_load_seconds=cache_load_seconds,
            cache_save_seconds=cache_save_seconds,
            skip_no_quote=skip_no_quote,
            skip_stale_quote=skip_stale_quote,
            skip_warmup=skip_warmup,
        )
