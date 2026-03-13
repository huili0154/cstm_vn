"""
扫描 159941.SZ: 日线 OHLC vs tick 数据一致性检查。

规则:
  - 只看 9:30 ~ 14:57 之间的 tick (排除盘前盘后 + 深圳收盘集合竞价)
  - open  = 第一个有效 tick 的 last_price
  - close = 14:57 前最后一个 tick 的 last_price
  - high  = 有效区间内 last_price 的 max
  - low   = 有效区间内 last_price 的 min
  - 对已知的 100 倍缩放做自动归一化, 检查归一后是否还有偏差
"""
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from collections import Counter

SYMBOL = "159941.SZ"
ds = Path("dataset")

# ── 加载全部日线 ──
daily_frames = []
for f in sorted((ds / "daily" / SYMBOL).glob("*.parquet")):
    daily_frames.append(pq.read_table(f).to_pandas())
daily = pd.concat(daily_frames, ignore_index=True)
daily = daily[daily["trade_date"] >= "20240101"].reset_index(drop=True)
print(f"Daily rows from 2024: {len(daily)}")

# ── 扫描 tick 数据 ──
tick_dir = ds / "ticks" / SYMBOL
tick_dates = sorted(
    f.stem
    for d in tick_dir.iterdir() if d.is_dir()
    for f in d.glob("*.parquet")
)
tick_dates = [d for d in tick_dates if d >= "20240101"]
print(f"Tick dates from 2024: {len(tick_dates)}")

# ── 逐日比对 ──
results = []  # list of dict
for date_str in tick_dates:
    month = date_str[:4] + "-" + date_str[4:6]
    tick_path = tick_dir / month / f"{date_str}.parquet"
    if not tick_path.exists():
        continue
    tdf = pq.read_table(tick_path).to_pandas()
    if tdf.empty:
        continue

    # 过滤: 只保留 9:30 ~ 14:57
    hm = tdf["datetime"].dt.strftime("%H:%M")
    valid = tdf[(hm >= "09:30") & (hm < "14:57")]
    if valid.empty:
        results.append({"date": date_str, "status": "NO_VALID_TICKS"})
        continue

    prices = valid["last_price"]
    tick_open = prices.iloc[0]
    tick_close = prices.iloc[-1]
    tick_high = prices.max()
    tick_low = prices.min()

    # 自动归一化: 如果 tick 价格在 50~200 范围 → 除以 100
    scale = 1.0
    if tick_close > 10:
        scale = 100.0
    tick_open_n = tick_open / scale
    tick_close_n = tick_close / scale
    tick_high_n = tick_high / scale
    tick_low_n = tick_low / scale

    # 查日线
    drow = daily[daily["trade_date"] == date_str]
    if drow.empty:
        results.append({
            "date": date_str, "status": "NO_DAILY", "scale": scale,
            "tick_open": tick_open_n, "tick_close": tick_close_n,
            "tick_high": tick_high_n, "tick_low": tick_low_n,
        })
        continue
    drow = drow.iloc[0]
    do, dc, dh, dl = drow["open"], drow["close"], drow["high"], drow["low"]

    # 比较 (允许 0.2% 误差)
    TOL = 0.002
    def close_enough(a, b):
        if b == 0:
            return a == 0
        return abs(a - b) / b < TOL

    ok_open  = close_enough(tick_open_n, do)
    ok_close = close_enough(tick_close_n, dc)
    ok_high  = close_enough(tick_high_n, dh)
    ok_low   = close_enough(tick_low_n, dl)

    entry = {
        "date": date_str,
        "scale": scale,
        "tick_open": tick_open_n, "tick_close": tick_close_n,
        "tick_high": tick_high_n, "tick_low": tick_low_n,
        "daily_open": do, "daily_close": dc,
        "daily_high": dh, "daily_low": dl,
        "ok_open": ok_open, "ok_close": ok_close,
        "ok_high": ok_high, "ok_low": ok_low,
    }
    if ok_open and ok_close and ok_high and ok_low:
        entry["status"] = "OK"
    else:
        entry["status"] = "MISMATCH"
        # 记录具体偏差
        diffs = []
        if not ok_open:
            diffs.append(f"open: tick={tick_open_n:.4f} daily={do:.4f} diff={tick_open_n-do:+.4f}")
        if not ok_close:
            diffs.append(f"close: tick={tick_close_n:.4f} daily={dc:.4f} diff={tick_close_n-dc:+.4f}")
        if not ok_high:
            diffs.append(f"high: tick={tick_high_n:.4f} daily={dh:.4f} diff={tick_high_n-dh:+.4f}")
        if not ok_low:
            diffs.append(f"low: tick={tick_low_n:.4f} daily={dl:.4f} diff={tick_low_n-dl:+.4f}")
        entry["diffs"] = diffs
    results.append(entry)

# ── 汇总 ──
ok_count = sum(1 for r in results if r["status"] == "OK")
mismatch = [r for r in results if r["status"] == "MISMATCH"]
no_daily = [r for r in results if r["status"] == "NO_DAILY"]
no_ticks = [r for r in results if r["status"] == "NO_VALID_TICKS"]
scale_100 = sum(1 for r in results if r.get("scale") == 100.0)
scale_1   = sum(1 for r in results if r.get("scale") == 1.0)

print(f"\n{'='*80}")
print(f"总天数: {len(results)}")
print(f"  完全匹配: {ok_count}")
print(f"  有偏差:   {len(mismatch)}")
print(f"  无日线:   {len(no_daily)}")
print(f"  无有效tick: {len(no_ticks)}")
print(f"  缩放100x天数: {scale_100}   缩放1x天数: {scale_1}")
print(f"{'='*80}")

if mismatch:
    # 统计偏差类型
    field_miss = Counter()
    for m in mismatch:
        for d in m.get("diffs", []):
            field = d.split(":")[0]
            field_miss[field] += 1
    print(f"\n偏差字段统计: {dict(field_miss)}")

    # 输出偏差详情
    print(f"\n{'Date':<10} {'Scale':>5} {'T_Open':>8} {'T_Close':>8} {'T_High':>8} {'T_Low':>8}"
          f" {'D_Open':>8} {'D_Close':>8} {'D_High':>8} {'D_Low':>8}  偏差说明")
    print("-" * 120)
    for m in mismatch[:100]:
        d = m["date"]
        s = m["scale"]
        to, tc = m["tick_open"], m["tick_close"]
        th, tl = m["tick_high"], m["tick_low"]
        do, dc = m["daily_open"], m["daily_close"]
        dh, dl = m["daily_high"], m["daily_low"]
        flags = ""
        if not m["ok_open"]:  flags += "O"
        if not m["ok_close"]: flags += "C"
        if not m["ok_high"]:  flags += "H"
        if not m["ok_low"]:   flags += "L"
        print(f"{d:<10} {s:>5.0f} {to:>8.4f} {tc:>8.4f} {th:>8.4f} {tl:>8.4f}"
              f" {do:>8.4f} {dc:>8.4f} {dh:>8.4f} {dl:>8.4f}  [{flags}]")
    if len(mismatch) > 100:
        print(f"... and {len(mismatch) - 100} more")

if no_daily:
    print(f"\n无日线的日期: {[r['date'] for r in no_daily]}")
