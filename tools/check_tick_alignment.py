"""
检查各品种 tick 数据的时间戳对齐情况。
"""
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
from collections import Counter


root = Path("dataset/ticks")
test_date = "20250102"


def load_df(sym, date):
    month_dir = f"{date[:4]}-{date[4:6]}"
    fp = root / sym / month_dir / f"{date}.parquet"
    df = pq.read_table(fp, columns=["datetime"]).to_pandas()
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ─── Part 1: Tick interval distribution ───
print("=== SH Tick Interval Distribution (seconds) ===")
for sym in ["510300.SH", "510310.SH", "513100.SH"]:
    df = load_df(sym, test_date)
    trading = df[df["datetime"] >= "2025-01-02 09:25:00"]
    diffs = trading["datetime"].diff().dropna().dt.total_seconds()
    cnt = Counter(diffs.values)
    top5 = cnt.most_common(5)
    print(f"  {sym}: {len(trading)} trading ticks, intervals:")
    for val, n in top5:
        print(f"    {val:.0f}s: {n} times ({n/len(diffs)*100:.1f}%)")
    print()

print("=== SZ Tick Interval Distribution (seconds) ===")
for sym in ["159300.SZ", "159919.SZ", "159612.SZ"]:
    df = load_df(sym, test_date)
    trading = df[df["datetime"] >= "2025-01-02 09:25:00"]
    diffs = trading["datetime"].diff().dropna().dt.total_seconds()
    cnt = Counter(diffs.values)
    top5 = cnt.most_common(5)
    print(f"  {sym}: {len(trading)} trading ticks, intervals:")
    for val, n in top5:
        print(f"    {val:.0f}s: {n} times ({n/len(diffs)*100:.1f}%)")
    print()

# ─── Part 2: 1-minute window comparison ───
print("=== SH 09:30:00-09:31:00 ===")
for sym in ["510300.SH", "510350.SH"]:
    df = load_df(sym, test_date)
    mask = (df["datetime"] >= "2025-01-02 09:30:00") & (df["datetime"] <= "2025-01-02 09:31:00")
    trading = df[mask]
    ts_list = trading["datetime"].tolist()
    print(f"  {sym}:")
    for t in ts_list:
        print(f"    {t}")
    print()

print("=== SZ 09:30:00-09:31:00 ===")
for sym in ["159300.SZ", "159919.SZ"]:
    df = load_df(sym, test_date)
    mask = (df["datetime"] >= "2025-01-02 09:30:00") & (df["datetime"] <= "2025-01-02 09:31:00")
    trading = df[mask]
    ts_list = trading["datetime"].tolist()
    print(f"  {sym}:")
    for t in ts_list:
        print(f"    {t}")
    print()

# ─── Part 3: Check if duplicates exist ───
print("=== Duplicate timestamps check ===")
for sym in ["510300.SH", "510310.SH", "159300.SZ", "159919.SZ"]:
    df = load_df(sym, test_date)
    dupes = df["datetime"].duplicated().sum()
    print(f"  {sym}: {dupes} duplicate timestamps out of {len(df)}")

# ─── Part 4: Multi-day consistency check ───
print()
print("=== Multi-day check: are tick counts stable? ===")
symbols = sorted([d.name for d in root.iterdir() if d.is_dir()])
for sym in ["510300.SH", "159300.SZ", "159612.SZ"]:
    sym_dir = root / sym
    dates_counts = []
    for md in sorted(sym_dir.iterdir()):
        if not md.is_dir():
            continue
        for fp in sorted(md.glob("*.parquet")):
            df = pq.read_table(fp, columns=["datetime"]).to_pandas()
            dates_counts.append((fp.stem, len(df)))
    print(f"  {sym}:")
    for d, c in dates_counts:
        print(f"    {d}: {c} ticks")
    print()

# ─── Part 5: Cross-exchange timestamp pattern ───
print("=== Seconds-in-minute pattern (trading hours) ===")
for sym in ["510300.SH", "510310.SH", "513100.SH", "159300.SZ", "159919.SZ"]:
    df = load_df(sym, test_date)
    trading = df[df["datetime"] >= "2025-01-02 09:30:00"]
    seconds = trading["datetime"].dt.second
    cnt = Counter(seconds.values)
    top_secs = sorted(cnt.items(), key=lambda x: -x[1])[:10]
    print(f"  {sym}: most common seconds in minute:")
    for sec, n in top_secs:
        print(f"    :{sec:02d} -> {n} times")
    print()
