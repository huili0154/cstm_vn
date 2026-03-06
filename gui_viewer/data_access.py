from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class Instrument:
    code: str
    ts_code: str
    name: str


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def dataset_root(root: Path | None = None) -> Path:
    return (root or project_root()) / "dataset"


def load_instruments(ds_root: Path) -> list[Instrument]:
    p = ds_root / "meta" / "instruments.parquet"
    df = pd.read_parquet(p)
    df = df[df.get("resolved", True) == True] if "resolved" in df.columns else df
    out: list[Instrument] = []
    for r in df.to_dict(orient="records"):
        ts_code = str(r.get("ts_code") or "").strip().upper()
        code = str(r.get("code") or "").strip()
        name = str(r.get("name") or "").strip()
        if not ts_code or not name or len(code) != 6:
            continue
        out.append(Instrument(code=code, ts_code=ts_code, name=name))
    out.sort(key=lambda x: x.ts_code)
    return out


def available_daily_years(ds_root: Path, ts_code: str) -> list[int]:
    p = ds_root / "daily" / ts_code
    if not p.exists():
        return []
    years = []
    for f in sorted(p.glob("*.parquet")):
        try:
            years.append(int(f.stem))
        except Exception:
            continue
    return sorted(set(years))


def load_daily(ds_root: Path, ts_code: str, year: int) -> pd.DataFrame:
    p = ds_root / "daily" / ts_code / f"{year}.parquet"
    df = pd.read_parquet(p)
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
    return df


def _iter_tick_files(ds_root: Path, ts_code: str) -> list[Path]:
    base = ds_root / "ticks" / ts_code
    if not base.exists():
        return []
    files: list[Path] = []
    for month_dir in sorted(base.glob("????-??")):
        if not month_dir.is_dir():
            continue
        files.extend(sorted(month_dir.glob("*.parquet")))
    return files


def available_tick_dates(ds_root: Path, ts_code: str) -> list[str]:
    dates = []
    for f in _iter_tick_files(ds_root, ts_code):
        if f.stem.isdigit() and len(f.stem) == 8:
            dates.append(f.stem)
    return sorted(set(dates))


def load_tick_day(ds_root: Path, ts_code: str, yyyymmdd: str) -> pd.DataFrame:
    month = f"{yyyymmdd[0:4]}-{yyyymmdd[4:6]}"
    p = ds_root / "ticks" / ts_code / month / f"{yyyymmdd}.parquet"
    df = pd.read_parquet(p)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df


def load_tick_series(ds_root: Path, ts_code: str, start: str, end: str, y_col: str) -> pd.DataFrame:
    if start > end:
        start, end = end, start
    dates = [d for d in available_tick_dates(ds_root, ts_code) if start <= d <= end]
    if not dates:
        return pd.DataFrame(columns=["datetime", y_col])
    frames = []
    for d in dates:
        df = load_tick_day(ds_root, ts_code, d)
        cols = [c for c in ["datetime", y_col] if c in df.columns]
        if "datetime" not in cols or y_col not in cols:
            continue
        frames.append(df[cols])
    if not frames:
        return pd.DataFrame(columns=["datetime", y_col])
    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["datetime"]).sort_values("datetime")
    return out


def load_tick_range_full(ds_root: Path, ts_code: str, start: str, end: str) -> pd.DataFrame:
    if start > end:
        start, end = end, start
    dates = [d for d in available_tick_dates(ds_root, ts_code) if start <= d <= end]
    if not dates:
        return pd.DataFrame()
    frames = []
    for d in dates:
        df = load_tick_day(ds_root, ts_code, d)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    if "datetime" in out.columns:
        out = out.dropna(subset=["datetime"]).sort_values("datetime")
    return out


def parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def format_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

