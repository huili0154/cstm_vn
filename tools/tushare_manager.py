import argparse
import time
from dataclasses import dataclass
from pathlib import Path
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import py7zr
import tushare as ts

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tools.universe import read_universe, normalize_ts_code


@dataclass(frozen=True)
class InstrumentsPaths:
    root: Path

    @property
    def meta_dir(self) -> Path:
        return self.root / "dataset" / "meta"

    @property
    def instruments_parquet(self) -> Path:
        return self.meta_dir / "instruments.parquet"

    @property
    def instruments_json(self) -> Path:
        return self.meta_dir / "instruments.json"

    @property
    def daily_dir(self) -> Path:
        return self.root / "dataset" / "daily"


def _get_token() -> str:
    import os

    env_token = os.environ.get("TUSHARE_TOKEN")
    if env_token:
        return env_token.strip()

    local_path = Path.cwd() / "dataset" / "meta" / "tushare_token.local"
    if local_path.exists():
        return local_path.read_text(encoding="utf-8").strip()

    home_path = Path.home() / ".tushare_token"
    if home_path.exists():
        return home_path.read_text(encoding="utf-8").strip()

    return ""


def get_pro() -> ts.pro_api:
    import os

    for k in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ]:
        if k in os.environ:
            os.environ.pop(k, None)
    os.environ.setdefault("NO_PROXY", "*")
    os.environ.setdefault("no_proxy", "*")

    token = _get_token()
    if not token:
        raise RuntimeError("missing_tushare_token: set env var TUSHARE_TOKEN or create dataset/meta/tushare_token.local")
    return ts.pro_api(token, timeout=90)


def _vendor_suffix_scan(rawdir: Path, codes: list[str]) -> dict[str, dict]:
    result: dict[str, dict] = {c: {"vendor_symbols": set(), "vendor_dates": set()} for c in codes}
    archives = sorted(rawdir.glob("*.7z"))
    for ap in archives:
        date = ap.stem
        try:
            with py7zr.SevenZipFile(ap, mode="r") as z:
                names = set(z.getnames())
        except Exception:
            continue
        for c in codes:
            for exch in ("SZ", "SH"):
                p = f"{date}/{c}.{exch}/行情.csv"
                if p in names:
                    result[c]["vendor_symbols"].add(f"{c}.{exch}")
                    result[c]["vendor_dates"].add(date)
    for c in codes:
        result[c]["vendor_symbols"] = sorted(result[c]["vendor_symbols"])
        result[c]["vendor_dates"] = sorted(result[c]["vendor_dates"])
    return result


def _fund_basic_try(pro, ts_code: str) -> pd.DataFrame:
    return pro.fund_basic(ts_code=ts_code, market="E", status="L", fields="ts_code,name,fund_type,list_date,delist_date,management,custodian")


def _load_instruments_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = df[df.get("resolved", False) == True] if "resolved" in df.columns else df
    if "ts_code" not in df.columns:
        raise ValueError("instruments_missing_ts_code")
    return df


def _fund_daily_year(pro, ts_code: str, year: int) -> pd.DataFrame:
    start_date = f"{year}0101"
    end_date = f"{year}1231"
    df = pro.fund_daily(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _fund_adj_year(pro, ts_code: str, year: int) -> pd.DataFrame:
    start_date = f"{year}0101"
    end_date = f"{year}1231"
    df = pro.fund_adj(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _compute_backward_adjust(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values("trade_date")
    if df.empty:
        return df
    base = float(df["adj_factor"].iloc[0])
    if not base:
        base = 1.0
    ratio = df["adj_factor"].astype(float) / base
    for col in ["open", "high", "low", "close", "pre_close"]:
        if col in df.columns:
            df[f"{col}_bwd"] = df[col].astype(float) * ratio
    return df


def fetch_daily_year(root: Path, instruments_parquet: Path, year: int, logger: callable = print, progress_callback: callable = None) -> list[Path]:
    pro = get_pro()
    paths = InstrumentsPaths(root)
    ins = _load_instruments_parquet(instruments_parquet)

    written: list[Path] = []
    records = ins.to_dict(orient="records")
    total = len(records)
    logger(f"Fetch Daily {year}: Found {total} instruments.")
    
    start_time = time.time()

    for idx, row in enumerate(records, 1):
        if progress_callback and idx % 5 == 0:
            elapsed = time.time() - start_time
            if idx > 10:
                avg_time = elapsed / idx
                remaining = avg_time * (total - idx)
                eta_str = f"{int(remaining // 60)}m {int(remaining % 60)}s"
            else:
                eta_str = "Calc..."
            progress_callback(idx, total, f"Daily {year}: {idx}/{total} (ETA: {eta_str})")

        ts_code = str(row.get("ts_code") or "").strip().upper()
        if not ts_code:
            continue
        
        # logger(f"[{idx}/{total}] Fetching {ts_code}...") # Too verbose if printed for every stock
        if idx % 10 == 0:
             logger(f"Processing {year}: {idx}/{total} ({ts_code})")

        df_daily = _fund_daily_year(pro, ts_code, year)
        if df_daily.empty:
            continue
        df_adj = _fund_adj_year(pro, ts_code, year)
        df_adj = df_adj[["trade_date", "adj_factor"]] if not df_adj.empty else pd.DataFrame(columns=["trade_date", "adj_factor"])

        df = df_daily.merge(df_adj, on="trade_date", how="left")
        df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce").fillna(1.0)
        df = _compute_backward_adjust(df)

        df["volume"] = pd.to_numeric(df.get("vol"), errors="coerce")
        df["turnover"] = pd.to_numeric(df.get("amount"), errors="coerce") * 1000.0
        df = df.drop(columns=[c for c in ["vol", "amount"] if c in df.columns])
        df["name"] = row.get("name")

        out_dir = paths.daily_dir / ts_code
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{year}.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out_path, compression="zstd")
        written.append(out_path)
        # Sleep slightly to avoid strict rate limit if needed, but not too long
        time.sleep(0.05)

    logger(f"Fetch Daily {year} Done: Downloaded {len(written)} files.")
    return written


def sync_instruments(root: Path, universe_file: Path, rawdir: Path) -> Path:
    items = read_universe(universe_file)
    codes = sorted({it.code for it in items})
    vendor = _vendor_suffix_scan(rawdir, codes)

    pro = get_pro()

    rows: list[dict] = []
    for it in items:
        candidates = []
        if it.preferred_exchange:
            candidates.append(normalize_ts_code(it.code, it.preferred_exchange))
        candidates.extend([normalize_ts_code(it.code, "SZ"), normalize_ts_code(it.code, "SH")])
        seen = set()
        candidates = [c for c in candidates if not (c in seen or seen.add(c))]

        info = None
        for cand in candidates:
            try:
                df = _fund_basic_try(pro, cand)
            except Exception:
                time.sleep(0.3)
                continue
            if df is not None and len(df):
                info = df.iloc[0].to_dict()
                break
            time.sleep(0.1)

        if not info:
            rows.append(
                {
                    "code": it.code,
                    "ts_code": None,
                    "name": None,
                    "fund_type": None,
                    "list_date": None,
                    "delist_date": None,
                    "vendor_symbols": vendor[it.code]["vendor_symbols"],
                    "vendor_dates": vendor[it.code]["vendor_dates"],
                    "resolved": False,
                }
            )
            continue

        ts_code = info.get("ts_code")
        rows.append(
            {
                "code": it.code,
                "ts_code": ts_code,
                "exchange": ts_code.split(".")[1] if ts_code else None,
                "name": info.get("name"),
                "fund_type": info.get("fund_type"),
                "list_date": info.get("list_date"),
                "delist_date": info.get("delist_date"),
                "management": info.get("management"),
                "custodian": info.get("custodian"),
                "vendor_symbols": vendor[it.code]["vendor_symbols"],
                "vendor_dates": vendor[it.code]["vendor_dates"],
                "resolved": True,
            }
        )

    df_out = pd.DataFrame(rows)
    paths = InstrumentsPaths(root)
    paths.meta_dir.mkdir(parents=True, exist_ok=True)

    pq.write_table(pa.Table.from_pandas(df_out, preserve_index=False), paths.instruments_parquet, compression="zstd")
    paths.instruments_json.write_text(df_out.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
    return paths.instruments_parquet


def main() -> None:
    parser = argparse.ArgumentParser(prog="TuShareManager")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync-instruments")
    p_sync.add_argument("--universe", default=str(Path(__file__).resolve().parents[1] / "dataset" / "meta" / "symbols.txt"))
    p_sync.add_argument("--rawdir", default=str(Path(__file__).resolve().parents[1] / "rawData"))

    p_daily = sub.add_parser("fetch-daily-year")
    p_daily.add_argument("--instruments", default=str(Path(__file__).resolve().parents[1] / "dataset" / "meta" / "instruments.parquet"))
    p_daily.add_argument("--year", type=int, required=True)

    args = parser.parse_args()
    root = Path(args.root)

    if args.cmd == "sync-instruments":
        out = sync_instruments(root, Path(args.universe), Path(args.rawdir))
        print(str(out))
    elif args.cmd == "fetch-daily-year":
        outs = fetch_daily_year(root, Path(args.instruments), args.year)
        print("\n".join(str(p) for p in outs))


if __name__ == "__main__":
    main()

