import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import chardet
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tools.dataset_manifest import load_manifest, save_manifest, upsert_file_entry


@dataclass
class CsvProbe:
    encoding: str
    delimiter: str
    has_header: bool
    header: list[str]


def _detect_encoding(file_path: Path, max_bytes: int = 1024 * 128) -> str:
    with file_path.open("rb") as f:
        raw = f.read(max_bytes)
    guess = chardet.detect(raw) or {}
    return guess.get("encoding") or "utf-8"


def _detect_delimiter(first_line: str) -> str:
    candidates = [",", "\t", ";", "|"]
    best = ","
    best_score = -1
    for c in candidates:
        score = first_line.count(c)
        if score > best_score:
            best_score = score
            best = c
    return best


def _looks_like_header(fields: list[str]) -> bool:
    if not fields:
        return False
    joined = "".join(fields)
    if any(ch.isalpha() for ch in joined):
        return True
    if any(re.search(r"[\u4e00-\u9fff]", f) for f in fields):
        return True
    return False


def probe_csv(file_path: Path) -> CsvProbe:
    encoding = _detect_encoding(file_path)
    with file_path.open("r", encoding=encoding, errors="replace", newline="") as f:
        first_line = f.readline()
    delimiter = _detect_delimiter(first_line)
    with file_path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        first_row = next(reader, [])
    has_header = _looks_like_header(first_row)
    header = [c.strip() for c in first_row]
    if not has_header:
        header = [f"col_{i}" for i in range(len(first_row))]
    return CsvProbe(encoding=encoding, delimiter=delimiter, has_header=has_header, header=header)


def _read_daily_csv(csv_path: Path) -> pd.DataFrame:
    probe = probe_csv(csv_path)
    df = pd.read_csv(
        csv_path,
        encoding=probe.encoding,
        sep=probe.delimiter,
        engine="python",
        header=0 if probe.has_header else None,
        names=None if probe.has_header else probe.header,
        on_bad_lines="skip",
    )
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _normalize_daily_df(df: pd.DataFrame, symbol: str) -> tuple[pd.DataFrame, dict]:
    date_col = None
    for cand in ["date", "日期", "交易日期", "trade_date"]:
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        raise ValueError("missing_date_col")

    dt = pd.to_datetime(df[date_col], errors="coerce")
    ok = dt.notna()
    df = df.loc[ok].copy()
    dt = dt.loc[ok]

    def pick(*names: str) -> str | None:
        for n in names:
            if n in df.columns:
                return n
        return None

    open_col = pick("open", "开盘", "open_price")
    high_col = pick("high", "最高", "high_price")
    low_col = pick("low", "最低", "low_price")
    close_col = pick("close", "收盘", "close_price")
    volume_col = pick("volume", "成交量")
    turnover_col = pick("turnover", "成交额")
    adj_col = pick("adj_factor", "复权因子", "adj")
    preclose_col = pick("pre_close", "前收盘")

    out = pd.DataFrame({
        "date": dt.dt.date.astype(str),
        "symbol": symbol,
    })

    for out_name, src in [
        ("open", open_col),
        ("high", high_col),
        ("low", low_col),
        ("close", close_col),
        ("pre_close", preclose_col),
        ("volume", volume_col),
        ("turnover", turnover_col),
        ("adj_factor", adj_col),
    ]:
        if src:
            out[out_name] = pd.to_numeric(df[src], errors="coerce")

    out = out.sort_values("date")
    meta = {
        "rows": int(len(out)),
        "start": out["date"].iloc[0] if len(out) else None,
        "end": out["date"].iloc[-1] if len(out) else None,
        "cols": list(out.columns),
    }
    return out, meta


def _daily_out_path(root: Path, symbol: str) -> Path:
    return root / "dataset" / "daily" / f"{symbol}.parquet"


def import_daily(root: Path, symbol: str, csv_path: Path) -> None:
    sym = symbol.upper()
    df_raw = _read_daily_csv(csv_path)
    df, meta = _normalize_daily_df(df_raw, sym)

    out_path = _daily_out_path(root, sym)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="zstd")

    manifest = load_manifest(root)
    upsert_file_entry(
        manifest.setdefault("daily", {}).setdefault("files", []),
        {
            "symbol": sym,
            "path": str(out_path.relative_to(root)).replace("\\", "/"),
            "source_csv": str(csv_path),
            "rows": meta["rows"],
            "start": meta["start"],
            "end": meta["end"],
            "cols": meta["cols"],
        },
        keys=["symbol"],
    )
    save_manifest(root, manifest)
    print(str(out_path))


def export_daily(root: Path, symbol: str, out_csv: Path) -> None:
    sym = symbol.upper()
    p = _daily_out_path(root, sym)
    if not p.exists():
        raise SystemExit("not_found")
    df = pd.read_parquet(p)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(str(out_csv))


def main() -> None:
    parser = argparse.ArgumentParser(prog="DailyParquetManager")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_import = sub.add_parser("import")
    p_import.add_argument("--symbol", required=True)
    p_import.add_argument("--csv", required=True)

    p_export = sub.add_parser("export")
    p_export.add_argument("--symbol", required=True)
    p_export.add_argument("--out", required=True)

    args = parser.parse_args()
    root = Path(args.root)

    if args.cmd == "import":
        import_daily(root, args.symbol, Path(args.csv))
    elif args.cmd == "export":
        export_daily(root, args.symbol, Path(args.out))


if __name__ == "__main__":
    main()

