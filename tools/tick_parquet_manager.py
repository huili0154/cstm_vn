import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import chardet
import pandas as pd
import py7zr
import pyarrow as pa
import pyarrow.parquet as pq
import pytz

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tools.dataset_manifest import load_manifest, save_manifest, upsert_file_entry
from tools.universe import read_universe


TZ = pytz.timezone("Asia/Shanghai")


@dataclass
class CsvProbe:
    encoding: str
    delimiter: str
    has_header: bool
    header: list[str]


def _load_instruments(instruments_path: Path | None) -> dict[str, dict]:
    if not instruments_path:
        return {}
    if not instruments_path.exists():
        raise FileNotFoundError(str(instruments_path))
    if instruments_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(instruments_path)
        rows = df.to_dict(orient="records")
    else:
        rows = json.loads(instruments_path.read_text(encoding="utf-8"))
    out: dict[str, dict] = {}
    for r in rows:
        code = str(r.get("code") or "").strip()
        if not re.fullmatch(r"\d{6}", code):
            continue
        out[code] = r
    return out


def _read_universe_codes(universe_file: Path) -> list[str]:
    items = read_universe(universe_file)
    return sorted({it.code for it in items})


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


def _parse_datetime_series(day: pd.Series, t: pd.Series) -> pd.Series:
    day_digits = day.astype(str).str.replace(r"\D", "", regex=True)
    t_digits = t.astype(str).str.replace(r"\D", "", regex=True)
    t_padded = t_digits.fillna("").str.zfill(9)
    hh = t_padded.str.slice(0, 2)
    mm = t_padded.str.slice(2, 4)
    ss = t_padded.str.slice(4, 6)
    ms = t_padded.str.slice(6, 9)
    ymd = day_digits.fillna("").str.slice(0, 8)
    dt_str = ymd + hh + mm + ss
    base = pd.to_datetime(dt_str, format="%Y%m%d%H%M%S", errors="coerce")
    ms_num = pd.to_numeric(ms, errors="coerce").fillna(0).astype(int)
    return base + pd.to_timedelta(ms_num, unit="ms")


def _infer_price_scale(df: pd.DataFrame) -> int:
    """Raw CSV prices are in units of 1/10000 yuan. Always divide by 10000."""
    for col in ["申买价1", "申卖价1", "成交价", "开盘价", "前收盘"]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            s = s[s > 0]
            if len(s) == 0:
                continue
            return 10000
    return 10000


def _symbol_dir_name(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith(".SZ"):
        return s.replace(".SZ", ".SZ")
    if s.endswith(".SH"):
        return s.replace(".SH", ".SH")
    raise ValueError(f"unsupported_symbol: {symbol}")


def _extract_targets_from_archive(archive_path: Path, date: str, codes: list[str], instruments: dict[str, dict]) -> list[str]:
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        names = set(z.getnames())

    targets = []
    for code in codes:
        vendor_symbols = []
        meta = instruments.get(code) or {}
        vs = meta.get("vendor_symbols")
        if isinstance(vs, list) and vs:
            vendor_symbols = [str(x).upper() for x in vs]
        if not vendor_symbols:
            vendor_symbols = [f"{code}.SZ", f"{code}.SH"]

        for vsym in vendor_symbols:
            p = f"{date}/{vsym}/行情.csv"
            if p in names:
                targets.append(p)
    return targets


def _extract_csvs(archive_path: Path, out_dir: Path, targets: list[str]) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        z.extract(path=out_dir, targets=targets)
    files = []
    for t in targets:
        p = out_dir / t
        if p.exists():
            files.append(p)
    return files


def _load_tick_csv(csv_path: Path) -> pd.DataFrame:
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


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _to_price(series: pd.Series, scale: int) -> pd.Series:
    return pd.to_numeric(series, errors="coerce") / float(scale)


def _compute_delta_from_cum(cum: pd.Series, dt: pd.Series) -> pd.Series:
    cum = _to_num(cum).fillna(0)
    d = cum.diff()
    day = dt.dt.date
    reset = day != day.shift(1)
    d[reset] = cum[reset]
    d = d.fillna(0)
    d[d < 0] = 0
    return d


def _normalize_tick_df(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    required = ["自然日", "时间", "成交价", "当日累计成交量", "当日成交额", "申买价1", "申卖价1"]
    for r in required:
        if r not in df.columns:
            raise ValueError(f"missing_column: {r}")

    dt = _parse_datetime_series(df["自然日"], df["时间"])
    ok = dt.notna()
    df = df.loc[ok].copy()
    dt = dt.loc[ok]
    df["datetime"] = dt

    scale = _infer_price_scale(df)

    out = pd.DataFrame({
        "datetime": df["datetime"],
        "last_price": _to_price(df["成交价"], scale).fillna(0),
        "cum_volume": _to_num(df["当日累计成交量"]).fillna(0),
        "cum_turnover": _to_price(df["当日成交额"], scale).fillna(0),
    })

    out["volume"] = _compute_delta_from_cum(out["cum_volume"], out["datetime"])
    out["turnover"] = _compute_delta_from_cum(out["cum_turnover"], out["datetime"])

    for field, src in [
        ("open_price", "开盘价"),
        ("high_price", "最高价"),
        ("low_price", "最低价"),
        ("pre_close", "前收盘"),
    ]:
        if src in df.columns:
            out[field] = _to_price(df[src], scale).fillna(0)

    if "成交笔数" in df.columns:
        out["trades_count"] = _to_num(df["成交笔数"]).fillna(0)
    if "BS标志" in df.columns:
        out["bs_flag"] = df["BS标志"].astype(str).fillna("")
    if "成交标志" in df.columns:
        out["trade_flag"] = df["成交标志"].astype(str).fillna("")
    if "IOPV" in df.columns:
        out["iopv"] = _to_num(df["IOPV"]).fillna(0)
    if "加权平均叫卖价" in df.columns:
        out["weighted_avg_ask_price"] = _to_price(df["加权平均叫卖价"], scale).fillna(0)
    if "加权平均叫买价" in df.columns:
        out["weighted_avg_bid_price"] = _to_price(df["加权平均叫买价"], scale).fillna(0)
    if "叫卖总量" in df.columns:
        out["total_ask_volume"] = _to_num(df["叫卖总量"]).fillna(0)
    if "叫买总量" in df.columns:
        out["total_bid_volume"] = _to_num(df["叫买总量"]).fillna(0)

    for i in range(1, 11):
        bp = f"申买价{i}"
        ap = f"申卖价{i}"
        bv = f"申买量{i}"
        av = f"申卖量{i}"
        if bp in df.columns:
            out[f"bid_price_{i}"] = _to_price(df[bp], scale).fillna(0)
        if ap in df.columns:
            out[f"ask_price_{i}"] = _to_price(df[ap], scale).fillna(0)
        if bv in df.columns:
            out[f"bid_volume_{i}"] = _to_num(df[bv]).fillna(0)
        if av in df.columns:
            out[f"ask_volume_{i}"] = _to_num(df[av]).fillna(0)

    out = out.sort_values("datetime")
    meta = {
        "price_scale": scale,
        "rows": int(len(out)),
        "start": out["datetime"].iloc[0].isoformat() if len(out) else None,
        "end": out["datetime"].iloc[-1].isoformat() if len(out) else None,
    }
    return out, meta


def _month_key(date: str) -> str:
    return f"{date[0:4]}-{date[4:6]}"


def _tick_out_path(root: Path, symbol: str, month: str, date: str) -> Path:
    return root / "dataset" / "ticks" / symbol / month / f"{date}.parquet"


def _write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="zstd")


def _process_single_archive(args: dict) -> dict:
    """Process one 7z archive — designed to run in a child process.

    *args* is a plain dict (pickle-safe) with keys:
        archive_path, date, codes, instruments, root, tmp_dir

    Returns a plain dict with per-archive results.
    """
    archive_path = Path(args["archive_path"])
    date: str = args["date"]
    codes: list[str] = args["codes"]
    instruments: dict[str, dict] = args["instruments"]
    root = Path(args["root"])
    tmp_dir = Path(args["tmp_dir"])

    result: dict = {
        "date": date,
        "written": 0,
        "skipped": 0,
        "missing_codes": [],
        "entries": [],
        "error": None,
    }

    if not archive_path.exists():
        result["error"] = f"archive not found: {archive_path}"
        return result

    month = _month_key(date)

    try:
        # --- 1. Open 7z ONCE: scan targets + extract needed CSVs ---
        with py7zr.SevenZipFile(archive_path, mode="r") as z:
            names = set(z.getnames())

            # Build targets (skip symbols whose parquet already exists)
            targets: list[str] = []
            for code in codes:
                vendor_symbols: list[str] = []
                meta = instruments.get(code) or {}
                vs = meta.get("vendor_symbols")
                if isinstance(vs, list) and vs:
                    vendor_symbols = [str(x).upper() for x in vs]
                if not vendor_symbols:
                    vendor_symbols = [f"{code}.SZ", f"{code}.SH"]
                for vsym in vendor_symbols:
                    p = f"{date}/{vsym}/行情.csv"
                    if p not in names:
                        continue
                    # Pre-check: skip if parquet already exists
                    ts_code = (meta.get("ts_code") or vsym).upper()
                    out_path = _tick_out_path(root, ts_code, month, date)
                    if out_path.exists():
                        result["skipped"] += 1
                        continue
                    targets.append(p)

            found_codes = sorted({t.split("/")[1].split(".")[0] for t in targets if "/" in t})
            result["missing_codes"] = sorted(set(codes) - set(found_codes))

            if not targets:
                return result

            # Extract only needed CSVs in one shot
            tmp_dir.mkdir(parents=True, exist_ok=True)
            z.extract(path=tmp_dir, targets=targets)

        # --- 2. Process each extracted CSV ---
        for t in targets:
            csv_path = tmp_dir / t
            if not csv_path.exists():
                continue

            parts = t.replace("\\", "/").split("/")
            if len(parts) < 3:
                continue
            vendor_sym = parts[1].upper()
            code = vendor_sym.split(".")[0]
            meta = instruments.get(code) or {}
            ts_code = (meta.get("ts_code") or vendor_sym).upper()
            name = meta.get("name")
            out_path = _tick_out_path(root, ts_code, month, date)

            # Double-check (another worker might have written it)
            if out_path.exists():
                result["skipped"] += 1
                try:
                    csv_path.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            df_raw = _load_tick_csv(csv_path)
            df, file_meta = _normalize_tick_df(df_raw)
            _write_parquet(df, out_path)
            result["written"] += 1

            result["entries"].append({
                "symbol": ts_code,
                "code": code,
                "name": name,
                "vendor_symbol": vendor_sym,
                "month": month,
                "date": date,
                "path": str(out_path.relative_to(root)).replace("\\", "/"),
                "source_archive": str(archive_path.name),
                "source_csv": t,
                "rows": file_meta["rows"],
                "start": file_meta["start"],
                "end": file_meta["end"],
                "price_scale": file_meta["price_scale"],
            })

            try:
                csv_path.unlink(missing_ok=True)
            except Exception:
                pass

    except Exception as e:
        result["error"] = str(e)

    return result


def import_from_raw(
    root: Path,
    rawdir: Path,
    universe_file: Path,
    dates: list[str] | None,
    tmp_dir: Path,
    instruments_path: Path | None,
    logger: callable = print,
    progress_callback: callable = None,
    archive_list: list[tuple[str, "Path"]] | None = None,
    max_workers: int = 1,
) -> dict:
    codes = _read_universe_codes(universe_file)
    instruments = _load_instruments(instruments_path)
    manifest = load_manifest(root)

    if archive_list:
        archives = list(archive_list)
    elif dates:
        archives = []
        for d in dates:
            archives.append((d, rawdir / f"{d}.7z"))
    else:
        archives = []
        for p in sorted(rawdir.glob("*.7z")):
            m = re.match(r"(\d{8})\.7z$", p.name)
            if m:
                archives.append((m.group(1), p))

    total_archives = len(archives)
    logger(f"Start Import: Found {total_archives} archives, workers={max_workers}")

    total_written = 0
    total_skipped = 0
    total_missing = 0
    missing_by_day: dict[str, list[str]] = {}

    start_time = time.time()

    # Build serialisable arg dicts for each archive
    task_args = []
    for date, archive_path in archives:
        task_args.append({
            "archive_path": str(archive_path),
            "date": date,
            "codes": codes,
            "instruments": instruments,
            "root": str(root),
            "tmp_dir": str(tmp_dir / date),  # per-date subdir avoids conflicts
        })

    def _handle_result(result: dict, idx: int) -> None:
        nonlocal total_written, total_skipped, total_missing
        date = result["date"]
        total_written += result["written"]
        total_skipped += result["skipped"]
        missing_by_day[date] = result.get("missing_codes", [])

        if result.get("error"):
            logger(f"  [{date}] ERROR: {result['error']}")

        if result["missing_codes"]:
            preview = ",".join(result["missing_codes"][:6])
            tail = "..." if len(result["missing_codes"]) > 6 else ""
            logger(f"  [{date}] Missing symbols: {len(result['missing_codes'])} {preview}{tail}")

        for entry in result.get("entries", []):
            upsert_file_entry(
                manifest.setdefault("tick", {}).setdefault("files", []),
                entry,
                keys=["symbol", "date"],
            )

        if not result.get("entries") and result["written"] == 0 and result["skipped"] == 0:
            total_missing += 1

        logger(f"  [{date}] Written={result['written']}, Skipped={result['skipped']}")

    if max_workers <= 1:
        # ── Serial mode (easy to debug, same behaviour as before) ──
        for idx, args in enumerate(task_args, 1):
            elapsed = time.time() - start_time
            if idx > 1:
                avg_time = elapsed / (idx - 1)
                remaining = avg_time * (total_archives - idx + 1)
                eta_str = f"{int(remaining // 60)}m{int(remaining % 60):02d}s"
            else:
                eta_str = "--:--"
            if progress_callback:
                progress_callback(idx, total_archives, f"Tick: {args['date']} (ETA: {eta_str})")
            logger(f"[{idx}/{total_archives}] Processing {args['date']}...")

            result = _process_single_archive(args)
            _handle_result(result, idx)
    else:
        # ── Parallel mode (ProcessPoolExecutor) ──
        logger(f"启动 {max_workers} 个进程并行导入...")
        completed_count = 0
        # Map future → (idx, date) for progress tracking
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_map = {}
            for idx, args in enumerate(task_args, 1):
                future = executor.submit(_process_single_archive, args)
                future_map[future] = (idx, args["date"])

            for future in as_completed(future_map):
                completed_count += 1
                idx, date = future_map[future]
                elapsed = time.time() - start_time
                if completed_count > 0:
                    avg_time = elapsed / completed_count
                    remaining = avg_time * (total_archives - completed_count)
                    eta_str = f"{int(remaining // 60)}m{int(remaining % 60):02d}s"
                else:
                    eta_str = "--:--"

                if progress_callback:
                    progress_callback(
                        completed_count, total_archives,
                        f"Tick: {date} (ETA: {eta_str}) [{completed_count}/{total_archives}]"
                    )
                try:
                    result = future.result()
                    _handle_result(result, idx)
                except Exception as e:
                    logger(f"  [{date}] Process error: {e}")

    # Save manifest once at the end
    save_manifest(root, manifest)

    logger(
        f"Import Finished: Written={total_written} Skipped={total_skipped} "
        f"MissingDays={total_missing} Workers={max_workers}"
    )

    return {
        "written": total_written,
        "skipped": total_skipped,
        "missing_days": total_missing,
        "archives": len(archives),
        "missing_by_day": missing_by_day,
    }


def export_csv(root: Path, symbol: str, start: str, end: str, columns: list[str] | None, out_csv: Path) -> None:
    sym = symbol.upper()
    start_dt = TZ.localize(datetime.strptime(start, "%Y-%m-%d"))
    end_dt = TZ.localize(datetime.strptime(end, "%Y-%m-%d"))

    months = sorted({start_dt.strftime("%Y-%m"), end_dt.strftime("%Y-%m")})
    if start_dt.strftime("%Y-%m") != end_dt.strftime("%Y-%m"):
        cur = datetime(start_dt.year, start_dt.month, 1)
        while cur <= datetime(end_dt.year, end_dt.month, 1):
            months.append(cur.strftime("%Y-%m"))
            if cur.month == 12:
                cur = datetime(cur.year + 1, 1, 1)
            else:
                cur = datetime(cur.year, cur.month + 1, 1)
        months = sorted(set(months))

    dfs = []
    for m in months:
        month_dir = root / "dataset" / "ticks" / sym / m
        if not month_dir.exists():
            continue
        for p in sorted(month_dir.glob("*.parquet")):
            df = pd.read_parquet(p, columns=columns)
            dfs.append(df)

    if not dfs:
        raise SystemExit("no_data")
    df_all = pd.concat(dfs, ignore_index=True)
    df_all["datetime"] = pd.to_datetime(df_all["datetime"], errors="coerce")
    df_all = df_all[(df_all["datetime"] >= start_dt) & (df_all["datetime"] < end_dt)].sort_values("datetime")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(out_csv, index=False)
    print(str(out_csv))


def main() -> None:
    parser = argparse.ArgumentParser(prog="TickParquetManager")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_import = sub.add_parser("import")
    p_import.add_argument("--rawdir", default=str(Path(__file__).resolve().parents[1] / "rawData"))
    p_import.add_argument("--symbols-file", required=True)
    p_import.add_argument("--dates", nargs="*")
    p_import.add_argument("--tmp-dir", default=str(Path(__file__).resolve().parents[1] / "rawData" / "_tmp_extract"))
    p_import.add_argument("--instruments", default=None)
    p_import.add_argument("--workers", type=int, default=1, help="并行进程数 (1-10, 默认1=串行)")

    p_export = sub.add_parser("export")
    p_export.add_argument("--symbol", required=True)
    p_export.add_argument("--start", required=True)
    p_export.add_argument("--end", required=True)
    p_export.add_argument("--columns", nargs="*")
    p_export.add_argument("--out", required=True)

    args = parser.parse_args()
    root = Path(args.root)

    if args.cmd == "import":
        import_from_raw(
            root=root,
            rawdir=Path(args.rawdir),
            universe_file=Path(args.symbols_file),
            dates=args.dates,
            tmp_dir=Path(args.tmp_dir),
            instruments_path=Path(args.instruments) if args.instruments else None,
            max_workers=max(1, min(10, args.workers)),
        )
    elif args.cmd == "export":
        export_csv(
            root=root,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            columns=args.columns,
            out_csv=Path(args.out),
        )


if __name__ == "__main__":
    main()

