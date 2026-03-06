import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import chardet
import pandas as pd
import py7zr
import pytz

from vnpy.trader.constant import Exchange
from vnpy.trader.database import get_database
from vnpy.trader.object import TickData


PRICE_SCALE = 10000
TZ = pytz.timezone("Asia/Shanghai")


@dataclass
class CsvProbe:
    encoding: str
    delimiter: str
    has_header: bool
    header: list[str]


def _read_symbols(symbols_file: Path) -> list[str]:
    raw = symbols_file.read_text(encoding="utf-8").splitlines()
    symbols = []
    for line in raw:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        symbols.append(s)
    return sorted(set(symbols))


def _detect_encoding(file_path: Path, max_bytes: int = 1024 * 128) -> str:
    with file_path.open("rb") as f:
        raw = f.read(max_bytes)
    guess = chardet.detect(raw) or {}
    enc = guess.get("encoding") or "utf-8"
    return enc


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


def _symbol_exchange(symbol: str) -> tuple[str, Exchange]:
    s = symbol.strip().upper()
    if s.endswith(".SZ"):
        return s.split(".")[0], Exchange.SZSE
    if s.endswith(".SH"):
        return s.split(".")[0], Exchange.SSE
    if s.endswith(".SZSE"):
        return s.split(".")[0], Exchange.SZSE
    if s.endswith(".SSE"):
        return s.split(".")[0], Exchange.SSE
    if re.fullmatch(r"\d{6}", s):
        return s, Exchange.SZSE
    raise ValueError(f"unsupported_symbol: {symbol}")


def _price(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce") / PRICE_SCALE


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _extract_targets_from_archive(archive_path: Path, date: str, symbols: list[str]) -> list[str]:
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        names = z.getnames()
    targets = []
    for sym in symbols:
        code, _ = _symbol_exchange(sym)
        if sym.upper().endswith(".SH"):
            folder = f"{code}.SH"
        elif sym.upper().endswith(".SSE"):
            folder = f"{code}.SH"
        else:
            folder = f"{code}.SZ"
        p = f"{date}/{folder}/行情.csv"
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


def _convert_tick_df_to_objects(df: pd.DataFrame, symbol: str, exchange: Exchange) -> list[TickData]:
    required = ["自然日", "时间", "成交价", "当日累计成交量", "当日成交额"]
    for r in required:
        if r not in df.columns:
            raise ValueError(f"missing_column: {r}")

    dt = _parse_datetime_series(df["自然日"], df["时间"])
    last_price = _price(df["成交价"]).fillna(0)
    volume = _num(df["当日累计成交量"]).fillna(0)
    turnover = _price(df["当日成交额"]).fillna(0)

    open_price = _price(df["开盘价"]).fillna(0) if "开盘价" in df.columns else None
    high_price = _price(df["最高价"]).fillna(0) if "最高价" in df.columns else None
    low_price = _price(df["最低价"]).fillna(0) if "最低价" in df.columns else None
    pre_close = _price(df["前收盘"]).fillna(0) if "前收盘" in df.columns else None

    bid_p = [_price(df.get(f"申买价{i}", pd.Series(dtype=float))).fillna(0) for i in range(1, 6)]
    ask_p = [_price(df.get(f"申卖价{i}", pd.Series(dtype=float))).fillna(0) for i in range(1, 6)]
    bid_v = [_num(df.get(f"申买量{i}", pd.Series(dtype=float))).fillna(0) for i in range(1, 6)]
    ask_v = [_num(df.get(f"申卖量{i}", pd.Series(dtype=float))).fillna(0) for i in range(1, 6)]

    ticks: list[TickData] = []
    for i in range(len(df)):
        dti = dt.iloc[i]
        if pd.isna(dti):
            continue
        tick = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=TZ.localize(dti.to_pydatetime()) if dti.tzinfo is None else dti.to_pydatetime(),
            volume=float(volume.iloc[i]) if not pd.isna(volume.iloc[i]) else 0,
            turnover=float(turnover.iloc[i]) if not pd.isna(turnover.iloc[i]) else 0,
            last_price=float(last_price.iloc[i]) if not pd.isna(last_price.iloc[i]) else 0,
            gateway_name="DB",
        )
        if open_price is not None:
            tick.open_price = float(open_price.iloc[i])
        if high_price is not None:
            tick.high_price = float(high_price.iloc[i])
        if low_price is not None:
            tick.low_price = float(low_price.iloc[i])
        if pre_close is not None:
            tick.pre_close = float(pre_close.iloc[i])

        tick.bid_price_1 = float(bid_p[0].iloc[i])
        tick.bid_price_2 = float(bid_p[1].iloc[i])
        tick.bid_price_3 = float(bid_p[2].iloc[i])
        tick.bid_price_4 = float(bid_p[3].iloc[i])
        tick.bid_price_5 = float(bid_p[4].iloc[i])

        tick.ask_price_1 = float(ask_p[0].iloc[i])
        tick.ask_price_2 = float(ask_p[1].iloc[i])
        tick.ask_price_3 = float(ask_p[2].iloc[i])
        tick.ask_price_4 = float(ask_p[3].iloc[i])
        tick.ask_price_5 = float(ask_p[4].iloc[i])

        tick.bid_volume_1 = float(bid_v[0].iloc[i])
        tick.bid_volume_2 = float(bid_v[1].iloc[i])
        tick.bid_volume_3 = float(bid_v[2].iloc[i])
        tick.bid_volume_4 = float(bid_v[3].iloc[i])
        tick.bid_volume_5 = float(bid_v[4].iloc[i])

        tick.ask_volume_1 = float(ask_v[0].iloc[i])
        tick.ask_volume_2 = float(ask_v[1].iloc[i])
        tick.ask_volume_3 = float(ask_v[2].iloc[i])
        tick.ask_volume_4 = float(ask_v[3].iloc[i])
        tick.ask_volume_5 = float(ask_v[4].iloc[i])

        ticks.append(tick)
    return ticks


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


def _state_path(rawdir: Path) -> Path:
    return rawdir / "_ingest_state.json"


def _load_state(rawdir: Path) -> dict:
    p = _state_path(rawdir)
    if not p.exists():
        return {"ingested": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"ingested": {}}


def _save_state(rawdir: Path, state: dict) -> None:
    p = _state_path(rawdir)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def import_ticks(rawdir: Path, symbols_file: Path, dates: list[str] | None, temp_dir: Path) -> None:
    symbols = _read_symbols(symbols_file)
    state = _load_state(rawdir)
    ingested = state.setdefault("ingested", {})

    archives = []
    if dates:
        for d in dates:
            archives.append((d, rawdir / f"{d}.7z"))
    else:
        for p in sorted(rawdir.glob("*.7z")):
            m = re.match(r"(\d{8})\.7z$", p.name)
            if m:
                archives.append((m.group(1), p))

    db = get_database()

    for date, archive_path in archives:
        if not archive_path.exists():
            continue
        targets = _extract_targets_from_archive(archive_path, date, symbols)
        if not targets:
            continue

        out_dir = temp_dir
        extracted = _extract_csvs(archive_path, out_dir, targets)

        for csv_path in extracted:
            rel = str(csv_path.relative_to(out_dir)).replace("\\", "/")
            sym_folder = rel.split("/")[1]
            key = f"{date}:{sym_folder}"
            if ingested.get(key):
                continue

            df = _load_tick_csv(csv_path)
            if "万得代码" in df.columns:
                sym = str(df["万得代码"].iloc[0]).strip()
            else:
                sym = sym_folder

            if sym.endswith(".SZ"):
                symbol, exchange = sym.split(".")[0], Exchange.SZSE
            elif sym.endswith(".SH"):
                symbol, exchange = sym.split(".")[0], Exchange.SSE
            else:
                symbol, exchange = _symbol_exchange(sym)

            ticks = _convert_tick_df_to_objects(df, symbol, exchange)
            if ticks:
                ok = db.save_tick_data(ticks, stream=True)
                if not ok:
                    raise RuntimeError(f"save_tick_failed: {key}")
            ingested[key] = {
                "csv": rel,
                "count": len(ticks),
            }
            _save_state(rawdir, state)

        for p in extracted:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


def show_overview(symbol: str) -> None:
    code, exchange = _symbol_exchange(symbol)
    db = get_database()
    for row in db.get_tick_overview():
        if row.symbol == code and row.exchange == exchange:
            print(row)
            return
    print("not_found")


def export_ticks(rawdir: Path, symbol: str, start: str, end: str, out_csv: Path) -> None:
    code, exchange = _symbol_exchange(symbol)
    start_dt = TZ.localize(datetime.strptime(start, "%Y-%m-%d"))
    end_dt = TZ.localize(datetime.strptime(end, "%Y-%m-%d"))
    db = get_database()
    ticks = db.load_tick_data(code, exchange, start_dt, end_dt)
    rows = []
    for t in ticks:
        rows.append(
            {
                "datetime": t.datetime.isoformat(),
                "last_price": t.last_price,
                "volume": t.volume,
                "turnover": t.turnover,
                "bid_price_1": t.bid_price_1,
                "bid_volume_1": t.bid_volume_1,
                "ask_price_1": t.ask_price_1,
                "ask_volume_1": t.ask_volume_1,
                "bid_price_2": t.bid_price_2,
                "bid_volume_2": t.bid_volume_2,
                "ask_price_2": t.ask_price_2,
                "ask_volume_2": t.ask_volume_2,
                "bid_price_3": t.bid_price_3,
                "bid_volume_3": t.bid_volume_3,
                "ask_price_3": t.ask_price_3,
                "ask_volume_3": t.ask_volume_3,
                "bid_price_4": t.bid_price_4,
                "bid_volume_4": t.bid_volume_4,
                "ask_price_4": t.ask_price_4,
                "ask_volume_4": t.ask_volume_4,
                "bid_price_5": t.bid_price_5,
                "bid_volume_5": t.bid_volume_5,
                "ask_price_5": t.ask_price_5,
                "ask_volume_5": t.ask_volume_5,
            }
        )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print(str(out_csv))


def main() -> None:
    parser = argparse.ArgumentParser(prog="TickDataManager")
    parser.add_argument("--rawdir", default=str(Path(__file__).resolve().parents[1] / "rawData"))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_import = sub.add_parser("import")
    p_import.add_argument("--symbols-file", required=True)
    p_import.add_argument("--dates", nargs="*")
    p_import.add_argument("--temp-dir", default=str(Path(__file__).resolve().parents[1] / "rawData" / "_tmp_extract"))

    p_overview = sub.add_parser("overview")
    p_overview.add_argument("--symbol", required=True)

    p_export = sub.add_parser("export")
    p_export.add_argument("--symbol", required=True)
    p_export.add_argument("--start", required=True)
    p_export.add_argument("--end", required=True)
    p_export.add_argument("--out", required=True)

    args = parser.parse_args()
    rawdir = Path(args.rawdir)

    if args.cmd == "import":
        import_ticks(
            rawdir=rawdir,
            symbols_file=Path(args.symbols_file),
            dates=args.dates,
            temp_dir=Path(args.temp_dir),
        )
    elif args.cmd == "overview":
        show_overview(args.symbol)
    elif args.cmd == "export":
        export_ticks(rawdir, args.symbol, args.start, args.end, Path(args.out))


if __name__ == "__main__":
    main()

