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


def _safe_read_sample(file_path: Path, probe: CsvProbe, nrows: int) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        encoding=probe.encoding,
        sep=probe.delimiter,
        engine="python",
        header=0 if probe.has_header else None,
        names=None if probe.has_header else probe.header,
        nrows=nrows,
        on_bad_lines="skip",
    )
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _infer_file_type(columns: list[str]) -> str:
    cols = {c.lower() for c in columns}
    if any(c.startswith("bid_price_") for c in cols) or "bid_price_1" in cols or "ask_price_1" in cols:
        return "tick"
    if any("申买价1" in c or "申卖价1" in c or "申买量1" in c or "申卖量1" in c for c in columns):
        return "tick"
    if any("trade" in c for c in cols) or any(c in cols for c in ["tradeid", "trade_id", "成交", "成交编号"]):
        return "trade"
    if any(c in cols for c in ["orderid", "order_id", "委托号", "委托编号"]):
        return "order"
    return "unknown"


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _parse_datetime(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.strip()
    s = s.replace({"nan": ""})
    parsed = pd.to_datetime(s, errors="coerce")
    if parsed.notna().any():
        return parsed

    digits = s.str.replace(r"\D", "", regex=True)
    candidates = [
        (digits.str.slice(0, 14), "%Y%m%d%H%M%S"),
        (digits.str.slice(0, 12), "%Y%m%d%H%M"),
        (digits.str.slice(0, 8), "%Y%m%d"),
    ]
    best = pd.Series([pd.NaT] * len(s))
    for part, fmt in candidates:
        try:
            dt = pd.to_datetime(part, format=fmt, errors="coerce")
        except Exception:
            continue
        if dt.notna().sum() > best.notna().sum():
            best = dt
    return best


def _parse_datetime_from_day_time(day: pd.Series, t: pd.Series) -> pd.Series:
    day_digits = day.astype(str).str.replace(r"\D", "", regex=True)
    t_digits = t.astype(str).str.replace(r"\D", "", regex=True)
    t_padded = t_digits.fillna("").str.zfill(9)
    hhmmss = t_padded.str.slice(0, 6)
    combined = (day_digits.fillna("") + hhmmss).str.slice(0, 14)
    return pd.to_datetime(combined, format="%Y%m%d%H%M%S", errors="coerce")


def _time_monotonic_ratio(dt: pd.Series) -> float:
    valid = dt.dropna()
    if len(valid) < 2:
        return 1.0
    diffs = valid.diff().dropna()
    non_negative = (diffs >= pd.Timedelta(0)).sum()
    return float(non_negative) / float(len(diffs))


def analyze_tick(df: pd.DataFrame) -> dict:
    cols = list(df.columns)
    info: dict = {}
    info["columns"] = cols

    time_col = None
    for cand in ["datetime", "time", "timestamp", "ts", "交易时间", "时间"]:
        if cand in df.columns:
            time_col = cand
            break
    if time_col is None:
        time_col = df.columns[0] if df.columns.size else None

    if time_col is not None:
        if "自然日" in df.columns and time_col == "时间":
            dt = _parse_datetime_from_day_time(df["自然日"], df[time_col])
        else:
            dt = _parse_datetime(df[time_col])
        info["time_col"] = time_col
        info["time_parse_rate"] = float(dt.notna().mean())
        info["time_monotonic_ratio"] = _time_monotonic_ratio(dt)
    else:
        info["time_col"] = None

    bid1 = None
    ask1 = None
    for cand in ["bid_price_1", "BidPrice1", "bid1", "买一价", "申买价1"]:
        if cand in df.columns:
            bid1 = cand
            break
    for cand in ["ask_price_1", "AskPrice1", "ask1", "卖一价", "申卖价1"]:
        if cand in df.columns:
            ask1 = cand
            break
    if bid1 and ask1:
        bid = _to_numeric(df[bid1])
        ask = _to_numeric(df[ask1])
        valid = bid.notna() & ask.notna()
        if valid.any():
            info["bid1_ask1_valid_rate"] = float(valid.mean())
            info["bid_le_ask_rate"] = float((bid[valid] <= ask[valid]).mean())
            info["spread_zero_rate"] = float((bid[valid] == ask[valid]).mean())
            info["spread_negative_rate"] = float((bid[valid] > ask[valid]).mean())
            info["spread_min"] = float((ask[valid] - bid[valid]).min())
            info["spread_max"] = float((ask[valid] - bid[valid]).max())
            a_valid = ask[valid & (ask > 0)]
            if len(a_valid) > 0:
                median = float(a_valid.median())
                info["price_median"] = median
                scale_hint = None
                if median > 10000:
                    for s in [10000, 1000, 100]:
                        scaled = median / s
                        if 0.5 <= scaled <= 5000:
                            scale_hint = s
                            break
                info["price_scale_hint"] = scale_hint
    else:
        info["bid1_ask1_valid_rate"] = 0.0

    last_price_col = None
    for cand in ["last_price", "LastPrice", "price", "成交价", "最新价"]:
        if cand in df.columns:
            last_price_col = cand
            break
    if last_price_col and bid1 and ask1:
        lastp = _to_numeric(df[last_price_col])
        bid = _to_numeric(df[bid1])
        ask = _to_numeric(df[ask1])
        valid = lastp.notna() & bid.notna() & ask.notna() & (bid <= ask)
        if valid.any():
            inside = (lastp[valid] >= bid[valid]) & (lastp[valid] <= ask[valid])
            info["last_price_inside_spread_rate"] = float(inside.mean())

    levels = []
    cn_levels = []
    for i in range(1, 11):
        cn_bp = f"申买价{i}"
        cn_ap = f"申卖价{i}"
        cn_bv = f"申买量{i}"
        cn_av = f"申卖量{i}"
        if cn_bp in df.columns or cn_ap in df.columns or cn_bv in df.columns or cn_av in df.columns:
            cn_levels.append(i)
    for i in range(1, 6):
        bp = f"bid_price_{i}"
        ap = f"ask_price_{i}"
        bv = f"bid_volume_{i}"
        av = f"ask_volume_{i}"
        if bp in df.columns or ap in df.columns or bv in df.columns or av in df.columns:
            levels.append(i)
    info["book_levels_detected"] = {"en": levels, "cn": cn_levels}

    bid_prices = []
    ask_prices = []
    if cn_levels:
        bid_prices = [f"申买价{i}" for i in cn_levels if f"申买价{i}" in df.columns]
        ask_prices = [f"申卖价{i}" for i in cn_levels if f"申卖价{i}" in df.columns]
    elif levels:
        bid_prices = [c for c in [f"bid_price_{i}" for i in levels] if c in df.columns]
        ask_prices = [c for c in [f"ask_price_{i}" for i in levels] if c in df.columns]

    if bid_prices:
        bmat = df[bid_prices].apply(pd.to_numeric, errors="coerce")
        if bmat.shape[1] >= 2:
            d = bmat.diff(axis=1).iloc[:, 1:]
            ok = (d <= 0).all(axis=1)
            info["bid_prices_nonincreasing_rate"] = float(ok.mean())
    if ask_prices:
        amat = df[ask_prices].apply(pd.to_numeric, errors="coerce")
        if amat.shape[1] >= 2:
            d = amat.diff(axis=1).iloc[:, 1:]
            ok = (d >= 0).all(axis=1)
            info["ask_prices_nondecreasing_rate"] = float(ok.mean())

    return info


def analyze_trade(df: pd.DataFrame) -> dict:
    cols = list(df.columns)
    info: dict = {"columns": cols}

    time_col = None
    for cand in ["datetime", "time", "timestamp", "ts", "成交时间", "时间"]:
        if cand in df.columns:
            time_col = cand
            break
    if time_col is None:
        time_col = df.columns[0] if df.columns.size else None
    if time_col is not None:
        if "自然日" in df.columns and time_col == "时间":
            dt = _parse_datetime_from_day_time(df["自然日"], df[time_col])
        else:
            dt = _parse_datetime(df[time_col])
        info["time_col"] = time_col
        info["time_parse_rate"] = float(dt.notna().mean())
        info["time_monotonic_ratio"] = _time_monotonic_ratio(dt)
    else:
        info["time_col"] = None

    price_col = None
    for cand in ["price", "成交价", "trade_price", "last_price", "成交价格"]:
        if cand in df.columns:
            price_col = cand
            break
    vol_col = None
    for cand in ["volume", "成交量", "qty", "trade_volume", "成交数量"]:
        if cand in df.columns:
            vol_col = cand
            break

    if price_col:
        p = _to_numeric(df[price_col])
        info["price_positive_rate"] = float((p > 0).mean())
        info["price_min"] = float(p.min()) if p.notna().any() else None
        info["price_max"] = float(p.max()) if p.notna().any() else None
        p_valid = p[p > 0]
        if len(p_valid) > 0:
            median = float(p_valid.median())
            info["price_median"] = median
            scale_hint = None
            if median > 10000:
                for s in [10000, 1000, 100]:
                    scaled = median / s
                    if 0.5 <= scaled <= 5000:
                        scale_hint = s
                        break
            info["price_scale_hint"] = scale_hint

    trade_code_col = None
    for cand in ["成交代码", "trade_code", "type"]:
        if cand in df.columns:
            trade_code_col = cand
            break
    if trade_code_col:
        vc = df[trade_code_col].astype(str).str.strip().value_counts().head(10)
        info["trade_code_col"] = trade_code_col
        info["trade_code_top_values"] = {str(k): int(v) for k, v in vc.items()}

    valid_trade = None
    if price_col and vol_col:
        p = _to_numeric(df[price_col])
        v = _to_numeric(df[vol_col])
        valid_trade = (p > 0) & (v > 0)
        if trade_code_col:
            code = df[trade_code_col].astype(str).str.strip()
            valid_trade = valid_trade & (code != "C")
        info["valid_trade_rate"] = float(valid_trade.mean())
    if vol_col:
        v = _to_numeric(df[vol_col])
        info["volume_positive_rate"] = float((v > 0).mean())
        info["volume_min"] = float(v.min()) if v.notna().any() else None
        info["volume_max"] = float(v.max()) if v.notna().any() else None

    side_col = None
    for cand in ["side", "bsflag", "direction", "买卖方向", "成交方向", "BS标志"]:
        if cand in df.columns:
            side_col = cand
            break
    info["side_col"] = side_col
    if side_col:
        vc = df[side_col].astype(str).str.strip().value_counts().head(10)
        info["side_top_values"] = {str(k): int(v) for k, v in vc.items()}

    order_id_cols = [c for c in df.columns if re.search(r"order|委托|bid_id|ask_id|buyer|seller", str(c), re.I)]
    info["order_id_like_cols"] = order_id_cols[:20]

    return info


def analyze_order(df: pd.DataFrame) -> dict:
    cols = list(df.columns)
    info: dict = {"columns": cols}

    time_col = None
    for cand in ["datetime", "time", "timestamp", "ts", "委托时间", "时间"]:
        if cand in df.columns:
            time_col = cand
            break
    if time_col is None:
        time_col = df.columns[0] if df.columns.size else None

    if time_col is not None:
        if "自然日" in df.columns and time_col == "时间":
            dt = _parse_datetime_from_day_time(df["自然日"], df[time_col])
        else:
            dt = _parse_datetime(df[time_col])
        info["time_col"] = time_col
        info["time_parse_rate"] = float(dt.notna().mean())
        info["time_monotonic_ratio"] = _time_monotonic_ratio(dt)
    else:
        info["time_col"] = None

    price_col = None
    for cand in ["委托价格", "price", "order_price"]:
        if cand in df.columns:
            price_col = cand
            break
    vol_col = None
    for cand in ["委托数量", "volume", "qty", "order_volume"]:
        if cand in df.columns:
            vol_col = cand
            break

    if price_col:
        p = _to_numeric(df[price_col])
        info["price_positive_rate"] = float((p > 0).mean())
        p_valid = p[p > 0]
        if len(p_valid) > 0:
            median = float(p_valid.median())
            info["price_median"] = median
            scale_hint = None
            if median > 10000:
                for s in [10000, 1000, 100]:
                    scaled = median / s
                    if 0.5 <= scaled <= 5000:
                        scale_hint = s
                        break
            info["price_scale_hint"] = scale_hint

    if vol_col:
        v = _to_numeric(df[vol_col])
        info["volume_positive_rate"] = float((v > 0).mean())

    type_col = None
    for cand in ["委托类型", "order_type", "type"]:
        if cand in df.columns:
            type_col = cand
            break
    info["type_col"] = type_col
    if type_col:
        vc = df[type_col].astype(str).str.strip().value_counts().head(15)
        info["type_top_values"] = {str(k): int(v) for k, v in vc.items()}

    code_col = None
    for cand in ["委托代码", "code", "side"]:
        if cand in df.columns:
            code_col = cand
            break
    info["code_col"] = code_col
    if code_col:
        vc = df[code_col].astype(str).str.strip().value_counts().head(15)
        info["code_top_values"] = {str(k): int(v) for k, v in vc.items()}

    id_cols = [c for c in df.columns if re.search(r"编号|委托号|orderid|order_id", str(c), re.I)]
    info["id_cols"] = id_cols
    return info


def analyze_csv(file_path: Path, sample_rows: int) -> dict:
    probe = probe_csv(file_path)
    df = _safe_read_sample(file_path, probe, sample_rows)
    file_type = _infer_file_type(list(df.columns))

    base = {
        "path": str(file_path),
        "size_bytes": file_path.stat().st_size,
        "encoding": probe.encoding,
        "delimiter": probe.delimiter,
        "has_header": probe.has_header,
        "header": list(df.columns),
        "sample_rows": int(len(df)),
        "type": file_type,
    }

    if file_type == "tick":
        base["analysis"] = analyze_tick(df)
    elif file_type == "trade":
        base["analysis"] = analyze_trade(df)
    elif file_type == "order":
        base["analysis"] = analyze_order(df)
    else:
        base["analysis"] = {"columns": list(df.columns)}
    return base


def extract_7z(archive_path: Path, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        names = z.getnames()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        sym_dirs = sorted({n.split("/")[1] for n in csv_names if n.count("/") >= 2})
        chosen: list[str] = []
        for sym in sym_dirs:
            prefix = f"{names[0]}/{sym}/" if names and "/" not in names[0] else f"{archive_path.stem}/{sym}/"
            cand_tick = [n for n in csv_names if n.endswith(f"/{sym}/行情.csv")]
            cand_trade = [n for n in csv_names if n.endswith(f"/{sym}/逐笔成交.csv")]
            cand_order = [n for n in csv_names if n.endswith(f"/{sym}/逐笔委托.csv")]
            if cand_tick and cand_trade:
                chosen.extend(cand_tick[:1])
                chosen.extend(cand_trade[:1])
                chosen.extend(cand_order[:1])
                break

        if not chosen:
            tick_re = re.compile(r"(tick|quote|snap|level|book|orderbook|盘口|行情)", re.I)
            order_re = re.compile(r"(order|委托|逐笔委托)", re.I)
            trade_re = re.compile(r"(trade|成交|trans|transaction)", re.I)
            tick_candidates = [n for n in csv_names if tick_re.search(n)]
            trade_candidates = [n for n in csv_names if trade_re.search(n)]
            order_candidates = [n for n in csv_names if order_re.search(n)]
            chosen.extend(tick_candidates[:1])
            chosen.extend(trade_candidates[:1])
            chosen.extend(order_candidates[:1])
            chosen = [c for i, c in enumerate(chosen) if c and c not in chosen[:i]]

        targets = chosen[:4] if chosen else csv_names[:4]
        if targets:
            z.extract(path=out_dir, targets=targets)
        else:
            z.extractall(path=out_dir)

    return [p for p in out_dir.rglob("*") if p.is_file()]


def write_markdown(report: dict, md_path: Path) -> None:
    lines: list[str] = []
    lines.append(f"# RawData 数据概览 - {report['date']}\n")
    lines.append(f"归档文件：`{report['archive']}`\n")
    lines.append(f"解压目录：`{report['extract_dir']}`\n")
    lines.append("## 文件清单\n")
    for f in report["files"]:
        lines.append(f"- `{f['rel_path']}` ({f['size_bytes']:,} bytes)")
    lines.append("\n## CSV 分析\n")

    for r in report["csv_reports"]:
        rel = r["rel_path"]
        lines.append(f"### {rel}\n")
        lines.append(f"- 类型：`{r['type']}`")
        lines.append(f"- 编码：`{r['encoding']}`；分隔符：`{r['delimiter']}`；有表头：`{r['has_header']}`")
        lines.append(f"- 文件大小：{r['size_bytes']:,} bytes；抽样行数：{r['sample_rows']}")
        lines.append("- 字段：")
        lines.append("  - " + ", ".join([f"`{c}`" for c in r["header"][:60]]) + (" ..." if len(r["header"]) > 60 else ""))

        a = r.get("analysis") or {}
        if r["type"] == "tick":
            lines.append("- 关键检查：")
            for k in [
                "time_col",
                "time_parse_rate",
                "time_monotonic_ratio",
                "bid1_ask1_valid_rate",
                "bid_le_ask_rate",
                "spread_negative_rate",
                "spread_zero_rate",
                "spread_min",
                "spread_max",
                "price_median",
                "price_scale_hint",
                "book_levels_detected",
                "bid_prices_nonincreasing_rate",
                "ask_prices_nondecreasing_rate",
                "last_price_inside_spread_rate",
            ]:
                if k in a:
                    lines.append(f"  - `{k}`: {a[k]}")
        elif r["type"] == "trade":
            lines.append("- 关键检查：")
            for k in [
                "time_col",
                "time_parse_rate",
                "time_monotonic_ratio",
                "price_positive_rate",
                "price_min",
                "price_max",
                "price_median",
                "price_scale_hint",
                "volume_positive_rate",
                "volume_min",
                "volume_max",
                "valid_trade_rate",
                "trade_code_col",
                "side_col",
            ]:
                if k in a:
                    lines.append(f"  - `{k}`: {a[k]}")
            if a.get("trade_code_top_values"):
                lines.append("  - `trade_code_top_values`: ")
                for kk, vv in a["trade_code_top_values"].items():
                    lines.append(f"    - `{kk}`: {vv}")
            if a.get("side_top_values"):
                lines.append("  - `side_top_values`: ")
                for kk, vv in a["side_top_values"].items():
                    lines.append(f"    - `{kk}`: {vv}")
            if a.get("order_id_like_cols"):
                lines.append("  - `order_id_like_cols`: " + ", ".join([f"`{c}`" for c in a["order_id_like_cols"]]))
        lines.append("")

        if r["type"] == "order":
            lines.append("- 关键检查：")
            for k in [
                "time_col",
                "time_parse_rate",
                "time_monotonic_ratio",
                "price_positive_rate",
                "price_median",
                "price_scale_hint",
                "volume_positive_rate",
                "type_col",
                "code_col",
            ]:
                if k in a:
                    lines.append(f"  - `{k}`: {a[k]}")
            if a.get("type_top_values"):
                lines.append("  - `type_top_values`: ")
                for kk, vv in a["type_top_values"].items():
                    lines.append(f"    - `{kk}`: {vv}")
            if a.get("code_top_values"):
                lines.append("  - `code_top_values`: ")
                for kk, vv in a["code_top_values"].items():
                    lines.append(f"    - `{kk}`: {vv}")
            if a.get("id_cols"):
                lines.append("  - `id_cols`: " + ", ".join([f"`{c}`" for c in a["id_cols"]]))
            lines.append("")

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rawdir", default=str(Path(__file__).resolve().parents[1] / "rawData"))
    parser.add_argument("--date", default="20250107")
    parser.add_argument("--sample-rows", type=int, default=5000)
    args = parser.parse_args()

    rawdir = Path(args.rawdir)
    archive = rawdir / f"{args.date}.7z"
    if not archive.exists():
        raise SystemExit(f"archive_not_found: {archive}")

    extract_dir = rawdir / "_extracted" / args.date
    print(f"extracting: {archive} -> {extract_dir}")
    extracted_files = extract_7z(archive, extract_dir)
    print(f"extracted_files: {len(extracted_files)}")

    files = []
    for p in sorted(extracted_files):
        rel = str(p.relative_to(extract_dir)).replace("\\", "/")
        files.append({"rel_path": rel, "size_bytes": p.stat().st_size})

    csv_reports = []
    for p in extracted_files:
        if p.suffix.lower() != ".csv":
            continue
        r = analyze_csv(p, args.sample_rows)
        r["rel_path"] = str(p.relative_to(extract_dir)).replace("\\", "/")
        csv_reports.append(r)

    report = {
        "date": args.date,
        "archive": str(archive),
        "extract_dir": str(extract_dir),
        "files": files,
        "csv_reports": csv_reports,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    report_dir = rawdir / "_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"{args.date}_report.json"
    md_path = report_dir / f"{args.date}_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(report, md_path)
    print(str(md_path))


if __name__ == "__main__":
    main()
