import argparse
import json
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tools.dataset_manifest import load_manifest, save_manifest
from tools.universe import read_universe


def _load_instruments(path: Path) -> dict[str, dict]:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
        rows = df.to_dict(orient="records")
    else:
        rows = json.loads(path.read_text(encoding="utf-8"))

    out: dict[str, dict] = {}
    for r in rows:
        code = str(r.get("code") or "").strip()
        if len(code) != 6:
            continue
        out[code] = r
    return out


def _vendor_symbol_from_source_csv(source_csv: str | None) -> str | None:
    if not source_csv:
        return None
    s = source_csv.replace("\\", "/")
    parts = s.split("/")
    if len(parts) >= 2 and "." in parts[1]:
        return parts[1].upper()
    return None


def rebuild_manifest(root: Path, instruments_path: Path) -> Path:
    if not instruments_path.is_absolute():
        instruments_path = (root / instruments_path).resolve()
    instruments = _load_instruments(instruments_path)
    manifest = load_manifest(root)
    manifest["version"] = "dataset_v0.2"
    manifest.setdefault("created_at", datetime.now().isoformat(timespec="seconds"))
    manifest["instruments"] = {
        "path": str(instruments_path.relative_to(root)).replace("\\", "/"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }

    tick_files = manifest.setdefault("tick", {}).setdefault("files", [])
    new_tick_files: list[dict] = []
    for e in tick_files:
        symbol = str(e.get("symbol") or "").upper()
        code = symbol.split(".")[0] if symbol else None
        if not code or len(code) != 6:
            new_tick_files.append(e)
            continue

        meta = instruments.get(code) or {}
        ts_code = str(meta.get("ts_code") or symbol).upper()
        name = meta.get("name")
        month = e.get("month")
        date = e.get("date")

        vendor_symbol = e.get("vendor_symbol")
        if not vendor_symbol:
            vendor_symbol = _vendor_symbol_from_source_csv(e.get("source_csv"))

        path = e.get("path")
        if ts_code and month and date:
            path = f"dataset/ticks/{ts_code}/{month}/{date}.parquet"

        new_tick_files.append(
            {
                **e,
                "symbol": ts_code,
                "code": code,
                "name": name,
                "vendor_symbol": vendor_symbol,
                "path": path,
            }
        )

    manifest["tick"]["files"] = new_tick_files

    universe_path = root / "dataset" / "meta" / "symbols.txt"
    expected_codes = sorted({it.code for it in read_universe(universe_path)}) if universe_path.exists() else sorted(instruments.keys())
    by_date: dict[str, set[str]] = {}
    for e in new_tick_files:
        d = str(e.get("date") or "")
        c = str(e.get("code") or "")
        if len(d) == 8 and len(c) == 6:
            by_date.setdefault(d, set()).add(c)
    missing_by_day: list[dict] = []
    missing_by_symbol: dict[str, list[str]] = {c: [] for c in expected_codes}
    for d in sorted(by_date.keys()):
        miss = sorted(set(expected_codes) - by_date.get(d, set()))
        if miss:
            missing_by_day.append({"date": d, "missing_codes": miss})
            for c in miss:
                missing_by_symbol.setdefault(c, []).append(d)

    manifest["tick"]["missing_by_day"] = missing_by_day
    manifest["tick"]["missing_by_symbol"] = missing_by_symbol

    daily_entries: list[dict] = []
    daily_root = root / "dataset" / "daily"
    if daily_root.exists():
        for sym_dir in sorted(daily_root.iterdir()):
            if not sym_dir.is_dir():
                continue
            ts_code = sym_dir.name.upper()
            code = ts_code.split(".")[0] if "." in ts_code else None
            name = instruments.get(code, {}).get("name") if code else None
            for p in sorted(sym_dir.glob("*.parquet")):
                year = p.stem
                daily_entries.append(
                    {
                        "symbol": ts_code,
                        "code": code,
                        "name": name,
                        "year": year,
                        "path": str(p.relative_to(root)).replace("\\", "/"),
                        "source": "tushare",
                    }
                )

    manifest.setdefault("daily", {})["files"] = daily_entries

    save_manifest(root, manifest)
    return (root / "dataset" / "meta" / "dataset_manifest.json")


def main() -> None:
    parser = argparse.ArgumentParser(prog="ManifestManager")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("rebuild")
    p.add_argument("--instruments", required=True)

    args = parser.parse_args()
    root = Path(args.root)
    if args.cmd == "rebuild":
        out = rebuild_manifest(root, Path(args.instruments))
        print(str(out))


if __name__ == "__main__":
    main()

