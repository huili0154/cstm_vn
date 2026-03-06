import argparse
import re
import shutil
from pathlib import Path

import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tools.manifest_manager import rebuild_manifest
from tools.tick_parquet_manager import import_from_raw
from tools.tushare_manager import fetch_daily_year, sync_instruments


def _detect_years_from_rawdir(rawdir: Path) -> list[int]:
    years: set[int] = set()
    for p in rawdir.glob("*.7z"):
        m = re.match(r"(\d{4})\d{4}\.7z$", p.name)
        if m:
            years.add(int(m.group(1)))
    return sorted(years)


def _purge_dataset(root: Path) -> None:
    dataset_dir = root / "dataset"
    if not dataset_dir.exists():
        return

    for p in [dataset_dir / "ticks", dataset_dir / "daily"]:
        if p.exists():
            shutil.rmtree(p)

    meta_dir = dataset_dir / "meta"
    for p in [
        meta_dir / "dataset_manifest.json",
        meta_dir / "instruments.parquet",
        meta_dir / "instruments.json",
    ]:
        if p.exists():
            p.unlink()


def update_dataset(
    root: Path,
    rawdir: Path,
    universe_file: Path,
    mode: str,
    years: list[int] | None,
    refresh_instruments: bool,
    refresh_daily: bool,
    force_daily: bool,
) -> None:
    if mode not in {"incremental", "overwrite"}:
        raise ValueError("mode must be incremental or overwrite")

    if mode == "overwrite":
        _purge_dataset(root)

    meta_dir = root / "dataset" / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    instruments_path = meta_dir / "instruments.parquet"

    if refresh_instruments or not instruments_path.exists():
        sync_instruments(root, universe_file, rawdir)

    report = import_from_raw(
        root=root,
        rawdir=rawdir,
        universe_file=universe_file,
        dates=None,
        tmp_dir=rawdir / "_tmp_extract",
        instruments_path=instruments_path,
    )

    miss_map = report.get("missing_by_day") if isinstance(report, dict) else None
    if isinstance(miss_map, dict):
        days_with_missing = [d for d, miss in miss_map.items() if miss]
        if days_with_missing:
            print(f"days_with_missing_symbols: {len(days_with_missing)}")
            for d in sorted(days_with_missing)[:10]:
                miss = miss_map[d]
                preview = ",".join(miss[:6])
                tail = "..." if len(miss) > 6 else ""
                print(f"  {d}: {len(miss)} {preview}{tail}")

    if years is None:
        years = _detect_years_from_rawdir(rawdir)

    if refresh_daily:
        for y in years:
            if not force_daily:
                ok = True
                for sym_dir in (root / "dataset" / "daily").glob("*.*"):
                    p = sym_dir / f"{y}.parquet"
                    if not p.exists():
                        ok = False
                        break
                if ok:
                    continue
            fetch_daily_year(root, instruments_path, y)

    rebuild_manifest(root, instruments_path)


def main() -> None:
    parser = argparse.ArgumentParser(prog="DatasetUpdater")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--rawdir", default=str(Path(__file__).resolve().parents[1] / "rawData"))
    parser.add_argument("--universe", default=str(Path(__file__).resolve().parents[1] / "dataset" / "meta" / "symbols.txt"))
    parser.add_argument("--mode", choices=["incremental", "overwrite"], default="incremental")
    parser.add_argument("--years", nargs="*", type=int)
    parser.add_argument("--refresh-instruments", action="store_true")
    parser.add_argument("--no-daily", action="store_true")
    parser.add_argument("--force-daily", action="store_true")
    parser.add_argument("--confirm-overwrite", action="store_true")

    args = parser.parse_args()
    root = Path(args.root)
    rawdir = Path(args.rawdir)
    universe = Path(args.universe)

    if args.mode == "overwrite" and not args.confirm_overwrite:
        raise SystemExit("overwrite mode requires --confirm-overwrite")

    update_dataset(
        root=root,
        rawdir=rawdir,
        universe_file=universe,
        mode=args.mode,
        years=args.years or None,
        refresh_instruments=args.refresh_instruments,
        refresh_daily=not args.no_daily,
        force_daily=args.force_daily,
    )


if __name__ == "__main__":
    main()

