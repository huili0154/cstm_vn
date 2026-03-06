import argparse
import json
import shutil
from pathlib import Path

import pandas as pd


def _load_code_to_ts_code(instruments_path: Path) -> dict[str, str]:
    df = pd.read_parquet(instruments_path) if instruments_path.suffix.lower() == ".parquet" else pd.DataFrame(json.loads(instruments_path.read_text(encoding="utf-8")))
    out: dict[str, str] = {}
    for r in df.to_dict(orient="records"):
        code = str(r.get("code") or "").strip()
        ts_code = str(r.get("ts_code") or "").strip()
        if len(code) == 6 and ts_code:
            out[code] = ts_code.upper()
    return out


def migrate_ticks(root: Path, instruments_path: Path) -> None:
    code_map = _load_code_to_ts_code(instruments_path)
    src_root = root / "dataset" / "ticks"
    if not src_root.exists():
        return

    for child in sorted(src_root.iterdir()):
        if not child.is_dir():
            continue
        name = child.name.upper()
        if "." not in name:
            continue
        code = name.split(".")[0]
        ts_code = code_map.get(code)
        if not ts_code or ts_code == name:
            continue
        dst = src_root / ts_code
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            for p in child.iterdir():
                shutil.move(str(p), str(dst / p.name))
            shutil.rmtree(child)
        else:
            shutil.move(str(child), str(dst))


def main() -> None:
    parser = argparse.ArgumentParser(prog="DatasetMigrate")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--instruments", required=True)
    args = parser.parse_args()
    migrate_ticks(Path(args.root), Path(args.instruments))


if __name__ == "__main__":
    main()

