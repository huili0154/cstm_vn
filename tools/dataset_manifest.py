import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ManifestPaths:
    root: Path

    @property
    def meta_dir(self) -> Path:
        return self.root / "dataset" / "meta"

    @property
    def manifest_path(self) -> Path:
        return self.meta_dir / "dataset_manifest.json"


def load_manifest(root: Path) -> dict:
    paths = ManifestPaths(root)
    if not paths.manifest_path.exists():
        return {
            "version": "dataset_v0.2",
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "tick": {"files": []},
            "daily": {"files": []},
            "instruments": {"path": None, "updated_at": None},
        }
    return json.loads(paths.manifest_path.read_text(encoding="utf-8"))


def save_manifest(root: Path, manifest: dict) -> None:
    paths = ManifestPaths(root)
    paths.meta_dir.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = datetime.now().isoformat(timespec="seconds")
    paths.manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def upsert_file_entry(entries: list[dict], entry: dict, keys: list[str]) -> None:
    def key_of(d: dict) -> tuple:
        return tuple(d.get(k) for k in keys)

    target = key_of(entry)
    for i, e in enumerate(entries):
        if key_of(e) == target:
            entries[i] = {**e, **entry}
            return
    entries.append(entry)

