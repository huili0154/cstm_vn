import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class UniverseItem:
    code: str
    preferred_exchange: str | None = None


_CODE_RE = re.compile(r"^(\d{6})(?:\.(SZ|SH))?$")


def read_universe(file_path: Path) -> list[UniverseItem]:
    items: list[UniverseItem] = []
    for raw in file_path.read_text(encoding="utf-8").splitlines():
        s = raw.strip().upper()
        if not s or s.startswith("#"):
            continue
        m = _CODE_RE.match(s)
        if not m:
            continue
        code, exch = m.group(1), m.group(2)
        items.append(UniverseItem(code=code, preferred_exchange=exch))
    seen: set[tuple[str, str | None]] = set()
    out: list[UniverseItem] = []
    for it in items:
        k = (it.code, it.preferred_exchange)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out


def normalize_ts_code(code: str, exchange: str) -> str:
    return f"{code}.{exchange.upper()}"

