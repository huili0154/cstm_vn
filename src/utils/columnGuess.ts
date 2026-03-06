import type { ColumnSchema } from "../../shared/ipc";

export function guessTimeColumn(cols: ColumnSchema[]): string | null {
  const names = cols.map((c) => c.name.toLowerCase());
  const candidates = ["trade_date", "date", "datetime", "time", "ts"];
  for (const c of candidates) {
    const idx = names.indexOf(c);
    if (idx >= 0) return cols[idx]!.name;
  }
  return cols[0]?.name ?? null;
}

export function guessValueColumn(cols: ColumnSchema[]): string | null {
  const names = cols.map((c) => c.name.toLowerCase());
  const candidates = ["close", "price", "last", "open_bwd", "close_bwd", "open", "high", "low"];
  for (const c of candidates) {
    const idx = names.indexOf(c);
    if (idx >= 0) return cols[idx]!.name;
  }
  return cols.find((c) => c.type === "number")?.name ?? cols[1]?.name ?? null;
}

