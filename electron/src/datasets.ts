import fs from "node:fs";
import path from "node:path";

import type { DatasetNode, DatasetRef, DataKind } from "../../shared/ipc";

export type InstrumentMeta = {
  code: string;
  ts_code: string;
  name: string | null;
};

function tryReadJson<T>(p: string): T | null {
  try {
    const s = fs.readFileSync(p, { encoding: "utf-8" });
    return JSON.parse(s) as T;
  } catch {
    return null;
  }
}

export function loadInstruments(datasetRoot: string): Record<string, InstrumentMeta> {
  const p = path.join(datasetRoot, "meta", "instruments.json");
  const raw = tryReadJson<InstrumentMeta[]>(p) ?? [];
  const out: Record<string, InstrumentMeta> = {};
  for (const r of raw) {
    if (!r?.ts_code) continue;
    out[String(r.ts_code).toUpperCase()] = {
      code: String(r.code ?? "").toUpperCase(),
      ts_code: String(r.ts_code).toUpperCase(),
      name: r.name ?? null,
    };
  }
  return out;
}

function listDirs(p: string): string[] {
  try {
    return fs
      .readdirSync(p, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => d.name);
  } catch {
    return [];
  }
}

function listParquets(p: string): string[] {
  try {
    return fs
      .readdirSync(p, { withFileTypes: true })
      .filter((d) => d.isFile() && d.name.toLowerCase().endsWith(".parquet"))
      .map((d) => d.name);
  } catch {
    return [];
  }
}

function makeRef(kind: DataKind, relPath: string): DatasetRef {
  return { kind, path: relPath.replace(/\\/g, "/") };
}

export function listDatasets(datasetRoot: string): DatasetNode[] {
  const instruments = loadInstruments(datasetRoot);
  const out: DatasetNode[] = [];

  const dailyRoot = path.join(datasetRoot, "daily");
  for (const sym of listDirs(dailyRoot)) {
    const meta = instruments[sym.toUpperCase()];
    const symDir = path.join(dailyRoot, sym);
    const years = listParquets(symDir)
      .map((f) => f.replace(/\.parquet$/i, ""))
      .sort();

    const partitions = years.map((y) => {
      const rel = path.join("dataset", "daily", sym, `${y}.parquet`);
      return { id: `${sym}:${y}`, label: y, ref: makeRef("daily", rel) };
    });

    out.push({
      id: `daily:${sym}`,
      label: meta?.name ? `${sym} ${meta.name}` : sym,
      kind: "daily",
      symbol: sym,
      name: meta?.name ?? null,
      partitions,
    });
  }

  const tickRoot = path.join(datasetRoot, "ticks");
  for (const sym of listDirs(tickRoot)) {
    const meta = instruments[sym.toUpperCase()];
    const symDir = path.join(tickRoot, sym);
    const months = listDirs(symDir).sort();
    const partitions: { id: string; label: string; ref: DatasetRef }[] = [];

    for (const m of months) {
      const monthDir = path.join(symDir, m);
      const days = listParquets(monthDir)
        .map((f) => f.replace(/\.parquet$/i, ""))
        .sort();
      for (const d of days) {
        const rel = path.join("dataset", "ticks", sym, m, `${d}.parquet`);
        partitions.push({ id: `${sym}:${m}:${d}`, label: `${m}/${d}`, ref: makeRef("tick", rel) });
      }
    }

    out.push({
      id: `tick:${sym}`,
      label: meta?.name ? `${sym} ${meta.name}` : sym,
      kind: "tick",
      symbol: sym,
      name: meta?.name ?? null,
      partitions,
    });
  }

  out.sort((a, b) => a.label.localeCompare(b.label));
  return out;
}

