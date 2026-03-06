import { ipcMain } from "electron";

import type { SeriesQuery, SeriesResult, TableQuery, TableResult } from "../../shared/ipc";
import { getDatasetRoot, safeResolveUnder } from "./paths";
import { createDuckDbClient } from "./duckdbClient";
import { listDatasets } from "./datasets";
import {
  buildCountSql,
  buildPreviewSql,
  buildSeriesSql,
  buildTableQuerySql,
} from "./queryBuilder";

const db = createDuckDbClient();

function toSchema(rows: Record<string, unknown>[]): { name: string; type: string }[] {
  const r = rows[0] ?? {};
  return Object.keys(r).map((k) => ({ name: k, type: typeof (r as any)[k] }));
}

export function registerIpcHandlers(): void {
  const datasetRoot = getDatasetRoot();

  ipcMain.handle("datasets:list", async () => {
    return listDatasets(datasetRoot);
  });

  ipcMain.handle("dataset:preview", async (_evt, args: { path: string; limit: number }) => {
    const abs = safeResolveUnder(datasetRoot, args.path.replace(/^dataset[\\/]/, ""));
    const q = buildPreviewSql(abs, Math.min(500, Math.max(1, args.limit ?? 100)));
    const rows = await db.all(q.sql, q.params);
    const columns = toSchema(rows);
    const out: TableResult = { columns, rows };
    return out;
  });

  ipcMain.handle("table:query", async (_evt, q: TableQuery) => {
    const abs = safeResolveUnder(datasetRoot, q.dataset.path.replace(/^dataset[\\/]/, ""));
    const sql = buildTableQuerySql(abs, q);
    const rows = await db.all(sql.sql, sql.params);
    const columns = toSchema(rows);

    const cntSql = buildCountSql(abs, q.filters);
    const cntRows = await db.all(cntSql.sql, cntSql.params);
    const totalEstimate = Number((cntRows[0] as any)?.cnt ?? 0);
    const out: TableResult = { columns, rows, totalEstimate };
    return out;
  });

  ipcMain.handle("series:query", async (_evt, q: SeriesQuery) => {
    const abs = safeResolveUnder(datasetRoot, q.dataset.path.replace(/^dataset[\\/]/, ""));
    const cntSql = buildCountSql(abs, q.filters);
    const cntRows = await db.all(cntSql.sql, cntSql.params);
    const total = Number((cntRows[0] as any)?.cnt ?? 0);
    const maxPoints = Math.min(20000, Math.max(100, q.maxPoints ?? 2000));
    const step = total > maxPoints ? Math.ceil(total / maxPoints) : 1;

    const sql = buildSeriesSql(abs, q.xCol, q.yCol, q.filters, maxPoints, step);
    const rows = await db.all(sql.sql, sql.params);
    const points = rows.map((r) => ({ x: (r as any).x, y: (r as any).y as number | null }));
    const out: SeriesResult = { points };
    return out;
  });
}

