import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";

import type { ColumnSchema, SeriesPoint, TableFilter, TableQuery, TableResult } from "../../shared/ipc";
import AppShell from "@/components/AppShell";
import DataTable from "@/components/DataTable";
import SeriesChart from "@/components/SeriesChart";
import WorkspaceSidebar from "@/components/WorkspaceSidebar";
import { useDatasetStore } from "@/store/datasetStore";
import { guessTimeColumn, guessValueColumn } from "@/utils/columnGuess";

export default function Workspace() {
  const { selected } = useDatasetStore();
  const [pageSize, setPageSize] = useState(200);
  const [page, setPage] = useState(0);
  const [columns, setColumns] = useState<ColumnSchema[]>([]);
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [filters, setFilters] = useState<TableFilter[]>([]);

  const [xCol, setXCol] = useState<string>("");
  const [yCol, setYCol] = useState<string>("");
  const [maxPoints, setMaxPoints] = useState<number>(5000);
  const [points, setPoints] = useState<SeriesPoint[]>([]);
  const [seriesLoading, setSeriesLoading] = useState(false);

  const canQuery = !!selected;

  const whereSummary = useMemo(() => {
    if (!filters.length) return "无筛选";
    return `筛选 ${filters.length} 条`;
  }, [filters]);

  const runQuery = async () => {
    if (!selected) return;
    setLoading(true);
    setError(null);
    try {
      const q: TableQuery = {
        dataset: selected.ref,
        limit: pageSize,
        offset: page * pageSize,
        filters,
        orderBy: xCol ? [{ col: xCol, desc: false }] : undefined,
      };
      const res: TableResult = await window.datasetApi.queryTable(q);
      setColumns(res.columns);
      setRows(res.rows);
      setTotal(typeof res.totalEstimate === "number" ? res.totalEstimate : null);
      if (!xCol) {
        const x = guessTimeColumn(res.columns);
        if (x) setXCol(x);
      }
      if (!yCol) {
        const y = guessValueColumn(res.columns);
        if (y) setYCol(y);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const runSeries = async () => {
    if (!selected || !xCol || !yCol) return;
    setSeriesLoading(true);
    try {
      const res = await window.datasetApi.querySeries({
        dataset: selected.ref,
        xCol,
        yCol,
        filters,
        maxPoints,
      });
      setPoints(res.points);
    } catch {
      setPoints([]);
    } finally {
      setSeriesLoading(false);
    }
  };

  useEffect(() => {
    setPage(0);
  }, [selected?.ref.path, pageSize]);

  useEffect(() => {
    if (!selected) {
      setColumns([]);
      setRows([]);
      setPoints([]);
      setTotal(null);
      return;
    }
    runQuery();
  }, [selected?.ref.path, page, pageSize, filters]);

  useEffect(() => {
    if (!selected) return;
    runSeries();
  }, [selected?.ref.path, filters, xCol, yCol, maxPoints]);

  return (
    <AppShell
      title="数据工作台"
      subtitle={selected ? `${selected.symbol} ${selected.name ?? ""} · ${selected.ref.path} · ${whereSummary}` : "请先在浏览器页选中数据集"}
      actions={
        <button
          type="button"
          onClick={() => {
            runQuery();
            runSeries();
          }}
          className="inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm text-[#EAF0FF]/90 hover:bg-white/5"
        >
          <RefreshCw className="h-4 w-4" />
          刷新
        </button>
      }
    >
      {!canQuery ? (
        <div className="rounded-xl border border-white/10 bg-white/5 p-6 text-sm text-[#9FB0D0]">去“浏览器”页选中一个数据集后再回来。</div>
      ) : (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-[1fr_360px]">
          <section className="flex h-[calc(100vh-170px)] flex-col gap-3">
            {error ? (
              <div className="flex items-center gap-2 rounded-xl border border-[#FF4D4F]/40 bg-[#FF4D4F]/10 px-3 py-2 text-sm text-[#EAF0FF]">
                <AlertTriangle className="h-4 w-4 text-[#FF4D4F]" />
                {error}
              </div>
            ) : null}

            <div className="grid grid-rows-[1fr_260px] gap-3">
              <DataTable columns={columns} rows={rows} loading={loading} />
              <div className={seriesLoading ? "opacity-70" : ""}>
                <SeriesChart points={points} xLabel={xCol} yLabel={yCol} />
              </div>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm">
              <div className="text-xs text-[#9FB0D0]">
                {total !== null ? `总行数(估计)：${total}` : ""}
              </div>
              <div className="flex items-center gap-2">
                <select
                  value={pageSize}
                  onChange={(e) => setPageSize(Number(e.target.value))}
                  className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm"
                >
                  {[100, 200, 500, 1000].map((n) => (
                    <option key={n} value={n}>
                      {n}/页
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => setPage((p) => Math.max(0, p - 1))}
                  className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm hover:bg-white/5"
                >
                  上一页
                </button>
                <div className="px-2 text-xs text-[#9FB0D0]">第 {page + 1} 页</div>
                <button
                  type="button"
                  onClick={() => setPage((p) => p + 1)}
                  className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm hover:bg-white/5"
                >
                  下一页
                </button>
              </div>
            </div>
          </section>

          <WorkspaceSidebar
            columns={columns}
            filters={filters}
            onFiltersChange={setFilters}
            xCol={xCol}
            yCol={yCol}
            maxPoints={maxPoints}
            onXColChange={setXCol}
            onYColChange={setYCol}
            onMaxPointsChange={setMaxPoints}
          />
        </div>
      )}
    </AppShell>
  );
}

