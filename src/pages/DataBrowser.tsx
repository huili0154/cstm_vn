import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { ArrowRight, Search } from "lucide-react";

import type { DatasetNode } from "../../shared/ipc";
import AppShell from "@/components/AppShell";
import DataTable from "@/components/DataTable";
import { useDatasetStore } from "@/store/datasetStore";

export default function DataBrowser() {
  const { nodes, status, error, refresh, selected, select } = useDatasetStore();
  const [q, setQ] = useState("");
  const [activeNode, setActiveNode] = useState<DatasetNode | null>(null);
  const [activePartitionId, setActivePartitionId] = useState<string | null>(null);
  const [preview, setPreview] = useState<{ columns: any[]; rows: any[] } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const filtered = useMemo(() => {
    const kw = q.trim().toLowerCase();
    if (!kw) return nodes;
    return nodes.filter((n) => n.label.toLowerCase().includes(kw) || (n.symbol ?? "").toLowerCase().includes(kw));
  }, [nodes, q]);

  const partitions = activeNode?.partitions ?? [];
  const activePartition = partitions.find((p) => p.id === activePartitionId) ?? partitions[0] ?? null;

  useEffect(() => {
    if (!activeNode) {
      setPreview(null);
      return;
    }
    const first = activeNode.partitions[0];
    setActivePartitionId(first?.id ?? null);
  }, [activeNode]);

  useEffect(() => {
    if (!activePartition) {
      setPreview(null);
      return;
    }
    let cancelled = false;
    setPreviewLoading(true);
    window.datasetApi
      .preview({ path: activePartition.ref.path, limit: 120 })
      .then((res) => {
        if (cancelled) return;
        setPreview({ columns: res.columns, rows: res.rows });
      })
      .catch(() => {
        if (cancelled) return;
        setPreview({ columns: [], rows: [] });
      })
      .finally(() => {
        if (cancelled) return;
        setPreviewLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [activePartition]);

  return (
    <AppShell
      title="GUI 数据查看器"
      subtitle={status === "loading" ? "正在扫描 dataset…" : error ? `扫描失败：${error}` : "浏览 dataset 内 Parquet"}
      actions={
        selected ? (
          <Link
            to="/workspace"
            className="inline-flex items-center gap-2 rounded-md bg-[#4F7DFF] px-3 py-2 text-sm font-medium text-white hover:brightness-110"
          >
            打开工作台
            <ArrowRight className="h-4 w-4" />
          </Link>
        ) : null
      }
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[360px_1fr]">
        <section className="rounded-xl border border-white/10 bg-white/5">
          <div className="flex items-center gap-2 border-b border-white/10 p-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-md bg-white/5 ring-1 ring-white/10">
              <Search className="h-4 w-4 text-[#9FB0D0]" />
            </div>
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="搜索：ts_code 或名称"
              className="h-9 w-full rounded-md border border-white/10 bg-[#111A33] px-3 text-sm text-[#EAF0FF] placeholder:text-[#9FB0D0] focus:outline-none focus:ring-2 focus:ring-[#4F7DFF]/50"
            />
          </div>

          <div className="max-h-[calc(100vh-180px)] overflow-auto p-2">
            {filtered.map((n) => {
              const active = activeNode?.id === n.id;
              return (
                <button
                  key={n.id}
                  type="button"
                  onClick={() => setActiveNode(n)}
                  className={
                    "w-full rounded-lg px-3 py-2 text-left transition " +
                    (active ? "bg-white/10 ring-1 ring-white/15" : "hover:bg-white/5")
                  }
                >
                  <div className="text-sm font-medium">{n.label}</div>
                  <div className="mt-0.5 text-xs text-[#9FB0D0]">{n.kind === "daily" ? "日线" : "Tick/分钟"} · {n.partitions.length} 文件</div>
                </button>
              );
            })}

            {!filtered.length && status !== "loading" ? (
              <div className="p-6 text-center text-sm text-[#9FB0D0]">没有匹配的数据集</div>
            ) : null}
          </div>
        </section>

        <section className="rounded-xl border border-white/10 bg-white/5">
          <div className="flex flex-col gap-2 border-b border-white/10 p-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <div className="text-sm font-semibold">预览</div>
              <div className="text-xs text-[#9FB0D0]">
                {activePartition ? activePartition.ref.path : "请选择左侧数据集"}
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <select
                value={activePartitionId ?? ""}
                onChange={(e) => setActivePartitionId(e.target.value)}
                disabled={!partitions.length}
                className="h-9 min-w-[160px] rounded-md border border-white/10 bg-[#111A33] px-3 text-sm text-[#EAF0FF] focus:outline-none focus:ring-2 focus:ring-[#4F7DFF]/50 disabled:opacity-50"
              >
                {partitions.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.label}
                  </option>
                ))}
              </select>

              <button
                type="button"
                onClick={() => {
                  if (!activeNode || !activePartition) return;
                  select({
                    nodeId: activeNode.id,
                    kind: activeNode.kind,
                    symbol: activeNode.symbol ?? "",
                    name: activeNode.name ?? null,
                    partitionId: activePartition.id,
                    ref: activePartition.ref,
                  });
                }}
                disabled={!activeNode || !activePartition}
                className="inline-flex h-9 items-center justify-center rounded-md bg-[#4F7DFF] px-3 text-sm font-medium text-white hover:brightness-110 disabled:opacity-50"
              >
                选中并进入工作台
              </button>
            </div>
          </div>

          <div className="h-[calc(100vh-210px)] p-3">
            <DataTable columns={(preview?.columns as any) ?? []} rows={(preview?.rows as any) ?? []} loading={previewLoading} />
          </div>
        </section>
      </div>
    </AppShell>
  );
}

