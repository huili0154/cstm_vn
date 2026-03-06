import { Plus, Trash2 } from "lucide-react";
import { useMemo, useState } from "react";

import type { ColumnSchema, TableFilter } from "../../shared/ipc";

const OPS: { op: TableFilter["op"]; label: string; needsValue?: boolean; needsValue2?: boolean }[] = [
  { op: "eq", label: "=", needsValue: true },
  { op: "contains", label: "包含", needsValue: true },
  { op: "gt", label: ">", needsValue: true },
  { op: "gte", label: ">=", needsValue: true },
  { op: "lt", label: "<", needsValue: true },
  { op: "lte", label: "<=", needsValue: true },
  { op: "between", label: "区间", needsValue: true, needsValue2: true },
  { op: "is_null", label: "为空" },
  { op: "not_null", label: "非空" },
];

export default function WorkspaceSidebar(props: {
  columns: ColumnSchema[];
  filters: TableFilter[];
  onFiltersChange: (filters: TableFilter[]) => void;
  xCol: string;
  yCol: string;
  maxPoints: number;
  onXColChange: (v: string) => void;
  onYColChange: (v: string) => void;
  onMaxPointsChange: (v: number) => void;
}) {
  const [pendingCol, setPendingCol] = useState<string>(props.columns[0]?.name ?? "");
  const [pendingOp, setPendingOp] = useState<TableFilter["op"]>("eq");
  const [pendingVal, setPendingVal] = useState<string>("");
  const [pendingVal2, setPendingVal2] = useState<string>("");

  const opMeta = useMemo(() => OPS.find((o) => o.op === pendingOp) ?? OPS[0]!, [pendingOp]);

  return (
    <aside className="h-[calc(100vh-170px)] overflow-auto rounded-xl border border-white/10 bg-white/5">
      <div className="border-b border-white/10 p-3">
        <div className="text-sm font-semibold">筛选</div>
        <div className="mt-1 text-xs text-[#9FB0D0]">按列生成过滤条件，影响表格与图表</div>
      </div>

      <div className="space-y-4 p-3">
        <div className="grid grid-cols-1 gap-2">
          <select
            value={pendingCol}
            onChange={(e) => setPendingCol(e.target.value)}
            className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm"
          >
            {props.columns.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name}
              </option>
            ))}
          </select>

          <select
            value={pendingOp}
            onChange={(e) => setPendingOp(e.target.value as any)}
            className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm"
          >
            {OPS.map((o) => (
              <option key={o.op} value={o.op}>
                {o.label}
              </option>
            ))}
          </select>

          {opMeta.needsValue ? (
            <input
              value={pendingVal}
              onChange={(e) => setPendingVal(e.target.value)}
              placeholder="值"
              className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm placeholder:text-[#9FB0D0]"
            />
          ) : null}

          {opMeta.needsValue2 ? (
            <input
              value={pendingVal2}
              onChange={(e) => setPendingVal2(e.target.value)}
              placeholder="值2"
              className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm placeholder:text-[#9FB0D0]"
            />
          ) : null}

          <button
            type="button"
            onClick={() => {
              if (!pendingCol) return;
              props.onFiltersChange(
                props.filters.concat({
                  col: pendingCol,
                  op: pendingOp,
                  value: opMeta.needsValue ? pendingVal : undefined,
                  value2: opMeta.needsValue2 ? pendingVal2 : undefined,
                }),
              );
              setPendingVal("");
              setPendingVal2("");
            }}
            className="inline-flex h-9 items-center justify-center gap-2 rounded-md bg-[#4F7DFF] px-3 text-sm font-medium text-white hover:brightness-110"
          >
            <Plus className="h-4 w-4" />
            添加条件
          </button>
        </div>

        <div className="space-y-2">
          {props.filters.map((f, idx) => (
            <div key={idx} className="flex items-center justify-between gap-2 rounded-lg border border-white/10 bg-[#111A33] px-3 py-2">
              <div className="min-w-0">
                <div className="truncate text-xs text-[#EAF0FF]">
                  {f.col} {OPS.find((o) => o.op === f.op)?.label ?? f.op}{" "}
                  {f.value ?? ""}{f.op === "between" ? ` ~ ${f.value2 ?? ""}` : ""}
                </div>
              </div>
              <button
                type="button"
                onClick={() => props.onFiltersChange(props.filters.filter((_, i) => i !== idx))}
                className="inline-flex h-8 w-8 items-center justify-center rounded-md hover:bg-white/5"
              >
                <Trash2 className="h-4 w-4 text-[#9FB0D0]" />
              </button>
            </div>
          ))}

          {props.filters.length ? (
            <button
              type="button"
              onClick={() => props.onFiltersChange([])}
              className="w-full rounded-md border border-white/10 bg-white/5 px-3 py-2 text-sm text-[#EAF0FF]/90 hover:bg-white/10"
            >
              清空筛选
            </button>
          ) : null}
        </div>

        <div className="border-t border-white/10 pt-3">
          <div className="text-sm font-semibold">曲线</div>
          <div className="mt-2 grid grid-cols-1 gap-2">
            <select
              value={props.xCol}
              onChange={(e) => props.onXColChange(e.target.value)}
              className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm"
            >
              <option value="">X轴列</option>
              {props.columns.map((c) => (
                <option key={c.name} value={c.name}>
                  {c.name}
                </option>
              ))}
            </select>
            <select
              value={props.yCol}
              onChange={(e) => props.onYColChange(e.target.value)}
              className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm"
            >
              <option value="">Y轴列</option>
              {props.columns.map((c) => (
                <option key={c.name} value={c.name}>
                  {c.name}
                </option>
              ))}
            </select>
            <select
              value={props.maxPoints}
              onChange={(e) => props.onMaxPointsChange(Number(e.target.value))}
              className="h-9 rounded-md border border-white/10 bg-[#111A33] px-3 text-sm"
            >
              {[1000, 5000, 20000].map((n) => (
                <option key={n} value={n}>
                  {n}点上限
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>
    </aside>
  );
}

