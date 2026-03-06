import type { ColumnSchema } from "../../shared/ipc";

export default function DataTable(props: {
  columns: ColumnSchema[];
  rows: Record<string, unknown>[];
  loading?: boolean;
}) {
  return (
    <div className="relative h-full overflow-hidden rounded-xl border border-white/10 bg-white/5">
      {props.loading ? (
        <div className="absolute inset-0 z-10 flex items-center justify-center bg-[#0B1020]/60 text-sm text-[#9FB0D0]">
          加载中…
        </div>
      ) : null}

      <div className="h-full overflow-auto">
        <table className="min-w-full text-left text-xs">
          <thead className="sticky top-0 bg-[#0B1020]/80 backdrop-blur">
            <tr>
              {props.columns.map((c) => (
                <th key={c.name} className="whitespace-nowrap border-b border-white/10 px-3 py-2 font-medium text-[#9FB0D0]">
                  {c.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {props.rows.map((r, i) => (
              <tr key={i} className="hover:bg-white/5">
                {props.columns.map((c) => (
                  <td key={c.name} className="whitespace-nowrap border-b border-white/5 px-3 py-2">
                    {String((r as any)[c.name] ?? "")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>

        {!props.rows.length && !props.loading ? (
          <div className="p-6 text-center text-sm text-[#9FB0D0]">无数据</div>
        ) : null}
      </div>
    </div>
  );
}

