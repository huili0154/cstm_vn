import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { SeriesPoint } from "../../shared/ipc";

export default function SeriesChart(props: { points: SeriesPoint[]; xLabel?: string; yLabel?: string }) {
  return (
    <div className="h-full overflow-hidden rounded-xl border border-white/10 bg-white/5">
      <div className="flex items-center justify-between border-b border-white/10 px-3 py-2">
        <div className="text-xs font-medium text-[#9FB0D0]">
          {props.xLabel ?? "X"} → {props.yLabel ?? "Y"}
        </div>
        <div className="text-xs text-[#9FB0D0]">{props.points.length} points</div>
      </div>
      <div className="h-[calc(100%-36px)] p-2">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={props.points} margin={{ left: 8, right: 8, top: 8, bottom: 8 }}>
            <XAxis dataKey="x" tick={{ fill: "#9FB0D0", fontSize: 11 }} axisLine={{ stroke: "rgba(255,255,255,0.1)" }} />
            <YAxis tick={{ fill: "#9FB0D0", fontSize: 11 }} axisLine={{ stroke: "rgba(255,255,255,0.1)" }} />
            <Tooltip
              contentStyle={{
                background: "#111A33",
                border: "1px solid rgba(255,255,255,0.12)",
                borderRadius: 8,
                fontSize: 12,
                color: "#EAF0FF",
              }}
              labelStyle={{ color: "#9FB0D0" }}
            />
            <Line type="monotone" dataKey="y" stroke="#4F7DFF" dot={false} strokeWidth={2} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

