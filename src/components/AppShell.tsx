import { Link } from "react-router-dom";
import { Database, LayoutDashboard, RefreshCw } from "lucide-react";
import type { ReactNode } from "react";

export default function AppShell(props: {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="min-h-screen bg-[#0B1020] text-[#EAF0FF]">
      <header className="sticky top-0 z-10 border-b border-white/10 bg-[#0B1020]/90 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-3">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-white/5 ring-1 ring-white/10">
              <Database className="h-5 w-5 text-[#4F7DFF]" />
            </div>
            <div>
              <div className="text-sm font-semibold">{props.title}</div>
              {props.subtitle ? <div className="text-xs text-[#9FB0D0]">{props.subtitle}</div> : null}
            </div>
          </div>

          <nav className="flex items-center gap-2">
            <Link
              to="/"
              className="inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm text-[#EAF0FF]/90 hover:bg-white/5"
            >
              <RefreshCw className="h-4 w-4" />
              浏览器
            </Link>
            <Link
              to="/workspace"
              className="inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm text-[#EAF0FF]/90 hover:bg-white/5"
            >
              <LayoutDashboard className="h-4 w-4" />
              工作台
            </Link>
            {props.actions}
          </nav>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-4">{props.children}</main>
    </div>
  );
}

