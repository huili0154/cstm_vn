from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

from .backtest import run_backtest
from .strategy import StrategyConfig


@dataclass(frozen=True)
class BatchTask:
    symbol: str
    date: str


def _run_single_task(
    symbol: str,
    date: str,
    output_dir: str,
    ds_root: str | None,
    strategy_config: StrategyConfig | None,
) -> dict:
    summary = run_backtest(
        symbol=symbol,
        date=date,
        output_dir=Path(output_dir),
        ds_root=Path(ds_root) if ds_root else None,
        strategy_config=strategy_config,
    )
    return summary


def run_batch_backtest(
    tasks: list[BatchTask],
    output_dir: Path,
    workers: int = 2,
    ds_root: Path | None = None,
    strategy_config: StrategyConfig | None = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now().isoformat(timespec="seconds")
    t0 = perf_counter()
    results: list[dict] = []
    failures: list[dict] = []
    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        fut_map = {}
        for task in tasks:
            fut = pool.submit(
                _run_single_task,
                task.symbol,
                task.date,
                str(output_dir),
                str(ds_root) if ds_root else None,
                strategy_config,
            )
            fut_map[fut] = task
        for fut in as_completed(fut_map):
            task = fut_map[fut]
            try:
                summary = fut.result()
                summary["task"] = {"symbol": task.symbol, "date": task.date}
                results.append(summary)
            except Exception as exc:
                failures.append(
                    {
                        "task": {"symbol": task.symbol, "date": task.date},
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    elapsed = perf_counter() - t0
    total_trades = sum(int(r.get("trades", 0)) for r in results)
    total_buy_volume = sum(int(r.get("buy_volume", 0)) for r in results)
    total_sell_volume = sum(int(r.get("sell_volume", 0)) for r in results)
    report = {
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "elapsed_seconds": round(elapsed, 3),
        "workers": max(1, workers),
        "tasks_total": len(tasks),
        "tasks_success": len(results),
        "tasks_failed": len(failures),
        "total_trades": total_trades,
        "total_buy_volume": total_buy_volume,
        "total_sell_volume": total_sell_volume,
        "results": sorted(results, key=lambda x: (x.get("symbol", ""), x.get("date", ""))),
        "failures": failures,
    }
    report_path = output_dir / "batch_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
