from __future__ import annotations

import argparse
import json
from pathlib import Path

from gui_viewer.data_access import dataset_root
from virtual_market.batch import BatchTask, run_batch_backtest
from virtual_market.strategy import StrategyConfig


def _parse_list(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(prog="VirtualMarketBatchBacktest")
    parser.add_argument("--symbols", default="510300.SH,159919.SZ")
    parser.add_argument("--dates", default="20250102")
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--output-dir", default="outputs/simulation_logs_batch")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--order-size", type=int, default=1000)
    parser.add_argument("--lookback-ticks", type=int, default=240)
    parser.add_argument("--cancel-after-ticks", type=int, default=90)
    args = parser.parse_args()

    ds_root = Path(args.dataset_root).resolve() if args.dataset_root else dataset_root()
    symbols = _parse_list(args.symbols)
    dates = _parse_list(args.dates)
    tasks = [BatchTask(symbol=s, date=d) for s in symbols for d in dates]
    config = StrategyConfig(
        order_size=args.order_size,
        lookback_ticks=args.lookback_ticks,
        cancel_after_ticks=args.cancel_after_ticks,
    )
    report = run_batch_backtest(
        tasks=tasks,
        output_dir=Path(args.output_dir),
        workers=args.workers,
        ds_root=ds_root,
        strategy_config=config,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
