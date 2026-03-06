from __future__ import annotations

import argparse
import json
from pathlib import Path

from gui_viewer.data_access import dataset_root
from virtual_market.backtest import run_backtest
from virtual_market.strategy import StrategyConfig


def main() -> None:
    parser = argparse.ArgumentParser(prog="VirtualMarketSimulation")
    parser.add_argument("--symbol", default="510300.SH")
    parser.add_argument("--date", default="20250102")
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--output-dir", default="outputs/simulation_logs")
    parser.add_argument("--order-size", type=int, default=1000)
    parser.add_argument("--lookback-ticks", type=int, default=240)
    parser.add_argument("--cancel-after-ticks", type=int, default=90)
    args = parser.parse_args()

    ds_root = Path(args.dataset_root).resolve() if args.dataset_root else dataset_root()
    config = StrategyConfig(
        order_size=args.order_size,
        lookback_ticks=args.lookback_ticks,
        cancel_after_ticks=args.cancel_after_ticks,
    )
    summary = run_backtest(
        symbol=args.symbol,
        date=args.date,
        output_dir=Path(args.output_dir),
        ds_root=ds_root,
        strategy_config=config,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
