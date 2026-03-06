from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from gui_viewer.data_access import dataset_root, load_tick_day

from .engine import MatchingEngine
from .strategy import LowBuyHighSellStrategy, StrategyConfig
from .types import Order, Tick, Trade


def _build_tick(symbol: str, row: pd.Series) -> Tick:
    return Tick(
        symbol=symbol,
        timestamp=pd.Timestamp(row["datetime"]).to_pydatetime(),
        last_price=float(row.get("last_price", 0.0) or 0.0),
        volume=float(row.get("volume", 0.0) or 0.0),
        bid_price_1=float(row.get("bid_price_1", 0.0) or 0.0),
        bid_volume_1=float(row.get("bid_volume_1", 0.0) or 0.0),
        ask_price_1=float(row.get("ask_price_1", 0.0) or 0.0),
        ask_volume_1=float(row.get("ask_volume_1", 0.0) or 0.0),
        bid_price_5=float(row.get("bid_price_5", 0.0) or 0.0),
        ask_price_5=float(row.get("ask_price_5", 0.0) or 0.0),
    )


def run_backtest(
    symbol: str,
    date: str,
    output_dir: Path,
    ds_root: Path | None = None,
    strategy_config: StrategyConfig | None = None,
) -> dict:
    root = ds_root or dataset_root()
    df = load_tick_day(root, symbol, date)
    if df.empty:
        raise RuntimeError("tick_data_empty")
    df = df.dropna(subset=["datetime"]).sort_values("datetime")
    engine = MatchingEngine(symbol=symbol)
    strategy = LowBuyHighSellStrategy(engine=engine, config=strategy_config)
    orders_timeline: list[dict] = []
    trades_timeline: list[dict] = []

    def on_order(order: Order) -> None:
        strategy.on_order(order)
        orders_timeline.append(
            {
                "order_id": order.order_id,
                "timestamp": order.updated_at.isoformat(),
                "side": order.side.value,
                "status": order.status.value,
                "price": order.price,
                "volume": order.volume,
                "remaining": order.remaining,
                "queue_ahead": order.queue_ahead,
                "age_ticks": order.age_ticks,
            }
        )

    def on_trade(trade: Trade) -> None:
        strategy.on_trade(trade)
        trades_timeline.append(
            {
                "trade_id": trade.trade_id,
                "order_id": trade.order_id,
                "timestamp": trade.timestamp.isoformat(),
                "side": trade.side.value,
                "price": trade.price,
                "volume": trade.volume,
            }
        )

    engine.bind_callbacks(on_order=on_order, on_trade=on_trade)

    for _, row in df.iterrows():
        tick = _build_tick(symbol, row)
        engine.process_tick(tick)
        strategy.on_tick(tick)

    if strategy.active_order_id:
        engine.cancel_order(strategy.active_order_id, reason="end_of_backtest")

    output_dir.mkdir(parents=True, exist_ok=True)
    event_path = output_dir / f"{symbol}_{date}_events.jsonl"
    order_path = output_dir / f"{symbol}_{date}_orders.jsonl"
    trade_path = output_dir / f"{symbol}_{date}_trades.jsonl"
    summary_path = output_dir / f"{symbol}_{date}_summary.json"

    with event_path.open("w", encoding="utf-8") as f:
        for item in engine.export_events():
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    with order_path.open("w", encoding="utf-8") as f:
        for item in orders_timeline:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    with trade_path.open("w", encoding="utf-8") as f:
        for item in trades_timeline:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    buy_volume = sum(t["volume"] for t in trades_timeline if t["side"] == "BUY")
    sell_volume = sum(t["volume"] for t in trades_timeline if t["side"] == "SELL")
    buy_turnover = sum(t["volume"] * t["price"] for t in trades_timeline if t["side"] == "BUY")
    sell_turnover = sum(t["volume"] * t["price"] for t in trades_timeline if t["side"] == "SELL")

    summary = {
        "symbol": symbol,
        "date": date,
        "rows": int(len(df)),
        "orders_events": len(orders_timeline),
        "trades": len(trades_timeline),
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "position_end": strategy.position,
        "avg_buy_price": (buy_turnover / buy_volume) if buy_volume else None,
        "avg_sell_price": (sell_turnover / sell_volume) if sell_volume else None,
        "event_log": str(event_path),
        "order_log": str(order_path),
        "trade_log": str(trade_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
