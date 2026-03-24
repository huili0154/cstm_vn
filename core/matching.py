"""
撮合引擎 — 虚拟股市核心。

三种撮合模式:
  - CLOSE_FILL:              日线收盘价全量成交
  - TICK_FILL:               Tick 即时全量成交
  - SMART_TICK_DELAY_FILL:   Tick 延迟深度撮合（部分成交 / 盘口限制 / 排队）

回测时由 backtest/engine.py 在每个时间步调用 match_bar() 或 match_tick()。
实盘时不使用此模块（订单直接发往券商）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Callable

from core.datatypes import (
    BarData,
    Direction,
    MatchingMode,
    Order,
    OrderStatus,
    OrderType,
    TickData,
    Trade,
)


class MatchingEngine:
    """
    撮合引擎。接收行情 + 管理挂单 → 产出成交记录。

    Parameters
    ----------
    pricetick : float
        最小价格变动（ETF = 0.001）。
    rate : float
        手续费率。
    slippage : float
        固定滑点（价格单位）。close_fill / tick_fill 叠加在成交价上；
        smart_tick_delay_fill 建议设 0（延迟+深度已包含隐含滑点）。
    min_commission : float
        最低佣金（ETF 一般 0 或 5 元）。
    mode : MatchingMode
        撮合模式。
    volume_limit_ratio : float
        smart_tick_delay_fill 专用。每档可吃量上限 = min(挂单量, 快照成交量) × ratio。
    """

    def __init__(
        self,
        pricetick: float = 0.001,
        rate: float = 0.0,
        slippage: float = 0.0,
        min_commission: float = 0.0,
        mode: MatchingMode = MatchingMode.TICK_FILL,
        volume_limit_ratio: float = 0.5,
        lot_size: int = 100,
    ) -> None:
        self.pricetick = pricetick
        self.rate = rate
        self.slippage = slippage
        self.min_commission = min_commission
        self.mode = mode
        self.volume_limit_ratio = volume_limit_ratio
        self.lot_size = lot_size

        # 挂单管理
        self._order_count: int = 0
        self._trade_count: int = 0
        self._active_orders: dict[str, Order] = {}
        self._all_orders: dict[str, Order] = {}
        self._trades: dict[str, Trade] = {}

        # smart_tick_delay_fill 专用
        self._order_submit_tick: dict[str, int] = {}   # order_id → 提交时 tick_seq
        self._current_tick_seq: int = 0
        self._prev_cum_volume: dict[str, int] = {}     # symbol → 上个 tick 的 cum_volume

        # 回调 — 由 BacktestEngine 设置
        self._on_order: Callable[[Order], None] | None = None
        self._on_trade: Callable[[Trade], None] | None = None

    # ────────────────────────────────────────────────
    #  回调注册
    # ────────────────────────────────────────────────

    def set_on_order(self, callback: Callable[[Order], None]) -> None:
        self._on_order = callback

    def set_on_trade(self, callback: Callable[[Trade], None]) -> None:
        self._on_trade = callback

    def _notify_order(self, order: Order) -> None:
        if self._on_order:
            self._on_order(order)

    def _notify_trade(self, trade: Trade) -> None:
        if self._on_trade:
            self._on_trade(trade)

    # ────────────────────────────────────────────────
    #  订单提交 / 撤销
    # ────────────────────────────────────────────────

    def submit_order(
        self,
        symbol: str,
        direction: Direction,
        order_type: OrderType,
        price: float,
        volume: int,
        dt: datetime | None = None,
    ) -> Order:
        """提交订单，返回 Order（状态 SUBMITTING）。"""
        self._order_count += 1
        order_id = f"O{self._order_count:06d}"

        order = Order(
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            order_type=order_type,
            price=price,
            volume=volume,
            traded=0,
            status=OrderStatus.SUBMITTING,
            create_time=dt,
            update_time=dt,
        )
        self._active_orders[order_id] = order
        self._all_orders[order_id] = order

        # smart 模式记录提交时的 tick 序号
        if self.mode == MatchingMode.SMART_TICK_DELAY_FILL:
            self._order_submit_tick[order_id] = self._current_tick_seq

        return order

    def cancel_order(self, order_id: str) -> bool:
        """撤销订单。返回是否成功。"""
        order = self._active_orders.get(order_id)
        if order is None:
            return False

        if order.traded > 0:
            order.status = OrderStatus.PART_CANCELLED
        else:
            order.status = OrderStatus.CANCELLED

        order.update_time = datetime.now()
        del self._active_orders[order_id]
        self._order_submit_tick.pop(order_id, None)
        self._notify_order(order)
        return True

    def cancel_all(self) -> list[Order]:
        """撤销所有活跃订单。"""
        cancelled = []
        for order_id in list(self._active_orders):
            if self.cancel_order(order_id):
                cancelled.append(self._all_orders[order_id])
        return cancelled

    def get_active_orders(self) -> list[Order]:
        return list(self._active_orders.values())

    def get_pending_orders(self, symbol: str = "") -> list[Order]:
        """获取未完成委托。可选按 symbol 过滤。"""
        if symbol:
            return [o for o in self._active_orders.values() if o.symbol == symbol]
        return list(self._active_orders.values())

    def get_all_trades(self) -> list[Trade]:
        return list(self._trades.values())

    def reset(self) -> None:
        """重置引擎状态（用于重新回测）。"""
        self._order_count = 0
        self._trade_count = 0
        self._active_orders.clear()
        self._all_orders.clear()
        self._trades.clear()
        self._order_submit_tick.clear()
        self._current_tick_seq = 0
        self._prev_cum_volume.clear()

    # ────────────────────────────────────────────────
    #  手续费
    # ────────────────────────────────────────────────

    def calc_commission(self, price: float, volume: int) -> float:
        """ETF 手续费 = max(turnover × rate, min_commission)。无印花税。"""
        turnover = price * volume
        return max(turnover * self.rate, self.min_commission)

    # ────────────────────────────────────────────────
    #  成交辅助
    # ────────────────────────────────────────────────

    def _round_lot(self, volume: int) -> int:
        """向下取整到 lot_size 的整数倍（ETF 默认 100 股/手）。"""
        return (volume // self.lot_size) * self.lot_size

    def _make_trade(
        self,
        order: Order,
        price: float,
        volume: int,
        dt: datetime | None,
    ) -> Trade:
        """生成一笔成交记录并更新订单。"""
        self._trade_count += 1
        trade_id = f"T{self._trade_count:06d}"

        commission = self.calc_commission(price, volume)

        trade = Trade(
            trade_id=trade_id,
            order_id=order.order_id,
            symbol=order.symbol,
            direction=order.direction,
            price=price,
            volume=volume,
            commission=commission,
            datetime=dt,
        )
        self._trades[trade_id] = trade

        # 更新订单
        order.traded += volume
        order.update_time = dt
        if order.traded >= order.volume:
            order.status = OrderStatus.ALL_TRADED
            self._active_orders.pop(order.order_id, None)
            self._order_submit_tick.pop(order.order_id, None)
        else:
            order.status = OrderStatus.PART_TRADED

        return trade

    def _apply_slippage(self, price: float, direction: Direction) -> float:
        """叠加滑点（对策略不利方向）。"""
        if direction == Direction.BUY:
            return price + self.slippage
        else:
            return price - self.slippage

    # ════════════════════════════════════════════════
    #  CLOSE_FILL — 日线收盘价模式
    # ════════════════════════════════════════════════

    def match_bar(self, bar: BarData) -> list[Trade]:
        """
        日线收盘价撮合。所有活跃挂单以 close_price 成交。
        仅 CLOSE_FILL 模式使用。
        """
        trades: list[Trade] = []

        for order in list(self._active_orders.values()):
            if order.symbol != bar.symbol:
                continue

            # SUBMITTING → ACTIVE 通知
            if order.status == OrderStatus.SUBMITTING:
                order.status = OrderStatus.ACTIVE
                self._notify_order(order)

            fill_price = self._apply_slippage(bar.close_price, order.direction)
            trade = self._make_trade(order, fill_price, order.remaining, bar.datetime)
            trades.append(trade)
            self._notify_order(order)
            self._notify_trade(trade)

        return trades

    # ════════════════════════════════════════════════
    #  TICK_FILL — Tick 即时全量成交
    # ════════════════════════════════════════════════

    def _match_tick_simple(self, tick: TickData) -> list[Trade]:
        """tick_fill 模式：价格满足即全量成交，不考虑盘口量。"""
        trades: list[Trade] = []

        for order in list(self._active_orders.values()):
            if order.symbol != tick.symbol:
                continue

            # SUBMITTING → ACTIVE 通知
            if order.status == OrderStatus.SUBMITTING:
                order.status = OrderStatus.ACTIVE
                self._notify_order(order)

            filled = False

            if order.order_type == OrderType.MARKET:
                # 市价单
                if order.direction == Direction.BUY and tick.ask_price_1 > 0:
                    fill_price = self._apply_slippage(tick.ask_price_1, Direction.BUY)
                    filled = True
                elif order.direction == Direction.SELL and tick.bid_price_1 > 0:
                    fill_price = self._apply_slippage(tick.bid_price_1, Direction.SELL)
                    filled = True
            else:
                # 限价单
                if order.direction == Direction.BUY:
                    if tick.ask_price_1 > 0 and order.price >= tick.ask_price_1:
                        fill_price = self._apply_slippage(
                            min(order.price, tick.ask_price_1), Direction.BUY
                        )
                        filled = True
                else:
                    if tick.bid_price_1 > 0 and order.price <= tick.bid_price_1:
                        fill_price = self._apply_slippage(
                            max(order.price, tick.bid_price_1), Direction.SELL
                        )
                        filled = True

            if filled:
                trade = self._make_trade(order, fill_price, order.remaining, tick.datetime)
                trades.append(trade)
                self._notify_order(order)
                self._notify_trade(trade)

        return trades

    # ════════════════════════════════════════════════
    #  SMART_TICK_DELAY_FILL — 延迟深度撮合
    # ════════════════════════════════════════════════

    def _get_volume_delta(self, tick: TickData) -> int:
        """计算本快照的成交量增量。"""
        prev = self._prev_cum_volume.get(tick.symbol, 0)
        delta = max(0, tick.cum_volume - prev)
        self._prev_cum_volume[tick.symbol] = tick.cum_volume
        return delta

    def _match_tick_smart(self, tick: TickData) -> list[Trade]:
        """smart_tick_delay_fill 模式。"""
        trades: list[Trade] = []
        remaining_delta = self._get_volume_delta(tick)

        for order in list(self._active_orders.values()):
            if order.symbol != tick.symbol:
                continue
            if order.remaining <= 0:
                continue
            # 如果回调中途撤单，快照中仍存在该订单，需跳过
            if not order.is_active:
                continue

            # 延迟检查：提交后至少经过 1 个完整 tick 才能撮合
            submit_seq = self._order_submit_tick.get(order.order_id, -1)
            if self._current_tick_seq - submit_seq <= 1:
                # SUBMITTING → ACTIVE 通知（但不撮合）
                if order.status == OrderStatus.SUBMITTING:
                    order.status = OrderStatus.ACTIVE
                    self._notify_order(order)
                continue

            # 无剩余流动性，跳过
            if remaining_delta <= 0:
                continue

            # SUBMITTING → ACTIVE
            if order.status == OrderStatus.SUBMITTING:
                order.status = OrderStatus.ACTIVE
                self._notify_order(order)

            if order.order_type == OrderType.MARKET:
                new_trades, consumed = self._match_market_order_smart(
                    order, tick, remaining_delta
                )
            else:
                new_trades, consumed = self._match_limit_order_smart(
                    order, tick, remaining_delta
                )

            remaining_delta -= consumed
            for t in new_trades:
                self._notify_order(order)
                self._notify_trade(t)
            trades.extend(new_trades)

        return trades

    def _match_market_order_smart(
        self, order: Order, tick: TickData, remaining_delta: int
    ) -> tuple[list[Trade], int]:
        """
        市价单撮合（smart 模式）。
        只按当前最优价（ask_1 / bid_1）成交。
        可成交量 = min(最优档挂单量, 剩余可用流动性) × volume_limit_ratio。
        返回 (trades, consumed_delta)。
        """
        trades: list[Trade] = []

        if order.direction == Direction.BUY:
            if tick.ask_price_1 <= 0:
                return trades, 0
            max_fill = self._round_lot(int(
                min(tick.ask_volume_1, remaining_delta) * self.volume_limit_ratio
            ))
            fill_vol = min(max_fill, order.remaining)
            if fill_vol > 0:
                trade = self._make_trade(order, tick.ask_price_1, fill_vol, tick.datetime)
                trades.append(trade)
                return trades, fill_vol
        else:
            if tick.bid_price_1 <= 0:
                return trades, 0
            max_fill = self._round_lot(int(
                min(tick.bid_volume_1, remaining_delta) * self.volume_limit_ratio
            ))
            fill_vol = min(max_fill, order.remaining)
            if fill_vol > 0:
                trade = self._make_trade(order, tick.bid_price_1, fill_vol, tick.datetime)
                trades.append(trade)
                return trades, fill_vol

        return trades, 0

    def _match_limit_order_smart(
        self, order: Order, tick: TickData, remaining_delta: int
    ) -> tuple[list[Trade], int]:
        """
        限价单撮合（smart 模式）。

        - 主动吃盘口：委托价穿越对手盘 → 从第1档到第10档逐档匹配。
        - 被动排队：委托价未穿越 → 等对手方价格穿越本方，按成交量比例估算。
        返回 (trades, consumed_delta)。
        """
        if order.direction == Direction.BUY:
            return self._match_buy_limit_smart(order, tick, remaining_delta)
        else:
            return self._match_sell_limit_smart(order, tick, remaining_delta)

    def _match_buy_limit_smart(
        self, order: Order, tick: TickData, remaining_delta: int
    ) -> tuple[list[Trade], int]:
        trades: list[Trade] = []
        consumed = 0
        ask_prices = tick.ask_prices()
        ask_volumes = tick.ask_volumes()

        if tick.ask_price_1 > 0 and order.price >= tick.ask_price_1:
            # ═══ 主动吃卖盘 ═══
            for i in range(10):
                if order.remaining <= 0 or remaining_delta <= 0:
                    break
                ap = ask_prices[i]
                av = ask_volumes[i]
                if ap <= 0 or av <= 0:
                    continue
                if order.price < ap:
                    break  # 委托价不够吃到这一档

                max_fill = self._round_lot(int(
                    min(av, remaining_delta) * self.volume_limit_ratio
                ))
                fill_vol = min(max_fill, order.remaining)
                if fill_vol > 0:
                    trade = self._make_trade(order, ap, fill_vol, tick.datetime)
                    trades.append(trade)
                    remaining_delta -= fill_vol
                    consumed += fill_vol
        else:
            # ═══ 被动排队等待 ═══
            queue_trades, queue_consumed = self._passive_queue_fill(
                order, tick, remaining_delta, is_buy=True
            )
            trades.extend(queue_trades)
            consumed += queue_consumed

        return trades, consumed

    def _match_sell_limit_smart(
        self, order: Order, tick: TickData, remaining_delta: int
    ) -> tuple[list[Trade], int]:
        trades: list[Trade] = []
        consumed = 0
        bid_prices = tick.bid_prices()
        bid_volumes = tick.bid_volumes()

        if tick.bid_price_1 > 0 and order.price <= tick.bid_price_1:
            # ═══ 主动吃买盘 ═══
            for i in range(10):
                if order.remaining <= 0 or remaining_delta <= 0:
                    break
                bp = bid_prices[i]
                bv = bid_volumes[i]
                if bp <= 0 or bv <= 0:
                    continue
                if order.price > bp:
                    break

                max_fill = self._round_lot(int(
                    min(bv, remaining_delta) * self.volume_limit_ratio
                ))
                fill_vol = min(max_fill, order.remaining)
                if fill_vol > 0:
                    trade = self._make_trade(order, bp, fill_vol, tick.datetime)
                    trades.append(trade)
                    remaining_delta -= fill_vol
                    consumed += fill_vol
        else:
            # ═══ 被动排队等待 ═══
            queue_trades, queue_consumed = self._passive_queue_fill(
                order, tick, remaining_delta, is_buy=False
            )
            trades.extend(queue_trades)
            consumed += queue_consumed

        return trades, consumed

    def _passive_queue_fill(
        self,
        order: Order,
        tick: TickData,
        remaining_delta: int,
        is_buy: bool,
    ) -> tuple[list[Trade], int]:
        """
        被动挂单排队估算。

        买单挂在 order.price 等待：如果 last_price <= order.price 且有成交量，
        按比例估算可获得的成交量。
        queue_factor=0.5 假设我们排在队列中间位置。
        返回 (trades, consumed_delta)。
        """
        trades: list[Trade] = []
        queue_factor = 0.5

        if remaining_delta <= 0:
            return trades, 0

        if is_buy:
            # 买单等卖方价格下来
            if tick.last_price <= order.price and tick.last_price > 0:
                # 估算该价格档的总挂单量，用 bid_volume 近似
                level_volume = tick.bid_volume_1 if tick.bid_volume_1 > 0 else 1
                estimated = self._round_lot(int(
                    remaining_delta
                    * (order.remaining / max(level_volume, order.remaining))
                    * queue_factor
                ))
                fill_vol = min(max(estimated, 0), order.remaining)
                if fill_vol > 0:
                    trade = self._make_trade(
                        order, order.price, fill_vol, tick.datetime
                    )
                    trades.append(trade)
                    return trades, fill_vol
        else:
            # 卖单等买方价格上来
            if tick.last_price >= order.price and tick.last_price > 0:
                level_volume = tick.ask_volume_1 if tick.ask_volume_1 > 0 else 1
                estimated = self._round_lot(int(
                    remaining_delta
                    * (order.remaining / max(level_volume, order.remaining))
                    * queue_factor
                ))
                fill_vol = min(max(estimated, 0), order.remaining)
                if fill_vol > 0:
                    trade = self._make_trade(
                        order, order.price, fill_vol, tick.datetime
                    )
                    trades.append(trade)
                    return trades, fill_vol

        return trades, 0

    # ────────────────────────────────────────────────
    #  统一入口
    # ────────────────────────────────────────────────

    def match_tick(self, tick: TickData) -> list[Trade]:
        """
        Tick 模式撮合统一入口。

        TICK_FILL → 即时全量。
        SMART_TICK_DELAY_FILL → 延迟深度。
        """
        if self.mode == MatchingMode.TICK_FILL:
            return self._match_tick_simple(tick)
        elif self.mode == MatchingMode.SMART_TICK_DELAY_FILL:
            self._current_tick_seq += 1
            return self._match_tick_smart(tick)
        else:
            return []
