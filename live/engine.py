"""
实盘引擎 — 实现 EngineBase 接口，驱动 QmtGateway + Strategy。

LiveEngine 负责:
  1. 连接 QMT 网关
  2. 订阅行情，将 tick 推给策略
  3. 将策略下单/撤单请求转发给 QMT
  4. 接收 QMT 回报，转发到策略回调
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime

from core.datatypes import (
    Account,
    Direction,
    Order,
    OrderStatus,
    OrderType,
    Position,
    TickData,
    Trade,
)
from core.strategy import EngineBase, StrategyBase
from live.qmt_gateway import QmtGateway

logger = logging.getLogger(__name__)


class LiveEngine(EngineBase):
    """
    实盘引擎。

    Parameters
    ----------
    gateway : QmtGateway
        已构造但未连接的 QMT 网关。
    pricetick : float
        ETF 最小价格变动，默认 0.001。
    """

    def __init__(
        self,
        gateway: QmtGateway,
        pricetick: float = 0.001,
    ) -> None:
        self._gateway = gateway
        self._pricetick = pricetick

        self._strategy: StrategyBase | None = None
        self._running: bool = False

        # 缓存
        self._latest_ticks: dict[str, TickData] = {}
        self._active_orders: dict[str, Order] = {}

        # 日志 (GUI 可读)
        self._logs: list[str] = []
        self._lock = threading.Lock()

        # 外部回调 (GUI 挂接)
        self.on_log_callback: None | callable = None
        self.on_tick_callback: None | callable = None
        self.on_order_callback: None | callable = None
        self.on_trade_callback: None | callable = None

    # ════════════════════════════════════════════════
    #  EngineBase 接口实现
    # ════════════════════════════════════════════════

    def send_order(
        self,
        strategy: StrategyBase,
        symbol: str,
        direction: Direction,
        order_type: OrderType,
        price: float,
        volume: int,
    ) -> str:
        # 品种合法性检查
        if symbol not in strategy.symbols:
            msg = (
                f"Order REJECTED: symbol '{symbol}' not in "
                f"strategy.symbols {strategy.symbols}"
            )
            logger.warning(msg)
            self._append_log(msg)
            order = Order(
                order_id="REJECTED",
                symbol=symbol,
                direction=direction,
                order_type=order_type,
                price=price,
                volume=volume,
                status=OrderStatus.REJECTED,
            )
            strategy.on_order(order)
            return ""

        if not self._running:
            logger.warning("Engine not running, order rejected")
            return ""

        order_id = self._gateway.send_order(
            symbol=symbol,
            direction=direction,
            order_type=order_type,
            price=price,
            volume=volume,
            strategy_name=strategy.strategy_name,
        )
        if order_id:
            # 本地记录活跃委托
            order = Order(
                order_id=order_id,
                symbol=symbol,
                direction=direction,
                order_type=order_type,
                price=price,
                volume=volume,
                status=OrderStatus.SUBMITTING,
                create_time=datetime.now(),
            )
            with self._lock:
                self._active_orders[order_id] = order
        return order_id

    def cancel_order(self, strategy: StrategyBase, order_id: str) -> None:
        self._gateway.cancel_order(order_id)

    def cancel_all(self, strategy: StrategyBase) -> None:
        with self._lock:
            order_ids = list(self._active_orders.keys())
        for oid in order_ids:
            self._gateway.cancel_order(oid)

    def get_pending_orders(
        self, strategy: StrategyBase, symbol: str = ""
    ) -> list[Order]:
        with self._lock:
            orders = list(self._active_orders.values())
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        return orders

    def get_position(self, strategy: StrategyBase, symbol: str) -> Position:
        positions = self._gateway.query_positions()
        return positions.get(symbol, Position(symbol=symbol))

    def get_account(self, strategy: StrategyBase) -> Account:
        return self._gateway.query_account()

    def write_log(self, msg: str, strategy: StrategyBase) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        log_line = f"[{ts}] [{strategy.strategy_name}] {msg}"
        self._append_log(log_line)
        logger.info(log_line)

    def get_pricetick(self, strategy: StrategyBase, symbol: str) -> float:
        return self._pricetick

    def get_latest_tick(
        self, strategy: StrategyBase, symbol: str
    ) -> TickData | None:
        if symbol not in strategy.symbols:
            raise ValueError(
                f"Symbol '{symbol}' not in strategy.symbols {strategy.symbols}. "
                f"Add it to the symbols list in strategy initialization."
            )
        return self._latest_ticks.get(symbol)

    # ════════════════════════════════════════════════
    #  生命周期
    # ════════════════════════════════════════════════

    def start(self, strategy: StrategyBase) -> bool:
        """
        启动实盘。

        1. 连接 QMT
        2. 注册回调
        3. 策略初始化 → 启动
        4. 订阅行情

        Returns
        -------
        bool
            是否启动成功。
        """
        self._strategy = strategy

        # 连接网关
        if not self._gateway.connected:
            if not self._gateway.connect():
                logger.error("Failed to connect QMT gateway")
                return False

        # 注册回调
        self._gateway.set_on_tick(self._on_tick)
        self._gateway.set_on_order(self._on_order)
        self._gateway.set_on_trade(self._on_trade)

        # 策略生命周期
        strategy.on_init()
        strategy.inited = True
        strategy.on_start()
        strategy.trading = True
        self._running = True

        # 订阅行情
        self._gateway.subscribe(strategy.symbols)

        # 获取初始快照
        ticks = self._gateway.get_full_tick(strategy.symbols)
        self._latest_ticks.update(ticks)

        self._append_log(f"LiveEngine started: {strategy.strategy_name}")
        logger.info(f"LiveEngine started: {strategy.strategy_name}")
        return True

    def stop(self) -> None:
        """停止实盘。"""
        if not self._running:
            return

        self._running = False

        if self._strategy:
            self._strategy.trading = False
            self._strategy.on_stop()

        self._gateway.unsubscribe(
            self._strategy.symbols if self._strategy else []
        )

        self._append_log("LiveEngine stopped")
        logger.info("LiveEngine stopped")

    # ════════════════════════════════════════════════
    #  内部回调
    # ════════════════════════════════════════════════

    def _on_tick(self, tick: TickData) -> None:
        """收到 tick 行情推送。"""
        self._latest_ticks[tick.symbol] = tick

        if not self._running or not self._strategy:
            return

        # 只有订阅品种列表中的第一只 (primary) 触发 on_tick
        if tick.symbol == self._strategy.symbols[0]:
            self._strategy.on_tick(tick)

        # GUI 回调
        if self.on_tick_callback:
            self.on_tick_callback(tick)

    def _on_order(self, order: Order) -> None:
        """收到委托状态更新。"""
        with self._lock:
            if order.is_active:
                self._active_orders[order.order_id] = order
            else:
                self._active_orders.pop(order.order_id, None)

        if self._strategy:
            self._strategy.on_order(order)

        if self.on_order_callback:
            self.on_order_callback(order)

    def _on_trade(self, trade: Trade) -> None:
        """收到成交回报。"""
        if self._strategy:
            self._strategy.on_trade(trade)

        if self.on_trade_callback:
            self.on_trade_callback(trade)

    # ════════════════════════════════════════════════
    #  辅助
    # ════════════════════════════════════════════════

    def _append_log(self, msg: str) -> None:
        self._logs.append(msg)
        if self.on_log_callback:
            self.on_log_callback(msg)

    @property
    def logs(self) -> list[str]:
        return list(self._logs)
