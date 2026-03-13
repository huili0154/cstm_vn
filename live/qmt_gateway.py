"""
QMT Gateway — mini-QMT (xtquant) 适配层。

职责:
  1. 管理 xtdata / XtQuantTrader 连接。
  2. 订阅行情 → 转换为 cstm_vn TickData。
  3. 下单 / 撤单 → 转换为 xtquant 调用。
  4. 接收回报 → 转换为 cstm_vn Order / Trade / Position / Account。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from xtquant import xtconstant, xtdata
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import StockAccount

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

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════
#  常量映射
# ════════════════════════════════════════════════════

# QMT order_status → OrderStatus
_QMT_STATUS_MAP: dict[int, OrderStatus] = {
    48: OrderStatus.SUBMITTING,     # UNREPORTED
    49: OrderStatus.SUBMITTING,     # WAIT_REPORTING
    50: OrderStatus.ACTIVE,         # REPORTED
    51: OrderStatus.ACTIVE,         # REPORTED_CANCEL
    52: OrderStatus.ACTIVE,         # REPORTED_CANCEL (柜台)
    53: OrderStatus.PART_TRADED,    # PARTSUCC_CANCEL
    54: OrderStatus.CANCELLED,      # CANCELED
    55: OrderStatus.PART_TRADED,    # PART_SUCC
    56: OrderStatus.ALL_TRADED,     # ORDER_SUCCEEDED
    57: OrderStatus.REJECTED,       # ORDER_JUNK
}

# QMT order_type → Direction
_QMT_DIRECTION_MAP: dict[int, Direction] = {
    xtconstant.STOCK_BUY: Direction.BUY,
    xtconstant.STOCK_SELL: Direction.SELL,
}


# ════════════════════════════════════════════════════
#  回调实现
# ════════════════════════════════════════════════════

class _TraderCallback(XtQuantTraderCallback):
    """XtQuantTrader 回调桥。所有回调转发给 QmtGateway。"""

    def __init__(self, gateway: QmtGateway) -> None:
        super().__init__()
        self._gw = gateway

    def on_connected(self) -> None:
        logger.info("QMT trader connected")
        self._gw._on_connected()

    def on_disconnected(self) -> None:
        logger.warning("QMT trader disconnected")
        self._gw._on_disconnected()

    def on_stock_order(self, order: Any) -> None:
        self._gw._on_qmt_order(order)

    def on_stock_trade(self, trade: Any) -> None:
        self._gw._on_qmt_trade(trade)

    def on_order_error(self, order_error: Any) -> None:
        self._gw._on_qmt_order_error(order_error)

    def on_cancel_error(self, cancel_error: Any) -> None:
        logger.warning(
            f"Cancel error: order_id={cancel_error.order_id} "
            f"error={cancel_error.error_msg}"
        )

    def on_stock_asset(self, asset: Any) -> None:
        self._gw._on_qmt_asset(asset)

    def on_stock_position(self, position: Any) -> None:
        self._gw._on_qmt_position(position)

    def on_account_status(self, status: Any) -> None:
        logger.info(f"Account status update: {status}")


# ════════════════════════════════════════════════════
#  QmtGateway
# ════════════════════════════════════════════════════

class QmtGateway:
    """
    mini-QMT 网关。

    Parameters
    ----------
    qmt_path : str
        QMT userdata_mini 路径。
    account_id : str
        资金账号。
    session_id : int
        会话 ID，任意正整数即可。
    """

    def __init__(
        self,
        qmt_path: str,
        account_id: str,
        session_id: int = 1,
    ) -> None:
        self._qmt_path = qmt_path
        self._account = StockAccount(account_id)
        self._session_id = session_id

        self._trader: XtQuantTrader | None = None
        self._connected: bool = False

        # 外部回调 (由 LiveEngine 设置)
        self._on_tick_cb: Callable[[TickData], None] | None = None
        self._on_order_cb: Callable[[Order], None] | None = None
        self._on_trade_cb: Callable[[Trade], None] | None = None

    # ──────────── 连接管理 ────────────

    def connect(self) -> bool:
        """连接 xtdata + XtQuantTrader。成功返回 True。"""
        # 1. 行情连接
        xtdata.connect()
        logger.info("xtdata connected")

        # 2. 交易连接
        self._trader = XtQuantTrader(self._qmt_path, self._session_id)
        callback = _TraderCallback(self)
        self._trader.register_callback(callback)
        self._trader.start()

        ret = self._trader.connect()
        if ret == 0:
            self._connected = True
            logger.info("XtQuantTrader connected")
            return True
        else:
            logger.error(f"XtQuantTrader connect failed, ret={ret}")
            return False

    def disconnect(self) -> None:
        """断开连接。"""
        if self._trader:
            self._trader.stop()
            self._trader = None
        self._connected = False
        logger.info("QMT disconnected")

    @property
    def connected(self) -> bool:
        return self._connected

    # ──────────── 回调注册 ────────────

    def set_on_tick(self, cb: Callable[[TickData], None]) -> None:
        self._on_tick_cb = cb

    def set_on_order(self, cb: Callable[[Order], None]) -> None:
        self._on_order_cb = cb

    def set_on_trade(self, cb: Callable[[Trade], None]) -> None:
        self._on_trade_cb = cb

    # ──────────── 行情订阅 ────────────

    def subscribe(self, symbols: list[str]) -> None:
        """订阅实时 tick 行情。"""
        xtdata.subscribe_whole_quote(symbols, callback=self._on_whole_quote)
        logger.info(f"Subscribed tick for {symbols}")

    def unsubscribe(self, symbols: list[str]) -> None:
        """取消订阅。"""
        for sym in symbols:
            xtdata.unsubscribe_quote(sym)
        logger.info(f"Unsubscribed {symbols}")

    def get_full_tick(self, symbols: list[str]) -> dict[str, TickData]:
        """同步获取最新 tick 快照。"""
        raw = xtdata.get_full_tick(symbols)
        result: dict[str, TickData] = {}
        for sym, data in raw.items():
            result[sym] = self._convert_tick(sym, data)
        return result

    # ──────────── 下单 / 撤单 ────────────

    def send_order(
        self,
        symbol: str,
        direction: Direction,
        order_type: OrderType,
        price: float,
        volume: int,
        strategy_name: str = "",
        order_remark: str = "",
    ) -> str:
        """下单，返回 order_id 字符串。"""
        if not self._connected or not self._trader:
            logger.error("Cannot send order: not connected")
            return ""

        qmt_order_type = (
            xtconstant.STOCK_BUY
            if direction == Direction.BUY
            else xtconstant.STOCK_SELL
        )
        qmt_price_type = (
            xtconstant.FIX_PRICE
            if order_type == OrderType.LIMIT
            else xtconstant.LATEST_PRICE
        )

        order_id = self._trader.order_stock(
            self._account,
            symbol,
            qmt_order_type,
            volume,
            qmt_price_type,
            price,
            strategy_name,
            order_remark,
        )
        logger.info(
            f"Order sent: {symbol} {direction.value} {order_type.value} "
            f"price={price} vol={volume} → order_id={order_id}"
        )
        return str(order_id)

    def cancel_order(self, order_id: str) -> None:
        """撤单。"""
        if not self._connected or not self._trader:
            logger.error("Cannot cancel order: not connected")
            return
        self._trader.cancel_order_stock(self._account, int(order_id))
        logger.info(f"Cancel request sent: order_id={order_id}")

    # ──────────── 查询 ────────────

    def query_account(self) -> Account:
        """查询资金账户。"""
        if not self._connected or not self._trader:
            return Account()
        asset = self._trader.query_stock_asset(self._account)
        if asset is None:
            return Account()
        return Account(
            balance=asset.total_asset,
            frozen=asset.frozen_cash,
            commission=0.0,  # QMT 不直接提供累计佣金
        )

    def query_positions(self) -> dict[str, Position]:
        """查询所有持仓。"""
        if not self._connected or not self._trader:
            return {}
        positions = self._trader.query_stock_positions(self._account)
        result: dict[str, Position] = {}
        for p in positions:
            if p.volume > 0:
                result[p.stock_code] = Position(
                    symbol=p.stock_code,
                    volume=p.volume,
                    frozen=p.volume - p.can_use_volume,
                    cost_price=p.open_price,
                    market_price=p.market_value / p.volume if p.volume > 0 else 0.0,
                )
        return result

    def query_orders(self) -> list[Order]:
        """查询当日委托。"""
        if not self._connected or not self._trader:
            return []
        qmt_orders = self._trader.query_stock_orders(self._account)
        return [self._convert_order(o) for o in qmt_orders]

    def query_trades(self) -> list[Trade]:
        """查询当日成交。"""
        if not self._connected or not self._trader:
            return []
        qmt_trades = self._trader.query_stock_trades(self._account)
        return [self._convert_trade(t) for t in qmt_trades]

    # ════════════════════════════════════════════════
    #  内部回调
    # ════════════════════════════════════════════════

    def _on_connected(self) -> None:
        self._connected = True

    def _on_disconnected(self) -> None:
        self._connected = False

    def _on_whole_quote(self, data: dict) -> None:
        """subscribe_whole_quote 推送回调。"""
        for sym, raw in data.items():
            tick = self._convert_tick(sym, raw)
            if self._on_tick_cb:
                self._on_tick_cb(tick)

    def _on_qmt_order(self, qmt_order: Any) -> None:
        """交易回调: 委托状态变化。"""
        order = self._convert_order(qmt_order)
        logger.info(
            f"Order update: {order.symbol} {order.direction.value} "
            f"status={order.status.value} traded={order.traded}/{order.volume}"
        )
        if self._on_order_cb:
            self._on_order_cb(order)

    def _on_qmt_trade(self, qmt_trade: Any) -> None:
        """交易回调: 成交回报。"""
        trade = self._convert_trade(qmt_trade)
        logger.info(
            f"Trade: {trade.symbol} {trade.direction.value} "
            f"price={trade.price} vol={trade.volume}"
        )
        if self._on_trade_cb:
            self._on_trade_cb(trade)

    def _on_qmt_order_error(self, order_error: Any) -> None:
        """下单错误。"""
        logger.error(
            f"Order error: order_id={order_error.order_id} "
            f"error_id={order_error.error_id} "
            f"error_msg={order_error.error_msg}"
        )
        # 构造一个 REJECTED 订单推给策略
        order = Order(
            order_id=str(order_error.order_id),
            symbol="",
            direction=Direction.BUY,
            order_type=OrderType.LIMIT,
            price=0,
            volume=0,
            status=OrderStatus.REJECTED,
        )
        if self._on_order_cb:
            self._on_order_cb(order)

    def _on_qmt_asset(self, asset: Any) -> None:
        """资金变化推送（仅记录日志）。"""
        logger.debug(
            f"Asset update: total={asset.total_asset} "
            f"cash={asset.cash} frozen={asset.frozen_cash}"
        )

    def _on_qmt_position(self, position: Any) -> None:
        """持仓变化推送（仅记录日志）。"""
        logger.debug(
            f"Position update: {position.stock_code} "
            f"vol={position.volume} available={position.can_use_volume}"
        )

    # ════════════════════════════════════════════════
    #  数据转换
    # ════════════════════════════════════════════════

    @staticmethod
    def _convert_tick(symbol: str, raw: dict) -> TickData:
        """将 xtdata tick dict 转换为 TickData。"""
        # 时间: timetag 为 毫秒时间戳 (float)
        timetag = raw.get("timetag", 0)
        if timetag:
            dt = datetime.fromtimestamp(timetag / 1000.0)
        else:
            dt = datetime.now()

        ask_prices = raw.get("askPrice", [0.0] * 5)
        ask_vols = raw.get("askVol", [0] * 5)
        bid_prices = raw.get("bidPrice", [0.0] * 5)
        bid_vols = raw.get("bidVol", [0] * 5)

        # 安全取值 (QMT 5 档, 我们 10 档, 6-10 留 0)
        def _ap(i: int) -> float:
            return float(ask_prices[i]) if i < len(ask_prices) else 0.0

        def _av(i: int) -> int:
            return int(ask_vols[i]) if i < len(ask_vols) else 0

        def _bp(i: int) -> float:
            return float(bid_prices[i]) if i < len(bid_prices) else 0.0

        def _bv(i: int) -> int:
            return int(bid_vols[i]) if i < len(bid_vols) else 0

        return TickData(
            symbol=symbol,
            datetime=dt,
            last_price=float(raw.get("lastPrice", 0)),
            cum_volume=int(raw.get("volume", 0)),
            cum_turnover=float(raw.get("amount", 0)),
            open_price=float(raw.get("open", 0)),
            high_price=float(raw.get("high", 0)),
            low_price=float(raw.get("low", 0)),
            pre_close=float(raw.get("lastClose", 0)),
            bid_price_1=_bp(0), bid_volume_1=_bv(0),
            ask_price_1=_ap(0), ask_volume_1=_av(0),
            bid_price_2=_bp(1), bid_volume_2=_bv(1),
            ask_price_2=_ap(1), ask_volume_2=_av(1),
            bid_price_3=_bp(2), bid_volume_3=_bv(2),
            ask_price_3=_ap(2), ask_volume_3=_av(2),
            bid_price_4=_bp(3), bid_volume_4=_bv(3),
            ask_price_4=_ap(3), ask_volume_4=_av(3),
            bid_price_5=_bp(4), bid_volume_5=_bv(4),
            ask_price_5=_ap(4), ask_volume_5=_av(4),
        )

    @staticmethod
    def _convert_order(qmt_order: Any) -> Order:
        """将 xtquant 委托对象转换为 Order。"""
        direction = _QMT_DIRECTION_MAP.get(
            qmt_order.order_type, Direction.BUY
        )
        order_type = (
            OrderType.MARKET
            if qmt_order.price_type == xtconstant.LATEST_PRICE
            else OrderType.LIMIT
        )
        status = _QMT_STATUS_MAP.get(
            qmt_order.order_status, OrderStatus.SUBMITTING
        )
        create_time = None
        if hasattr(qmt_order, "order_time") and qmt_order.order_time:
            try:
                create_time = datetime.fromtimestamp(qmt_order.order_time)
            except (ValueError, OSError):
                pass

        return Order(
            order_id=str(qmt_order.order_id),
            symbol=qmt_order.stock_code,
            direction=direction,
            order_type=order_type,
            price=qmt_order.price,
            volume=qmt_order.order_volume,
            traded=qmt_order.traded_volume,
            status=status,
            create_time=create_time,
        )

    @staticmethod
    def _convert_trade(qmt_trade: Any) -> Trade:
        """将 xtquant 成交对象转换为 Trade。"""
        direction = _QMT_DIRECTION_MAP.get(
            qmt_trade.order_type, Direction.BUY
        )
        traded_time = None
        if hasattr(qmt_trade, "traded_time") and qmt_trade.traded_time:
            try:
                traded_time = datetime.fromtimestamp(qmt_trade.traded_time)
            except (ValueError, OSError):
                pass

        return Trade(
            trade_id=str(qmt_trade.traded_id),
            order_id=str(qmt_trade.order_id),
            symbol=qmt_trade.stock_code,
            direction=direction,
            price=qmt_trade.traded_price,
            volume=qmt_trade.traded_volume,
            commission=qmt_trade.commission if hasattr(qmt_trade, "commission") else 0.0,
            datetime=traded_time,
        )
