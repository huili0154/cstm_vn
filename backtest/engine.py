"""
回测引擎 — 驱动 MatchingEngine + DataFeed + Strategy 的主循环。

三种撮合模式共用同一个 BacktestEngine，通过 MatchingMode 选择：
  - CLOSE_FILL:              日线驱动, on_day_begin(bar) 下单 → 收盘价成交
  - TICK_FILL:               tick 驱动, primary symbol 时间线推进
  - SMART_TICK_DELAY_FILL:   tick 驱动, 延迟+深度撮合
"""

from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from core.data_feed import ParquetBarFeed, ParquetTickFeed
from core.datatypes import (
    Account,
    BarData,
    Direction,
    MatchingMode,
    Order,
    OrderStatus,
    OrderType,
    Position,
    TickData,
    Trade,
)
from core.matching import MatchingEngine
from core.strategy import EngineBase, StrategyBase


logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """回测结果汇总。"""

    trades: list[Trade] = field(default_factory=list)
    daily_nav: list[tuple[str, float]] = field(default_factory=list)
    total_commission: float = 0.0
    start_balance: float = 0.0
    end_balance: float = 0.0


# ============================================================
#  Tick 预读线程: 后台提前加载未来 N 天的 tick 数据
# ============================================================


class _TickPrefetcher:
    """Multi-symbol tick 预读器。

    后台线程持续加载未来 ``look_ahead`` 天的 tick 数据，
    主线程通过 ``get(date)`` 获取已缓冲的数据，实现 I/O 与计算并行。
    """

    def __init__(
        self,
        tick_feed: "ParquetTickFeed",
        symbols: list[str],
        trading_dates: list[str],
        look_ahead: int = 10,
    ) -> None:
        self._tick_feed = tick_feed
        self._symbols = symbols
        self._dates = trading_dates
        self._look_ahead = look_ahead

        # date_str → {symbol: list[TickData]}
        self._cache: OrderedDict[str, dict[str, list]] = OrderedDict()
        self._lock = threading.Lock()
        self._ready = threading.Event()  # 当前请求的日期已就绪
        self._request_date: str | None = None
        self._stop = False

        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def _worker(self) -> None:
        """Background thread: sequentially load tick data ahead of main loop."""
        for date_str in self._dates:
            if self._stop:
                return

            # 读取所有品种当日 tick
            day_data: dict[str, list] = {}
            for sym in self._symbols:
                day_data[sym] = self._tick_feed.load_day(sym, date_str)

            with self._lock:
                self._cache[date_str] = day_data
                # 如果缓存太大，移除最早的 (已被消费的) 条目
                while len(self._cache) > self._look_ahead + 2:
                    self._cache.popitem(last=False)
                # 通知主线程
                if self._request_date and self._request_date in self._cache:
                    self._ready.set()

    def get(self, date_str: str) -> dict[str, list]:
        """Get tick data for *date_str*. Blocks until data is ready."""
        with self._lock:
            if date_str in self._cache:
                return self._cache[date_str]

        # 设置请求并等待
        self._ready.clear()
        self._request_date = date_str
        self._ready.wait()
        with self._lock:
            return self._cache.get(date_str, {})

    def stop(self) -> None:
        self._stop = True
        self._ready.set()  # unblock if waiting


class BacktestEngine(EngineBase):
    """
    回测引擎。实现 EngineBase 接口，驱动主循环。

    Parameters
    ----------
    dataset_dir : str | Path
        dataset 根目录 (含 daily/ 和 ticks/ 子目录)。
    mode : MatchingMode
        撮合模式。
    initial_capital : float
        初始资金。
    rate : float
        佣金费率。
    slippage : float
        固定滑点（close_fill / tick_fill 使用）。
    min_commission : float
        最低佣金。
    pricetick : float
        最小价格变动。
    volume_limit_ratio : float
        smart 模式每档可吃量比例。
    credit_ratio : float
        信用额度比例 (0~1)。买入时可用资金不足时，
        额外提供 总净值 × credit_ratio 的信用额度。
    """

    def __init__(
        self,
        dataset_dir: str | Path,
        mode: MatchingMode = MatchingMode.TICK_FILL,
        initial_capital: float = 1_000_000.0,
        rate: float = 0.00005,
        slippage: float = 0.0,
        min_commission: float = 0.0,
        pricetick: float = 0.001,
        volume_limit_ratio: float = 0.5,
        enable_t0: bool = False,
        credit_ratio: float = 0.0,
    ) -> None:
        self._dataset_dir = Path(dataset_dir)
        self._mode = mode
        self._initial_capital = initial_capital
        self._pricetick = pricetick
        self._enable_t0 = enable_t0
        self._credit_ratio = credit_ratio

        # 数据源
        self._bar_feed = ParquetBarFeed(dataset_dir)
        self._tick_feed = ParquetTickFeed(dataset_dir)

        # 撮合引擎
        self._matching = MatchingEngine(
            pricetick=pricetick,
            rate=rate,
            slippage=slippage,
            min_commission=min_commission,
            mode=mode,
            volume_limit_ratio=volume_limit_ratio,
        )

        # 策略引用 (run 时绑定)
        self._strategy: StrategyBase | None = None

        # 状态管理
        self._positions: dict[str, Position] = {}
        self._account = Account(balance=initial_capital)
        self._latest_ticks: dict[str, TickData] = {}

        # 结果记录
        self._all_trades: list[Trade] = []
        self._daily_nav: list[tuple[str, float]] = []  # (date_str, nav)
        self._logs: list[str] = []

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
        # 品种合法性检查：symbol 必须在策略订阅列表中
        if symbol not in strategy.symbols:
            msg = (
                f"Order REJECTED: symbol '{symbol}' not in "
                f"strategy.symbols {strategy.symbols}"
            )
            logger.warning(msg)
            self._logs.append(msg)
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

        # 资金/持仓前检
        if direction == Direction.BUY:
            est_cost = price * volume if price > 0 else self._estimate_market_price(symbol) * volume
            if est_cost > self._account.available + self._credit_line():
                msg = (
                    f"Order REJECTED: BUY {symbol} {volume}@{price:.4f} "
                    f"est_cost={est_cost:,.2f} > available={self._account.available:,.2f}"
                    f"+credit={self._credit_line():,.2f}"
                )
                logger.warning(msg)
                self._logs.append(msg)
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
            # 冻结资金
            self._account.frozen += est_cost
        else:
            pos = self._positions.get(symbol)
            if pos is None or pos.available < volume:
                avail = pos.available if pos else 0
                msg = (
                    f"Order REJECTED: SELL {symbol} {volume}@{price:.4f} "
                    f"available_pos={avail} < requested={volume}"
                )
                logger.warning(msg)
                self._logs.append(msg)
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
            # 冻结持仓
            pos.frozen += volume

        order = self._matching.submit_order(
            symbol=symbol,
            direction=direction,
            order_type=order_type,
            price=price,
            volume=volume,
        )
        return order.order_id

    def cancel_order(self, strategy: StrategyBase, order_id: str) -> None:
        # 撤单前先获取 order 以恢复冻结
        order = self._matching._all_orders.get(order_id)
        if order is None:
            return
        remaining = order.remaining
        symbol = order.symbol

        success = self._matching.cancel_order(order_id)
        if success:
            # 解冻
            if order.direction == Direction.BUY:
                self._account.frozen -= order.price * remaining
                self._account.frozen = max(0.0, self._account.frozen)
            else:
                pos = self._positions.get(symbol)
                if pos:
                    pos.frozen -= remaining
                    pos.frozen = max(0, pos.frozen)

    def cancel_all(self, strategy: StrategyBase) -> None:
        for order in self._matching.get_active_orders():
            self.cancel_order(strategy, order.order_id)

    def get_pending_orders(
        self, strategy: StrategyBase, symbol: str = ""
    ) -> list[Order]:
        return self._matching.get_pending_orders(symbol)

    def get_position(self, strategy: StrategyBase, symbol: str) -> Position:
        if symbol not in self._positions:
            self._positions[symbol] = Position(symbol=symbol, enable_t0=self._enable_t0)
        return self._positions[symbol]

    def get_account(self, strategy: StrategyBase) -> Account:
        return self._account

    def write_log(self, msg: str, strategy: StrategyBase) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        log_line = f"[{ts}] [{strategy.strategy_name}] {msg}"
        self._logs.append(log_line)
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
    #  回测主入口
    # ════════════════════════════════════════════════

    def run(
        self,
        strategy: StrategyBase,
        start_date: str,
        end_date: str,
        initial_positions: dict[str, tuple[int, float]] | None = None,
        progress_callback=None,
    ) -> BacktestResult:
        """
        运行回测。

        Parameters
        ----------
        strategy : StrategyBase
            策略实例 (已构造, 未 init)。
        start_date : str
            起始日期 YYYYMMDD。
        end_date : str
            结束日期 YYYYMMDD。
        initial_positions : dict[str, tuple[int, float]] | None
            初始持仓 {symbol: (volume, cost_price)}，可选。
        progress_callback : callable | None
            进度回调 callback(msg: str)，用于 GUI 实时显示。

        Returns
        -------
        BacktestResult
        """
        self._strategy = strategy
        self._progress_cb = progress_callback or (lambda msg: None)

        # 注册 matching 回调
        self._matching.set_on_order(self._on_matching_order)
        self._matching.set_on_trade(self._on_matching_trade)

        # 重置状态
        self._matching.reset()
        self._positions.clear()
        self._account = Account(balance=self._initial_capital)
        self._latest_ticks.clear()
        self._all_trades.clear()
        self._daily_nav.clear()
        self._logs.clear()

        # 注入初始持仓（从初始资金中扣除持仓成本）
        if initial_positions:
            for sym, (vol, cp) in initial_positions.items():
                self._positions[sym] = Position(symbol=sym, volume=vol, cost_price=cp, enable_t0=self._enable_t0)
                self._account.balance -= vol * cp

        # 策略生命周期
        self._progress_cb("正在加载日线数据 (on_init)...")
        strategy.on_init()
        strategy.inited = True
        strategy.on_start()
        strategy.trading = True

        self._progress_cb("数据加载完成，开始回测...")
        if self._mode == MatchingMode.CLOSE_FILL:
            self._run_close_fill(strategy, start_date, end_date)
        else:
            self._run_tick(strategy, start_date, end_date)

        # 收尾
        strategy.trading = False
        strategy.on_stop()

        return BacktestResult(
            trades=list(self._all_trades),
            daily_nav=list(self._daily_nav),
            total_commission=self._account.commission,
            start_balance=self._initial_capital,
            end_balance=self._calc_nav(),
        )

    # ════════════════════════════════════════════════
    #  CLOSE_FILL 主循环
    # ════════════════════════════════════════════════

    def _run_close_fill(
        self,
        strategy: StrategyBase,
        start_date: str,
        end_date: str,
    ) -> None:
        primary = strategy.symbols[0]
        bars = self._bar_feed.load(primary, start_date, end_date)
        if not bars:
            logger.warning(f"No daily bar data for {primary} in [{start_date}, {end_date}]")
            return

        for bar in bars:
            date_str = bar.datetime.strftime("%Y%m%d")

            # 先撮合昨日挂单
            self._matching.match_bar(bar)

            # 推给策略
            strategy.on_day_begin(bar)

            # 更新持仓市价 & 记录日净值
            self._update_market_prices_bar(bar)
            self._daily_nav.append((date_str, self._calc_nav()))

    # ════════════════════════════════════════════════
    #  TICK_FILL / SMART_TICK_DELAY_FILL 主循环
    # ════════════════════════════════════════════════

    def _run_tick(
        self,
        strategy: StrategyBase,
        start_date: str,
        end_date: str,
    ) -> None:
        primary = strategy.symbols[0]
        other_symbols = strategy.symbols[1:]

        # 获取交易日列表 — 优先从 tick 数据获取
        trading_dates = self._tick_feed.get_available_dates(primary)
        trading_dates = [
            d for d in trading_dates
            if (not start_date or d >= start_date)
            and (not end_date or d <= end_date)
        ]
        if not trading_dates:
            logger.warning(f"No tick data for {primary} in [{start_date}, {end_date}]")
            self._progress_cb(f"警告: {primary} 在 [{start_date}, {end_date}] 无 tick 数据")
            return

        total_days = len(trading_dates)
        self._progress_cb(f"共 {total_days} 个交易日待处理，正在加载日线索引...")

        # 预加载日线 (用于 on_day_begin, 可能不完全覆盖 tick 日期)
        bars_map: dict[str, BarData] = {}
        all_bars = self._bar_feed.load(primary)
        for b in all_bars:
            bars_map[b.datetime.strftime("%Y%m%d")] = b

        # 启动后台预读线程 (提前加载未来 10 天的 tick)
        all_symbols = [primary] + list(other_symbols)
        prefetcher = _TickPrefetcher(
            self._tick_feed, all_symbols, trading_dates, look_ahead=10
        )

        prev_bar: BarData | None = None

        try:
            for day_idx, date_str in enumerate(trading_dates, 1):
                self._progress_cb(f"处理交易日: {date_str} ({day_idx}/{total_days})")
                # ── 每日开始: 推送前日日线 (如有) ──
                if prev_bar is not None:
                    strategy.on_day_begin(prev_bar)

                # 新的一天: 重置 T+1 当日买入量
                self._reset_day()

                # 从预读缓存获取当日 tick (已由后台线程加载)
                day_ticks = prefetcher.get(date_str)
                primary_ticks = day_ticks.get(primary, [])
                other_ticks: dict[str, list[TickData]] = {
                    sym: day_ticks.get(sym, []) for sym in other_symbols
                }

                # 初始化游标
                cursors: dict[str, int] = {sym: 0 for sym in other_symbols}

                # 按 primary tick 驱动
                for primary_tick in primary_ticks:
                    # 跳过盘前无效数据
                    if primary_tick.last_price <= 0 or primary_tick.cum_volume <= 0:
                        continue

                    T = primary_tick.datetime

                    # 1. 推进非主品种到 ≤ T
                    for sym in other_symbols:
                        sym_tick_list = other_ticks[sym]
                        while cursors[sym] < len(sym_tick_list):
                            tick_s = sym_tick_list[cursors[sym]]
                            if tick_s.datetime > T:
                                break
                            # 跳过无效 tick
                            if tick_s.last_price <= 0 or tick_s.cum_volume <= 0:
                                cursors[sym] += 1
                                continue
                            self._latest_ticks[sym] = tick_s
                            # 有挂单才撮合
                            if self._has_pending_for(sym):
                                self._matching.match_tick(tick_s)
                            cursors[sym] += 1

                    # 2. 撮合主品种
                    self._latest_ticks[primary] = primary_tick
                    if self._has_pending_for(primary):
                        self._matching.match_tick(primary_tick)

                    # 3. 推给策略
                    strategy.on_tick(primary_tick)

                # 日终: 撤销未完成挂单，释放冻结资源
                self.cancel_all(strategy)

                # 日终: 更新所有持仓市价 & 记录净值
                bar_today = bars_map.get(date_str)
                if bar_today:
                    self._update_market_prices_bar(bar_today)
                # 用最新 tick 更新非主品种持仓市价
                for sym, pos in self._positions.items():
                    if pos.volume > 0 and sym != primary:
                        lt = self._latest_ticks.get(sym)
                        if lt and lt.last_price > 0:
                            pos.market_price = lt.last_price
                self._daily_nav.append((date_str, self._calc_nav()))

                prev_bar = bars_map.get(date_str)
        finally:
            prefetcher.stop()

    # ════════════════════════════════════════════════
    #  Matching 回调处理
    # ════════════════════════════════════════════════

    def _on_matching_order(self, order: Order) -> None:
        """MatchingEngine 订单状态变更回调。"""
        if not self._strategy:
            return

        # 撤单时解冻已在 cancel_order 中处理
        # 这里只负责转发给策略
        self._strategy.on_order(order)

    def _on_matching_trade(self, trade: Trade) -> None:
        """MatchingEngine 成交回调。更新持仓/资金后转发给策略。"""
        if not self._strategy:
            return

        self._all_trades.append(trade)

        # 更新持仓
        pos = self._positions.get(trade.symbol)
        if pos is None:
            pos = Position(symbol=trade.symbol, enable_t0=self._enable_t0)
            self._positions[trade.symbol] = pos

        if trade.direction == Direction.BUY:
            # 解冻资金 & 扣减
            order = self._matching._all_orders.get(trade.order_id)
            if order and order.order_type == OrderType.LIMIT:
                frozen_per_share = order.price
            else:
                frozen_per_share = trade.price
            self._account.frozen -= frozen_per_share * trade.volume
            self._account.frozen = max(0.0, self._account.frozen)

            actual_cost = trade.price * trade.volume + trade.commission
            self._account.balance -= actual_cost

            # 更新持仓成本
            total_cost = pos.cost_price * pos.volume + trade.price * trade.volume
            pos.volume += trade.volume
            pos.cost_price = total_cost / pos.volume if pos.volume > 0 else 0.0
            pos.today_bought += trade.volume
        else:
            # 卖出: 解冻持仓
            pos.frozen -= trade.volume
            pos.frozen = max(0, pos.frozen)
            pos.volume -= trade.volume

            # 资金回笼
            proceeds = trade.price * trade.volume - trade.commission
            self._account.balance += proceeds

        # 扣手续费
        self._account.commission += trade.commission

        # 更新市价
        pos.market_price = trade.price

        self._strategy.on_trade(trade)

    # ════════════════════════════════════════════════
    #  内部辅助
    # ════════════════════════════════════════════════

    def _credit_line(self) -> float:
        """基于总资产的信用额度 = 总净值 × credit_ratio。"""
        if self._credit_ratio <= 0:
            return 0.0
        return self._calc_nav() * self._credit_ratio

    def _has_pending_for(self, symbol: str) -> bool:
        """检查某个品种是否有活跃挂单。"""
        for order in self._matching._active_orders.values():
            if order.symbol == symbol:
                return True
        return False

    def _estimate_market_price(self, symbol: str) -> float:
        """估算市价 (用于市价单资金冻结)。"""
        tick = self._latest_ticks.get(symbol)
        if tick and tick.last_price > 0:
            return tick.last_price
        return 0.0

    def _update_market_prices_bar(self, bar: BarData) -> None:
        """用日线收盘价更新持仓市价。"""
        pos = self._positions.get(bar.symbol)
        if pos and pos.volume > 0:
            pos.market_price = bar.close_price

    def _calc_nav(self) -> float:
        """
        计算当前净值 = 账户总资金 + 所有持仓市值。

        注: Account.balance 定义为总资金（含冻结），
        所以这里不能再重复加 frozen。
        """
        nav = self._account.balance
        for pos in self._positions.values():
            if pos.volume > 0:
                nav += pos.volume * pos.market_price
        return nav

    def _reset_day(self) -> None:
        """日切操作: 重置 T+1 买入量。"""
        for pos in self._positions.values():
            pos.today_bought = 0
