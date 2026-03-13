"""
MSTR — Multi-Stock Tick-level Reversion Strategy.

设计文档: docs/design_mstr_strategy.md

在一组高相关性 ETF 中，利用品种间价格比率（Ratio）均值回归特性，
在 Ratio 偏离均值超过阈值时执行 Block 换仓操作。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from enum import Enum
from pathlib import Path

from core.data_feed import ParquetBarFeed
from core.datatypes import BarData, Direction, Order, OrderStatus, TickData, Trade
from core.strategy import StrategyBase


# ════════════════════════════════════════════════════════════════
#  Section 1: Data Structures
# ════════════════════════════════════════════════════════════════


class BlockState(Enum):
    """Block 交易状态 (设计文档 3.5.6, 3.5.9)"""

    # ── Rotation mode ──
    PENDING = "PENDING"
    MATCHING = "MATCHING"
    CHASING = "CHASING"
    DONE = "DONE"
    TIMEOUT = "TIMEOUT"
    CRITICAL = "CRITICAL"
    # ── Pure buy mode ──
    PENDING_BUY = "PENDING_BUY"
    FILLING = "FILLING"


@dataclass
class SubOrder:
    """子单跟踪（设计文档 3.5.1）"""

    order_id: str
    side: str  # "buy" or "sell"
    symbol: str
    volume: int
    price: float
    filled: int = 0
    is_aggressive: bool = False
    is_active: bool = True


@dataclass
class LogEvent:
    """Block 交易日志事件（设计文档 3.7.2）"""

    time: datetime
    tick_seq: int
    event_type: str
    detail: dict = field(default_factory=dict)


@dataclass
class BlockTrade:
    """Block 交易完整状态（设计文档 3.7.1）"""

    # ── 交易标识 ──
    block_id: str
    trade_date: str
    block_seq: int
    mode: str  # "rotation" or "pure_buy"

    # ── 决策快照 ──
    signal_time: datetime | None = None
    sell_symbol: str = ""
    buy_symbol: str = ""
    trigger_score: float = 0.0
    trigger_dev: float = 0.0
    trigger_max_dev: float = 0.0
    trigger_ratio: float = 0.0
    mu: float = 0.0
    candidate_list: list = field(default_factory=list)
    chosen_reason: str = ""

    # ── 市场快照 ──
    buy_bid1: float = 0.0
    buy_ask1: float = 0.0
    sell_bid1: float = 0.0
    sell_ask1: float = 0.0
    buy_signal_price: float = 0.0   # 信号触发时买入品种 last_price
    sell_signal_price: float = 0.0  # 信号触发时卖出品种 last_price

    # ── 账户快照 (信号触发时) ──
    signal_cash: float = 0.0   # 信号触发时现金 (balance)
    signal_nav: float = 0.0    # 信号触发时账户总市值

    # ── 账户快照 (block 结束时) ──
    end_cash: float = 0.0      # block 执行完后的现金 (balance)
    end_nav: float = 0.0       # block 执行完后的账户总市值

    # ── 执行参数 ──
    block_size: float = 0.0
    cash_blocks_at_start: int = 0
    buy_target: int = 0
    sell_target: int = 0
    n_sub_lots: int = 5

    # ── State tracking ──
    state: BlockState = BlockState.PENDING
    buy_filled: int = 0
    sell_filled: int = 0
    buy_aggressive: int = 0  # 累计已提交的激进买量
    sell_aggressive: int = 0  # 累计已提交的激进卖量

    # ── Sub-orders ──
    buy_orders: list = field(default_factory=list)
    sell_orders: list = field(default_factory=list)

    # ── Timing ──
    start_time: datetime | None = None
    first_fill_time: datetime | None = None
    timeout_deadline: datetime | None = None

    # ── Chase state ──
    chase_order_id: str = ""
    chase_side: str = ""
    chase_submit_tick: int = 0
    chase_round: int = 0

    # ── Forced aggressive flag ──
    force_aggressive_sent: bool = False

    # ── Events ──
    events: list = field(default_factory=list)

    # ── Results ──
    end_time: datetime | None = None
    buy_cost: float = 0.0
    sell_proceeds: float = 0.0
    buy_commission: float = 0.0
    sell_commission: float = 0.0

    @property
    def buy_avg_price(self) -> float:
        return self.buy_cost / self.buy_filled if self.buy_filled > 0 else 0.0

    @property
    def sell_avg_price(self) -> float:
        return self.sell_proceeds / self.sell_filled if self.sell_filled > 0 else 0.0

    @property
    def total_duration(self) -> float:
        if self.signal_time and self.end_time:
            return (self.end_time - self.signal_time).total_seconds()
        return 0.0

    def add_event(self, dt: datetime, tick_seq: int, event_type: str, **detail):
        self.events.append(
            LogEvent(time=dt, tick_seq=tick_seq, event_type=event_type, detail=detail)
        )


# ════════════════════════════════════════════════════════════════
#  Section 2: MSTR Strategy
# ════════════════════════════════════════════════════════════════


class MstrStrategy(StrategyBase):
    """
    MSTR — Multi-Stock Tick-level Reversion Strategy.

    信号层:  品种对 Ratio 滚动偏离 → Score 触发
    执行层:  Block 资金分块 + 双边被动挂单 + Fill Matching
    """

    author = "cstm_vn"

    # ── 可配置参数 ──
    parameters = [
        "window",
        "k_threshold",
        "least_bias",
        "num_blocks",
        "sub_lots",
        "cooldown_1",
        "cooldown_2",
        "chase_wait_ticks",
        "block_timeout",
        "near_optimal_delta",
        "trading_cutoff_str",
        "max_positions",
        "max_single_weight",
        "min_cash_reserve",
        "enable_t0",
        "dataset_dir",
    ]

    variables = [
        "cash_blocks",
        "day_block_count",
    ]

    # ── Signal parameters (5.1) ──
    window: int = 20
    k_threshold: float = 0.8
    least_bias: float = 0.01

    # ── Execution parameters (5.2) ──
    num_blocks: int = 5
    sub_lots: int = 5
    cooldown_1: int = 10  # minutes
    cooldown_2: int = 15  # minutes
    chase_wait_ticks: int = 2
    max_chase_rounds: int = 30
    block_timeout: int = 20  # minutes
    near_optimal_delta: float = 0.1
    trading_cutoff_str: str = "14:55:00"

    # ── Position parameters (5.3) ──
    max_positions: int = 3
    max_single_weight: float = 0.5
    min_cash_reserve: float = 100.0

    # ── T+0 parameter ──
    enable_t0: bool = False

    # ── Dataset ──
    dataset_dir: str = ""

    # ────────────────────────────────────────────────
    #  Constructor
    # ────────────────────────────────────────────────

    def __init__(self, engine, strategy_name, symbols, setting=None):
        super().__init__(engine, strategy_name, symbols, setting)

        # Derived parameters (computed in on_init)
        self.max_block_per_symbol: int = 0
        self.trading_cutoff: time = time(14, 55)

        # ── Pre-loaded daily data (loaded once in on_init) ──
        self._bar_feed: ParquetBarFeed | None = None
        # {symbol: {date_str: (close_adj, adj_factor)}}
        self._all_daily: dict[str, dict[str, tuple[float, float]]] = {}

        # ── Signal model working state ──
        # {symbol: [close_adj_day1, ..., close_adj_dayN-1]}  sliding window
        self._daily_closes: dict[str, list[float]] = {}
        # {symbol: adj_factor}
        self._adj_factors: dict[str, float] = {}
        # Pair statistics: {(A, B): value}
        self._mu: dict[tuple[str, str], float] = {}
        self._max_val: dict[tuple[str, str], float] = {}
        self._max_dev: dict[tuple[str, str], float] = {}

        # ── Block management ──
        self.block_count: dict[str, int] = {}
        self.cash_blocks: int = 0

        # ── Cooldown ──
        self.day_block_count: int = 0
        self._cooldown_end: datetime | None = None

        # ── Active block ──
        self._active_block: BlockTrade | None = None
        self._block_seq_today: int = 0

        # ── Tick counting (per day, reset in on_day_begin) ──
        self._tick_count: int = 0

        # ── Trade log archive ──
        self._block_logs: list[BlockTrade] = []

        # ── Order tracking ──
        self._order_map: dict[str, SubOrder] = {}

        # ── Current adjusted prices ──
        self._current_adj_prices: dict[str, float] = {}

        # ── Day tracking ──
        self._current_date: str = ""
        self._stats_initialized: bool = False

        # ── Recovery queue for one-sided CRITICAL blocks ──
        self._recovery_queue: list[dict] = []

        # ── Daily params archive (for visualization) ──
        # {date_str: {"mu": {(A,B): float}, "max_dev": {(A,B): float}, "adj": {sym: float}}}
        self._daily_params: dict[str, dict] = {}

    # ────────────────────────────────────────────────
    #  on_init  (设计文档 Section 6)
    # ────────────────────────────────────────────────

    def on_init(self):
        # Derived params
        self.max_block_per_symbol = math.floor(
            self.num_blocks * self.max_single_weight
        )
        h, m, s = (int(x) for x in self.trading_cutoff_str.split(":"))
        self.trading_cutoff = time(h, m, s)
        self.cash_blocks = self.num_blocks

        # Load all daily bar data for all symbols
        self._bar_feed = ParquetBarFeed(self.dataset_dir)
        for symbol in self.symbols:
            bars = self._bar_feed.load(symbol)
            daily = {}
            for bar in bars:
                d = bar.datetime.strftime("%Y%m%d")
                daily[d] = (bar.close_price, bar.adj_factor)
            self._all_daily[symbol] = daily

        self.write_log(
            f"MSTR on_init: {len(self.symbols)} symbols loaded, "
            f"window={self.window}, num_blocks={self.num_blocks}, "
            f"max_block_per_symbol={self.max_block_per_symbol}"
        )

    # ────────────────────────────────────────────────
    #  _initialize_stats — called once on first tick
    # ────────────────────────────────────────────────

    def _initialize_stats(self, current_date: str):
        """Build _daily_closes from bars strictly before current_date."""
        for symbol in self.symbols:
            daily = self._all_daily.get(symbol, {})
            sorted_dates = sorted(d for d in daily if d < current_date)
            self._daily_closes[symbol] = [daily[d][0] for d in sorted_dates]
            if sorted_dates:
                self._adj_factors[symbol] = daily[sorted_dates[-1]][1]
            else:
                self._adj_factors[symbol] = 1.0

        self._recompute_pair_stats()
        self._stats_initialized = True

    # ────────────────────────────────────────────────
    #  on_day_begin  (设计文档 Section 6)
    # ────────────────────────────────────────────────

    def on_day_begin(self, bar: BarData):
        """
        每个交易日开盘前调用。
        Tick 模式下 bar 是前一个交易日的主品种 BarData。
        """
        if not bar:
            return

        prev_date = bar.datetime.strftime("%Y%m%d")

        # Append previous day's close for all symbols
        for symbol in self.symbols:
            if symbol == bar.symbol:
                close_adj = bar.close_price
                adj = bar.adj_factor
            else:
                entry = self._all_daily.get(symbol, {}).get(prev_date)
                if entry:
                    close_adj, adj = entry
                else:
                    continue
            self._daily_closes.setdefault(symbol, []).append(close_adj)
            self._adj_factors[symbol] = adj

        # Trim history to avoid unbounded growth
        max_history = self.window * 3
        for symbol in self.symbols:
            closes = self._daily_closes.get(symbol, [])
            if len(closes) > max_history:
                self._daily_closes[symbol] = closes[-max_history:]

        # Recompute pair statistics
        self._recompute_pair_stats()

        # ── Reset daily state ──
        self.day_block_count = 0
        self._block_seq_today = 0
        self._cooldown_end = None
        self._tick_count = 0

        # ── Cross-day integrity check (设计文档 Section 6 跨日一致性校验) ──
        self._cross_day_integrity_check()

    # ────────────────────────────────────────────────
    #  on_tick  (设计文档 Section 6)
    # ────────────────────────────────────────────────

    def on_tick(self, tick: TickData):
        # Filter invalid ticks (设计文档 D1)
        if tick.last_price <= 0:
            return
        tick_time = tick.datetime.time()
        if tick_time < time(9, 30) or tick_time >= time(15, 0):
            return

        self._tick_count += 1
        date_str = tick.datetime.strftime("%Y%m%d")
        if date_str != self._current_date:
            self._current_date = date_str

        # Lazy initialization on first tick
        if not self._stats_initialized:
            self._initialize_stats(date_str)

        # Check warmup
        min_len = min(
            (len(self._daily_closes.get(s, [])) for s in self.symbols),
            default=0,
        )
        if min_len < self.window - 1:
            return

        # ── Archive daily params for visualization (once per day) ──
        if date_str not in self._daily_params:
            self._daily_params[date_str] = {
                "mu": dict(self._mu),
                "max_dev": dict(self._max_dev),
                "adj": dict(self._adj_factors),
            }

        # ── Collect current adjusted prices ──
        self._update_current_prices(tick)

        # ── Trading cutoff (设计文档 3.8) ──
        past_cutoff = tick_time >= self.trading_cutoff

        if past_cutoff and self._active_block:
            self._force_cutoff_timeout(self._active_block, tick)
            return

        # Hard deadline: cancel everything before 14:57
        if tick_time >= time(14, 56, 50):
            if self.get_pending_orders():
                self.cancel_all()
            return

        # ── Manage active block ──
        if self._active_block:
            self._manage_active_block(tick)
            return

        # ── No active block ──
        if past_cutoff:
            return

        # Check cooldown
        if self._cooldown_end and tick.datetime < self._cooldown_end:
            return

        # Block 时间安全检查 (设计文档 3.8):
        # 剩余时间不足 block_timeout 则不启动新 Block，避免刚启动就被 cutoff 强制超时
        cutoff_dt = tick.datetime.replace(
            hour=self.trading_cutoff.hour,
            minute=self.trading_cutoff.minute,
            second=self.trading_cutoff.second,
        )
        remaining_min = (cutoff_dt - tick.datetime).total_seconds() / 60
        if remaining_min < self.block_timeout:
            return

        # ── Recovery check: prioritize recovery blocks ──
        if self._recovery_queue:
            self._try_recovery(tick)
            return

        # ── Signal calculation and block trigger ──
        held_symbols = [s for s, cnt in self.block_count.items() if cnt > 0]
        if held_symbols:
            self._try_rotation(tick, held_symbols)
        else:
            # Empty portfolio or only cash
            if self.cash_blocks > 0:
                self._try_pure_buy(tick)

    # ────────────────────────────────────────────────
    #  on_order
    # ────────────────────────────────────────────────

    def on_order(self, order: Order):
        sub = self._order_map.get(order.order_id)
        if not sub:
            return

        if order.status == OrderStatus.REJECTED:
            sub.is_active = False
            if self._active_block:
                self._active_block.add_event(
                    order.update_time or datetime.min,
                    self._tick_count,
                    "ORDER_REJECTED",
                    order_id=order.order_id,
                    side=sub.side,
                    volume=sub.volume,
                )
            return

        if order.status in (OrderStatus.CANCELLED, OrderStatus.PART_CANCELLED):
            sub.is_active = False
            if self._active_block:
                unfilled = sub.volume - sub.filled
                self._active_block.add_event(
                    order.update_time or datetime.min,
                    self._tick_count,
                    "CANCEL_CONFIRMED",
                    order_id=order.order_id,
                    side=sub.side,
                    cancelled_qty=unfilled,
                    filled_qty=sub.filled,
                )

        if order.status == OrderStatus.ALL_TRADED:
            sub.is_active = False
            # sub.filled 由 on_trade 维护，此处仅更新 is_active

    # ────────────────────────────────────────────────
    #  on_trade
    # ────────────────────────────────────────────────

    def on_trade(self, trade: Trade):
        sub = self._order_map.get(trade.order_id)
        if not sub:
            return

        block = self._active_block
        if not block:
            return

        # Update sub-order filled
        sub.filled += trade.volume

        # Update block aggregates
        if sub.side == "buy":
            block.buy_filled += trade.volume
            block.buy_cost += trade.price * trade.volume
            block.buy_commission += trade.commission
            block.add_event(
                trade.datetime,
                self._tick_count,
                "AGGRESSIVE_FILL" if sub.is_aggressive else "PASSIVE_FILL",
                order_id=trade.order_id,
                filled_qty=trade.volume,
                fill_price=trade.price,
                cum_filled=block.buy_filled,
            )
        else:
            block.sell_filled += trade.volume
            block.sell_proceeds += trade.price * trade.volume
            block.sell_commission += trade.commission
            block.add_event(
                trade.datetime,
                self._tick_count,
                "AGGRESSIVE_FILL" if sub.is_aggressive else "PASSIVE_FILL",
                order_id=trade.order_id,
                filled_qty=trade.volume,
                fill_price=trade.price,
                cum_filled=block.sell_filled,
            )

        # ── Pure buy: first fill starts timeout timer (设计文档 3.5.9) ──
        if block.mode == "pure_buy" and block.state == BlockState.PENDING_BUY:
            block.state = BlockState.FILLING
            block.first_fill_time = trade.datetime
            block.timeout_deadline = trade.datetime + timedelta(
                minutes=self.block_timeout
            )

        # ── Rotation / Recovery: transition PENDING → MATCHING ──
        if block.mode in ("rotation", "recovery_sell") and block.state == BlockState.PENDING:
            block.state = BlockState.MATCHING

        # ── Check if block is complete ──
        if self._is_block_complete(block):
            self._complete_block(block, trade.datetime)
            return

        # ── One-side complete → immediate chase (设计文档 3.5.3a) ──
        if block.mode == "rotation" and block.state in (
            BlockState.PENDING, BlockState.MATCHING, BlockState.CHASING,
        ):
            if (block.sell_filled >= block.sell_target
                    and block.buy_filled < block.buy_target):
                self._start_immediate_chase(block, "buy", trade.datetime)
                return
            if (block.buy_filled >= block.buy_target
                    and block.sell_filled < block.sell_target):
                self._start_immediate_chase(block, "sell", trade.datetime)
                return

        # ── Fill Matching: check excess (设计文档 3.5.3) ──
        if block.mode == "rotation" and block.state in (
            BlockState.MATCHING,
            BlockState.CHASING,
        ):
            self._check_and_handle_excess(block, trade.datetime)

    # ────────────────────────────────────────────────
    #  on_stop
    # ────────────────────────────────────────────────

    def on_stop(self):
        # Cancel any active block
        if self._active_block:
            self._cancel_block_orders(self._active_block)
            self._active_block.state = BlockState.TIMEOUT
            self._block_logs.append(self._active_block)
            self._active_block = None

        total = len(self._block_logs)
        done = sum(1 for b in self._block_logs if b.state == BlockState.DONE)
        timeout = sum(1 for b in self._block_logs if b.state == BlockState.TIMEOUT)
        critical = sum(1 for b in self._block_logs if b.state == BlockState.CRITICAL)
        self.write_log(
            f"MSTR stopped. Blocks: {total} total, "
            f"{done} DONE, {timeout} TIMEOUT, {critical} CRITICAL"
        )

    # ════════════════════════════════════════════════════════════════
    #  Signal Computation  (设计文档 Section 2)
    # ════════════════════════════════════════════════════════════════

    def _recompute_pair_stats(self):
        """Recompute μ, Max, MaxDev for all pairs from last (window-1) days."""
        self._mu.clear()
        self._max_val.clear()
        self._max_dev.clear()

        n = self.window - 1
        for i, sym_a in enumerate(self.symbols):
            closes_a = self._daily_closes.get(sym_a, [])
            if len(closes_a) < n:
                continue
            hist_a = closes_a[-n:]

            for j, sym_b in enumerate(self.symbols):
                if i == j:
                    continue
                closes_b = self._daily_closes.get(sym_b, [])
                if len(closes_b) < n:
                    continue
                hist_b = closes_b[-n:]

                ratios = []
                for ca, cb in zip(hist_a, hist_b):
                    if cb > 0:
                        ratios.append(ca / cb)
                if len(ratios) < n:
                    continue

                mu = sum(ratios) / len(ratios)
                max_val = max(ratios)
                max_dev = max_val - mu

                pair = (sym_a, sym_b)
                self._mu[pair] = mu
                self._max_val[pair] = max_val
                self._max_dev[pair] = max_dev

    def _compute_score(
        self, h: str, si: str
    ) -> tuple[float, float, float, float]:
        """
        Compute realtime Ratio_N, Dev, MaxDev, Score for (H, Si).
        Returns (ratio_n, dev, max_dev, score).  score < 0 → invalid.
        """
        pair = (h, si)
        mu = self._mu.get(pair)
        max_dev = self._max_dev.get(pair)
        if mu is None or max_dev is None or max_dev <= 0:
            return 0.0, 0.0, 0.0, -1.0

        price_h = self._current_adj_prices.get(h, 0.0)
        price_si = self._current_adj_prices.get(si, 0.0)
        if price_h <= 0 or price_si <= 0:
            return 0.0, 0.0, 0.0, -1.0

        ratio_n = price_h / price_si
        dev = ratio_n - mu
        score = dev / max_dev
        return ratio_n, dev, max_dev, score

    def _collect_signals(self, held_symbols: list[str]) -> list[dict]:
        """
        Collect all triggered (H → Si) pairs (设计文档 2.4, 2.7).
        Sorted by Dev descending.
        """
        signals = []
        for h in held_symbols:
            for si in self.symbols:
                if si == h:
                    continue
                ratio_n, dev, max_dev, score = self._compute_score(h, si)
                if (
                    dev > 0
                    and max_dev > 0
                    and score > self.k_threshold
                    and dev > self.least_bias
                ):
                    signals.append(
                        {
                            "h": h,
                            "si": si,
                            "score": score,
                            "dev": dev,
                            "max_dev": max_dev,
                            "ratio": ratio_n,
                            "mu": self._mu.get((h, si), 0.0),
                        }
                    )
        signals.sort(key=lambda x: x["dev"], reverse=True)
        return signals

    def _safe_candidate_algorithm(self) -> tuple[str | None, list, str]:
        """
        空仓择股 (设计文档 4.4.1):
        找 "如果持有它，不会立刻被信号要求卖出" 的最低估品种。

        Returns (target, candidate_list, chosen_reason)
        candidate_list 中每个元素包含 symbol, total_dev 及 worst pair 的
        score/dev/max_dev/ratio/mu。

        对称性原则：卖出要求 Score > k_threshold 才触发，买入同样要求
        至少存在一个品种对使得 Score < -k_threshold（即候选标的在历史
        波动范围内处于显著低估区间），否则不买入，避免在噪音级偏差上
        做无意义交易。
        """
        safe = []
        for c in self.symbols:
            is_safe = True
            worst_score = -1.0
            min_score = float('inf')  # 最负 score = 最被低估
            worst_pair: dict = {}
            total_dev = 0.0
            for si in self.symbols:
                if si == c:
                    continue
                ratio_n, dev, max_dev, score = self._compute_score(c, si)
                if max_dev <= 0 and score < 0:
                    # 价格数据不完整，跳过该候选
                    is_safe = False
                    break
                total_dev += dev
                if (
                    dev > 0
                    and max_dev > 0
                    and score > self.k_threshold
                    and dev > self.least_bias
                ):
                    is_safe = False
                    break
                if max_dev > 0 and score < min_score:
                    min_score = score
                if score > worst_score:
                    worst_score = score
                    mu = self._mu.get((c, si), 0.0)
                    worst_pair = {
                        "vs": si,
                        "score": round(score, 6),
                        "dev": round(dev, 6),
                        "max_dev": round(max_dev, 6),
                        "ratio": round(ratio_n, 6),
                        "mu": round(mu, 6),
                    }
            if is_safe:
                safe.append((c, total_dev, worst_pair, min_score))

        if not safe:
            return None, [], "no_safe_candidate"

        safe.sort(key=lambda x: x[1])
        candidate_list = [
            {"symbol": s, "total_dev": round(d, 6), "min_score": round(ms, 6), **wp}
            for s, d, wp, ms in safe
        ]

        # 对称性门槛：至少存在一个品种对使 Score < -k_threshold
        # 即候选相对于池中某标的处于显著低估，才值得买入
        chosen = safe[0]
        if chosen[3] > -self.k_threshold:
            return (
                None, candidate_list,
                f"undervaluation_insufficient: min_score={chosen[3]:.4f}, "
                f"need < -{self.k_threshold}"
            )

        reason = (
            f"lowest total_dev={chosen[1]:.6f} among {len(safe)} safe, "
            f"min_score={chosen[3]:.4f}"
        )
        return chosen[0], candidate_list, reason

    def _netting_check(self, signals: list[dict]):
        """Detect and log signal conflicts (设计文档 2.8)."""
        pair_set = {(s["h"], s["si"]) for s in signals}
        for s in signals:
            if (s["si"], s["h"]) in pair_set:
                self.write_log(
                    f"NETTING_CONFLICT: sell {s['h']} buy {s['si']} "
                    f"vs sell {s['si']} buy {s['h']}"
                )

    # ════════════════════════════════════════════════════════════════
    #  Price Helpers
    # ════════════════════════════════════════════════════════════════

    def _update_current_prices(self, primary_tick: TickData):
        """Update current prices for all symbols (raw, same scale as _daily_closes)."""
        for symbol in self.symbols:
            if symbol == primary_tick.symbol:
                raw = primary_tick.last_price
            else:
                t = self.get_latest_tick(symbol)
                raw = t.last_price if t and t.last_price > 0 else 0.0
            if raw > 0:
                self._current_adj_prices[symbol] = raw

    @staticmethod
    def _round_down(volume: int | float, lot: int = 100) -> int:
        """Round down to nearest lot size."""
        return (int(volume) // lot) * lot

    def _budget_cap_buy(self, block: BlockTrade, qty: int, price: float) -> int:
        """Cap buy quantity so total buy cost does not exceed budget.

        Budget = sell_proceeds (already realized) + per-block idle cash subsidy.
        This allows rotation blocks to recover spread/rounding losses using
        idle cash, preventing block value from shrinking over time.
        """
        if block.mode != "rotation" or price <= 0:
            return qty
        acct = self.get_account()
        idle_cash = max(0, acct.available - self.min_cash_reserve)
        cash_subsidy = idle_cash / max(1, self.num_blocks)
        budget = block.sell_proceeds - block.buy_cost + cash_subsidy
        if budget <= 0:
            return 0
        max_qty = self._round_down(int(budget / price))
        return min(qty, max_qty)

    def _get_tick_price(self, symbol: str, side: str, fallback_tick: TickData):
        """Get passive or aggressive price for a symbol."""
        tick = self.get_latest_tick(symbol) or fallback_tick
        if side == "buy_passive":
            return tick.bid_price_1 or tick.last_price
        elif side == "buy_aggressive":
            return tick.ask_price_1 or tick.last_price
        elif side == "sell_passive":
            return tick.ask_price_1 or tick.last_price
        elif side == "sell_aggressive":
            return tick.bid_price_1 or tick.last_price
        return tick.last_price

    # ════════════════════════════════════════════════════════════════
    #  Rotation Mode  (设计文档 3.5, 4.3)
    # ════════════════════════════════════════════════════════════════

    def _try_rotation(self, tick: TickData, held_symbols: list[str]):
        """Attempt to start a rotation block trade."""
        # ── Fix D: block_count cap — don't rotate when inflated ──
        total = sum(self.block_count.values()) + self.cash_blocks
        if total > self.num_blocks:
            return

        # Also consider pure buy if cash blocks available and nothing to sell
        signals = self._collect_signals(held_symbols)

        if not signals:
            # No rotation signal; try pure buy if possible
            if self.cash_blocks > 0:
                self._try_pure_buy(tick)
            return

        self._netting_check(signals)

        # Select best (H → Si) pair with capacity checks
        best = self._select_candidate(signals)
        if not best:
            return

        h_symbol = best["h"]
        si_symbol = best["si"]

        # ── Fix A: Pre-flight tick freshness check ──
        buy_latest = self.get_latest_tick(si_symbol)
        sell_latest = self.get_latest_tick(h_symbol)
        if not buy_latest or not sell_latest:
            return
        stale = timedelta(minutes=5)
        if (tick.datetime - buy_latest.datetime) > stale:
            return
        if (tick.datetime - sell_latest.datetime) > stale:
            return

        # Compute sell volume (设计文档 3.2.2)
        h_count = self.block_count.get(h_symbol, 0)
        if h_count <= 0:
            return
        pos = self.get_position(h_symbol)
        avail = pos.available if self.enable_t0 else max(0, pos.volume - pos.frozen - pos.today_bought)
        if avail <= 0:
            return
        sell_total = self._round_down(avail // h_count)
        if sell_total <= 0:
            return
        # 把不足一手的孤儿股也卖掉，避免永久残留
        remainder = avail - sell_total * h_count
        if 0 < remainder < 100:
            sell_total += remainder

        # Compute buy volume from sell-side value + idle cash subsidy
        sell_price = self._get_tick_price(h_symbol, "sell_passive", tick)
        buy_price = self._get_tick_price(si_symbol, "buy_passive", tick)
        if sell_price <= 0 or buy_price <= 0:
            return
        sell_expected_amount = sell_total * sell_price
        # 用闲置现金补贴 rotation 的 buy 端，防止 block 价值缩水
        acct = self.get_account()
        idle_cash = max(0, acct.available - self.min_cash_reserve)
        cash_subsidy = idle_cash / max(1, self.num_blocks)
        buy_budget = sell_expected_amount + cash_subsidy
        buy_total = self._round_down(int(buy_budget / buy_price))
        if buy_total <= 0:
            return

        # Start block
        self._start_rotation_block(
            tick, h_symbol, si_symbol, buy_total, sell_total, best, signals
        )

    def _select_candidate(self, signals: list[dict]) -> dict | None:
        """Select best candidate respecting block limits and max_positions."""
        for sig in signals:
            si = sig["si"]
            h = sig["h"]
            si_count = self.block_count.get(si, 0)

            # Single weight check
            if si_count >= self.max_block_per_symbol:
                continue

            # Max positions check for new symbol
            if si_count == 0:
                num_held = len([s for s, c in self.block_count.items() if c > 0])
                # After selling H (-1 block), if H still held, count stays.
                # If H drops to 0, we get a new slot.
                h_after = self.block_count.get(h, 0) - 1
                effective_held = num_held - (1 if h_after <= 0 else 0)
                if effective_held >= self.max_positions:
                    continue

            # Near-optimal check: within δ of best Dev
            if sig != signals[0]:
                if sig["dev"] < signals[0]["dev"] * (1 - self.near_optimal_delta):
                    break  # All remaining are too far
            return sig
        return None

    def _start_rotation_block(
        self,
        tick: TickData,
        h_symbol: str,
        si_symbol: str,
        buy_total: int,
        sell_total: int,
        signal: dict,
        all_signals: list[dict],
    ):
        """Create and start a rotation Block trade."""
        now = tick.datetime
        self._block_seq_today += 1

        buy_tick = self.get_latest_tick(si_symbol) or tick
        sell_tick = self.get_latest_tick(h_symbol) or tick
        acct = self.get_account()

        block = BlockTrade(
            block_id=f"{self._current_date}-{self._block_seq_today:03d}",
            trade_date=self._current_date,
            block_seq=self._block_seq_today,
            mode="rotation",
            signal_time=now,
            sell_symbol=h_symbol,
            buy_symbol=si_symbol,
            trigger_score=signal["score"],
            trigger_dev=signal["dev"],
            trigger_max_dev=signal["max_dev"],
            trigger_ratio=signal["ratio"],
            mu=signal["mu"],
            candidate_list=[
                (s["si"], round(s["score"], 4), round(s["dev"], 6))
                for s in all_signals[:5]
            ],
            chosen_reason=f"Dev最大 ({signal['dev']:.4f})",
            buy_bid1=buy_tick.bid_price_1,
            buy_ask1=buy_tick.ask_price_1,
            sell_bid1=sell_tick.bid_price_1,
            sell_ask1=sell_tick.ask_price_1,
            buy_signal_price=buy_tick.last_price,
            sell_signal_price=sell_tick.last_price,
            signal_cash=acct.balance,
            signal_nav=acct.balance + acct.frozen + sum(
                p.volume * p.market_price
                for p in (self.get_position(s) for s in self.symbols)
                if p and p.volume > 0
            ),
            block_size=buy_total * (buy_tick.bid_price_1 or buy_tick.last_price),
            cash_blocks_at_start=self.cash_blocks,
            buy_target=buy_total,
            sell_target=sell_total,
            n_sub_lots=self.sub_lots,
            state=BlockState.PENDING,
            start_time=now,
            timeout_deadline=now + timedelta(minutes=self.block_timeout),
        )

        block.add_event(
            now,
            self._tick_count,
            "SIGNAL",
            score=signal["score"],
            dev=signal["dev"],
            max_dev=signal["max_dev"],
            ratio=signal["ratio"],
        )

        # Submit passive orders (设计文档 3.5.2)
        # 卖单先提交，产生信用额度后再提交买单
        buy_price = self._get_tick_price(si_symbol, "buy_passive", tick)
        sell_price = self._get_tick_price(h_symbol, "sell_passive", tick)
        self._submit_sub_lots(block, "sell", h_symbol, sell_total, sell_price)
        self._submit_sub_lots(block, "buy", si_symbol, buy_total, buy_price)

        block.add_event(
            now,
            self._tick_count,
            "PASSIVE_SUBMIT",
            buy_symbol=si_symbol,
            buy_price=buy_price,
            buy_volume=buy_total,
            sell_symbol=h_symbol,
            sell_price=sell_price,
            sell_volume=sell_total,
        )

        self._active_block = block
        self.write_log(
            f"Block {block.block_id}: sell {h_symbol} → buy {si_symbol}, "
            f"Score={signal['score']:.3f}, Dev={signal['dev']:.4f}"
        )

    def _submit_sub_lots(
        self, block: BlockTrade, side: str, symbol: str, total: int, price: float
    ):
        """Submit sub_lots orders for one side of a block."""
        sub_vol = self._round_down(total // self.sub_lots)
        for i in range(self.sub_lots):
            vol = (
                total - sub_vol * (self.sub_lots - 1)
                if i == self.sub_lots - 1
                else sub_vol
            )
            if vol <= 0:
                continue
            if side == "buy":
                oid = self.buy(symbol, price, vol)
            else:
                oid = self.sell(symbol, price, vol)
            if oid:
                sub = SubOrder(oid, side, symbol, vol, price)
                self._order_map[oid] = sub
                if side == "buy":
                    block.buy_orders.append(sub)
                else:
                    block.sell_orders.append(sub)

    # ════════════════════════════════════════════════════════════════
    #  Active Block Management (on_tick)
    # ════════════════════════════════════════════════════════════════

    def _manage_active_block(self, tick: TickData):
        """Top-level per-tick management for the active block."""
        block = self._active_block
        if not block:
            return
        now = tick.datetime

        # ── Timeout check ──
        if block.timeout_deadline and now >= block.timeout_deadline:
            self._handle_block_timeout(block, tick)
            return

        # ── One-side complete safety net (rotation MATCHING) ──
        if block.mode == "rotation" and block.state == BlockState.MATCHING:
            if (block.sell_filled >= block.sell_target
                    and block.buy_filled < block.buy_target):
                self._start_immediate_chase(block, "buy", now, tick)
            elif (block.buy_filled >= block.buy_target
                    and block.sell_filled < block.sell_target):
                self._start_immediate_chase(block, "sell", now, tick)

        # ── Chase timer (rotation / recovery_sell) ──
        if block.mode in ("rotation", "recovery_sell") and block.state == BlockState.CHASING:
            if block.chase_order_id:
                self._check_chase_timeout(block, tick)

        # ── Pure buy management ──
        if block.mode == "pure_buy":
            self._manage_pure_buy_on_tick(block, tick)

    # ════════════════════════════════════════════════════════════════
    #  Fill Matching  (设计文档 3.5.3)
    # ════════════════════════════════════════════════════════════════

    def _check_and_handle_excess(self, block: BlockTrade, now: datetime):
        """Detect excess and submit aggressive order if needed.

        Excess = one side filled more than the other side has *covered*
        (= aggressive committed + pending passive unfilled).
        Only the uncovered portion triggers new aggressive orders, and never
        more than the remaining target of the receiving side.
        """
        # Pending (unfilled) volume still in the book — 包含被动和激进
        pending_sell = sum(
            s.volume - s.filled
            for s in block.sell_orders
            if s.is_active
        )
        pending_buy = sum(
            s.volume - s.filled
            for s in block.buy_orders
            if s.is_active
        )

        # Buy excess: 买方已成交 > 卖方已成交 + 卖方待成交
        buy_excess = block.buy_filled - block.sell_filled - pending_sell
        # Sell excess: 卖方已成交 > 买方已成交 + 买方待成交
        sell_excess = block.sell_filled - block.buy_filled - pending_buy

        if buy_excess > 0:
            # Cap: never push sell beyond its target (含已提交未成交)
            sell_room = block.sell_target - block.sell_filled - pending_sell
            qty = min(buy_excess, sell_room) if sell_room > 0 else 0
            if qty > 0:
                block.add_event(
                    now,
                    self._tick_count,
                    "EXCESS_DETECT",
                    buy_filled=block.buy_filled,
                    sell_filled=block.sell_filled,
                    excess=qty,
                    side="sell",
                )
                self._handle_excess(block, "sell", qty, now)
        elif sell_excess > 0:
            # Cap: never push buy beyond its target (含已提交未成交)
            buy_room = block.buy_target - block.buy_filled - pending_buy
            qty = min(sell_excess, buy_room) if buy_room > 0 else 0
            if qty > 0:
                block.add_event(
                    now,
                    self._tick_count,
                    "EXCESS_DETECT",
                    buy_filled=block.buy_filled,
                    sell_filled=block.sell_filled,
                    excess=qty,
                    side="buy",
                )
                self._handle_excess(block, "buy", qty, now)

    def _handle_excess(
        self, block: BlockTrade, side: str, qty: int, now: datetime
    ):
        """
        Cancel passive orders to free volume, submit aggressive order.
        side = "sell" means we need aggressive sell; "buy" means aggressive buy.
        """
        # Identify passive orders to cancel on the SAME side as the aggressive need
        if side == "sell":
            passives = [
                s
                for s in block.sell_orders
                if s.is_active and not s.is_aggressive
            ]
            symbol = block.sell_symbol
        else:
            passives = [
                s
                for s in block.buy_orders
                if s.is_active and not s.is_aggressive
            ]
            symbol = block.buy_symbol

        # Sort: fully unfilled first (设计文档 3.5.3 撤单优先级)
        passives.sort(key=lambda s: (0 if s.filled == 0 else 1, s.filled))

        cancelled_vol = 0
        for sub in passives:
            if cancelled_vol >= qty:
                break
            remaining = sub.volume - sub.filled
            if remaining <= 0:
                continue
            self.cancel_order(sub.order_id)
            sub.is_active = False
            cancelled_vol += remaining
            block.add_event(
                now,
                self._tick_count,
                "PASSIVE_CANCEL",
                order_id=sub.order_id,
                cancel_qty=remaining,
            )

        # Submit aggressive order
        if side == "sell":
            price = self._get_tick_price(symbol, "sell_aggressive", None)
            # Clamp to actual available position after cancel unfreeze
            pos = self.get_position(symbol)
            actual_qty = min(qty, pos.available) if pos else 0
            oid = self.sell(symbol, price, actual_qty) if price > 0 and actual_qty > 0 else ""
        else:
            price = self._get_tick_price(symbol, "buy_aggressive", None)
            actual_qty = self._budget_cap_buy(block, qty, price)
            oid = self.buy(symbol, price, actual_qty) if price > 0 and actual_qty > 0 else ""

        if oid:
            new_sub = SubOrder(
                oid, side, symbol, actual_qty, price, is_aggressive=True
            )
            self._order_map[oid] = new_sub
            if side == "sell":
                block.sell_orders.append(new_sub)
                block.sell_aggressive += actual_qty
            else:
                block.buy_orders.append(new_sub)
                block.buy_aggressive += actual_qty
            block.add_event(
                now,
                self._tick_count,
                "AGGRESSIVE_SUBMIT",
                side=side,
                order_id=oid,
                volume=actual_qty,
                price=price,
            )
            # Enter chasing state
            block.state = BlockState.CHASING
            block.chase_order_id = oid
            block.chase_side = side
            block.chase_submit_tick = self._tick_count
        else:
            if side == "sell":
                block.sell_aggressive += actual_qty
            else:
                block.buy_aggressive += actual_qty

        # Handle over-cancelled volume
        over_cancel = cancelled_vol - qty
        if over_cancel > 0:
            re_vol = self._round_down(over_cancel)
            if re_vol >= 100:
                if side == "sell":
                    re_price = self._get_tick_price(symbol, "sell_passive", None)
                    re_oid = self.sell(symbol, re_price, re_vol) if re_price > 0 else ""
                else:
                    re_price = self._get_tick_price(symbol, "buy_passive", None)
                    re_oid = self.buy(symbol, re_price, re_vol) if re_price > 0 else ""
                if re_oid:
                    re_sub = SubOrder(re_oid, side, symbol, re_vol, re_price)
                    self._order_map[re_oid] = re_sub
                    if side == "sell":
                        block.sell_orders.append(re_sub)
                    else:
                        block.buy_orders.append(re_sub)
                    block.add_event(
                        now, self._tick_count, "PASSIVE_RESUBMIT",
                        side=side, order_id=re_oid, volume=re_vol, price=re_price,
                    )

    # ════════════════════════════════════════════════════════════════
    #  Immediate Chase  (设计文档 3.5.3a)
    # ════════════════════════════════════════════════════════════════

    def _start_immediate_chase(
        self, block: BlockTrade, side: str, now: datetime,
        tick: TickData = None,
    ):
        """One side fully filled → cancel all orders on the other side
        and chase aggressively with continuous repricing.

        side = "buy" or "sell" → which side needs aggressive chasing.
        """
        # Cancel ALL active orders on the chase side (passive + stale aggressive)
        if side == "buy":
            orders = block.buy_orders
            symbol = block.buy_symbol
        else:
            orders = block.sell_orders
            symbol = block.sell_symbol

        cancelled_ids = []
        for sub in orders:
            if sub.is_active:
                self.cancel_order(sub.order_id)
                sub.is_active = False
                cancelled_ids.append(sub.order_id)

        if cancelled_ids:
            block.add_event(
                now, self._tick_count, "CANCEL_FOR_CHASE",
                side=side, count=len(cancelled_ids),
                order_ids=",".join(cancelled_ids),
            )

        # Compute remaining
        if side == "buy":
            remaining = block.buy_target - block.buy_filled
        else:
            remaining = block.sell_target - block.sell_filled

        if remaining <= 0:
            return

        # Get aggressive price and apply constraints
        if side == "buy":
            price = self._get_tick_price(symbol, "buy_aggressive", tick)
            uncapped = remaining
            remaining = self._budget_cap_buy(block, remaining, price)
            if remaining <= 0 < uncapped:
                # 预算不足以买入剩余量 → 接受当前成果，直接完成
                budget = block.sell_proceeds - block.buy_cost
                block.add_event(
                    now, self._tick_count, "BUDGET_DONE",
                    side="buy",
                    unfilled=uncapped,
                    budget=round(budget, 2),
                    price=price,
                )
                self.write_log(
                    f"Block {block.block_id} budget exhausted: "
                    f"unfilled {uncapped}, budget {budget:.2f}"
                )
                block.buy_target = block.buy_filled
                self._complete_block(block, now)
                return
        else:
            price = self._get_tick_price(symbol, "sell_aggressive", tick)
            pos = self.get_position(symbol)
            remaining = min(remaining, pos.available) if pos else 0

        if price <= 0 or remaining <= 0:
            return

        # Submit aggressive order
        if side == "buy":
            oid = self.buy(symbol, price, remaining)
        else:
            oid = self.sell(symbol, price, remaining)

        if oid:
            sub = SubOrder(
                oid, side, symbol, remaining, price, is_aggressive=True
            )
            self._order_map[oid] = sub
            if side == "buy":
                block.buy_orders.append(sub)
                block.buy_aggressive += remaining
            else:
                block.sell_orders.append(sub)
                block.sell_aggressive += remaining

            block.state = BlockState.CHASING
            block.chase_order_id = oid
            block.chase_side = side
            block.chase_submit_tick = self._tick_count

            block.add_event(
                now, self._tick_count, "IMMEDIATE_CHASE",
                side=side, order_id=oid, volume=remaining, price=price,
            )

    # ════════════════════════════════════════════════════════════════
    #  Chase Repricing  (设计文档 3.5.4)
    # ════════════════════════════════════════════════════════════════

    def _check_chase_timeout(self, block: BlockTrade, tick: TickData):
        """Check if chase order needs repricing."""
        ticks_waited = self._tick_count - block.chase_submit_tick
        if ticks_waited < self.chase_wait_ticks:
            return

        # ── Fix B: Max chase rounds → abort as CRITICAL ──
        if block.chase_round >= self.max_chase_rounds:
            self._cancel_block_orders(block)
            block.state = BlockState.CRITICAL
            block.end_time = tick.datetime
            block.add_event(
                tick.datetime, self._tick_count, "CHASE_ABORT",
                reason="max_chase_rounds",
                rounds=block.chase_round,
                buy_filled=block.buy_filled,
                sell_filled=block.sell_filled,
            )
            self.write_log(
                f"Block {block.block_id} CHASE_ABORT: {block.chase_round} rounds, "
                f"buy {block.buy_filled}/{block.buy_target}, "
                f"sell {block.sell_filled}/{block.sell_target}"
            )
            self._finalize_block(block)
            return

        chase_sub = self._order_map.get(block.chase_order_id)
        if not chase_sub or not chase_sub.is_active:
            # Chase order already filled/cancelled → back to MATCHING
            block.state = BlockState.MATCHING
            block.chase_order_id = ""
            return

        # Still unfilled → cancel and re-price
        remaining = chase_sub.volume - chase_sub.filled
        if remaining <= 0:
            block.state = BlockState.MATCHING
            block.chase_order_id = ""
            return

        old_price = chase_sub.price
        now = tick.datetime
        symbol = chase_sub.symbol
        side = chase_sub.side

        # ── 先看新价格，价格没变就不撤单，继续等 ──
        if side == "sell":
            new_price = self._get_tick_price(symbol, "sell_aggressive", tick)
        else:
            new_price = self._get_tick_price(symbol, "buy_aggressive", tick)

        if new_price == old_price:
            # 市场没动，重置计时器继续等待，不浪费撤单重发
            block.chase_submit_tick = self._tick_count
            return

        # ── 价格变了，撤旧单、发新单 ──
        self.cancel_order(block.chase_order_id)
        chase_sub.is_active = False

        block.chase_round += 1
        block.add_event(
            now,
            self._tick_count,
            "CHASE_CANCEL",
            round=block.chase_round,
            side=side,
            order_id=block.chase_order_id,
            old_price=old_price,
            new_price=new_price,
            unfilled_qty=remaining,
        )

        if side == "sell":
            # Clamp to actual available position after cancel unfreeze
            pos = self.get_position(symbol)
            remaining = min(remaining, pos.available) if pos else 0
        else:
            # Budget cap: 价格变高时压缩数量
            uncapped = remaining
            remaining = self._budget_cap_buy(block, remaining, new_price)
            if remaining <= 0 < uncapped:
                # 预算不足 → 接受当前成果
                budget = block.sell_proceeds - block.buy_cost
                block.add_event(
                    now, self._tick_count, "BUDGET_DONE",
                    side="buy", unfilled=uncapped,
                    budget=round(budget, 2), price=new_price,
                )
                self.write_log(
                    f"Block {block.block_id} budget exhausted in chase: "
                    f"unfilled {uncapped}, budget {budget:.2f}"
                )
                block.buy_target = block.buy_filled
                block.chase_order_id = ""
                self._complete_block(block, now)
                return

        if side == "buy":
            new_oid = self.buy(symbol, new_price, remaining) if new_price > 0 else ""
        else:
            new_oid = self.sell(symbol, new_price, remaining) if new_price > 0 else ""

        if new_oid:
            new_sub = SubOrder(
                new_oid, side, symbol, remaining, new_price, is_aggressive=True
            )
            self._order_map[new_oid] = new_sub
            if side == "buy":
                block.buy_orders.append(new_sub)
            else:
                block.sell_orders.append(new_sub)
            block.chase_order_id = new_oid
            block.chase_submit_tick = self._tick_count
            block.add_event(
                now,
                self._tick_count,
                "CHASE_SUBMIT",
                round=block.chase_round,
                order_id=new_oid,
                volume=remaining,
                price=new_price,
            )
        else:
            block.chase_order_id = ""
            block.state = BlockState.MATCHING

    # ════════════════════════════════════════════════════════════════
    #  Block Timeout  (设计文档 3.5.5)
    # ════════════════════════════════════════════════════════════════

    def _handle_block_timeout(self, block: BlockTrade, tick: TickData):
        """Handle block timeout."""
        now = tick.datetime

        if block.buy_filled == 0 and block.sell_filled == 0:
            # ── Scenario A: zero fills → TIMEOUT ──
            self._cancel_block_orders(block)
            block.state = BlockState.TIMEOUT
            block.end_time = now
            block.add_event(
                now,
                self._tick_count,
                "BLOCK_TIMEOUT",
                block_elapsed_time=block.total_duration,
            )
            self.write_log(f"Block {block.block_id} TIMEOUT (zero fills)")
            self._finalize_block(block)

        elif not block.force_aggressive_sent:
            # ── Scenario B (first time): force aggressive ──
            block.force_aggressive_sent = True
            self._cancel_block_orders(block)
            self._force_aggressive_all(block, tick)
            # Extend deadline by 2 min for aggressive orders to fill
            block.timeout_deadline = now + timedelta(minutes=2)

        else:
            # ── Scenario C: forced aggressive also timed out → CRITICAL ──
            self._cancel_block_orders(block)
            block.state = BlockState.CRITICAL
            block.end_time = now
            block.add_event(
                now,
                self._tick_count,
                "BLOCK_CRITICAL",
                buy_filled=block.buy_filled,
                sell_filled=block.sell_filled,
                unfilled_buy=block.buy_target - block.buy_filled,
                unfilled_sell=block.sell_target - block.sell_filled,
            )
            self.write_log(
                f"Block {block.block_id} CRITICAL: "
                f"buy {block.buy_filled}/{block.buy_target}, "
                f"sell {block.sell_filled}/{block.sell_target}"
            )
            self._finalize_block(block)

    def _force_aggressive_all(self, block: BlockTrade, tick: TickData):
        """Submit aggressive orders for all unfilled quantities.

        与普通 chase 不同, 这里可能同时提交买卖两边的激进单。
        为了让追价机制能工作, 追踪最后一个提交的 order_id。
        """
        now = tick.datetime
        last_oid = ""
        last_side = ""

        buy_remaining = block.buy_target - block.buy_filled
        if buy_remaining > 0:
            price = self._get_tick_price(block.buy_symbol, "buy_aggressive", tick)
            # Budget cap: 价格变高时压缩数量
            buy_remaining = self._budget_cap_buy(block, buy_remaining, price)
            if price > 0 and buy_remaining > 0:
                oid = self.buy(block.buy_symbol, price, buy_remaining)
                if oid:
                    sub = SubOrder(
                        oid, "buy", block.buy_symbol, buy_remaining, price,
                        is_aggressive=True,
                    )
                    block.buy_orders.append(sub)
                    self._order_map[oid] = sub
                    block.add_event(
                        now, self._tick_count, "AGGRESSIVE_SUBMIT",
                        side="buy", order_id=oid, volume=buy_remaining, price=price,
                    )
                    last_oid = oid
                    last_side = "buy"

        sell_remaining = block.sell_target - block.sell_filled
        if sell_remaining > 0:
            # Clamp to actual available position
            pos = self.get_position(block.sell_symbol)
            sell_remaining = min(sell_remaining, pos.available) if pos else 0
        if sell_remaining > 0:
            price = self._get_tick_price(block.sell_symbol, "sell_aggressive", tick)
            if price > 0:
                oid = self.sell(block.sell_symbol, price, sell_remaining)
                if oid:
                    sub = SubOrder(
                        oid, "sell", block.sell_symbol, sell_remaining, price,
                        is_aggressive=True,
                    )
                    block.sell_orders.append(sub)
                    self._order_map[oid] = sub
                    block.add_event(
                        now, self._tick_count, "AGGRESSIVE_SUBMIT",
                        side="sell", order_id=oid, volume=sell_remaining, price=price,
                    )
                    last_oid = oid
                    last_side = "sell"

        block.state = BlockState.CHASING
        # 设置 chase 追踪, 使 _check_chase_timeout 能对强制激进单追价
        if last_oid:
            block.chase_order_id = last_oid
            block.chase_side = last_side
            block.chase_submit_tick = self._tick_count

    def _force_cutoff_timeout(self, block: BlockTrade, tick: TickData):
        """Trading cutoff: force immediate timeout (设计文档 3.8)."""
        self.write_log(f"Block {block.block_id}: trading cutoff, forcing timeout")
        block.timeout_deadline = tick.datetime
        self._handle_block_timeout(block, tick)

    # ════════════════════════════════════════════════════════════════
    #  Recovery Sell Mode  (Fix C: one-sided CRITICAL recovery)
    # ════════════════════════════════════════════════════════════════

    def _try_recovery(self, tick: TickData):
        """Attempt to execute the first recovery task (sell to restore balance)."""
        if not self._recovery_queue:
            return

        task = self._recovery_queue[0]
        symbol = task['symbol']

        pos = self.get_position(symbol)
        if not pos:
            return
        avail = (
            pos.available if self.enable_t0
            else max(0, pos.volume - pos.frozen - pos.today_bought)
        )
        if avail <= 0:
            return  # Can't sell yet (T+1 or no position)

        # Tick freshness check
        last_tick = self.get_latest_tick(symbol)
        if not last_tick or (tick.datetime - last_tick.datetime) > timedelta(minutes=5):
            return

        h_count = self.block_count.get(symbol, 0)
        if h_count <= 0:
            self._recovery_queue.pop(0)
            return
        sell_total = self._round_down(avail // h_count)
        if sell_total <= 0:
            return

        sell_price = self._get_tick_price(symbol, "sell_passive", tick)
        if sell_price <= 0:
            return

        self._start_recovery_block(tick, symbol, sell_total, sell_price, task)
        self._recovery_queue.pop(0)

    def _start_recovery_block(
        self,
        tick: TickData,
        sell_symbol: str,
        sell_total: int,
        sell_price: float,
        task: dict,
    ):
        """Create a recovery sell block to restore block balance."""
        now = tick.datetime
        self._block_seq_today += 1
        acct = self.get_account()
        sell_tick = self.get_latest_tick(sell_symbol) or tick

        block = BlockTrade(
            block_id=f"{self._current_date}-{self._block_seq_today:03d}",
            trade_date=self._current_date,
            block_seq=self._block_seq_today,
            mode="recovery_sell",
            signal_time=now,
            sell_symbol=sell_symbol,
            buy_symbol="",
            trigger_score=0.0,
            trigger_dev=0.0,
            trigger_max_dev=0.0,
            trigger_ratio=0.0,
            mu=0.0,
            candidate_list=[],
            chosen_reason=f"Recovery from {task['source_block']}",
            buy_bid1=0.0,
            buy_ask1=0.0,
            sell_bid1=sell_tick.bid_price_1,
            sell_ask1=sell_tick.ask_price_1,
            buy_signal_price=0.0,
            sell_signal_price=sell_tick.last_price,
            signal_cash=acct.balance,
            signal_nav=acct.balance + acct.frozen + sum(
                p.volume * p.market_price
                for p in (self.get_position(s) for s in self.symbols)
                if p and p.volume > 0
            ),
            block_size=sell_total * (sell_tick.bid_price_1 or sell_tick.last_price),
            cash_blocks_at_start=self.cash_blocks,
            buy_target=0,
            sell_target=sell_total,
            n_sub_lots=self.sub_lots,
            state=BlockState.PENDING,
            start_time=now,
            timeout_deadline=now + timedelta(minutes=self.block_timeout),
        )

        block.add_event(
            now, self._tick_count, "RECOVERY_SIGNAL",
            source=task['source_block'],
            symbol=sell_symbol,
            volume=sell_total,
        )

        self._submit_sub_lots(block, "sell", sell_symbol, sell_total, sell_price)

        block.add_event(
            now, self._tick_count, "PASSIVE_SUBMIT",
            sell_symbol=sell_symbol,
            sell_price=sell_price,
            sell_volume=sell_total,
        )

        self._active_block = block
        self.write_log(
            f"Recovery Block {block.block_id}: sell {sell_symbol} x{sell_total} "
            f"(from {task['source_block']})"
        )

    # ════════════════════════════════════════════════════════════════
    #  Pure Buy Mode  (设计文档 3.5.9, 4.4)
    # ════════════════════════════════════════════════════════════════

    def _try_pure_buy(self, tick: TickData):
        """Attempt to start a pure buy block."""
        if self.cash_blocks <= 0:
            return

        target, candidate_list, chosen_reason = self._safe_candidate_algorithm()
        if not target:
            return

        # Position limits
        target_count = self.block_count.get(target, 0)
        if target_count >= self.max_block_per_symbol:
            return
        if target_count == 0:
            num_held = len([s for s, c in self.block_count.items() if c > 0])
            if num_held >= self.max_positions:
                return

        # Calculate block_size and buy volume
        account = self.get_account()
        available_cash = account.available - self.min_cash_reserve
        if available_cash <= 0:
            return
        block_size = available_cash / self.cash_blocks

        buy_price = self._get_tick_price(target, "buy_passive", tick)
        if buy_price <= 0:
            return
        buy_total = self._round_down(int(block_size / buy_price))
        if buy_total <= 0:
            return

        self._start_pure_buy_block(tick, target, buy_total, buy_price,
                                    candidate_list, chosen_reason)

    def _start_pure_buy_block(
        self, tick: TickData, target: str, buy_total: int, buy_price: float,
        candidate_list: list | None = None, chosen_reason: str = "",
    ):
        """Create and start a pure buy Block trade."""
        now = tick.datetime
        self._block_seq_today += 1

        target_tick = self.get_latest_tick(target) or tick
        acct = self.get_account()

        # Extract worst-pair metrics from candidate_list for the chosen target
        wp = {}
        if candidate_list:
            for entry in candidate_list:
                if entry.get("symbol") == target:
                    wp = entry
                    break

        block = BlockTrade(
            block_id=f"{self._current_date}-{self._block_seq_today:03d}",
            trade_date=self._current_date,
            block_seq=self._block_seq_today,
            mode="pure_buy",
            signal_time=now,
            buy_symbol=target,
            trigger_score=wp.get("score", 0.0),
            trigger_dev=wp.get("dev", 0.0),
            trigger_max_dev=wp.get("max_dev", 0.0),
            trigger_ratio=wp.get("ratio", 0.0),
            mu=wp.get("mu", 0.0),
            buy_bid1=target_tick.bid_price_1,
            buy_ask1=target_tick.ask_price_1,
            buy_signal_price=target_tick.last_price,
            signal_cash=acct.balance,
            signal_nav=acct.balance + acct.frozen + sum(
                p.volume * p.market_price
                for p in (self.get_position(s) for s in self.symbols)
                if p and p.volume > 0
            ),
            block_size=buy_total * buy_price,
            cash_blocks_at_start=self.cash_blocks,
            buy_target=buy_total,
            n_sub_lots=self.sub_lots,
            state=BlockState.PENDING_BUY,
            start_time=now,
            candidate_list=candidate_list or [],
            chosen_reason=chosen_reason,
        )

        block.add_event(
            now, self._tick_count, "SIGNAL", mode="pure_buy", target=target
        )

        self._submit_sub_lots(block, "buy", target, buy_total, buy_price)
        block.add_event(
            now,
            self._tick_count,
            "PASSIVE_SUBMIT",
            buy_symbol=target,
            buy_price=buy_price,
            buy_volume=buy_total,
        )

        self._active_block = block
        self.write_log(
            f"Pure buy block {block.block_id}: buy {target}, "
            f"vol={buy_total}, price={buy_price:.3f}"
        )

    def _manage_pure_buy_on_tick(self, block: BlockTrade, tick: TickData):
        """Per-tick management for pure buy block."""
        if block.state == BlockState.PENDING_BUY:
            # Phase 1: no fills yet — can switch symbol or refresh price
            new_target, _, _ = self._safe_candidate_algorithm()
            if new_target and new_target != block.buy_symbol:
                # Symbol changed → cancel and re-submit
                self._cancel_block_orders(block)
                block.buy_symbol = new_target
                block.buy_orders.clear()

                account = self.get_account()
                avail = account.available - self.min_cash_reserve
                if avail <= 0 or self.cash_blocks <= 0:
                    return
                new_price = self._get_tick_price(new_target, "buy_passive", tick)
                if new_price <= 0:
                    return
                block.buy_target = self._round_down(
                    int(avail / self.cash_blocks / new_price)
                )
                if block.buy_target <= 0:
                    return
                self._submit_sub_lots(
                    block, "buy", new_target, block.buy_target, new_price
                )
                return

            # Check bid price refresh
            new_bid = self._get_tick_price(block.buy_symbol, "buy_passive", tick)
            active_orders = [s for s in block.buy_orders if s.is_active]
            if (
                active_orders
                and new_bid > 0
                and abs(active_orders[0].price - new_bid) >= self.get_pricetick(block.buy_symbol)
            ):
                self._cancel_block_orders(block)
                block.buy_orders = [s for s in block.buy_orders if s.is_active]
                remaining = block.buy_target - block.buy_filled
                if remaining > 0:
                    self._submit_sub_lots(
                        block, "buy", block.buy_symbol, remaining, new_bid
                    )

        elif block.state == BlockState.FILLING:
            # Phase 2: has fills — can refresh price, NOT switch symbol
            new_bid = self._get_tick_price(block.buy_symbol, "buy_passive", tick)
            active_passive = [
                s
                for s in block.buy_orders
                if s.is_active and not s.is_aggressive
            ]
            if (
                active_passive
                and new_bid > 0
                and abs(active_passive[0].price - new_bid)
                >= self.get_pricetick(block.buy_symbol)
            ):
                for s in active_passive:
                    self.cancel_order(s.order_id)
                    s.is_active = False
                remaining = block.buy_target - block.buy_filled
                if remaining > 0:
                    oid = self.buy(block.buy_symbol, new_bid, remaining)
                    if oid:
                        sub = SubOrder(
                            oid, "buy", block.buy_symbol, remaining, new_bid
                        )
                        block.buy_orders.append(sub)
                        self._order_map[oid] = sub

    # ════════════════════════════════════════════════════════════════
    #  Block Completion  (设计文档 Section 6)
    # ════════════════════════════════════════════════════════════════

    def _is_block_complete(self, block: BlockTrade) -> bool:
        if block.mode == "rotation":
            return (
                block.buy_filled >= block.buy_target
                and block.sell_filled >= block.sell_target
            )
        if block.mode == "recovery_sell":
            return block.sell_filled >= block.sell_target
        return block.buy_filled >= block.buy_target

    def _complete_block(self, block: BlockTrade, now: datetime):
        """Block completed successfully → DONE."""
        block.state = BlockState.DONE
        block.end_time = now

        # Capture end-of-block score
        end_score = -1.0
        end_dev = 0.0
        if block.sell_symbol and block.buy_symbol:
            _, end_dev, _, end_score = self._compute_score(
                block.sell_symbol, block.buy_symbol
            )

        block.add_event(
            now,
            self._tick_count,
            "BLOCK_DONE",
            buy_filled=block.buy_filled,
            sell_filled=block.sell_filled,
            duration=block.total_duration,
            end_score=end_score,
            end_dev=end_dev,
        )

        self.write_log(
            f"Block {block.block_id} DONE ({block.total_duration:.1f}s): "
            f"buy {block.buy_filled}@{block.buy_avg_price:.4f}, "
            f"sell {block.sell_filled}@{block.sell_avg_price:.4f}"
        )
        self._finalize_block(block)

    def _finalize_block(self, block: BlockTrade):
        """Update block_count, cash_blocks, cooldown, archive.

        For CRITICAL / TIMEOUT blocks that have partial fills on one side,
        we must still adjust block_count to match the actual position change.
        """
        # ── 采集结束时账户快照 ──
        # 先取消残留的未成交订单，释放 frozen 资金/持仓
        self._cancel_block_orders(block)
        acct = self.get_account()
        block.end_cash = acct.balance
        block.end_nav = acct.balance + acct.frozen + sum(
            p.volume * p.market_price
            for p in (self.get_position(s) for s in self.symbols)
            if p and p.volume > 0
        )
        if block.state == BlockState.DONE:
            if block.mode == "rotation":
                # sell H: -1
                if block.sell_symbol:
                    cnt = self.block_count.get(block.sell_symbol, 0) - 1
                    if cnt <= 0:
                        self.block_count.pop(block.sell_symbol, None)
                    else:
                        self.block_count[block.sell_symbol] = cnt
                # buy Si: +1
                if block.buy_symbol:
                    self.block_count[block.buy_symbol] = (
                        self.block_count.get(block.buy_symbol, 0) + 1
                    )
                # cash_blocks unchanged in rotation

            elif block.mode == "pure_buy":
                if block.buy_symbol:
                    self.block_count[block.buy_symbol] = (
                        self.block_count.get(block.buy_symbol, 0) + 1
                    )
                self.cash_blocks -= 1

            elif block.mode == "recovery_sell":
                # Sold to restore block balance → decrement
                if block.sell_symbol:
                    cnt = self.block_count.get(block.sell_symbol, 0) - 1
                    if cnt <= 0:
                        self.block_count.pop(block.sell_symbol, None)
                    else:
                        self.block_count[block.sell_symbol] = cnt

        elif block.state in (BlockState.CRITICAL, BlockState.TIMEOUT):
            # Partial fill: adjust block_count by actual fills only.
            if block.mode == "rotation":
                # If sell side actually filled → decrement seller
                if block.sell_filled > 0 and block.sell_symbol:
                    cnt = self.block_count.get(block.sell_symbol, 0) - 1
                    if cnt <= 0:
                        self.block_count.pop(block.sell_symbol, None)
                    else:
                        self.block_count[block.sell_symbol] = cnt
                # If buy side actually filled → increment buyer
                if block.buy_filled > 0 and block.buy_symbol:
                    self.block_count[block.buy_symbol] = (
                        self.block_count.get(block.buy_symbol, 0) + 1
                    )

                # ── Fix C: Enqueue recovery if total blocks exceeded ──
                new_total = sum(self.block_count.values()) + self.cash_blocks
                if new_total > self.num_blocks:
                    if block.sell_filled == 0 and block.buy_filled > 0:
                        self._recovery_queue.append({
                            'action': 'sell',
                            'symbol': block.sell_symbol,
                            'source_block': block.block_id,
                        })
                        self.write_log(
                            f"RECOVERY_ENQUEUE: sell {block.sell_symbol} "
                            f"to restore block balance (from {block.block_id})"
                        )
                elif new_total < self.num_blocks:
                    if block.buy_filled == 0 and block.sell_filled > 0:
                        self._recovery_queue.append({
                            'action': 'buy',
                            'symbol': block.buy_symbol,
                            'source_block': block.block_id,
                        })
                        self.write_log(
                            f"RECOVERY_ENQUEUE: buy {block.buy_symbol} "
                            f"to restore block balance (from {block.block_id})"
                        )

            elif block.mode == "pure_buy":
                if block.buy_filled > 0 and block.buy_symbol:
                    self.block_count[block.buy_symbol] = (
                        self.block_count.get(block.buy_symbol, 0) + 1
                    )
                    self.cash_blocks -= 1

            elif block.mode == "recovery_sell":
                if block.sell_filled > 0 and block.sell_symbol:
                    cnt = self.block_count.get(block.sell_symbol, 0) - 1
                    if cnt <= 0:
                        self.block_count.pop(block.sell_symbol, None)
                    else:
                        self.block_count[block.sell_symbol] = cnt
                # Re-enqueue if still inflated
                re_total = sum(self.block_count.values()) + self.cash_blocks
                if re_total > self.num_blocks:
                    self._recovery_queue.append({
                        'action': 'sell',
                        'symbol': block.sell_symbol,
                        'source_block': block.block_id,
                    })
                    self.write_log(
                        f"RECOVERY_RE_ENQUEUE: sell {block.sell_symbol} "
                        f"(recovery failed, retrying)"
                    )

        # Cooldown (设计文档 3.3)
        self.day_block_count += 1
        if block.end_time:
            if self.day_block_count == 1:
                self._cooldown_end = block.end_time + timedelta(
                    minutes=self.cooldown_1
                )
            else:
                self._cooldown_end = block.end_time + timedelta(
                    minutes=self.cooldown_2
                )

        # Archive
        self._block_logs.append(block)

        # Clean up order map
        block_oids = {s.order_id for s in block.buy_orders + block.sell_orders}
        self._order_map = {
            k: v for k, v in self._order_map.items() if k not in block_oids
        }
        self._active_block = None

    # ════════════════════════════════════════════════════════════════
    #  Utility
    # ════════════════════════════════════════════════════════════════

    def _cancel_block_orders(self, block: BlockTrade):
        """Cancel all active orders belonging to a block."""
        cancelled = []
        for sub in block.buy_orders + block.sell_orders:
            if sub.is_active:
                self.cancel_order(sub.order_id)
                sub.is_active = False
                cancelled.append((sub.order_id, sub.side, sub.volume - sub.filled))
        if cancelled:
            block.add_event(
                datetime.now(), self._tick_count, "CANCEL_ALL",
                count=len(cancelled),
                total_unfilled=sum(c[2] for c in cancelled),
                sides=",".join(c[1] for c in cancelled),
            )

    def _cross_day_integrity_check(self):
        """跨日一致性校验 (设计文档 Section 6)."""
        total = sum(self.block_count.values()) + self.cash_blocks
        if total != self.num_blocks:
            self.write_log(
                f"INTEGRITY_WARN: block_count={dict(self.block_count)}, "
                f"cash_blocks={self.cash_blocks}, sum={total}, "
                f"expected={self.num_blocks}"
            )

    @property
    def block_logs(self) -> list[BlockTrade]:
        """Public access to archived block trade logs."""
        return self._block_logs
