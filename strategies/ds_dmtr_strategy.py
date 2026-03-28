"""
DS_DMTR — Dual-Stock Dual-MA Tick Reversion Strategy.

设计文档: docs/design_ds_dmtr_strategy.md

在两只 ETF 之间构建价格比率 ABratio = price_A / price_B，
利用 30 分钟 K 线和日线两个时间尺度的均值回归特性进行轮动交易。
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from enum import Enum

from core.data_feed import ParquetBarFeed, ParquetTickFeed
from core.datatypes import BarData, Direction, Order, OrderStatus, TickData, Trade
from core.strategy import StrategyBase
from strategies.decisions import DecisionContext, create_decision


# ════════════════════════════════════════════════════════════════
#  Block 数据结构 — 轮动操作跟踪
# ════════════════════════════════════════════════════════════════


class BlockState(Enum):
    """Block 状态"""
    PENDING = "PENDING"
    MATCHING = "MATCHING"
    CHASING = "CHASING"
    DONE = "DONE"               # 买卖均完成
    PARTIAL = "PARTIAL"         # 部分成交（卖出或买入有一侧为 0）
    BUY_ONLY = "BUY_ONLY"      # 仅买入（初始建仓，无持仓可卖）
    TIMEOUT = "TIMEOUT"
    CRITICAL = "CRITICAL"
    REJECTED = "REJECTED"       # 被拒绝
    REVERTED = "REVERTED"       # 信号失效，提前终止


@dataclass
class BlockOrderMeta:
    side: str
    is_aggressive: bool
    submit_seq: int = 0


@dataclass
class RotationBlock:
    """一次轮动操作的完整跟踪记录。

    DS_DMTR 的每次信号触发产生一个 Block，记录：
    - 信号快照（触发时的统计量、价格、账户状态）
    - 期望下单量和实际成交量
    - 成交均价
    - 完成后的账户状态
    """

    # ── 标识 ──
    block_id: str = ""
    trade_date: str = ""
    block_seq: int = 0

    # ── 信号快照 ──
    signal_time: datetime | None = None
    direction: str = ""             # "SELL_A_BUY_B" or "SELL_B_BUY_A"
    sell_symbol: str = ""
    buy_symbol: str = ""
    trade_pct: float = 0.0         # base_pct or high_pct
    is_high_pct: bool = False      # 是否加码

    # ── 触发指标快照 ──
    ab_ratio: float = 0.0
    delta_sigma_minutes: float = 0.0
    delta_sigma_days: float = 0.0
    delta_minutes: float = 0.0
    mu_min: float = 0.0
    sigma_min: float = 0.0
    mu_day: float = 0.0
    sigma_day: float = 0.0
    a_percentage: float = 0.0
    signal_reason: str = ""          # 决策器给出的触发原因

    # ── 信号时市场价格 ──
    sell_signal_price: float = 0.0
    buy_signal_price: float = 0.0

    # ── 账户快照 (信号触发时) ──
    signal_cash: float = 0.0
    signal_nav: float = 0.0

    # ── 期望下单 ──
    desired_sell_volume: int = 0
    desired_buy_volume: int = 0

    # ── 实际发单 ──
    sell_order_volume: int = 0     # 实际发出的卖单量
    buy_order_volume: int = 0      # 实际发出的买单量

    # ── 成交结果 ──
    sell_filled: int = 0
    buy_filled: int = 0
    sell_cost: float = 0.0         # 卖出总金额
    buy_cost: float = 0.0          # 买入总金额
    sell_commission: float = 0.0
    buy_commission: float = 0.0

    # ── 账户快照 (block 结束后) ──
    end_cash: float = 0.0
    end_nav: float = 0.0

    # ── 状态 ──
    state: BlockState = BlockState.PENDING
    end_time: datetime | None = None
    timeout_deadline: datetime | None = None
    timeout_budget_seconds: int = 0
    timeout_anchor_time: datetime | None = None
    force_aggressive_sent: bool = False
    chase_order_id: str = ""
    chase_side: str = ""
    chase_submit_tick: int = 0
    chase_round: int = 0
    order_ids: list[str] = field(default_factory=list)
    active_order_ids: set[str] = field(default_factory=set)

    @property
    def sell_avg_price(self) -> float:
        return self.sell_cost / self.sell_filled if self.sell_filled > 0 else 0.0

    @property
    def buy_avg_price(self) -> float:
        return self.buy_cost / self.buy_filled if self.buy_filled > 0 else 0.0

    @property
    def total_duration(self) -> float:
        if self.signal_time and self.end_time:
            return (self.end_time - self.signal_time).total_seconds()
        return 0.0

    @property
    def slippage_sell(self) -> float:
        """卖出滑点 = (信号价 - 成交均价) / 信号价"""
        if self.sell_signal_price > 0 and self.sell_filled > 0:
            return (self.sell_signal_price - self.sell_avg_price) / self.sell_signal_price
        return 0.0

    @property
    def slippage_buy(self) -> float:
        """买入滑点 = (成交均价 - 信号价) / 信号价"""
        if self.buy_signal_price > 0 and self.buy_filled > 0:
            return (self.buy_avg_price - self.buy_signal_price) / self.buy_signal_price
        return 0.0


# ════════════════════════════════════════════════════════════════
#  30-min Bar 聚合辅助
# ════════════════════════════════════════════════════════════════

# A 股交易时段边界 (分钟数, 从 00:00 起)
_AM_START = 9 * 60 + 30   # 09:30
_AM_END = 11 * 60 + 30    # 11:30
_PM_START = 13 * 60        # 13:00
_PM_END = 15 * 60          # 15:00


def _time_to_minutes(t: time) -> int:
    """将 time 转为当日分钟数。"""
    return t.hour * 60 + t.minute


def _bar_slot(dt: datetime, interval: int) -> int | None:
    """
    根据 datetime 计算该 tick 属于哪根 bar 的 slot 编号。

    slot 编号从每日第一根 bar 开始，跨午休、跨日连续递增。
    返回 None 表示不在交易时段内。
    """
    t = dt.time()
    m = _time_to_minutes(t)

    if m < _AM_START or m >= _PM_END:
        return None

    # 计算距离当日交易起始的交易分钟数
    if m < _AM_END:
        trading_minutes = m - _AM_START
    elif m < _PM_START:
        # 午休期间，归入上午最后一根 bar
        trading_minutes = _AM_END - _AM_START - 1
    else:
        trading_minutes = (_AM_END - _AM_START) + (m - _PM_START)

    # 加上日期偏移量 (每天 240 分钟交易时间)
    day_offset = dt.toordinal() * 240
    return day_offset + trading_minutes // interval


class _RatioBarAggregator:
    """将 tick 级 ABratio 聚合为固定周期 K 线并维护收盘价环形缓冲区。"""

    def __init__(self, interval: int, maxlen: int) -> None:
        self.interval = interval
        self.closes: deque[float] = deque(maxlen=maxlen)
        self._current_slot: int | None = None
        self._bar_open: float = 0.0
        self._bar_high: float = 0.0
        self._bar_low: float = 0.0
        self._bar_close: float = 0.0

    def update(self, dt: datetime, ratio: float) -> bool:
        """
        喂入一个 ABratio 数据点。当 bar 切换时返回 True。
        """
        slot = _bar_slot(dt, self.interval)
        if slot is None:
            return False

        if self._current_slot is None:
            # 首次初始化
            self._current_slot = slot
            self._bar_open = self._bar_high = self._bar_low = self._bar_close = ratio
            return False

        if slot == self._current_slot:
            # 仍在同一根 bar
            self._bar_high = max(self._bar_high, ratio)
            self._bar_low = min(self._bar_low, ratio)
            self._bar_close = ratio
            return False

        # bar 切换: 完成当前 bar
        self.closes.append(self._bar_close)

        # 开始新 bar
        self._current_slot = slot
        self._bar_open = self._bar_high = self._bar_low = self._bar_close = ratio
        return True

    def flush(self) -> None:
        """强制完成当前 bar（用于预热结束时）。"""
        if self._current_slot is not None and self._bar_close > 0:
            self.closes.append(self._bar_close)
            self._current_slot = None


# ════════════════════════════════════════════════════════════════
#  统计量辅助
# ════════════════════════════════════════════════════════════════

def _sma_std(data, window: int) -> tuple[float, float]:
    """计算滑动均值和标准差。data 可以是 deque 或 list。"""
    if len(data) < window:
        return 0.0, 0.0
    recent = list(data)[-window:]
    n = len(recent)
    mu = sum(recent) / n
    variance = sum((x - mu) ** 2 for x in recent) / n
    sigma = math.sqrt(variance) if variance > 0 else 0.0
    return mu, sigma


# ════════════════════════════════════════════════════════════════
#  DS_DMTR 策略
# ════════════════════════════════════════════════════════════════


class DsDmtrStrategy(StrategyBase):
    """Dual-Stock Dual-MA Tick Reversion Strategy."""

    author = "cstm"

    # ── 可配置参数 ──
    symbol_a: str = ""
    symbol_b: str = ""
    dataset_dir: str = ""
    bar_interval_minutes: int = 30
    window_minutes: int = 20
    window_days: int = 20
    k_sigma_minutes: float = 2.0
    k_sigma_days: float = 2.0
    thresh_sigma_min: float = 0.5
    thresh_sigma_min_high: float = 1.0
    thresh_sigma_day: float = 1.5
    thresh_delta_min: float = 0.005
    cooldown_seconds: int = 1800
    trading_cutoff_str: str = "14:55:00"
    block_timeout_minutes: int = 20
    timeout_recover_minutes: int = 2
    timeout_recover_policy: str = "balance"
    chase_wait_ticks: int = 3
    max_chase_rounds: int = 20
    passive_slice_count: int = 3
    cancel_priority: str = "newest_unfilled_first"
    base_pct: float = 0.1
    high_pct: float = 0.3
    min_order_ratio: float = 0.1
    open_wait_minutes: int = 5
    enable_signal_check: bool = True

    # ── T+0 parameter ──
    enable_t0: bool = False

    # ── 决策器类型 ──
    decision_type: str = "original"

    parameters = [
        "symbol_a", "symbol_b", "dataset_dir",
        "bar_interval_minutes", "window_minutes", "window_days",
        "k_sigma_minutes", "k_sigma_days",
        "thresh_sigma_min", "thresh_sigma_min_high",
        "thresh_sigma_day", "thresh_delta_min",
        "cooldown_seconds", "trading_cutoff_str",
        "block_timeout_minutes", "timeout_recover_minutes",
        "timeout_recover_policy",
        "chase_wait_ticks", "max_chase_rounds",
        "passive_slice_count", "cancel_priority",
        "base_pct", "high_pct", "min_order_ratio", "open_wait_minutes",
        "enable_signal_check",
        "enable_t0",
        "decision_type",
    ]

    variables = [
        "ab_ratio", "mu_min", "sigma_min",
        "mu_day", "sigma_day",
        "delta_sigma_minutes", "delta_sigma_days",
        "delta_minutes", "a_percentage",
    ]

    def __init__(self, engine, strategy_name, symbols, setting=None):
        super().__init__(engine, strategy_name, symbols, setting)

        # ── 实时指标 ──
        self.ab_ratio: float = 0.0
        self.mu_min: float = 0.0
        self.sigma_min: float = 0.0
        self.mu_day: float = 0.0
        self.sigma_day: float = 0.0
        self.delta_sigma_minutes: float = 0.0
        self.delta_sigma_days: float = 0.0
        self.delta_minutes: float = 0.0
        self.a_percentage: float = 0.0

        # ── 内部状态 ──
        self._bar_feed: ParquetBarFeed | None = None
        self._tick_feed: ParquetTickFeed | None = None

        # 30-min bar 聚合器
        self._bar_agg: _RatioBarAggregator | None = None

        # 日线 ABratio 序列
        self._ratio_day_closes: list[float] = []
        # 全部日线数据 {symbol: {date_str: (close_adj, adj_factor)}}
        self._all_daily: dict[str, dict[str, tuple[float, float]]] = {}

        # 最新 tick 价格
        self._price_a: float = 0.0
        self._price_b: float = 0.0

        # 交易跟踪
        self._last_trade_direction: str = ""   # "SELL_A_BUY_B" or "SELL_B_BUY_A"
        self._last_trade_time: datetime | None = None

        # 当前日期
        self._current_date: str = ""

        # 预热完成标志
        self._warmup_done: bool = False

        # 分钟级统计量是否有效
        self._min_stats_valid: bool = False
        # 日线级统计量是否有效
        self._day_stats_valid: bool = False

        # ── Block 跟踪 ──
        self._block_logs: list[RotationBlock] = []
        self._block_seq: int = 0          # 当日 block 序号
        self._active_block: RotationBlock | None = None
        self._order_to_block: dict[str, RotationBlock] = {}
        self._order_meta: dict[str, BlockOrderMeta] = {}
        self._order_submit_seq: int = 0
        self._tick_seq: int = 0
        self._trading_cutoff_time: time = time(14, 55, 0)
        self._timeout_policy: str = "balance"

        # 交易日志（供 GUI 使用，保留兼容）
        self._trade_logs: list[dict] = []

        # ── 可插拔决策器 ──
        self._decision = create_decision(self.decision_type, setting)

    # ────────────────────────────────────────────────
    #  on_init
    # ────────────────────────────────────────────────

    def on_init(self):
        self._bar_feed = ParquetBarFeed(self.dataset_dir)
        self._tick_feed = ParquetTickFeed(self.dataset_dir)
        self._trading_cutoff_time = self._parse_cutoff_time(
            self.trading_cutoff_str
        )
        policy = str(self.timeout_recover_policy).strip().lower()
        if policy == "recover":
            policy = "balance"
        if policy not in {"full", "balance", "abort"}:
            self.write_log(
                f"DS_DMTR: timeout_recover_policy={self.timeout_recover_policy} 非法，回退 balance"
            )
            policy = "balance"
        self._timeout_policy = policy

        # 初始化 bar 聚合器（如果已经预热过，不重置，避免丢失预热数据）
        if not self._warmup_done:
            self._bar_agg = _RatioBarAggregator(
                interval=self.bar_interval_minutes,
                maxlen=self.window_minutes + 10,
            )

        # 加载全部日线数据
        for symbol in [self.symbol_a, self.symbol_b]:
            bars = self._bar_feed.load(symbol)
            daily: dict[str, tuple[float, float]] = {}
            for bar in bars:
                d = bar.datetime.strftime("%Y%m%d")
                daily[d] = (bar.close_price, bar.adj_factor)
            self._all_daily[symbol] = daily

        self.write_log(
            f"DS_DMTR on_init: A={self.symbol_a}, B={self.symbol_b}, "
            f"bar_interval={self.bar_interval_minutes}min, "
            f"window_min={self.window_minutes}, window_day={self.window_days}, "
            f"cutoff={self._trading_cutoff_time.strftime('%H:%M:%S')}, "
            f"timeout_stage2={self.timeout_recover_minutes}min/{self._timeout_policy}"
        )

    def _parse_cutoff_time(self, value: str) -> time:
        try:
            parts = [int(x) for x in str(value).split(":")]
            if len(parts) != 3:
                raise ValueError()
            h, m, s = parts
            return time(hour=h, minute=m, second=s)
        except Exception:
            self.write_log(
                f"DS_DMTR: trading_cutoff_str={value} 非法，回退 14:55:00"
            )
            return time(14, 55, 0)

    # ────────────────────────────────────────────────
    #  on_day_begin
    # ────────────────────────────────────────────────

    def on_day_begin(self, bar: BarData):
        if not bar:
            return

        prev_date = bar.datetime.strftime("%Y%m%d")

        if self._active_block:
            self._cancel_block_orders(self._active_block)
            day_close_time = bar.datetime.replace(
                hour=15, minute=0, second=0, microsecond=0
            )
            self._finalize_block(
                self._active_block, day_close_time, BlockState.TIMEOUT
            )

        # 重置当日 block 序号
        self._block_seq = 0

        # 更新日线 ABratio 序列
        daily_a = self._all_daily.get(self.symbol_a, {})
        daily_b = self._all_daily.get(self.symbol_b, {})

        entry_a = daily_a.get(prev_date)
        entry_b = daily_b.get(prev_date)

        if entry_a and entry_b and entry_b[0] > 0:
            ratio = entry_a[0] / entry_b[0]
            self._ratio_day_closes.append(ratio)

            # 裁剪历史，避免无限增长
            max_history = self.window_days * 3
            if len(self._ratio_day_closes) > max_history:
                self._ratio_day_closes = self._ratio_day_closes[-max_history:]

        # 重算日线级布林统计量
        self.mu_day, self.sigma_day = _sma_std(
            self._ratio_day_closes, self.window_days
        )
        self._day_stats_valid = self.sigma_day > 0

    # ────────────────────────────────────────────────
    #  预热：在回测正式开始前构建分钟级统计量
    # ────────────────────────────────────────────────

    def warmup(self, start_date: str) -> None:
        """
        在回测正式开始之前调用，加载 start_date 前足够多天的 tick 数据
        并进行 bar 聚合，使分钟级布林统计量在回测首日即可用。
        """
        daily_a = self._all_daily.get(self.symbol_a, {})
        daily_b = self._all_daily.get(self.symbol_b, {})
        prefill_dates = sorted(
            d for d in daily_a.keys()
            if d in daily_b and d < start_date
        )
        if prefill_dates:
            keep = max(self.window_days * 3, self.window_days)
            for d in prefill_dates[-keep:]:
                close_a = daily_a[d][0]
                close_b = daily_b[d][0]
                if close_a > 0 and close_b > 0:
                    self._ratio_day_closes.append(close_a / close_b)
            self.mu_day, self.sigma_day = _sma_std(
                self._ratio_day_closes, self.window_days
            )
            self._day_stats_valid = self.sigma_day > 0

        bars_per_day = 240 // self.bar_interval_minutes
        warmup_days_needed = math.ceil(self.window_minutes / bars_per_day) + 2

        # 获取 A 的可用 tick 日期
        available_dates = self._tick_feed.get_available_dates(self.symbol_a)
        dates_before = [d for d in available_dates if d < start_date]
        warmup_dates = dates_before[-warmup_days_needed:]

        if not warmup_dates:
            self.write_log("DS_DMTR warmup: 无可用预热数据")
            return

        self.write_log(
            f"DS_DMTR warmup: 加载 {len(warmup_dates)} 天 tick 数据 "
            f"({warmup_dates[0]} ~ {warmup_dates[-1]})"
        )

        for date_str in warmup_dates:
            ticks_a = self._tick_feed.load_day(self.symbol_a, date_str)
            ticks_b = self._tick_feed.load_day(self.symbol_b, date_str)

            # 构建 B 的时间→价格映射
            b_prices: dict[datetime, float] = {}
            for t in ticks_b:
                if t.last_price > 0:
                    b_prices[t.datetime] = t.last_price

            last_b_price = 0.0
            for t_a in ticks_a:
                if t_a.last_price <= 0:
                    continue
                tt = t_a.datetime.time()
                if tt < time(9, 30) or tt >= time(15, 0):
                    continue

                # 找到 B 在该时刻的价格（前向填充）
                if t_a.datetime in b_prices:
                    last_b_price = b_prices[t_a.datetime]
                else:
                    # 找最近的 <= 当前时间的 B 价格
                    for tb in ticks_b:
                        if tb.datetime <= t_a.datetime and tb.last_price > 0:
                            last_b_price = tb.last_price

                if last_b_price <= 0:
                    continue

                ratio = t_a.last_price / last_b_price
                self._bar_agg.update(t_a.datetime, ratio)

        # 完成预热后 flush 当前 bar
        self._bar_agg.flush()

        # 计算初始分钟级统计量
        self.mu_min, self.sigma_min = _sma_std(
            self._bar_agg.closes, self.window_minutes
        )
        self._min_stats_valid = self.sigma_min > 0

        self.write_log(
            f"DS_DMTR warmup done: {len(self._bar_agg.closes)} bars generated, "
            f"mu_min={self.mu_min:.6f}, sigma_min={self.sigma_min:.6f}"
        )
        self._warmup_done = True

    # ────────────────────────────────────────────────
    #  on_tick
    # ────────────────────────────────────────────────

    def on_tick(self, tick: TickData):
        if tick.last_price <= 0:
            return
        tick_time = tick.datetime.time()
        if tick_time < time(9, 30) or tick_time >= time(15, 0):
            return

        if tick.symbol == self.symbol_a:
            self._price_a = tick.last_price
        elif tick.symbol == self.symbol_b:
            self._price_b = tick.last_price
        else:
            return

        if tick.symbol == self.symbol_a:
            tick_b = self.get_latest_tick(self.symbol_b)
            if tick_b and tick_b.last_price > 0:
                self._price_b = tick_b.last_price

        if self._price_a <= 0 or self._price_b <= 0:
            return

        self.ab_ratio = self._price_a / self._price_b

        bar_switched = self._bar_agg.update(tick.datetime, self.ab_ratio)
        if bar_switched:
            self.mu_min, self.sigma_min = _sma_std(
                self._bar_agg.closes, self.window_minutes
            )
            self._min_stats_valid = self.sigma_min > 0

        if self._min_stats_valid:
            self.delta_sigma_minutes = (
                (self.ab_ratio - self.mu_min) / self.sigma_min
            )
            self.delta_minutes = (
                (self.ab_ratio - self.mu_min) / abs(self.ab_ratio)
            )
        else:
            self.delta_sigma_minutes = 0.0
            self.delta_minutes = 0.0

        if self._day_stats_valid:
            self.delta_sigma_days = (
                (self.ab_ratio - self.mu_day) / self.sigma_day
            )
        else:
            self.delta_sigma_days = 0.0

        account = self.get_account()
        pos_a = self.get_position(self.symbol_a)
        pos_b = self.get_position(self.symbol_b)
        nav = (
            account.balance
            + pos_a.volume * self._price_a
            + pos_b.volume * self._price_b
        )
        if nav > 0:
            self.a_percentage = (pos_a.volume * self._price_a) / nav
        else:
            self.a_percentage = 0.0

        if tick.symbol != self.symbol_a:
            return

        self._tick_seq += 1

        if self._active_block:
            if tick_time >= self._trading_cutoff_time:
                self._handle_block_timeout(self._active_block, tick.datetime)
                return
            self._manage_active_block(tick)
            return

        if tick_time >= self._trading_cutoff_time:
            return

        # 开盘等待：避免开盘波动期发出信号
        if self.open_wait_minutes > 0:
            open_time = time(9, 30, 0)
            wait_end = time(
                9, 30 + self.open_wait_minutes, 0
            ) if self.open_wait_minutes <= 30 else time(
                9 + (30 + self.open_wait_minutes) // 60,
                (30 + self.open_wait_minutes) % 60, 0
            )
            if open_time <= tick_time < wait_end:
                return

        pending = self.get_pending_orders()
        if pending:
            return

        cutoff_dt = tick.datetime.replace(
            hour=self._trading_cutoff_time.hour,
            minute=self._trading_cutoff_time.minute,
            second=self._trading_cutoff_time.second,
            microsecond=0,
        )
        remaining_min = (cutoff_dt - tick.datetime).total_seconds() / 60.0
        if remaining_min < self.block_timeout_minutes:
            return

        ctx = self._build_decision_context(tick.datetime)
        signal = self._decision.decide(ctx)
        if signal:
            self._execute(signal.direction, signal.trade_pct, tick.datetime, nav,
                          signal_reason=signal.reason)

    # ────────────────────────────────────────────────
    #  决策上下文构建
    # ────────────────────────────────────────────────

    def _build_decision_context(self, current_time: datetime) -> DecisionContext:
        """从策略当前状态构建 DecisionContext 只读快照。"""
        return DecisionContext(
            ab_ratio=self.ab_ratio,
            mu_min=self.mu_min,
            sigma_min=self.sigma_min,
            mu_day=self.mu_day,
            sigma_day=self.sigma_day,
            delta_sigma_minutes=self.delta_sigma_minutes,
            delta_sigma_days=self.delta_sigma_days,
            delta_minutes=self.delta_minutes,
            min_stats_valid=self._min_stats_valid,
            day_stats_valid=self._day_stats_valid,
            a_percentage=self.a_percentage,
            last_trade_direction=self._last_trade_direction,
            last_trade_time=self._last_trade_time,
            current_time=current_time,
        )

    # ────────────────────────────────────────────────
    #  决策函数（保留，供向后兼容）
    # ────────────────────────────────────────────────

    def _decide(
        self, current_time: datetime
    ) -> tuple[bool, str, float]:
        """
        决策函数。

        返回 (should_act, direction, trade_pct)。
        direction: "SELL_A_BUY_B" 或 "SELL_B_BUY_A"
        """
        if not self._min_stats_valid:
            return False, "", 0.0

        abs_dsm = abs(self.delta_sigma_minutes)
        abs_dm = abs(self.delta_minutes)

        if abs_dsm <= self.thresh_sigma_min or abs_dm <= self.thresh_delta_min:
            return False, "", 0.0

        # 方向
        if self.delta_sigma_minutes > 0:
            direction = "SELL_A_BUY_B"
        else:
            direction = "SELL_B_BUY_A"

        # 冷却检查
        if self._last_trade_direction == direction and self._last_trade_time:
            elapsed = (current_time - self._last_trade_time).total_seconds()
            if elapsed < self.cooldown_seconds:
                return False, "", 0.0

        trade_pct = self.base_pct

        abs_dsd = abs(self.delta_sigma_days) if self._day_stats_valid else 0.0
        day_minute_same_dir = (
            self._day_stats_valid
            and self.delta_sigma_minutes * self.delta_sigma_days > 0
        )
        if (
            abs_dsm > self.thresh_sigma_min_high
            or (
                abs_dsd > self.thresh_sigma_day
                and day_minute_same_dir
            )
        ):
            trade_pct = self.high_pct

        return True, direction, trade_pct

    # ────────────────────────────────────────────────
    #  执行函数
    # ────────────────────────────────────────────────

    def _execute(
        self,
        direction: str,
        trade_pct: float,
        current_time: datetime,
        nav: float,
        signal_reason: str = "",
    ) -> None:
        """执行轮动交易，并创建 RotationBlock 跟踪。"""
        if direction == "SELL_A_BUY_B":
            sell_sym, buy_sym = self.symbol_a, self.symbol_b
            sell_price, buy_price = self._price_a, self._price_b
        else:
            sell_sym, buy_sym = self.symbol_b, self.symbol_a
            sell_price, buy_price = self._price_b, self._price_a

        if sell_price <= 0 or buy_price <= 0:
            return

        pos_sell = self.get_position(sell_sym)
        avail = (
            pos_sell.volume if self.enable_t0
            else max(0, pos_sell.volume - pos_sell.today_bought)
        )
        desired_sell_vol = int(nav * trade_pct / sell_price / 100) * 100
        sell_volume = min(avail, desired_sell_vol)

        account = self.get_account()
        cash_now = account.available + sell_volume * sell_price
        buy_price_passive = self._get_order_price(
            buy_sym, "buy", aggressive=False
        )
        desired_buy_vol = int(nav * trade_pct / buy_price / 100) * 100
        max_affordable = int(max(0.0, cash_now) / buy_price / 100) * 100
        buy_volume = min(desired_buy_vol, max_affordable)

        sell_notional = desired_sell_vol * sell_price
        buy_notional = desired_buy_vol * buy_price
        max_allowed_gap = max(sell_price, buy_price) * 100
        if abs(sell_notional - buy_notional) > max_allowed_gap:
            self.write_log(
                "WARN rotation: notional gap too large "
                f"sell={sell_notional:.2f}, buy={buy_notional:.2f}, "
                f"allowed={max_allowed_gap:.2f}"
            )

        if sell_volume < 100 and buy_volume < 100:
            return

        # min_order_ratio 检查：两腿发单量都低于期望量的比例时放弃
        if self.min_order_ratio > 0:
            sell_ok = (desired_sell_vol == 0) or (
                sell_volume >= desired_sell_vol * self.min_order_ratio
            )
            buy_ok = (desired_buy_vol == 0) or (
                buy_volume >= desired_buy_vol * self.min_order_ratio
            )
            if not sell_ok and not buy_ok:
                self.write_log(
                    f"SKIP block: min_order_ratio={self.min_order_ratio} "
                    f"sell={sell_volume}/{desired_sell_vol} "
                    f"buy={buy_volume}/{desired_buy_vol}"
                )
                return

        self._block_seq += 1
        trade_date = current_time.strftime("%Y%m%d")
        block = RotationBlock(
            block_id=f"{trade_date}-{self._block_seq:03d}",
            trade_date=trade_date,
            block_seq=self._block_seq,
            signal_time=current_time,
            direction=direction,
            sell_symbol=sell_sym,
            buy_symbol=buy_sym,
            trade_pct=trade_pct,
            is_high_pct=(trade_pct == self.high_pct),
            # 指标快照
            ab_ratio=self.ab_ratio,
            delta_sigma_minutes=self.delta_sigma_minutes,
            delta_sigma_days=self.delta_sigma_days,
            delta_minutes=self.delta_minutes,
            mu_min=self.mu_min,
            sigma_min=self.sigma_min,
            mu_day=self.mu_day,
            sigma_day=self.sigma_day,
            a_percentage=self.a_percentage,
            signal_reason=signal_reason,
            # 市场价格
            sell_signal_price=sell_price,
            buy_signal_price=buy_price,
            # 账户快照
            signal_cash=account.balance,
            signal_nav=nav,
            desired_sell_volume=desired_sell_vol,
            desired_buy_volume=desired_buy_vol,
            sell_order_volume=sell_volume,
            buy_order_volume=buy_volume,
            state=BlockState.PENDING,
            timeout_deadline=current_time + timedelta(
                minutes=self.block_timeout_minutes
            ),
            timeout_budget_seconds=max(1, int(self.block_timeout_minutes)) * 60,
            timeout_anchor_time=current_time,
        )

        sell_passive_price = self._get_order_price(sell_sym, "sell", aggressive=False)
        buy_passive_price = buy_price_passive

        if sell_volume >= 100:
            self._submit_sliced_passive_orders(
                block, "sell", sell_sym, sell_passive_price, sell_volume
            )
            self.write_log(
                f"SELL {sell_sym} {sell_volume} @ {sell_passive_price:.4f} "
                f"(dsm={self.delta_sigma_minutes:.3f}, "
                f"dsd={self.delta_sigma_days:.3f})"
            )

        if buy_volume >= 100:
            self._submit_sliced_passive_orders(
                block, "buy", buy_sym, buy_passive_price, buy_volume
            )
            self.write_log(
                f"BUY {buy_sym} {buy_volume} @ {buy_passive_price:.4f}"
            )

        if sell_volume < 100 and buy_volume >= 100:
            block.state = BlockState.BUY_ONLY
        elif sell_volume >= 100 and buy_volume < 100:
            block.state = BlockState.PARTIAL

        self._active_block = block
        self._block_logs.append(block)

        # 兼容旧 _trade_logs
        self._trade_logs.append({
            "time": current_time,
            "direction": direction,
            "sell_sym": sell_sym,
            "sell_vol": sell_volume,
            "sell_price": sell_price,
            "buy_sym": buy_sym,
            "buy_vol": buy_volume,
            "buy_price": buy_price,
            "trade_pct": trade_pct,
            "delta_sigma_minutes": self.delta_sigma_minutes,
            "delta_sigma_days": self.delta_sigma_days,
            "delta_minutes": self.delta_minutes,
            "a_percentage": self.a_percentage,
        })

    def _register_order(
        self,
        order_id: str,
        block: RotationBlock,
        side: str,
        is_aggressive: bool,
    ) -> None:
        self._order_submit_seq += 1
        self._order_to_block[order_id] = block
        self._order_meta[order_id] = BlockOrderMeta(
            side=side,
            is_aggressive=is_aggressive,
            submit_seq=self._order_submit_seq,
        )
        block.order_ids.append(order_id)
        block.active_order_ids.add(order_id)

    def _split_volume(self, total: int, n: int) -> list[int]:
        total = (int(total) // 100) * 100
        if total < 100:
            return []
        n = max(1, int(n))
        max_parts = max(1, total // 100)
        n = min(n, max_parts)
        lots = total // 100
        base = lots // n
        rem = lots % n
        out: list[int] = []
        for i in range(n):
            q = (base + (1 if i < rem else 0)) * 100
            if q >= 100:
                out.append(q)
        return out

    def _submit_sliced_passive_orders(
        self,
        block: RotationBlock,
        side: str,
        symbol: str,
        price: float,
        total_qty: int,
    ) -> None:
        if price <= 0 or total_qty < 100:
            return
        slices = self._split_volume(total_qty, self.passive_slice_count)
        if not slices:
            return
        for qty in slices:
            if side == "buy":
                oid = self.buy(symbol, price, qty)
            else:
                oid = self.sell(symbol, price, qty)
            if oid:
                self._register_order(str(oid), block, side, False)

    def _update_block_snapshot(self, block: RotationBlock) -> None:
        account = self.get_account()
        pos_a = self.get_position(self.symbol_a)
        pos_b = self.get_position(self.symbol_b)
        block.end_cash = account.balance
        block.end_nav = (
            account.balance
            + pos_a.volume * self._price_a
            + pos_b.volume * self._price_b
        )

    def _is_block_complete(self, block: RotationBlock) -> bool:
        sell_done = (
            block.sell_order_volume < 100 or block.sell_filled >= block.sell_order_volume
        )
        buy_done = (
            block.buy_order_volume < 100 or block.buy_filled >= block.buy_order_volume
        )
        return sell_done and buy_done

    def _finalize_block(
        self,
        block: RotationBlock,
        end_time: datetime,
        final_state: BlockState | None = None,
    ) -> None:
        block.end_time = end_time
        self._update_block_snapshot(block)
        if final_state is not None:
            block.state = final_state
        elif self._is_block_complete(block):
            block.state = BlockState.DONE
        elif block.sell_filled > 0 or block.buy_filled > 0:
            block.state = BlockState.PARTIAL
        else:
            block.state = BlockState.REJECTED

        for oid in list(block.order_ids):
            self._order_to_block.pop(oid, None)
            self._order_meta.pop(oid, None)
        block.active_order_ids.clear()
        if self._active_block is block:
            self._active_block = None

    def _get_order_price(
        self, symbol: str, side: str, aggressive: bool
    ) -> float:
        tick = self.get_latest_tick(symbol)
        last = self._price_a if symbol == self.symbol_a else self._price_b
        if tick:
            if side == "buy":
                if aggressive and tick.ask_price_1 > 0:
                    return tick.ask_price_1
                if not aggressive and tick.bid_price_1 > 0:
                    return tick.bid_price_1
            else:
                if aggressive and tick.bid_price_1 > 0:
                    return tick.bid_price_1
                if not aggressive and tick.ask_price_1 > 0:
                    return tick.ask_price_1
            if tick.last_price > 0:
                return tick.last_price
        return max(last, 0.0)

    def _cancel_side_orders(self, block: RotationBlock, side: str) -> None:
        for oid in list(block.active_order_ids):
            meta = self._order_meta.get(oid)
            if not meta:
                continue
            if meta.side != side:
                continue
            self.cancel_order(oid)

    def _cancel_block_orders(self, block: RotationBlock) -> None:
        for oid in list(block.active_order_ids):
            self.cancel_order(oid)

    def _trading_seconds_between(self, start: datetime, end: datetime) -> int:
        if end <= start:
            return 0
        total = 0
        day = start.date()
        end_day = end.date()
        while day <= end_day:
            am_start = datetime.combine(day, time(9, 30, 0))
            am_end = datetime.combine(day, time(11, 30, 0))
            pm_start = datetime.combine(day, time(13, 0, 0))
            pm_end = datetime.combine(day, time(15, 0, 0))
            s1 = max(start, am_start)
            e1 = min(end, am_end)
            if e1 > s1:
                total += int((e1 - s1).total_seconds())
            s2 = max(start, pm_start)
            e2 = min(end, pm_end)
            if e2 > s2:
                total += int((e2 - s2).total_seconds())
            day += timedelta(days=1)
        return max(0, total)

    def _timeout_expired(self, block: RotationBlock, now: datetime) -> bool:
        anchor = block.timeout_anchor_time
        if anchor is None:
            block.timeout_anchor_time = now
            return block.timeout_budget_seconds <= 0
        elapsed = self._trading_seconds_between(anchor, now)
        if elapsed > 0:
            block.timeout_budget_seconds = max(
                0, int(block.timeout_budget_seconds) - elapsed
            )
        block.timeout_anchor_time = now
        return block.timeout_budget_seconds <= 0

    def _start_immediate_chase(
        self, block: RotationBlock, side: str, now: datetime, qty: int | None = None
    ) -> None:
        if qty is None or qty < 100:
            return

        target_qty = (int(qty) // 100) * 100
        pending_now = self._pending_volume(block, side)
        if side == "buy":
            filled = block.buy_filled
            max_total = block.buy_order_volume
        else:
            filled = block.sell_filled
            max_total = block.sell_order_volume

        room_now = max(0, max_total - filled - pending_now)
        if room_now < target_qty:
            need_cancel = target_qty - room_now
            self._cancel_passive_for_chase(block, side, need_cancel)
        target_qty = min(target_qty, max(0, room_now))
        target_qty = (target_qty // 100) * 100
        if target_qty < 100:
            return

        if side == "buy":
            symbol = block.buy_symbol
            price = self._get_order_price(symbol, "buy", aggressive=True)
            if target_qty < 100 or price <= 0:
                return
            oid = self.buy(symbol, price, target_qty)
        else:
            symbol = block.sell_symbol
            price = self._get_order_price(symbol, "sell", aggressive=True)
            pos = self.get_position(symbol)
            target_qty = min(target_qty, pos.available)
            if target_qty < 100 or price <= 0:
                return
            oid = self.sell(symbol, price, target_qty)

        if oid:
            oid = str(oid)
            self._register_order(oid, block, side, True)
            block.state = BlockState.CHASING
            block.chase_order_id = oid
            block.chase_side = side
            block.chase_submit_tick = self._tick_seq

    def _pending_volume(self, block: RotationBlock, side: str) -> int:
        total = 0
        orders = {o.order_id: o for o in self.get_pending_orders()}
        for oid in block.active_order_ids:
            meta = self._order_meta.get(oid)
            if not meta:
                continue
            if meta.side != side:
                continue
            order = orders.get(oid)
            if order:
                total += max(0, order.remaining)
        return total

    def _pending_amount(self, block: RotationBlock, side: str, ref_price: float) -> float:
        if ref_price <= 0:
            return 0.0
        return float(self._pending_volume(block, side)) * ref_price

    def _cancel_passive_for_chase(
        self, block: RotationBlock, side: str, target_qty: int
    ) -> int:
        need = (int(target_qty) // 100) * 100
        if need < 100:
            return 0
        orders = {o.order_id: o for o in self.get_pending_orders()}
        candidates: list[tuple[int, str]] = []
        for oid in list(block.active_order_ids):
            meta = self._order_meta.get(oid)
            if not meta or meta.side != side or meta.is_aggressive:
                continue
            order = orders.get(oid)
            if not order or order.remaining < 100:
                continue
            priority = 0
            if self.cancel_priority == "newest_unfilled_first":
                is_unfilled = 1 if order.traded == 0 else 0
                priority = is_unfilled * 10_000_000 + meta.submit_seq
            else:
                priority = meta.submit_seq
            candidates.append((priority, oid))
        candidates.sort(reverse=True)

        canceled = 0
        for _priority, oid in candidates:
            if canceled >= need:
                break
            order = orders.get(oid)
            if not order:
                continue
            self.cancel_order(oid)
            canceled += (order.remaining // 100) * 100
        return canceled

    def _calc_gap_qty(self, block: RotationBlock) -> tuple[str, int]:
        buy_ref = self._get_order_price(block.buy_symbol, "buy", aggressive=True)
        sell_ref = self._get_order_price(block.sell_symbol, "sell", aggressive=True)
        if buy_ref <= 0 or sell_ref <= 0:
            return "", 0

        buy_gap_amt = block.sell_cost - block.buy_cost
        if buy_gap_amt >= buy_ref * 100:
            qty = int(buy_gap_amt / buy_ref / 100) * 100
            return "buy", max(0, qty)

        sell_gap_amt = block.buy_cost - block.sell_cost
        if sell_gap_amt >= sell_ref * 100:
            qty = int(sell_gap_amt / sell_ref / 100) * 100
            return "sell", max(0, qty)

        return "", 0

    def _calc_full_recover_qty(self, block: RotationBlock) -> tuple[str, int]:
        buy_room = max(
            0,
            block.buy_order_volume
            - block.buy_filled
            - self._pending_volume(block, "buy"),
        )
        sell_room = max(
            0,
            block.sell_order_volume
            - block.sell_filled
            - self._pending_volume(block, "sell"),
        )
        buy_room = (buy_room // 100) * 100
        sell_room = (sell_room // 100) * 100
        if buy_room < 100 and sell_room < 100:
            return "", 0
        if buy_room >= sell_room and buy_room >= 100:
            return "buy", buy_room
        if sell_room >= 100:
            return "sell", sell_room
        return "", 0

    def _check_and_handle_excess(self, block: RotationBlock, now: datetime) -> None:
        if block.force_aggressive_sent and self._timeout_policy == "full":
            side, gap_qty = self._calc_full_recover_qty(block)
        else:
            side, gap_qty = self._calc_gap_qty(block)
        if not side or gap_qty < 100:
            if block.state == BlockState.CHASING:
                if block.chase_order_id:
                    self.cancel_order(block.chase_order_id)
                block.chase_order_id = ""
                block.chase_side = ""
                block.state = BlockState.MATCHING
            return
        if block.chase_order_id:
            return
        self._start_immediate_chase(block, side, now, gap_qty)

    def _check_chase_timeout(self, block: RotationBlock, now: datetime) -> None:
        if not block.chase_order_id:
            return
        if self._tick_seq - block.chase_submit_tick < self.chase_wait_ticks:
            return
        if block.chase_round >= self.max_chase_rounds:
            self._cancel_block_orders(block)
            self._finalize_block(block, now, BlockState.CRITICAL)
            return

        chase_id = block.chase_order_id
        if chase_id not in block.active_order_ids:
            block.chase_order_id = ""
            block.chase_side = ""
            block.state = BlockState.MATCHING
            return

        meta = self._order_meta.get(chase_id)
        side = meta.side if meta else block.chase_side
        if side not in {"buy", "sell"}:
            block.chase_order_id = ""
            block.chase_side = ""
            block.state = BlockState.MATCHING
            return
        orders = {o.order_id: o for o in self.get_pending_orders()}
        chase_order = orders.get(chase_id)
        remaining = chase_order.remaining if chase_order else 0
        self.cancel_order(chase_id)
        block.chase_round += 1
        if remaining < 100:
            block.chase_order_id = ""
            block.chase_side = ""
            block.state = BlockState.MATCHING
            return

        if side == "buy":
            symbol = block.buy_symbol
            price = self._get_order_price(symbol, "buy", aggressive=True)
            if remaining < 100 or price <= 0:
                block.chase_order_id = ""
                block.state = BlockState.MATCHING
                return
            new_oid = self.buy(symbol, price, remaining)
        else:
            symbol = block.sell_symbol
            pos = self.get_position(symbol)
            remaining = min(remaining, pos.available)
            price = self._get_order_price(symbol, "sell", aggressive=True)
            if remaining < 100 or price <= 0:
                block.chase_order_id = ""
                block.state = BlockState.MATCHING
                return
            new_oid = self.sell(symbol, price, remaining)

        if new_oid:
            new_oid = str(new_oid)
            self._register_order(new_oid, block, side, True)
            block.chase_order_id = new_oid
            block.chase_side = side
            block.chase_submit_tick = self._tick_seq
            block.state = BlockState.CHASING
        else:
            self._finalize_block(block, now, BlockState.CRITICAL)

    def _handle_block_timeout(self, block: RotationBlock, now: datetime) -> None:
        if block.sell_filled == 0 and block.buy_filled == 0:
            self._cancel_block_orders(block)
            self._finalize_block(block, now, BlockState.TIMEOUT)
            return

        if self._timeout_policy == "abort":
            self._cancel_block_orders(block)
            self._finalize_block(block, now, BlockState.PARTIAL)
            return

        if not block.force_aggressive_sent:
            block.force_aggressive_sent = True
            self._cancel_block_orders(block)
            if self._timeout_policy == "full":
                side, gap_qty = self._calc_full_recover_qty(block)
            else:
                side, gap_qty = self._calc_gap_qty(block)
            if side and gap_qty >= 100:
                self._start_immediate_chase(block, side, now, gap_qty)
            extra = max(1, int(self.timeout_recover_minutes))
            block.timeout_deadline = now + timedelta(minutes=extra)
            block.timeout_budget_seconds = extra * 60
            block.timeout_anchor_time = now
            if block.chase_order_id:
                block.state = BlockState.CHASING
            elif self._timeout_policy == "full" and self._is_block_complete(block):
                self._finalize_block(block, now, BlockState.DONE)
            elif self._timeout_policy != "full" and self._is_balanced(block):
                self._finalize_block(block, now, BlockState.DONE)
            else:
                self._finalize_block(block, now, BlockState.PARTIAL)
            return

        self._cancel_block_orders(block)
        if self._timeout_policy == "full" and self._is_block_complete(block):
            self._finalize_block(block, now, BlockState.DONE)
        elif self._timeout_policy != "full" and self._is_balanced(block):
            self._finalize_block(block, now, BlockState.DONE)
        else:
            self._finalize_block(block, now, BlockState.CRITICAL)

    def _cap_buy_volume_by_budget(self, qty: int, price: float) -> int:
        if qty <= 0 or price <= 0:
            return 0
        account = self.get_account()
        buy_budget = max(0.0, account.available)
        max_buy = int(buy_budget / price / 100) * 100
        return min(qty, max_buy)

    def _is_balanced(self, block: RotationBlock) -> bool:
        buy_ref = self._get_order_price(block.buy_symbol, "buy", aggressive=True)
        sell_ref = self._get_order_price(block.sell_symbol, "sell", aggressive=True)
        lot_amt = max(buy_ref, sell_ref) * 100
        if lot_amt <= 0:
            return abs(block.sell_cost - block.buy_cost) < 1e-6
        return abs(block.sell_cost - block.buy_cost) < lot_amt

    def _check_signal_reversion(
        self, block: RotationBlock, now: datetime
    ) -> bool:
        """检测信号是否失效。失效时处理再平衡并 finalize block。

        返回 True 表示已处理（block 已终止或进入 rebalance chase）。
        """
        if not self.enable_signal_check:
            return False

        ctx = self._build_decision_context(now)
        if self._decision.is_still_valid(ctx, block.direction):
            return False  # 信号仍然有效

        # 信号失效
        if self._is_balanced(block):
            # Case 1: 已对等（或双零），直接撤单结束
            self._cancel_block_orders(block)
            self.write_log(
                f"REVERT {block.block_id}: signal invalid, balanced "
                f"sell_cost={block.sell_cost:.2f} buy_cost={block.buy_cost:.2f}"
            )
            self._finalize_block(block, now, BlockState.REVERTED)
            return True

        # Case 2: 不对等，全部撤单后追齐少的一方
        self._cancel_block_orders(block)
        side, gap_qty = self._calc_gap_qty(block)
        if side and gap_qty >= 100:
            self.write_log(
                f"REVERT {block.block_id}: signal invalid, rebalance "
                f"chase {side} {gap_qty} "
                f"sell_cost={block.sell_cost:.2f} buy_cost={block.buy_cost:.2f}"
            )
            self._start_immediate_chase(block, side, now, gap_qty)
        else:
            self.write_log(
                f"REVERT {block.block_id}: signal invalid, no gap to chase"
            )
            self._finalize_block(block, now, BlockState.REVERTED)
        return True

    def _manage_active_block(self, tick: TickData) -> None:
        block = self._active_block
        if not block:
            return
        now = tick.datetime

        if self._is_block_complete(block):
            self._finalize_block(block, now, BlockState.DONE)
            return

        # 信号失效检测（仅 PENDING/MATCHING 阶段，CHASING 阶段不中断）
        if block.state in (BlockState.PENDING, BlockState.MATCHING):
            if self._check_signal_reversion(block, now):
                return

        if self._timeout_expired(block, now):
            self._handle_block_timeout(block, now)
            return

        if block.state == BlockState.CHASING and block.chase_order_id:
            self._check_chase_timeout(block, now)
            if not self._active_block:
                return
            block = self._active_block
            if self._is_block_complete(block):
                self._finalize_block(block, now, BlockState.DONE)
                return

        if block.state in {BlockState.MATCHING, BlockState.CHASING}:
            self._check_and_handle_excess(block, now)
            if not self._active_block:
                return

        if block.state == BlockState.PENDING and (
            block.sell_filled > 0 or block.buy_filled > 0
        ):
            block.state = BlockState.MATCHING

    # ────────────────────────────────────────────────
    #  on_trade
    # ────────────────────────────────────────────────

    def on_trade(self, trade: Trade):
        self._last_trade_time = trade.datetime

        if trade.direction == Direction.SELL:
            if trade.symbol == self.symbol_a:
                self._last_trade_direction = "SELL_A_BUY_B"
            else:
                self._last_trade_direction = "SELL_B_BUY_A"
        elif trade.direction == Direction.BUY:
            if trade.symbol == self.symbol_a:
                self._last_trade_direction = "SELL_B_BUY_A"
            else:
                self._last_trade_direction = "SELL_A_BUY_B"

        block = self._order_to_block.get(str(trade.order_id))
        if block:
            if trade.direction == Direction.SELL:
                block.sell_filled += trade.volume
                block.sell_cost += trade.price * trade.volume
                block.sell_commission += trade.commission
            elif trade.direction == Direction.BUY:
                block.buy_filled += trade.volume
                block.buy_cost += trade.price * trade.volume
                block.buy_commission += trade.commission
            block.end_time = trade.datetime
            self._update_block_snapshot(block)
            if block.state == BlockState.PENDING:
                block.state = BlockState.MATCHING

    # ────────────────────────────────────────────────
    #  on_order (日志用)
    # ────────────────────────────────────────────────

    def on_order(self, order: Order):
        oid = str(order.order_id)
        block = self._order_to_block.get(oid)

        if order.status == OrderStatus.REJECTED:
            if block and block.sell_filled == 0 and block.buy_filled == 0:
                block.state = BlockState.REJECTED
                block.end_time = order.update_time or block.signal_time
                self._update_block_snapshot(block)
            self.write_log(
                f"Order REJECTED: {order.direction.value} {order.symbol} "
                f"{order.volume} @ {order.price:.4f}"
            )

        if order.status in {
            OrderStatus.ALL_TRADED,
            OrderStatus.CANCELLED,
            OrderStatus.PART_CANCELLED,
            OrderStatus.REJECTED,
        }:
            if block:
                block.active_order_ids.discard(oid)
                if oid == block.chase_order_id and oid not in block.active_order_ids:
                    block.chase_order_id = ""
                    block.chase_side = ""
                    if block.state == BlockState.CHASING:
                        block.state = BlockState.MATCHING

                if (
                    block.state == BlockState.REJECTED
                    and not block.active_order_ids
                    and block.sell_filled == 0
                    and block.buy_filled == 0
                ):
                    self._finalize_block(
                        block,
                        order.update_time or datetime.now(),
                        BlockState.REJECTED,
                    )

    def on_stop(self):
        if self._active_block:
            self._cancel_block_orders(self._active_block)
            self._finalize_block(
                self._active_block,
                self._last_trade_time or datetime.now(),
                BlockState.TIMEOUT,
            )
