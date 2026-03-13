# 撮合引擎设计文档 (core/matching.py)

## 1. 概述

撮合引擎接收行情数据（Tick 或日线）和挂单列表，判断哪些订单可以成交，生成 Trade 记录并触发回调。
回测中由 `backtest/engine.py` 在每个时间步调用；实盘中不使用此模块（订单直接发往券商 mini-qmt）。

**三模式设计** — 根据不同的回测用途提供三种撮合精度，从最快速的策略验证到最拟真的交易模拟：

| 模式 | 数据源 | 速度 | 拟真度 | 典型用途 |
|------|--------|------|--------|----------|
| `close_fill` | 日线 | ★★★★★ | ★☆☆☆☆ | 参数扫描、长周期策略 |
| `tick_fill` | Tick | ★★★☆☆ | ★★★☆☆ | 逻辑验证、快速迭代 |
| `smart_tick_delay_fill` | Tick (10档盘口) | ★★☆☆☆ | ★★★★★ | 最终评估、做市策略 |

## 2. vnpy 撮合逻辑分析 (参考)

### 2.1 vnpy 的 cross_limit_order 流程

```
每收到一个新 Bar/Tick:
  1. 确定撮合参考价格
     - Bar 模式：long_cross_price = bar.low,  short_cross_price = bar.high
                 long_best_price  = bar.open, short_best_price  = bar.open
     - Tick 模式：long_cross_price = tick.ask_price_1, short_cross_price = tick.bid_price_1
                  long_best_price  = long_cross_price,  short_best_price  = short_cross_price

  2. 遍历所有 active_limit_orders:
     - 首次遇到 SUBMITTING 状态 → 改为 NOTTRADED，通知策略 on_order
     - 买单成交条件：order.price >= long_cross_price 且 long_cross_price > 0
     - 卖单成交条件：order.price <= short_cross_price 且 short_cross_price > 0
     - 成交 → 全量成交（无部分成交）
     - 成交价：买单 = min(order.price, long_best_price)  → 对策略有利
              卖单 = max(order.price, short_best_price) → 对策略有利
     - 生成 TradeData，更新 strategy.pos
     - 从 active_limit_orders 中移除
```

### 2.2 vnpy 的 cross_stop_order 流程

```
每收到新 Bar/Tick:
  1. 确定触发参考价
     - Bar 模式：long_cross = bar.high, short_cross = bar.low
     - Tick 模式：两者都用 tick.last_price

  2. 遍历 active_stop_orders:
     - 买方停止单触发条件：stop.price <= long_cross_price
     - 卖方停止单触发条件：stop.price >= short_cross_price
     - 触发后 → 创建一个限价单（直接全部成交）
     - 成交价：买单 = max(stop.price, long_best_price)  → 对策略不利（滑点惩罚）
              卖单 = min(stop.price, short_best_price) → 对策略不利
```

### 2.3 vnpy 撮合的局限性

| 问题 | 说明 |
|------|------|
| **无部分成交** | 订单一触即全部成交，不支持 partial fill |
| **不用盘口深度** | Tick 模式只看 bid/ask_1，不考虑挂单量 |
| **无排队模型** | 限价单不排队，挂即成交（若价格满足） |
| **无延迟** | 同一 tick 下单后立即撮合，过于乐观 |
| **单品种** | BacktestingEngine 一次只回测一个品种 |
| **无做市场景** | 做市策略需要判断挂单能否被吃掉，vnpy 无法模拟 |

## 3. 我们的撮合引擎设计

### 3.1 设计目标

1. **三级精度** — close_fill / tick_fill / smart_tick_delay_fill 覆盖从快速验证到拟真回测
2. **统一委托回调** — 所有模式通过相同的 `on_order(order)` 回调通知策略（成交/部分成交/撤单/拒绝）
3. **独立模块** — 纯函数式+类组合，不依赖事件总线或 UI
4. **面向 ETF 场景** — T+1 卖出限制、无印花税、100 份整数倍
5. **smart_tick_delay_fill 做到极致拟真** — 延迟成交、部分成交、盘口深度限制、挂单排队

### 3.2 撮合精度级别（MatchingMode）

```python
class MatchingMode(Enum):
    CLOSE_FILL = "close_fill"
    # 日线收盘价模式。
    # 策略在 on_day_begin(bar) 中以当日收盘价下单，全量立即成交。
    # 最快速度，用于参数扫描和长周期策略验证。

    TICK_FILL = "tick_fill"
    # Tick 即时成交模式。
    # 限价单只要价格满足 bid/ask_1 即全量成交，不考虑盘口量。
    # 与 vnpy TICK_SIMPLE 一致，用于策略逻辑验证。

    SMART_TICK_DELAY_FILL = "smart_tick_delay_fill"
    # Tick 延迟深度撮合模式（我们独有）。
    # - 委托至少延迟到下一个 tick 才开始撮合（模拟网络+交易所延迟）
    # - 主动吃盘口：按 10 档深度逐档匹配，受量限制，支持部分成交
    # - 被动挂单：排队等待对手方价格穿越，用成交量比例估算可得量
    # - 可配置成交量上限比例 (volume_limit_ratio)
    # - 剩余未成交部分保留挂单，等待后续 tick 继续撮合
    # 最拟真，用于最终策略评估和做市策略。
```

### 3.3 核心类 MatchingEngine

```python
class MatchingEngine:
    """
    撮合引擎。接收行情 + 管理挂单 → 产出成交记录。
    回测引擎每个时间步调用 match_bar() 或 match_tick()，撮合完毕后引擎
    通过回调通知策略订单状态变更。
    """

    def __init__(
        self,
        pricetick: float = 0.001,      # 最小价格变动 (ETF = 0.001)
        rate: float = 0.0,             # 手续费率
        slippage: float = 0.0,         # 固定滑点（价格单位）
        min_commission: float = 0.0,   # 最低佣金
        mode: MatchingMode = MatchingMode.TICK_FILL,
        volume_limit_ratio: float = 0.5,
        # 仅 SMART_TICK_DELAY_FILL 生效。
        # 每档可吃量上限 = min(该档挂单量, 快照成交量增量) × volume_limit_ratio。
        # 默认 0.5 = 假设只能吃到可用量的一半（推荐默认值）。
        # 设为 1.0 = 完全信任盘口量（乐观上界）。
        # 设为 0.1 表示最多吃 10%（极度保守，适合大资金评估冲击）。
    ):
        self.pricetick = pricetick
        self.rate = rate
        self.slippage = slippage
        self.min_commission = min_commission
        self.mode = mode
        self.volume_limit_ratio = volume_limit_ratio

        # 挂单管理
        self._order_count: int = 0        # 累计订单序号，用于生成 order_id "O000001"
        self._trade_count: int = 0        # 累计成交序号，用于生成 trade_id "T000001"
        self._active_orders: dict[str, Order] = {}   # order_id → Order (活跃挂单)
        self._all_orders: dict[str, Order] = {}       # order_id → Order (全部历史)
        self._trades: dict[str, Trade] = {}           # trade_id → Trade (全部成交)

        # SMART_TICK_DELAY_FILL 专用
        self._order_submit_tick: dict[str, int] = {}  # order_id → 提交时的 tick_seq
        self._current_tick_seq: int = 0                # 全局 tick 计数器
        self._prev_cum_volume: dict[str, int] = {}     # symbol → 上一个 tick 的 cum_volume

        # 回调 — 由 BacktestEngine 设置
        self._on_order: Callable[[Order], None] | None = None
        self._on_trade: Callable[[Trade], None] | None = None
```

### 3.4 核心方法签名

```python
def submit_order(
    self,
    symbol: str,
    direction: Direction,      # BUY / SELL
    order_type: OrderType,     # LIMIT / MARKET
    price: float,
    volume: int,
    dt: datetime | None = None,  # 订单创建时间
) -> Order:
    """
    提交订单，返回 Order 对象（状态=SUBMITTING）。
    order_id 格式: "O000001", "O000002", ... 顺序递增。
    SMART_TICK_DELAY_FILL 模式下，记录 _order_submit_tick[order_id] = _current_tick_seq，
    通过 _current_tick_seq - submit_seq <= 1 判断延迟是否满足。
    """

def cancel_order(self, order_id: str) -> bool:
    """
    撤销订单。返回是否成功撤销。
    如果 order_id 不在 _active_orders 中，返回 False。
    如果订单已部分成交 (traded > 0)，状态 → PART_CANCELLED。
    如果订单未成交，状态 → CANCELLED。
    同时从 _active_orders 和 _order_submit_tick 中移除。
    """

def match_bar(self, bar: BarData) -> list[Trade]:
    """
    日线模式撮合（CLOSE_FILL 模式专用）。
    遍历与 bar.symbol 匹配的所有活跃订单:
      1. SUBMITTING → ACTIVE（通知 on_order）
      2. 成交价 = _apply_slippage(bar.close_price, direction)
      3. 以 order.remaining 全量成交
      4. 通知 on_order + on_trade
    """

def match_tick(self, tick: TickData) -> list[Trade]:
    """
    Tick 模式撮合统一入口。
    TICK_FILL        → 调用 _match_tick_simple()
    SMART_TICK_DELAY → 先 _current_tick_seq += 1，再调用 _match_tick_smart()
    其他模式         → 返回空列表
    """

def cancel_all(self) -> list[Order]:
    """撤销所有活跃订单，返回被撤订单列表。"""

def get_active_orders(self) -> list[Order]:
    """获取当前所有活跃订单。"""

def get_pending_orders(self, symbol: str = "") -> list[Order]:
    """获取未完成委托列表。可选按 symbol 过滤。"""

def get_all_trades(self) -> list[Trade]:
    """获取全部历史成交。"""

def reset(self) -> None:
    """重置引擎状态。清空: _order_count, _trade_count,
       _active_orders, _all_orders, _trades,
       _order_submit_tick, _current_tick_seq, _prev_cum_volume。"""
```

### 3.5 撮合规则详解

#### 3.5.1 CLOSE_FILL — 日线收盘价模式

```
入口方法: match_bar(bar)
策略回调: on_day_begin(bar) — 每日一次，bar 包含当日 OHLCV + 收盘价

实现流程 (match_bar):
  遍历 _active_orders.values() 中 order.symbol == bar.symbol 的订单:
    1. if order.status == SUBMITTING:
         order.status = ACTIVE
         _notify_order(order)          # 通知策略订单已激活
    2. fill_price = _apply_slippage(bar.close_price, order.direction)
         买单: fill_price = close_price + slippage
         卖单: fill_price = close_price - slippage
    3. trade = _make_trade(order, fill_price, order.remaining, bar.datetime)
         → 全量成交（volume = remaining）
         → order.traded += remaining
         → order.status = ALL_TRADED
         → 从 _active_orders 中移除
    4. _notify_order(order)            # 通知状态变更
       _notify_trade(trade)            # 通知成交

成交规则:
  - 所有挂单以 bar.close_price ± slippage 成交（买卖同基准价）
  - 全量成交，无部分成交
  - 无延迟（同日下单同日成交）
  - 不区分限价/市价，统一以收盘价成交

适用场景:
  - 参数扫描（需要跑大量参数组合，要求速度快）
  - 周/月线级别策略
  - 粗略验证日线策略信号

注意:
  - 这个模式不调用 on_tick()，策略只在 on_day_begin() 做决策
  - 真实 T+1 限制在此模式下仍生效（当日买入次日才能卖出）
```

#### 3.5.2 TICK_FILL — Tick 即时成交模式

```
入口方法: match_tick(tick) → 内部调用 _match_tick_simple(tick)
策略回调: on_tick(tick) — 每个 tick 一次

实现流程 (_match_tick_simple):
  遍历 _active_orders.values() 中 order.symbol == tick.symbol 的订单:
    1. if order.status == SUBMITTING:
         order.status = ACTIVE
         _notify_order(order)

    2. 判断是否可以成交:
       ┌─ 市价单:
       │   买: tick.ask_price_1 > 0 → fill_price = ask_price_1 + slippage
       │   卖: tick.bid_price_1 > 0 → fill_price = bid_price_1 - slippage
       │
       └─ 限价单:
           买: tick.ask_price_1 > 0 且 order.price >= tick.ask_price_1
               → fill_price = min(order.price, tick.ask_price_1) + slippage
           卖: tick.bid_price_1 > 0 且 order.price <= tick.bid_price_1
               → fill_price = max(order.price, tick.bid_price_1) - slippage

    3. 如果可成交:
       trade = _make_trade(order, fill_price, order.remaining, tick.datetime)
       → 全量成交，order.status = ALL_TRADED
       _notify_order(order)
       _notify_trade(trade)

成交规则 (与 vnpy TICK_SIMPLE 一致):
  - 限价买单成交条件: order.price >= tick.ask_price_1 且 ask_price_1 > 0
  - 限价卖单成交条件: order.price <= tick.bid_price_1 且 bid_price_1 > 0
  - 限价单成交价取"对策略有利"方: 买 min(委托价, 卖一), 卖 max(委托价, 买一)
  - 市价单直接用对手方最优价
  - slippage 在上述价格基础上叠加
  - 全量成交，不考虑盘口挂单量（即使 ask_volume_1=100, 买 999999 也全量成交）
  - 同一 tick 下单可立即撮合（无延迟）

适用场景:
  - Tick 级策略的逻辑验证
  - 快速迭代开发阶段（确认买卖逻辑正确）
  - 不关心冲击成本的场景

注意:
  - 不考虑盘口量，理想化假设
  - 对于大单交易，结果会比真实情况乐观
```

#### 3.5.3 SMART_TICK_DELAY_FILL — Tick 延迟深度撮合模式（核心创新）

```
入口方法: match_tick(tick) → self._current_tick_seq += 1 → _match_tick_smart(tick)
策略回调: on_tick(tick)  ← 每个 tick 一次


═══ 总体流程 (_match_tick_smart) ═══

  1. volume_delta = _get_volume_delta(tick)
       prev = _prev_cum_volume.get(tick.symbol, 0)
       delta = max(0, tick.cum_volume - prev)
       _prev_cum_volume[tick.symbol] = tick.cum_volume

  2. 遍历 _active_orders.values() 中 order.symbol == tick.symbol 的订单:

     a. 延迟检查:
          submit_seq = _order_submit_tick.get(order.order_id, -1)
          if _current_tick_seq - submit_seq <= 1:
              # 提交后尚未经过 1 个完整 tick，跳过撮合
              # 但如果 status == SUBMITTING，先转为 ACTIVE 并通知
              continue

     b. if status == SUBMITTING → ACTIVE, _notify_order()

     c. 根据订单类型分发:
          MARKET → _match_market_order_smart(order, tick, volume_delta)
          LIMIT  → _match_limit_order_smart(order, tick, volume_delta)

     d. 对每笔成交: _notify_order() + _notify_trade()


═══ 延迟机制详解 ═══

  关键规则: 委托提交后至少经过 1 个完整 tick 间隔才能开始撮合。
  原因: 模拟真实交易中的网络延迟 + 交易所处理延迟。

  实现:
    submit_order() 时:
      _order_submit_tick[order_id] = _current_tick_seq   # 记录提交时刻

    match_tick() 时:
      _current_tick_seq += 1                              # 在撮合前递增
      延迟判断: _current_tick_seq - submit_seq <= 1 → 跳过

  时序示例:
    tick_seq=0 (初始)
    tick_seq=1: match_tick(tick1)  → submit_order() 在此 tick 之后被调用
                                     _order_submit_tick[O1] = 1
    tick_seq=2: match_tick(tick2)  → 2 - 1 = 1 ≤ 1 → 跳过 (尚未满 1 个间隔)
    tick_seq=3: match_tick(tick3)  → 3 - 1 = 2 > 1 → 可以撮合 ✓

  注意: 如果订单在 tick1 和 tick2 之间提交 (submit_seq=1)，
        则最早在 tick3 才能参与撮合。保证了至少经过一个完整快照间隔。


═══ 成交量增量 (volume_delta) ═══

  volume_delta = tick.cum_volume - 上一个 tick 的 cum_volume
  含义: 本快照时段内场内的真实成交量，是市场流动性的直接度量。
  用途: 所有模式（主动/被动/市价）的可成交量都受 volume_delta 约束，
        确保不高估可得流动性。


═══ 市价单撮合 (_match_market_order_smart) ═══

  设计原则: 市价单只吃当前最优价（卖一/买一），不逐档扫盘口。
  原因: 市价单本质是"尽快以当前最优价成交"，但受限于真实可得量，
        不假设能瞬间吃穿多档。

  买单 (Direction.BUY):
    前提: tick.ask_price_1 > 0 （有卖盘）
    max_fill = int( min(tick.ask_volume_1, volume_delta) × volume_limit_ratio )
    fill_vol = min(max_fill, order.remaining)
    if fill_vol > 0:
      成交价 = tick.ask_price_1
      trade = _make_trade(order, ask_price_1, fill_vol, tick.datetime)

  卖单 (Direction.SELL):
    前提: tick.bid_price_1 > 0 （有买盘）
    max_fill = int( min(tick.bid_volume_1, volume_delta) × volume_limit_ratio )
    fill_vol = min(max_fill, order.remaining)
    if fill_vol > 0:
      成交价 = tick.bid_price_1
      trade = _make_trade(order, bid_price_1, fill_vol, tick.datetime)

  示例:
    委托买入 100000 份, ask_volume_1=50000, volume_delta=80000, ratio=0.5
    max_fill = int(min(50000, 80000) × 0.5) = 25000
    fill_vol = min(25000, 100000) = 25000
    → 本 tick 成交 25000 份 @ ask_price_1，剩余 75000 等下个 tick


═══ 限价单撮合 — 主动吃盘口 (Aggressive Fill) ═══

  触发条件:
    买单: order.price >= tick.ask_price_1 且 ask_price_1 > 0 → "穿越"卖盘
    卖单: order.price <= tick.bid_price_1 且 bid_price_1 > 0 → "穿越"买盘

  买单流程 (_match_buy_limit_smart):
    ask_prices = tick.ask_prices()    # [ask_1, ask_2, ..., ask_10]
    ask_volumes = tick.ask_volumes()  # [ask_vol_1, ..., ask_vol_10]
    for i in range(10):
      if order.remaining <= 0: break
      ap, av = ask_prices[i], ask_volumes[i]
      if ap <= 0 or av <= 0: continue   # 该档无数据
      if order.price < ap: break        # 委托价不够吃到这一档

      max_fill = int( min(av, volume_delta) × volume_limit_ratio )
      fill_vol = min(max_fill, order.remaining)
      if fill_vol > 0:
        trade = _make_trade(order, ap, fill_vol, tick.datetime)
        # 注意: 成交价 = 该档实际价格，不是委托价

  卖单流程 (_match_sell_limit_smart):
    对称处理，从 bid_price_1 到 bid_price_10 逐档扫:
    for each level:
      if order.price > bp: break       # 卖价高于该档买价，不可成交
      max_fill = int( min(bv, volume_delta) × volume_limit_ratio )
      fill_vol = min(max_fill, order.remaining)
      成交价 = bp (该档买盘价格)

  示例: 卖单委托价 3.995, 盘口如下:
    买一: 3.999 × 30000
    买二: 3.998 × 20000
    买三: 3.997 × 10000
    volume_delta = 100000, ratio = 0.5

    第1档: 3.995 ≤ 3.999 → 可吃
      max_fill = int(min(30000, 100000) × 0.5) = 15000
      → 成交 15000 @ 3.999

    第2档: 3.995 ≤ 3.998 → 可吃
      max_fill = int(min(20000, 100000) × 0.5) = 10000
      → 成交 10000 @ 3.998

    第3档: 3.995 ≤ 3.997 → 可吃
      max_fill = int(min(10000, 100000) × 0.5) = 5000
      → 成交 5000 @ 3.997

    总成交: 15000+10000+5000 = 30000 份, 分 3 笔 Trade, 各档价格不同


═══ 限价单撮合 — 被动排队 (Passive Queue, _passive_queue_fill) ═══

  触发条件:
    买单: order.price < tick.ask_price_1 → 委托价未穿越卖盘, 挂在买方排队
    卖单: order.price > tick.bid_price_1 → 委托价未穿越买盘, 挂在卖方排队

  核心思路: 当 last_price 穿越了我方挂单价, 说明有对手方来吃我们这一边,
           用成交量 × 队列位置比例估算能分到多少成交。

  常量: queue_factor = 0.5 (硬编码, 假设我们排在队列中间位置)

  买单排队 (is_buy=True):
    if volume_delta <= 0: 无成交, 跳过
    if tick.last_price <= order.price 且 last_price > 0:
      # 最新成交价已经低于或等于我的买入价 → 有卖方在我的价位成交
      level_volume = tick.bid_volume_1 if bid_volume_1 > 0 else 1
      estimated = int(
        volume_delta
        × (order.remaining / max(level_volume, order.remaining))
        × queue_factor
      )
      fill_vol = min( max(estimated, 0), order.remaining )
      if fill_vol > 0:
        成交价 = order.price (以自己的委托价成交, 被动成交)
        trade = _make_trade(order, order.price, fill_vol, tick.datetime)

  卖单排队 (is_buy=False):
    if tick.last_price >= order.price 且 last_price > 0:
      # 最新成交价已经高于或等于我的卖出价 → 有买方在我的价位成交
      level_volume = tick.ask_volume_1 if ask_volume_1 > 0 else 1
      estimated = int(
        volume_delta
        × (order.remaining / max(level_volume, order.remaining))
        × queue_factor
      )
      fill_vol = min( max(estimated, 0), order.remaining )
      成交价 = order.price

  公式解读:
    remaining / max(level_volume, remaining) — 我的挂单量占该价位总量的比例
      如果 remaining < level_volume → 比例 < 1 (小单, 分到的少)
      如果 remaining >= level_volume → 比例 = 1 (大单, 但最多拿全部)
    乘以 queue_factor=0.5 → 假设排在中间, 只能拿到一半
    乘以 volume_delta → 与实际成交量成正比


═══ _make_trade 辅助方法 ═══

  每笔成交的统一处理:
    1. _trade_count += 1, trade_id = "T{_trade_count:06d}"
    2. commission = calc_commission(price, volume)
    3. 创建 Trade 对象
    4. 更新 Order:
       order.traded += volume
       if order.traded >= order.volume → ALL_TRADED, 从 _active_orders 移除
       else → PART_TRADED (保留在 _active_orders, 等后续 tick 继续撮合)


适用场景:
  - 策略最终评估（上线前模拟真实情况）
  - 做市策略（需要模拟挂单被吃的过程）
  - 大单策略（需要评估冲击成本）
  - 高频策略（延迟对收益影响大）

volume_limit_ratio 配置指导:
  1.0  — 完全信任盘口量（乐观上界）
  0.5  — 假设只能吃到一半（推荐默认值）
  0.1  — 极度保守（适合大资金评估冲击）
```

### 3.6 手续费计算

```python
def calc_commission(self, price: float, volume: int) -> float:
    """
    计算手续费。在 _make_trade() 中每笔成交时调用。
    ETF 特点：
    - 买卖都收佣金（费率 rate），无印花税
    - 佣金 = max(price × volume × rate, min_commission)
    - 注意 min_commission 是每笔成交的最低佣金，不是每笔订单

    示例:
      rate=0.0003, min_commission=5.0
      → 成交额 100元: max(100 × 0.0003, 5.0) = 5.0  (按最低佣金)
      → 成交额 40000元: max(40000 × 0.0003, 5.0) = 12.0  (按费率)
    """
    turnover = price * volume
    commission = max(turnover * self.rate, self.min_commission)
    return commission
```

### 3.7 滑点处理

```python
def _apply_slippage(self, price: float, direction: Direction) -> float:
    """叠加滑点（对策略不利方向）。"""
    if direction == Direction.BUY:
        return price + self.slippage    # 买贵了
    else:
        return price - self.slippage    # 卖便宜了

# 调用位置:
#   close_fill:  fill_price = _apply_slippage(bar.close_price, direction)
#   tick_fill:   fill_price = _apply_slippage(base_price, direction)
#     其中 base_price 对限价买单 = min(order.price, ask_1), 限价卖单 = max(order.price, bid_1)
#     市价买单 base_price = ask_1, 市价卖单 base_price = bid_1
#
#   smart_tick_delay_fill: 不调用 _apply_slippage
#     延迟 + 逐档匹配 + volume_limit_ratio 本身已包含真实滑点效应
#     成交价直接用盘口各档价格（ask_price_i / bid_price_i）或 order.price（被动排队）
#     如果仍想叠加额外滑点（模拟极端情况），需用户自行增大 slippage
```

### 3.8 T+1 卖出限制

```
ETF 场景特有规则:
  - 当日买入的份额，当日不能卖出
  - MatchingEngine 需要跟踪每笔买入成交的日期
  - 卖单撮合前检查: 可卖量 = position.volume - 当日买入量 - frozen

三种模式均生效。BacktestEngine 在日切时更新可用量。
```

### 3.9 订单状态流转

```
                 submit_order()
                      │
                      ▼
                 ┌─────────┐
                 │SUBMITTING│
                 └────┬─────┘
                      │ 下一个 tick / bar 触发撮合
                      ▼
           ┌──── 可以成交？ ────┐
           │ 是                │ 否
           ▼                   ▼
     ┌───────────┐      ┌──────────┐
     │  ACTIVE   │      │  ACTIVE  │ (挂单等待)
     │(部分成交) │      │(未成交)  │
     └─────┬─────┘      └────┬─────┘
           │                  │
           │ 继续撮合/        │ cancel_order()
           │ 全部成交         │
           ▼                  ▼
     ┌───────────┐    ┌───────────────┐
     │ALL_TRADED │    │  CANCELLED    │ (未成交撤单)
     └───────────┘    │PART_CANCELLED │ (部分成交后撤剩余)
                      └───────────────┘

每次状态变更 → 通过 BacktestEngine 回调 strategy.on_order(order)
```

### 3.10 数据流

```
═══ close_fill 模式 ═══

backtest/engine.py
    │  加载日线 BarData 序列
    ▼  每日:
    ┌───────────────────────────────┐
    │  matching.match_bar(bar)     │  ← 撮合昨日挂单（以收盘价）
    │  → 产出 trades[]             │
    │  → 回调 on_order / on_trade  │
    ├───────────────────────────────┤
    │  strategy.on_day_begin(bar)  │  ← 策略决策（看到当日 OHLCV）
    │  → 可能调用 buy/sell/cancel  │
    │  → 新订单进入 matching       │
    └───────────────────────────────┘
    # 新订单在下一日的 match_bar 中撮合


═══ tick_fill / smart_tick_delay_fill 模式 ═══

backtest/engine.py
    │  加载 Tick 序列（Parquet）
    │
    │  每日开始前:
    │    strategy.on_day_begin(daily_bar)   ← 推送前一日日线汇总
    │
    ▼  每个 tick:
    ┌─────────────────────────────────┐
    │  matching.match_tick(tick)      │  ← 先撮合已有挂单
    │  → 产出 trades[]               │
    │  → 回调 on_order / on_trade    │
    ├─────────────────────────────────┤
    │  strategy.on_tick(tick)         │  ← 策略决策
    │  → 可能调用 buy/sell/cancel    │
    │  → 新订单进入 matching         │
    └─────────────────────────────────┘
         ↓
    backtest/engine 更新 Position/Account
```

**关键顺序**：先撮合、再推送行情（与 vnpy 一致）。这样策略在 on_tick/on_day_begin 中看到的持仓是最新的。

## 4. 三种模式对比总结

| 特性 | close_fill | tick_fill | smart_tick_delay_fill |
|------|-----------|-----------|----------------------|
| 数据源 | 日线 BarData | Tick | Tick (10档盘口) |
| 策略回调 | on_day_begin | on_tick | on_day_begin + on_tick |
| 成交延迟 | 无 | 无 | ≥1 tick |
| 部分成交 | ❌ | ❌ | ✅ |
| 盘口深度匹配 | ❌ | ❌ | ✅ 10档逐档 |
| 被动挂单排队 | ❌ | ❌ | ✅ |
| volume_limit_ratio | 不适用 | 不适用 | ✅ 可配置 |
| T+1 限制 | ✅ | ✅ | ✅ |
| 回测速度 | 极快 (秒级) | 中等 (分钟级) | 较慢 (分钟-十分钟级) |
| 拟真度 | 粗略 | 中等 | 最接近实盘 |
| 典型用途 | 参数扫描 | 逻辑验证 | 最终评估 |

## 5. 与 vnpy 的对比总结

| 特性 | vnpy BacktestingEngine | 我们的 MatchingEngine |
|------|----------------------|---------------------|
| 日线撮合 | ✅ (BAR 模式) | ✅ (close_fill) |
| Tick 撮合 | ✅ (只看 bid/ask_1) | ✅ (tick_fill) |
| 10 档盘口深度 | ❌ | ✅ (smart_tick_delay_fill) |
| 成交延迟模拟 | ❌ | ✅ (smart_tick_delay_fill) |
| 部分成交 | ❌ | ✅ (smart_tick_delay_fill) |
| 被动挂单排队 | ❌ | ✅ (smart_tick_delay_fill) |
| 成交量限制 | ❌ | ✅ (volume_limit_ratio) |
| 手续费模型 | 固定费率 | 费率 + 最低佣金 |
| 滑点 | 固定值 | 固定值 / 盘口深度隐含 |
| T+1 卖出 | ❌ 不处理 | ✅ |
| 多品种 | ❌ 单品种 | ✅ symbol 字段隔离 |
| 代码位置 | BacktestingEngine 内嵌 | 独立模块 core/matching.py |

## 6. 依赖的数据类型

详见 `design_strategy_interface.md` 中的数据类型定义。
核心类型：`Direction`, `OrderType`, `OrderStatus`, `Order`, `Trade`, `TickData`, `BarData`。
