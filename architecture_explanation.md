# VeighNa (vn.py) 核心模块与仿真回测复用指南

为了实现高精度的“订单簿排队仿真 (Queue Simulation)”回测，我们将复用 VeighNa 的三个核心模块。以下是这些模块的关键概念和核心算法解析。

## 1. 基础数据对象模块 (`vnpy.trader.object`)
这是整个交易系统的“基石”，定义了所有标准化的数据结构。复用它意味着你的代码可以无缝对接 VeighNa 生态的所有组件（数据库、策略、UI）。

### 关键概念
*   **TickData (切片行情)**:
    *   **核心字段**: `bid_price_1/bid_volume_1` (买一价/量), `ask_price_1/ask_volume_1` (卖一价/量), `last_price` (最新成交价), `volume` (当日累计成交量)。
    *   **用途**: 我们的排队算法将严重依赖 `bid/ask volume` 来计算队列深度，依赖 `volume` 变化来计算撮合速度。
*   **OrderData (委托单)**:
    *   **核心字段**: `price` (委托价), `volume` (委托量), `traded` (已成交量), `status` (状态: 未成交/部分成交/全成交/已撤销)。
    *   **用途**: 在回测中跟踪你的每一笔挂单状态。
*   **TradeData (成交单)**:
    *   **核心字段**: `price` (成交价), `volume` (成交量), `direction` (买/卖), `offset` (开/平)。
    *   **用途**: 记录成交明细，用于后续计算盈亏 (PnL)。

---

## 2. 策略模板模块 (`vnpy_ctastrategy.template`)
这是策略开发的“标准接口”。复用它意味着你写出的策略代码既能回测，也能实盘，无需修改。

### 关键概念
*   **CtaTemplate (CTA策略基类)**:
    *   **`on_tick(tick)`**: 策略入口。每收到一个 Tick 推送，就调用一次。你的高频策略逻辑将写在这里。
    *   **`buy/sell/short/cover`**: 标准下单接口。
        *   `buy`: 买开 (做多)
        *   `sell`: 卖平 (平多)
        *   `short`: 卖开 (做空)
        *   `cover`: 买平 (平空)
    *   **`pos`**: 当前持仓量。系统会自动维护，不用你自己算。

---

## 3. 回测引擎模块 (`vnpy_ctastrategy.backtesting`)
这是我们需要“魔改”的核心。它负责加载历史数据、模拟撮合、计算资金曲线。

### 核心算法 (复用 vs 重写)

#### A. 100% 复用的部分 (基础设施)
*   **数据加载 (`load_data`)**: 自动从数据库或 CSV 加载历史 Tick 数据到内存。
*   **每日结算 (`calculate_result`)**: 每天收盘后，根据你的成交记录计算当天的盈亏、手续费、滑点。
*   **统计指标 (`calculate_statistics`)**:
    *   **Sharpe Ratio (夏普比率)**: 衡量风险收益比。
    *   **Max Drawdown (最大回撤)**: 衡量最坏情况下的亏损幅度。
    *   **Annual Return (年化收益)**: 你的策略一年能赚多少。
*   **图表绘制 (`show_chart`)**: 调用 Plotly 画出资金曲线、回撤曲线和每日盈亏柱状图。

#### B. 需要重写的部分 (撮合核心)
原生的 `cross_limit_order` 算法过于简单（只看价格是否到达），我们需要用 **“排队仿真算法”** 替换它。

**原生算法 (Current Logic):**
```python
if 买一价 >= 你的卖出价:
    立即全部成交
```

**我们要实现的排队算法 (New Logic):**
1.  **挂单时刻**:
    *   记录 `Initial_Queue = 当前盘口挂单量` (例如卖一量 5000 手)。
2.  **Tick 更新时刻**:
    *   计算 `Delta_Volume = 当前Tick成交量 - 上一Tick成交量`。
    *   `Remaining_Queue = Initial_Queue - Delta_Volume`。
3.  **成交判断**:
    *   如果 `Remaining_Queue <= 0`: 你的单子成交。
    *   否则: 继续排队，等待下一个 Tick 的成交量来消耗队列。

---

## 总结
通过复用这三个模块，我们相当于站在巨人的肩膀上开发。
*   **代码量**: 你只需要编写约 **200行** 的 `TickQueueBacktestingEngine` 类。
*   **功能**: 却能获得一个包含 **数据管理 + 高精撮合 + 资金结算 + 专业报表 + 可视化图表** 的完整量化系统。
