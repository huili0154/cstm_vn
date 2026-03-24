# DS_DMTR 策略设计文档

> **DS_DMTR** — Dual-Stock Dual-MA Tick Reversion
> 双股双均线 Tick 级回归轮动策略

***

## 修订记录

| 版本 | 日期       | 内容           |
| ---- | ---------- | -------------- |
| v0.5 | 2026-03-20 | 执行链路修订：追价方向绑定订单元数据；修正“实际发生量超过发单量”问题；恢复二阶段三目标（full/balance/abort） |
| v0.4 | 2026-03-18 | 执行口径修订：补单统一为单一 gap 模式；对冲口径从股数改为金额等额；timeout 策略与字段语义同步 |
| v0.3 | 2026-03-17 | 对齐 MSTR 执行机制：补充被动/激进回补、实现期(timeout)与追价参数（block_timeout/chase） |
| v0.2 | 2026-03-16 | 对齐 MSTR 可视化与 Block 跟踪：新增 Block 状态表/详情，净值对比增强 |
| v0.1 | 2026-03-15 | 初稿：完整设计 |

***

## 1. 策略概述

DS_DMTR 是一个面向 **两只 ETF** 的 Tick 级价格比率均值回归轮动策略。

核心思想：构建虚拟价格比率 ABratio = price_A / price_B，在 30 分钟 K 线和日线两个时间尺度上计算布林通道统计量。当 ABratio 偏离均值超过阈值时，在 A/B 之间执行轮动操作 —— 卖出相对"贵"的，买入相对"便宜"的，利用比值的均值回归特性获利。

**核心特性**：

| 特性       | 说明                                                                 |
| ---------- | -------------------------------------------------------------------- |
| **标的**   | 恰好两只 ETF（A 和 B），构建价格比率                                |
| **信号**   | 双时间尺度布林统计：30 分钟 K 线 + 日线级                           |
| **执行**   | 盘中实时，信号触发即轮动                                             |
| **仓位**   | 二选一轮动，按比例分块交易                                           |
| **风控**   | 同方向冷却期 + T+0/T+1 可选 + 买卖量取 min 保护                       |

***

## 2. 指标计算模型

### 2.1 ABratio 构建

每收到 tick，取 A、B 最新价计算：

```
ABratio(t) = price_A(t) / price_B(t)
```

### 2.2 分钟级布林通道（30 分钟 K 线）

将 tick 数据自行聚合为 `bar_interval_minutes` 分钟 K 线的 ABratio 收盘价序列，取最近 `window_minutes` 根 K 线：

```
μ_min = SMA(ABratio_30min, window_minutes)
σ_min = STD(ABratio_30min, window_minutes)
```

布林带（用于可视化）：

```
上轨 = μ_min + k_sigma_minutes × σ_min
下轨 = μ_min - k_sigma_minutes × σ_min
```

### 2.3 日线级布林通道

使用 `dataset/daily/` 中的日线数据，分别读取 A、B 的日线收盘价相除得到 ABratio 日线序列，取最近 `window_days` 个交易日：

```
μ_day = SMA(ABratio_daily, window_days)
σ_day = STD(ABratio_daily, window_days)
```

布林带（用于可视化）：

```
上轨 = μ_day + k_sigma_days × σ_day
下轨 = μ_day - k_sigma_days × σ_day
```

> **说明**：布林带的 `k_sigma_*` 参数仅影响可视化绘图中上下轨的位置，不用于交易决策。交易决策使用独立的 `thresh_*` 门限参数。

### 2.4 统计量计算时机（仅已完成 bar）

- **分钟级**：μ_min 和 σ_min 仅基于已收盘的 30 分钟 bar 序列计算，当前正在进行中的 bar 不参与统计。当前 tick 的 ABratio 仅作为"待比较值"与这些统计量做差。统计量只在 bar 切换时更新，不随 tick 变化。
- **日线级**：μ_day 和 σ_day 仅基于已收盘的历史日线计算，当日 tick 不计入。统计量只在 `on_day_begin()` 时更新。

> **优点**：计算量最小；信号稳定，不会因当前价格极端偏离导致 σ 被撑大而产生"自我抑制"效应。

### 2.5 实时信号指标

每 tick 实时计算以下指标：

| 指标                  | 公式                                       | 含义                        |
| --------------------- | ------------------------------------------ | --------------------------- |
| `delta_sigma_minutes` | (ABratio - μ_min) / σ_min                  | 分钟级偏离（以 σ 为单位）   |
| `delta_sigma_days`    | (ABratio - μ_day) / σ_day                  | 日线级偏离（以 σ 为单位）   |
| `delta_minutes`       | (ABratio - μ_min) / \|ABratio\|            | 分钟级相对偏离率            |
| `a_percentage`        | A 持仓市值 / 总净值                        | A 股在组合中的比例          |
| `latest_trade_time`   | 上次成功成交的时间                         | 用于冷却期判断              |

***

## 3. 交易决策函数

### 3.1 函数签名

```
输入：delta_sigma_minutes, delta_sigma_days, delta_minutes,
      a_percentage, latest_trade_time, current_time,
      last_trade_direction
输出：(should_act: bool, target_direction: Direction)
      其中 Direction ∈ {SELL_A_BUY_B, SELL_B_BUY_A}
```

### 3.2 决策逻辑

所有门限判断统一使用 **绝对值** 比较。

#### 3.2.1 方向判定

```
IF delta_sigma_minutes > 0:
    direction = SELL_A_BUY_B      # ABratio 偏高，A 相对贵
ELSE:
    direction = SELL_B_BUY_A      # ABratio 偏低，B 相对贵

day_minute_same_dir = (delta_sigma_minutes * delta_sigma_days > 0)
                      AND (delta_sigma_days != 0)
```

#### 3.2.2 触发条件

```
IF |delta_sigma_minutes| > thresh_sigma_min
  AND |delta_minutes| > thresh_delta_min:

   IF last_trade_direction ≠ direction
      OR (current_time - latest_trade_time) > cooldown_seconds:

      trade_pct = base_pct

      IF |delta_sigma_minutes| > thresh_sigma_min_high
         OR (|delta_sigma_days| > thresh_sigma_day AND day_minute_same_dir):
         trade_pct = high_pct

      RETURN (True, direction, trade_pct)

RETURN (False, None, 0)
```


***

## 4. 下单执行逻辑（被动单 / 机动单 / timeout）

当决策函数返回 `should_act = True` 后，进入单 Block 串行执行（反向对称）。

### 4.1 目标量计算

```
# T+0: 可卖量=当前持仓
# T+1: 可卖量=max(0, 当前持仓-当日买入量)
available_sell = position_A.volume (if enable_t0)
             OR max(0, position_A.volume - today_bought) (if not enable_t0)

desired_sell = floor(net_value × trade_pct / price_A / 100) × 100
desired_buy  = floor(net_value × trade_pct / price_B / 100) × 100

# 执行目标量（写入 *_order_volume）受可卖/资金约束
sell_target = min(desired_sell, available_sell)

# 买侧预算只使用账户当前可用资金 + 本单预计卖出回笼
cash_now = account.available + sell_target × price_A
max_affordable = floor(max(0, cash_now) / price_B / 100) × 100
buy_target  = min(desired_buy, max_affordable)

sell_value = sell_target × price_A
buy_value  = buy_target × price_B
allowed_gap = max(price_A, price_B) × 100

IF |sell_value - buy_value| > allowed_gap:
    记录 warning（目标不对称），但继续执行下单流程
```

- 字段映射：
  - `desired_sell_volume / desired_buy_volume` 对应 `desired_sell / desired_buy`；
  - `sell_order_volume / buy_order_volume` 对应 `sell_target / buy_target`（执行目标量）。

### 4.2 初始被动挂单（Passive）

- 双边同时按执行目标量挂被动价：买侧挂 `bid1`，卖侧挂 `ask1`。
- 初始被动单不是 1 笔整单，而是按参数 `passive_slice_count = n` 拆成 n 个子单（整手分配，余量分摊到前几单）。
- 拆单目标：在后续需要回补时，尽量少撤单、少损失原有排队位置。
- 被动等待由实现期总时钟控制，参数 `block_timeout_minutes`。

### 4.3 机动/激进回补（Aggressive/Chase）

- 统一原则：只有一套回补流程，目标是让买卖两侧名义金额重新收敛。
- 触发时机：任一方向金额差达到一手阈值即进入回补评估。
- 回补与追价关系：
  - 回补是“决策层”：决定本轮需要补哪一侧、补多少（`chase_qty`）以及被动保留量；
  - 追价是“执行层”：对回补量对应的激进单进行提交、等待、撤改重挂；
  - 二者不是并列模式，也不是一次性动作；流程是“回补决策 → 追价执行 → 再评估回补”，循环直到金额差收敛或进入超时分支。
- 金额差口径：  
  已成交金额差 = `sell_cost - buy_cost`；  
  金额差折算为股数后按整手向下取整。
- 回补处理过程（简洁版）：
  - 第一步（计算应追量 `gap_qty`）：
    - 若卖侧领先，则对买侧计算：`buy_gap_amt = sell_cost - buy_cost`；
      当 `buy_gap_amt >= buy_ref_price * 100` 时，`gap_qty_buy = floor(buy_gap_amt / buy_ref_price / 100) * 100`，否则为 0。
    - 若买侧领先，则对卖侧计算：`sell_gap_amt = buy_cost - sell_cost`；
      当 `sell_gap_amt >= sell_ref_price * 100` 时，`gap_qty_sell = floor(sell_gap_amt / sell_ref_price / 100) * 100`，否则为 0。
    - `gap_qty` 指当前落后侧对应的 `gap_qty_buy` 或 `gap_qty_sell`。
  - 第二步：计算落后侧剩余可补空间 `room = order_volume - filled - pending`；
  - 第三步：得到本轮目标回补量 `chase_qty = min(gap_qty, room)`；
  - 第四步（仅做执行计划，不下单）：把落后侧剩余执行量规划为“被动保留量 + 激进追价量”，其中激进量 = `chase_qty`，其余为被动保留量；
  - 第五步（执行细节1）：仅撤销落后侧中“为腾挪追价所必需”的子单，优先撤最新且未成交子单，尽量保留已排队更久/已部分成交子单；
  - 第六步（执行细节2）：提交激进追价单（买 `ask1` / 卖 `bid1`），不足盘口则退回 `last_price`；执行阶段不再按 `account.available` 二次裁买量。
- 追价循环：
  - 激进单提交后进入 `CHASING`；
  - `CHASING` 必须绑定有效 `chase_order_id`，追价方向以该订单元数据中的 `side` 为准；
  - `room=0` 仅表示当前在途单占满，不是退出条件；仅当 `gap < 一手阈值` 才退出 `CHASING`；
  - 每隔 `chase_wait_ticks` 检查一次，未完成则只重挂激进部分，不打断被动保留单；
  - 最多重挂 `max_chase_rounds`，超限后进入超时分支（`full/balance/abort`）。
- 设计意图：
  - 不做“全撤全重挂”，而做“最小撤单改追价”，核心是保留队列优势并提高剩余被动单成交概率。
- 示例（与你提出的场景一致）：
  - 买侧目标 10000，已成交 300；卖侧等额进度对应买侧应达 5000；
  - 则买侧应追量为 4700，剩余 5000 继续被动；
  - 若初始买侧拆为 3400/3300/3300，优先撤两笔未成交子单做腾挪，可形成“4700 激进 + 900 被动”，并尽量保留原先 3100 的在队被动单。

### 4.4 实现期超时（Timeout）

`block_timeout_minutes` 从 Block 启动时开始计时：

- **零成交超时**：`TIMEOUT`，终止本 Block，继续后续信号跟踪。
- **部分成交超时**：按 `timeout_recover_policy` 进入二阶段：
  - 进入二阶段前，先撤销不再需要的挂单，避免历史挂单继续占用额度或引入反向成交；
  - 二阶段保留的挂单全部按“激进单”管理，不再新增被动单；
  - `full`：二阶段目标为“补齐双边执行目标量”，优先完成 `buy_order_volume` 与 `sell_order_volume`；
  - `balance`：二阶段目标为“收敛对冲差”，优先将买卖成交金额差收敛到一手金额阈值以内；
  - `abort`：不再补单，直接结束（`PARTIAL`）。
- 二阶段时长由 `timeout_recover_minutes` 控制；
  到时若金额差已收敛到一手以内（按补单侧价格）则 `DONE`，否则 `CRITICAL`。
- 二阶段结束时，必须先撤销剩余活动单，再落状态（`DONE/PARTIAL/CRITICAL`），保证 Block 收口后无悬挂订单。
- 字段口径约束（用于日志与可视化解释）：
  - `desired_*_volume`：信号时计算出的期望量；
  - `*_order_volume`：Block 初始执行目标量（信号启动时确定，且为执行硬上限）；
  - `*_filled`：实际成交累计量；
  - 约束：`*_filled <= *_order_volume`，若无法继续回补则按 timeout 分支进入 `PARTIAL/CRITICAL`。

### 4.5 日内边界

- 临近收盘不再启动新 Block。
- 未完成 Block 必须在日内走完超时分支，不跨日悬挂。

***

## 5. 完整参数表

### 5.1 策略参数

| 参数名                | 类型  | 默认值   | 说明                       |
| --------------------- | ----- | -------- | -------------------------- |
| `symbol_a`            | str   | —        | A 股票代码                 |
| `symbol_b`            | str   | —        | B 股票代码                 |
| `bar_interval_minutes`| int   | 30       | 分钟 K 线聚合周期（分钟）  |
| `window_minutes`      | int   | 20       | 分钟级布林均线窗口长度     |
| `window_days`         | int   | 20       | 日线级布林均线窗口长度     |
| `k_sigma_minutes`     | float | 2.0      | 分钟级布林 σ 倍数（仅绘图）|
| `k_sigma_days`        | float | 2.0      | 日线级布林 σ 倍数（仅绘图）|
| `thresh_sigma_min`    | float | 0.5      | \|delta_sigma_minutes\| 基础触发门限 |
| `thresh_sigma_min_high`| float| 1.0      | \|delta_sigma_minutes\| 加码触发门限 |
| `thresh_sigma_day`    | float | 1.5      | \|delta_sigma_days\| 加码触发门限    |
| `thresh_delta_min`    | float | 0.005    | \|delta_minutes\| 触发门限           |
| `cooldown_seconds`    | int   | 1800     | 同方向交易冷却时间（秒）   |
| `block_timeout_minutes`| int  | 20       | 单个 Block 实现期（被动+激进总时限） |
| `timeout_recover_minutes`| int| 2        | 超时后二阶段激进补单时长（分钟） |
| `timeout_recover_policy`| str| balance  | 超时后二阶段策略：full / balance / abort（兼容旧值 recover，映射为 balance） |
| `chase_wait_ticks`    | int   | 3        | 激进单每轮等待 tick 数      |
| `max_chase_rounds`    | int   | 20       | 激进改单最大轮数            |
| `passive_slice_count` | int   | 3        | 初始被动拆单数量 n（用于最小撤单改追价） |
| `cancel_priority`     | str   | newest_unfilled_first | 撤单优先级：优先撤最新且未成交子单，保留老队列与部分成交单 |
| `base_pct`            | float | 0.1      | 基础轮动比例               |
| `high_pct`            | float | 0.3      | 加码轮动比例               |
| `enable_t0`           | bool  | False    | 策略层 T+0 开关，启用后当日买入可当日卖出 |

### 5.2 引擎参数

| 参数名           | 类型  | 默认值   | 说明               |
| ---------------- | ----- | -------- | ------------------ |
| `initial_cash`   | float | 1000000  | 初始资金           |
| `commission_rate`| float | 0.00005  | 手续费率           |
| `slippage_ticks` | int   | 1        | 滑点（tick 数）    |
| `enable_t0`      | bool  | False    | 引擎层 T+0 开关，启用后仓位不锁当日买入 |

### 5.3 初始仓位（可选）

支持设定 A、B 两只标的的初始持仓量和成本价，与 MSTR 模式一致：

```json
{
  "initial_positions": {
    "159300.SZ": {"volume": 10000, "cost_price": 4.123},
    "510300.SH": {"volume": 5000,  "cost_price": 3.876}
  }
}
```

***

## 6. 数据来源

| 数据         | 来源                    | 用途                     |
| ------------ | ----------------------- | ------------------------ |
| 日线数据     | `dataset/daily/{symbol}/` | 计算日线级 ABratio 及布林统计量 |
| Tick 数据    | `dataset/ticks/{symbol}/` | 盘中实时信号 & 自行聚合 30 分钟 K 线 |

- 日线 ABratio 直接使用 daily 目录的收盘价相除。
- 30 分钟 K 线由策略从 tick 数据自行聚合。

***

## 7. 文件结构

```
strategies/
    ds_dmtr_strategy.py          # DS_DMTR 策略实现（继承 StrategyBase）

ds_dmtr_gui/
    __init__.py
    __main__.py                 # python -m ds_dmtr_gui 入口
    launcher.py                 # 参数配置 & 回测启动
    main.py                     # 主窗口（Launcher + LogViewer 双 Tab）
    log_viewer.py               # 回测结果查看

ds_dmtr_params.json              # 参数持久化文件

docs/
    design_ds_dmtr_strategy.md   # 本设计文档
```

***

## 8. GUI 界面设计

主窗口采用与 `mstr_gui` 一致的 **双 Tab** 结构。

### 8.1 Tab 1：策略配置（Launcher）

多排布局（与 mstr_gui 一致）：

**第一排（stretch=1，可拉伸）**：

| 左列 (stretch=3) | 中列 (stretch=2) | 右列 (stretch=2) |
|---|---|---|
| 候选股票池 | 已选标的 + 回测区间 | 布林通道参数 |
| 搜索过滤框 | Symbol A 显示 | K线周期、窗口长度 |
| 品种列表（多选） | Symbol B 显示 | sigma 绘图倍数 |
| [添加选中→] [全部添加→] | 起始/结束日期 | |
| [→设为A] [→设为B] | | |

**第二排（stretch=0，固定高度）**：

| 左列 (stretch=3) | 中列 (stretch=3) | 右列 (stretch=3) |
|---|---|---|
| 触发门限参数 | 执行参数 + 初始仓位 | 引擎参数 |
| sigma基础/加码门限 | 冷却时间、基础/加码比例 | 初始资金、手续费率 |
| 日线sigma门限 | 初始仓位表格 [+][-] | 滑点、最小价格变动 |
| delta偏离门限 | | 撮合模式 |

**按钮区**：[保存参数] [加载参数] ... [▶ 运行回测]

**进度 & 日志区**：进度条 + 只读日志文本框

---

**左栏** — 候选品种池（与 mstr_gui 完全一致）：
- 列出所有可用品种（代码 + 名称），来源 `_ALL_ETFS` 列表
- 搜索过滤框：支持字符串过滤快速定位（代码/名称模糊匹配）
- 多选模式：可一次选中多个品种
- [添加选中→] / [全部添加→] 按钮：将选中品种添加到已选列表
- [→设为A] / [→设为B] 按钮：从已选列表中指定 Symbol A / Symbol B

**中栏** — 已选标的 + 回测区间：
- 已选品种列表（QListWidget），支持删除
- Symbol A / Symbol B 显示标签（只读，由按钮设定）
- 起始/结束日期选择（QDateEdit，日历弹出）
- [清除全部] 按钮

**右栏** — 布林通道参数：
- K 线聚合周期、分钟窗口长度、日线窗口长度
- 分钟/日线 sigma 绘图倍数

**第二排**：
- 触发门限参数组（sigma 基础/加码门限、日线 sigma 门限、delta 门限）
- 执行参数组（冷却时间、基础/加码比例）+ 初始仓位表格
- 引擎参数组（初始资金、手续费率、滑点、最小价格变动、撮合模式）

### 8.2 Tab 2：回测结果（LogViewer）

对齐 `mstr_gui` 风格，采用多 Tab 结果页：

| Tab | 内容 |
| --- | --- |
| 总览 | 回测绩效统计 + Block 状态统计（DONE/PARTIAL/BUY_ONLY/REJECTED） |
| Block 跟踪表 | 每个信号对应一个 Block，展示：方向、状态、触发指标、卖/买信号价、期望量、发单量、成交均价、成交量、结束现金、结束净值、耗时 |
| Block 详情 | 选中 Block 后展示详细文本快照（触发参数、执行结果、滑点）+ 价格对比图（信号价 vs 成交均价） |
| 净值曲线 | **策略净值 vs 两只 ETF 买入并持有基准**；下方展示超额收益（vs max/mean 基准）与超额回撤 |
| 日交易可视化 | 上图：A/B 价格双轴 + Block 信号点/成交点 + 状态色；下图：Tick 级 ABratio 与触发时刻竖线 |

> 说明：DS_DMTR 的 Block 为“单次轮动执行跟踪块”，不同于 MSTR 的多子单撮合机理；但在 GUI 展示层保持“每次信号一个 Block”的一致性，便于诊断。

### 8.3 Block 跟踪字段（DS_DMTR）

每个 Block 至少记录以下字段（供 GUI 表格/详情使用）：

- 标识：`block_id`, `trade_date`, `block_seq`
- 信号快照：`signal_time`, `direction`, `trade_pct`, `is_high_pct`
- 指标快照：`ab_ratio`, `delta_sigma_minutes`, `delta_sigma_days`, `delta_minutes`, `mu_min`, `sigma_min`, `mu_day`, `sigma_day`, `a_percentage`
- 市场快照：`sell_symbol`, `buy_symbol`, `sell_signal_price`, `buy_signal_price`
- 账户快照：`signal_cash`, `signal_nav`, `end_cash`, `end_nav`
- 期望与执行：`desired_sell_volume`, `desired_buy_volume`, `sell_order_volume`, `buy_order_volume`
- 成交结果：`sell_filled`, `buy_filled`, `sell_avg_price`, `buy_avg_price`, `sell_commission`, `buy_commission`
- 诊断指标：`slippage_sell`, `slippage_buy`, `total_duration`, `state`

***

## 9. 策略类设计

```python
class DsDmtrStrategy(StrategyBase):
    """Dual-Stock Dual-MA Tick Reversion Strategy"""

    author = "cstm"

    # ── 可配置参数 ──
    symbol_a: str = ""
    symbol_b: str = ""
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
    base_pct: float = 0.1
    high_pct: float = 0.3
    enable_t0: bool = False

    parameters = [
        "symbol_a", "symbol_b",
        "bar_interval_minutes", "window_minutes", "window_days",
        "k_sigma_minutes", "k_sigma_days",
        "thresh_sigma_min", "thresh_sigma_min_high",
        "thresh_sigma_day", "thresh_delta_min",
        "cooldown_seconds", "base_pct", "high_pct",
        "enable_t0",
    ]

    variables = [
        "ab_ratio", "mu_min", "sigma_min",
        "mu_day", "sigma_day",
        "delta_sigma_minutes", "delta_sigma_days",
        "delta_minutes", "a_percentage",
    ]

    # ── 内部状态 ──
    # ratio_bar_closes: deque    # 30min ABratio 收盘价环形缓冲
    # ratio_day_closes: list     # 日线 ABratio 序列
    # last_trade_direction: str  # 上次交易方向
    # last_trade_time: datetime  # 上次交易时间
    # current_bar_start: datetime  # 当前 30min K 线起始时间
    # current_bar_ohlc: dict     # 当前 30min K 线 OHLC 聚合状态
```

### 9.1 生命周期

| 回调              | 职责                                                   |
| ----------------- | ------------------------------------------------------ |
| `on_init()`       | 初始化缓冲区、加载日线数据计算初始日线布林统计量       |
| `on_day_begin(bar)` | 更新日线 ABratio 序列、重算日线布林统计量            |
| `on_tick(tick)`   | 更新 ABratio → 聚合 30min K 线 → 信号计算 → 决策下单  |
| `on_order(order)` | 记录订单状态                                           |
| `on_trade(trade)` | 更新 `last_trade_time`、`last_trade_direction`         |

### 9.2 30 分钟 K 线聚合

在 `on_tick()` 中：

1. 计算当前 tick 属于第几根 30min bar（按 `bar_interval_minutes` 对齐到交易时段整点，如 09:30、10:00、10:30、11:00、11:30、13:00、13:30...）
2. 如果 tick 属于当前 bar → 更新 OHLC
3. 如果 tick 属于新 bar → 完成当前 bar（推入 `ratio_bar_closes`），开始新 bar
4. 当 `ratio_bar_closes` 长度 ≥ `window_minutes` 时，计算分钟级布林统计量

**时间连续性规则**：
- 午休跨越（11:30→13:00）：11:30 bar 正常收盘，13:00 直接开新 bar，中间不产生跨越 bar
- 隔夜跨越（15:00→次日 09:30）：同理，bar 仅按实际交易时间连续拼接
- 缓冲区跨日不清空，持续滚动

### 9.3 预热期处理

为保证回测正式开始日（用户设定的 `start_date`）第一个 tick 起，分钟级布林统计量即已就绪：

1. 根据 `window_minutes` 和 `bar_interval_minutes` 计算需要的最少 bar 数（= `window_minutes`）
2. 每交易日产生约 `240 / bar_interval_minutes` 根 bar（4 小时交易时间 = 240 分钟）
3. 倒推预热天数：`ceil(window_minutes / bars_per_day)` + 余量
4. 在 `on_init()` 阶段，从 `start_date` 之前加载足够天数的 tick 数据，预先聚合 30 分钟 K 线并填满 `ratio_bar_closes` 缓冲区
5. 预热阶段不产生交易信号，仅用于构建统计量

同理，日线级预热由 `window_days` 决定，从 daily 数据中向前取足够天数。

***

## 10. JSON 参数文件格式

`ds_dmtr_params.json` 示例：

```json
{
  "engine": {
    "initial_cash": 1000000,
    "commission_rate": 0.00005,
    "slippage_ticks": 1
  },
  "strategy": {
    "symbol_a": "159300.SZ",
    "symbol_b": "510300.SH",
    "start_date": "2025-01-01",
    "end_date": "2025-06-30",
    "bar_interval_minutes": 30,
    "window_minutes": 20,
    "window_days": 20,
    "k_sigma_minutes": 2.0,
    "k_sigma_days": 2.0,
    "thresh_sigma_min": 1.0,
    "thresh_sigma_min_high": 2.0,
    "thresh_sigma_day": 1.5,
    "thresh_delta_min": 0.005,
    "cooldown_seconds": 1800,
    "base_pct": 0.1,
    "high_pct": 0.3
  },
  "initial_positions": {
    "159300.SZ": {"volume": 10000, "cost_price": 4.123},
    "510300.SH": {"volume": 5000,  "cost_price": 3.876}
  }
}
```
