# cstm_vn 项目架构分析文档

> 基于 VeighNa (vnpy_evo) 的量化交易平台，集成桌面 GUI、Web 数据浏览器与订单簿排队仿真回测引擎。

---

## 目录

1. [项目概述](#1-项目概述)
2. [系统架构总览](#2-系统架构总览)
3. [核心模块详解](#3-核心模块详解)
4. [前端架构](#4-前端架构-react--electron)
5. [数据流与存储设计](#5-数据流与存储设计)
6. [脚本与入口点](#6-脚本与入口点)
7. [外部依赖](#7-外部依赖)
8. [VeighNa 核心模块复用指南](#8-veighna-核心模块复用指南)

---

## 1. 项目概述

**名称**: cstm_vn (Customized VeighNa)
**定位**: 面向 A 股与加密货币的量化交易平台

**核心能力**:
- 高精度回测引擎（订单簿排队仿真，非简单价格穿越）
- 多源行情数据管理（TuShare、BaoStock、原始 CSV/7z）
- 桌面 GUI 数据浏览器（PyQt5 + Electron/React 双栈）
- 分布式批量回测（ProcessPoolExecutor 并行）

---

## 2. 系统架构总览

```
┌──────────────────────────────────────────────────────────────┐
│                   桌面应用 (Electron)                         │
│  ┌────────────────────────────────────────────────────────┐  │
│  │     React UI (DataBrowser + Workspace)                 │  │
│  │  - DataBrowser: 数据集浏览与品种搜索                      │  │
│  │  - Workspace:  SQL 查询、时序可视化                      │  │
│  └────────────────────────────────────────────────────────┘  │
│              IPC Bridge (preload.ts / contextBridge)          │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Electron Main Process                                 │  │
│  │  - DuckDB 内存 SQL 引擎 → 直读 Parquet                  │  │
│  │  - IPC Handlers: list / preview / query / series       │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                            ↕
┌──────────────────────────────────────────────────────────────┐
│                   Python 后端                                │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ vnpy_evo/    │  │virtual_market│  │    tools/         │   │
│  │ 交易引擎框架  │  │ 仿真回测引擎  │  │  数据管理工具集   │   │
│  │ 事件驱动架构  │  │ 排队撮合策略  │  │  采集·清洗·编目   │   │
│  └──────────────┘  └──────────────┘  └──────────────────┘   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           gui_viewer/ (PyQt5 数据浏览器)               │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                            ↕
┌──────────────────────────────────────────────────────────────┐
│                    数据层 (文件系统)                           │
│  dataset/                                                    │
│  ├── meta/          instruments.json, dataset_manifest.json  │
│  ├── ticks/         TS_CODE/YYYY-MM/YYYYMMDD.parquet        │
│  └── daily/         TS_CODE/YYYY.parquet                    │
│                                                              │
│  rawData/           原始 CSV/7z 归档（不入 git）              │
│  outputs/           回测日志与结果（不入 git）                 │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. 核心模块详解

### 3.1 vnpy_evo/ — VeighNa 交易引擎框架

**职责**: 提供事件驱动交易基础设施，包括行情网关、订单管理、日志、告警。

| 子模块 | 说明 |
|--------|------|
| `trader/engine.py` | **MainEngine** (核心引擎)、**LogEngine** (loguru 日志)、**TelegramEngine** (告警推送) |
| `trader/` | 网关集成、订单/成交处理、UI 组件 |
| `event/` | EventEngine 事件总线 — 所有组件通过回调注册通信 |
| `rpc/` | RpcServer / RpcClient — ZMQ 分布式交易 |
| `rest/` | REST API 请求封装 |
| `websocket/` | WebSocket 行情数据订阅客户端 |
| `chart/` | K 线图表工具 |

**设计模式**: 事件驱动 — 所有组件向 `EventEngine` 注册回调，通过事件类型解耦。

```python
# 核心引擎初始化流程
MainEngine.__init__()
  → EventEngine().start()
  → LogEngine()          # loguru 替代默认日志
  → OmsEngine()          # 订单管理系统
  → EmailEngine()        # 邮件告警
  → TelegramEngine()     # Telegram 推送
```

---

### 3.2 virtual_market/ — 仿真回测引擎

**职责**: 高保真订单撮合仿真，核心亮点是**订单簿排队模拟**（区别于传统价格穿越回测）。

#### 数据模型 (`types.py`)

```python
@dataclass
class Tick:       # 行情切片: bid/ask 价量、最新价、累计成交量
class Order:      # 委托单: side, price, volume, queue_ahead, age_ticks
class Trade:      # 成交记录: order_id, price, volume, timestamp
class OrderStatus:  # SUBMITTED → PARTIAL → FILLED / CANCELLED
class Side:       # BUY / SELL
```

#### 撮合引擎 (`engine.py` — MatchingEngine)

```
核心 API:
  submit_limit_order(side, price, volume) → order_id
  cancel_order(order_id)
  process_tick(tick)  ← 每个行情切片驱动撮合

排队仿真逻辑:
  1. 挂单时记录当前 bid/ask 队列深度 → queue_ahead
  2. 每个 tick 根据成交量变化递减 queue_ahead
  3. queue_ahead ≤ 0 时触发成交
  4. 回调: on_order() / on_trade() 通知策略
```

#### 策略 (`strategy.py` — LowBuyHighSellStrategy)

| 参数 | 说明 |
|------|------|
| `order_size` | 单笔委托量 |
| `lookback_ticks` | 回看窗口长度 |
| `buy_buffer` | 买入缓冲系数 |
| `sell_buffer` | 卖出缓冲系数 |
| `cancel_after_ticks` | 超时撤单阈值 |

**策略逻辑**:
- 买入: price ≤ min(lookback) × (1 + buy_buffer)
- 卖出: price ≥ max(lookback) × (1 - sell_buffer)
- 超时自动撤单

#### 回测执行 (`backtest.py` / `batch.py`)

```
单日回测 (run_backtest):
  加载 parquet → MatchingEngine + Strategy → 逐 tick 处理
  → 输出 events.jsonl, orders.jsonl, trades.jsonl, summary.json

批量回测 (run_batch_backtest):
  BatchTask(symbol, date) × N
  → ProcessPoolExecutor 并行执行
  → 汇总: 成交量、盈亏、耗时
```

---

### 3.3 tools/ — 数据管理工具集

**职责**: 多源数据采集、自动清洗、格式标准化、元数据编目。

| 文件 | 功能 |
|------|------|
| `tick_parquet_manager.py` | 7z 压缩包解压 → 自动检测编码/分隔符 → Tick 数据转 Parquet |
| `daily_parquet_manager.py` | CSV → 日线 OHLCV Parquet，列名标准化 |
| `tushare_manager.py` | TuShare API 对接：日线下载、品种元数据同步 |
| `dataset_updater.py` | 数据更新编排器：增量 / 全量模式 |
| `manifest_manager.py` | 生成 `dataset_manifest.json` 数据目录 |
| `rawdata_inspector.py` | 原始 CSV 智能探测：编码、分隔符、字段结构、数据质量校验 |
| `universe.py` | 交易品种集定义 (symbols.txt → UniverseItem 列表) |

**数据标准化规范**:
- Tick: `dataset/ticks/{TS_CODE}/{YYYY-MM}/{YYYYMMDD}.parquet`
- 日线: `dataset/daily/{TS_CODE}/{YYYY}.parquet`
- 元数据: `dataset/meta/instruments.json`, `dataset_manifest.json`

---

### 3.4 gui_viewer/ — PyQt5 桌面数据浏览器

**职责**: 独立运行的 Parquet 数据探索工具。

| 文件 | 功能 |
|------|------|
| `main.py` | 入口: `python -m gui_viewer --dataset-root /path` |
| `ui_main.py` | 主窗口: 左侧品种树 + 右侧数据表/图表 + 导入工具集成 |
| `data_access.py` | Parquet I/O: `load_daily()`, `load_tick_day()`, 可用数据列表 |
| `table_model.py` | `DataFrameTableModel` — PyQt5 TableView 的 pandas 适配器 |

---

## 4. 前端架构 (React + Electron)

### 4.1 React UI (src/)

```
src/
├── App.tsx              路由: "/" → DataBrowser, "/workspace" → Workspace
├── main.tsx             React 入口
├── pages/
│   ├── DataBrowser.tsx  品种列表、搜索、预览 (前 120 行)
│   └── Workspace.tsx    SQL 查询、分页表格、时序图表
├── components/
│   ├── AppShell.tsx     导航头部框架
│   ├── DataTable.tsx    虚拟化数据表格
│   ├── SeriesChart.tsx  Recharts 折线图
│   └── WorkspaceSidebar.tsx  筛选条件构建器
├── store/
│   └── datasetStore.ts  Zustand 状态管理 (品种列表、选中项)
├── hooks/
│   └── useTheme.ts      暗色主题 hook
└── utils/
    └── columnGuess.ts   自动识别时间/数值列
```

**筛选操作符**: `eq`, `contains`, `gt`, `gte`, `lt`, `lte`, `between`, `is_null`, `not_null`

### 4.2 Electron 主进程 (electron/src/)

| 文件 | 职责 |
|------|------|
| `main.ts` | 窗口管理: dev 加载 `localhost:5173`，prod 加载打包产物 |
| `preload.ts` | 安全 IPC 桥: `window.datasetApi` (contextBridge 隔离) |
| `ipc.ts` | IPC 处理器: `datasets:list`, `dataset:preview`, `table:query`, `series:query` |
| `duckdbClient.ts` | DuckDB 内存 SQL 引擎，直接 `read_parquet()` 查询 |
| `queryBuilder.ts` | SQL 构建器 (参数化查询，防注入)，含降采样逻辑 |
| `datasets.ts` | 数据集发现: 扫描目录 → DatasetNode 树 |
| `paths.ts` | 路径安全: `safeResolveUnder()` 防路径穿越 |

### 4.3 IPC 类型定义 (shared/ipc.ts)

```typescript
DataKind = "daily" | "tick"
TableQuery  = { dataset, columns?, filters?, orderBy?, limit, offset }
SeriesQuery = { dataset, xCol, yCol, filters, maxPoints }
TableResult = { columns: Column[], rows: any[][], totalEstimate: number }
```

### 4.4 技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| React | 18.3.1 | UI 框架 |
| Vite | 6.3.5 | 构建 + HMR 开发服务器 |
| Electron | 35.0.0 | 桌面宿主 |
| DuckDB | 1.3.2 | 内存 SQL (Parquet 直查) |
| Recharts | 2.15.1 | 图表渲染 |
| Zustand | 5.0.3 | 状态管理 |
| Tailwind CSS | 3.4.17 | 暗色主题样式 |

---

## 5. 数据流与存储设计

### 5.1 数据采集管线

```
外部数据源 (TuShare / BaoStock / 原始 CSV)
    ↓  download_baostock_*.py / tushare_manager.py
rawData/  (原始归档，不入版本控制)
    ↓  rawdata_inspector.py (自动探测编码/分隔符/结构)
    ↓  tick_parquet_manager.py / daily_parquet_manager.py
dataset/ticks/  和  dataset/daily/  (Parquet 列式存储)
    ↓  manifest_manager.py
dataset/meta/dataset_manifest.json  (数据目录)
dataset/meta/instruments.json       (品种映射)
```

### 5.2 前端查询路径

```
React UI 操作 (筛选 / 翻页 / 选时序列)
    ↓  ipcRenderer.invoke()
Electron IPC Handler
    ↓  queryBuilder.ts 生成 SQL
DuckDB  SELECT ... FROM read_parquet('{path}') WHERE ... LIMIT ...
    ↓  结果集
React UI 渲染 (DataTable / SeriesChart)
```

### 5.3 回测数据路径

```
dataset/ticks/{SYMBOL}/{YYYY-MM}/{YYYYMMDD}.parquet
    ↓  backtest.py 加载
MatchingEngine + Strategy (逐 tick 仿真)
    ↓  撮合事件流
outputs/simulation_logs/
├── {SYMBOL}_{DATE}_events.jsonl
├── {SYMBOL}_{DATE}_orders.jsonl
├── {SYMBOL}_{DATE}_trades.jsonl
└── {SYMBOL}_{DATE}_summary.json
```

---

## 6. 脚本与入口点

### 回测脚本

| 脚本 | 说明 |
|------|------|
| `run_virtual_market_simulation.py` | 单日仿真回测 (virtual_market 引擎) |
| `run_virtual_market_batch.py` | 批量并行回测 (ProcessPoolExecutor) |
| `run_simple_backtest.py` | VeighNa BacktestingEngine + DoubleMaStrategy (传统模式) |
| `run_with_backtest.py` | 完整 VeighNa 桌面应用 (BinanceLinear 网关) |

### 数据脚本

| 脚本 | 说明 |
|------|------|
| `download_baostock_data.py` | BaoStock 数据下载 |
| `download_baostock_*_5min.py` | 特定品种 5 分钟级数据下载 |
| `import_baostock_*.py` | CSV 导入特定品种 |
| `import_data.py` | 通用数据导入 |
| `generate_mock_data.py` | 生成 1 分钟模拟 OHLCV 数据 |

### GUI 启动

```bash
# PyQt5 数据浏览器
python -m gui_viewer --dataset-root ./dataset

# Electron 桌面应用 (开发模式)
npm run dev

# Electron 桌面应用 (生产构建)
npm run build
```

### 示例程序 (examples/)

| 目录 | 说明 |
|------|------|
| `examples/evo_trader/` | VeighNa UI + Binance 网关 + NovaStrategy |
| `examples/simple_rpc/` | ZMQ 分布式交易 RPC 服务端/客户端 |
| `examples/candle_chart/` | K 线图表示例 |

---

## 7. 外部依赖

### Python

| 包 | 用途 |
|----|------|
| vnpy | 交易框架核心 |
| loguru | 结构化日志 |
| pandas / numpy | 数据处理 |
| pyarrow | Parquet I/O |
| py7zr | 7z 解压 |
| chardet | 编码检测 |
| tushare | A 股行情 API |
| baostock | A 股行情 API |
| PySide6-Fluent-Widgets | Fluent UI 组件 |
| PyQt5 | GUI 框架 |
| websocket-client | WebSocket 连接 |

### Node.js

| 包 | 用途 |
|----|------|
| react / react-dom | UI 框架 |
| electron | 桌面应用宿主 |
| duckdb | 内存 SQL 引擎 |
| recharts | 折线图 |
| zustand | 状态管理 |
| tailwindcss | 样式 |
| lucide-react | 图标库 |
| vite | 构建工具 |

---

## 8. VeighNa 核心模块复用指南

> 以下为 VeighNa 核心模块在仿真回测中的复用说明。

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
