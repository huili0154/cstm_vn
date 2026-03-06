# cstm_vn 项目架构分析

## 1. 项目概述

cstm_vn 是一个 **ETF 分笔（Tick）数据管理与可视化** 桌面应用。核心功能包括：

- 从 7z 压缩包中提取原始 Tick CSV 数据，清洗后转为 Parquet 格式存储
- 通过 TuShare API 下载 ETF 日线行情及复权因子
- 基于 PyQt5 的交互式数据浏览器，支持日线/Tick 表格查看、时序图表绘制、数据导出

技术栈：Python 3.10+ / PyQt5 / pandas / pyarrow / pyqtgraph / py7zr / tushare

---

## 2. 目录结构

```
cstm_vn/
├── gui_viewer/                  # 桌面 GUI 应用
│   ├── __init__.py
│   ├── __main__.py              # python -m gui_viewer 入口
│   ├── main.py                  # 解析命令行参数，创建 QApplication 并启动主窗口
│   ├── data_access.py           # 数据访问层：读取 Parquet 文件
│   ├── table_model.py           # QAbstractTableModel 封装，驱动 QTableView
│   └── ui_main.py               # MainWindow：全部 UI 布局、交互逻辑、图表绑定
│
├── tools/                       # 数据处理工具库
│   ├── __init__.py
│   ├── universe.py              # 证券代码池读取（读 symbols.txt）
│   ├── dataset_manifest.py      # dataset_manifest.json 读写
│   ├── tick_parquet_manager.py  # 7z → CSV → Parquet 转换引擎
│   └── tushare_manager.py       # TuShare API 客户端（日线下载、instruments 同步）
│
├── dataset/                     # 数据存储（.gitignore 排除）
│   ├── meta/
│   │   ├── symbols.txt          # 关注品种列表（21 只 ETF 代码）
│   │   ├── instruments.parquet  # 品种元数据（TuShare 同步生成）
│   │   ├── instruments.json     # 同上的 JSON 副本
│   │   ├── dataset_manifest.json# 数据集清单（记录已导入的文件信息）
│   │   └── tushare_token.local  # TuShare API Token（本地文件，不入库）
│   ├── daily/{ts_code}/{year}.parquet    # 日线数据
│   └── ticks/{ts_code}/{YYYY-MM}/{YYYYMMDD}.parquet  # Tick 数据
│
├── rawData/                     # 原始 7z 压缩包（.gitignore 排除）
│   └── {YYYYMMDD}.7z            # 每日行情压缩包
│
├── requirements.txt             # Python 依赖
├── .gitignore
├── LICENSE
└── README.md
```

---

## 3. 模块依赖关系

```
gui_viewer/main.py
    └── gui_viewer/ui_main.py (MainWindow)
            ├── gui_viewer/data_access.py     # Parquet 数据读取
            ├── gui_viewer/table_model.py     # 表格模型
            ├── tools/tick_parquet_manager.py  # import_from_raw()
            │       ├── tools/dataset_manifest.py
            │       └── tools/universe.py
            ├── tools/tushare_manager.py       # fetch_daily_year()
            │       └── tools/universe.py
            └── tools/universe.py              # read_universe()
```

> `data_access.py` 和 `table_model.py` 无任何 tools/ 依赖，仅使用 pandas + pyarrow + PyQt5。

---

## 4. 核心模块详解

### 4.1 gui_viewer/data_access.py — 数据访问层

纯数据读取模块，无外部项目依赖。

| 函数 | 功能 |
|------|------|
| `load_instruments(ds_root)` | 从 `instruments.parquet` 加载品种列表 |
| `available_daily_years(ds_root, ts_code)` | 扫描某品种的日线 Parquet 年份 |
| `load_daily(ds_root, ts_code, year)` | 加载指定年份日线 DataFrame |
| `available_tick_dates(ds_root, ts_code)` | 扫描某品种的 Tick 日期列表 |
| `load_tick_day(ds_root, ts_code, yyyymmdd)` | 加载单日 Tick DataFrame |
| `load_tick_series(ds_root, ts_code, start, end, y_col)` | 加载日期范围内指定列的 Tick 序列 |
| `load_tick_range_full(ds_root, ts_code, start, end)` | 加载日期范围内全部列的 Tick 数据 |

### 4.2 gui_viewer/ui_main.py — 主窗口

约 1850 行，是整个应用最大的模块，包含：

**界面布局（左右分栏）：**
- 左侧面板：
  - 数据管理区（symbols 文件选择、7z 源目录、日线日期范围、导入/下载按钮、进度条）
  - 清除数据按钮（分别清除日线/Tick）
  - 导出按钮（CSV / PNG）
  - 可视化控件（标的选择、数据类型切换、年份/日期/范围选择）
  - 图表选项（Y 轴列选择、主/次双轴、显示圆点、过滤零值、断开空档、自适应 Y 轴）
  - 消息流（日志输出区域）
- 右侧面板：
  - 上方：QTableView 数据表格
  - 下方：pyqtgraph 时序图表（支持双 Y 轴、DateAxisItem、悬浮 Tooltip、滚轮缩放）

**核心交互流程：**
1. 用户选择标的 → `_on_symbol_changed()` → 重新加载该品种的日期/年份列表
2. 切换日线/Tick 模式 → `_apply_mode_visibility()` → 显示/隐藏对应控件
3. 切换年份/日期 → `_refresh_table()` 更新表格 + `_refresh_chart()` 更新图表
4. 点击"开始导入/下载" → `_on_start_import()` → 依次执行 Tick 导入和日线下载
5. 关闭窗口 → `_save_settings()` 持久化所有界面状态（标的、模式、列选择等）

**图表特性：**
- 主/次双 Y 轴（独立或共享坐标）
- 降采样渲染（Tick 最多 20000 点，日线最多 10000 点）
- 时间空档断线（Tick 默认 5 分钟，日线默认 3 天）
- 零值过滤
- Hover Tooltip 显示完整行数据（含十档盘口）
- Ctrl+滚轮 Y 轴缩放，普通滚轮 X 轴缩放
- 自动 Y 轴范围跟随可视区域

### 4.3 tools/tick_parquet_manager.py — Tick 数据导入引擎

将原始供应商数据（7z 压缩包内的 CSV）转换为标准化 Parquet 文件。

**处理流程：**
```
{date}.7z  →  解压提取  →  读取 CSV  →  清洗标准化  →  写入 Parquet
                                            │
                                            ├── 时间解析（自然日+时间 → datetime）
                                            ├── 价格缩放（自动检测万分/百分/原值）
                                            ├── 量差计算（累计量 → 逐笔增量）
                                            └── 盘口标准化（十档买卖价量）
```

**输出 Parquet schema（核心字段）：**
`datetime`, `last_price`, `volume`, `turnover`, `cum_volume`, `cum_turnover`, `open_price`, `high_price`, `low_price`, `pre_close`, `trades_count`, `bs_flag`, `trade_flag`, `iopv`, `bid_price_1..10`, `ask_price_1..10`, `bid_volume_1..10`, `ask_volume_1..10`

**辅助功能：**
- `export_csv()` — 将 Parquet Tick 数据导出为 CSV
- `probe_csv()` — 自动探测 CSV 编码、分隔符、表头
- 每次导入后自动更新 `dataset_manifest.json`

### 4.4 tools/tushare_manager.py — TuShare API 客户端

通过 TuShare Pro API 获取 ETF 基本信息和日线行情。

| 函数 | 功能 |
|------|------|
| `sync_instruments(root, universe_file, rawdir)` | 从 symbols.txt 读取代码，查询 TuShare 获取 ETF 基本信息，生成 `instruments.parquet` |
| `fetch_daily_year(root, instruments_parquet, year)` | 下载指定年份所有品种的日线行情 + 复权因子，计算后复权价格，写入 Parquet |

**Token 加载优先级：** 环境变量 `TUSHARE_TOKEN` → `dataset/meta/tushare_token.local` → `~/.tushare_token`

**日线 Parquet schema：**
`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `volume`, `turnover`, `adj_factor`, `open_bwd`, `high_bwd`, `low_bwd`, `close_bwd`, `pre_close_bwd`, `name`

### 4.5 tools/universe.py — 证券代码池

从文本文件逐行读取证券代码（支持 `510300` 或 `510300.SH` 格式），跳过注释和空行，去重返回。

### 4.6 tools/dataset_manifest.py — 数据集清单

管理 `dataset/meta/dataset_manifest.json`，记录每个已导入文件的元数据（品种、日期、行数、时间范围等）。支持 upsert 操作。

---

## 5. 数据流

```
                          ┌──────────────────┐
                          │  symbols.txt     │ ← 21只ETF代码
                          └────────┬─────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
         ▼                         ▼                         ▼
┌─────────────────┐    ┌───────────────────┐    ┌──────────────────┐
│  rawData/*.7z   │    │  TuShare Pro API  │    │  instruments     │
│  (供应商原始数据) │    │  (在线行情接口)     │    │  .parquet/.json  │
└────────┬────────┘    └─────────┬─────────┘    └────────┬─────────┘
         │                       │                       │
    tick_parquet_           tushare_                      │
    manager.py             manager.py                    │
         │                       │                       │
         ▼                       ▼                       │
┌─────────────────┐    ┌───────────────────┐             │
│ dataset/ticks/  │    │  dataset/daily/   │             │
│  {Parquet}      │    │   {Parquet}       │             │
└────────┬────────┘    └─────────┬─────────┘             │
         │                       │                       │
         └───────────┬───────────┘                       │
                     │                                   │
                     ▼                                   ▼
              ┌──────────────────────────────────────────────┐
              │            gui_viewer (PyQt5)                │
              │                                              │
              │  data_access.py  ←  读取 Parquet 文件         │
              │  table_model.py  ←  驱动 QTableView          │
              │  ui_main.py      ←  图表 + 交互 + 数据管理    │
              └──────────────────────────────────────────────┘
```

---

## 6. 启动方式

```bash
# 启动 GUI 数据浏览器
python -m gui_viewer

# 指定自定义数据目录
python -m gui_viewer --dataset-root /path/to/dataset

# 命令行：从 7z 导入 Tick 数据
python -m tools.tick_parquet_manager import --symbols-file dataset/meta/symbols.txt --rawdir rawData

# 命令行：导出 Tick 为 CSV
python -m tools.tick_parquet_manager export --symbol 510300.SH --start 2025-01-01 --end 2025-02-01 --out out.csv

# 命令行：同步 instruments 元数据
python -m tools.tushare_manager sync-instruments

# 命令行：下载指定年份日线数据
python -m tools.tushare_manager fetch-daily-year --year 2025
```

---

## 7. 关键依赖

| 包 | 用途 |
|----|------|
| PyQt5 | GUI 框架 |
| pyqtgraph | 时序图表渲染 |
| pandas | 数据处理 |
| pyarrow | Parquet 读写 |
| py7zr | 7z 压缩包解压 |
| tushare | ETF 行情 API |
| chardet | CSV 编码自动检测 |
| numpy | 数值计算（图表降采样、坐标变换） |
| pytz | 时区处理（Asia/Shanghai） |

> 注：`requirements.txt` 当前只列了 `PySide6==6.3.0`（历史遗留），实际代码使用的是 PyQt5，需要更新。
