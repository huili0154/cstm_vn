# cstm_vn — ETF 量化回测与实盘交易系统

## 项目简介

基于 A 股 ETF Tick/日线数据的量化交易系统，包含数据管理、回测引擎、实盘交易和可视化工具。

## 核心模块

| 模块 | 用途 | 入口 |
|------|------|------|
| `core/` | 数据类型、策略接口、撮合引擎（回测/实盘共用） | - |
| `backtest/` | 回测引擎，支持日线/Tick/延迟深度三种撮合模式 | `engine.py` |
| `strategies/` | 策略实现（grid、mstr、ds_dmtr） | 各策略文件 |
| `live/` | 实盘引擎，接入 mini-QMT (xtquant) | `python -m live` |
| `gui_viewer/` | PyQt5 数据浏览器 + 数据导入工具（7z Tick 导入、日线下载、表格/图表查看） | `python -m gui_viewer` |
| `mstr_gui/` | MSTR 策略 GUI | `python -m mstr_gui` |
| `ds_dmtr_gui/` | DS DMTR 策略 GUI | `python -m ds_dmtr_gui` |
| `tools/` | 数据工具（7z 导入、TuShare 下载、代码池管理） | - |

## 数据目录（不纳入 git）

- `dataset/meta/` — 品种元数据（instruments.parquet、symbols.txt）
- `dataset/daily/` — 日线 Parquet
- `dataset/tick/` — Tick Parquet
- `rawData/` — 原始 7z 压缩数据

## 技术栈

- Python 3.10+
- PyQt5、pandas、pyarrow、pyqtgraph、py7zr、tushare、xtquant

## 常用命令

```bash
python -m gui_viewer          # 启动数据浏览器
python -m mstr_gui            # 启动 MSTR 策略 GUI
python -m ds_dmtr_gui         # 启动 DS DMTR 策略 GUI
python -m live                # 启动实盘交易
python run_ds_dmtr_backtest.py  # 运行 DS DMTR 回测
```

## 设计文档索引

详细设计方案参见 `docs/` 目录：
- `docs/design_ds_dmtr_strategy.md` — DS DMTR 策略设计
- `docs/design_mstr_strategy.md` — MSTR 策略设计
- `docs/design_matching_engine.md` — 撮合引擎设计
- `docs/design_strategy_interface.md` — 策略接口设计
- `docs/策略快速回测开发指南.md` — 快速回测开发指南

## 开发约定

- 临时分析/诊断脚本以 `_` 前缀命名，已在 .gitignore 中排除
- 策略参数文件为 JSON 格式（如 `mstr_params.json`、`ds_dmtr_params.json`）
- 回测/实盘共用 `core/strategy.py` 中的 StrategyBase 接口，策略代码零修改切换

## 开发流程

- 涉及新策略或架构变更时，先在 `docs/` 下写设计文档，经用户确认后再编码
- bug 修复、简单功能调整、UI 微调等无需设计文档
