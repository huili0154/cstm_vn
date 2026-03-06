# cstm_vn

ETF Tick 数据浏览器 — 基于 PyQt5 的桌面应用，用于查看和管理 Parquet 格式的 ETF 分笔数据。

## 项目结构

```
gui_viewer/    # PyQt5 桌面数据浏览器
tools/         # 数据处理工具（导入、下载、代码池管理）
dataset/       # Parquet 数据存储（不纳入 git）
rawData/       # 原始 7z 压缩数据（不纳入 git）
```

## 快速开始

```bash
pip install -r requirements.txt
python -m gui_viewer
```

## 依赖

- Python 3.10+
- PyQt5、pandas、pyarrow、py7zr、tushare、pyqtgraph
