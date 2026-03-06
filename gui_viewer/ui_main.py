from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from datetime import datetime
import contextlib
import io
import shutil
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from PyQt5.QtCore import QDate, QSettings, Qt, QThread, pyqtSignal
from PyQt5.QtGui import QCursor, QKeySequence
from PyQt5.QtWidgets import (
    QApplication,
    QAction,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFrame,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QSplitter,
    QTableView,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from gui_viewer.data_access import (
    available_daily_years,
    available_tick_dates,
    dataset_root as default_dataset_root,
    format_yyyymmdd,
    load_daily,
    load_instruments,
    load_tick_day,
    load_tick_range_full,
    load_tick_series,
    parse_yyyymmdd,
)
from gui_viewer.table_model import DataFrameTableModel
from tools.tick_parquet_manager import import_from_raw
from tools.tushare_manager import fetch_daily_year
from tools.universe import read_universe


@dataclass
class TickRange:
    start: str
    end: str


def _clip_points(x: np.ndarray, y: np.ndarray, max_points: int) -> tuple[np.ndarray, np.ndarray]:
    n = len(x)
    if n <= max_points:
        return x, y
    step = max(1, n // max_points)
    return x[::step], y[::step]


def _downsample_indices(n: int, max_points: int, extra: np.ndarray | None = None) -> np.ndarray:
    if n <= max_points:
        idx = np.arange(n, dtype=int)
    else:
        step = max(1, n // max_points)
        idx = np.arange(0, n, step, dtype=int)
    if extra is not None and len(extra):
        idx = np.unique(np.concatenate([idx, extra.astype(int)]))
    return idx


def _to_epoch_seconds(dt: pd.Series) -> np.ndarray:
    dt = pd.to_datetime(dt, errors="coerce")
    if dt.dt.tz is None:
        dt = dt.dt.tz_localize("Asia/Shanghai")
    return (dt.astype("int64") // 10**9).to_numpy(dtype=np.int64)


class ImportWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int, str)  # current, total, message
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent)
        self.kwargs = kwargs

    def run(self):
        try:
            self.log_signal.emit("=== 任务开始 ===")
            
            # Unpack args
            raw_dir = self.kwargs['raw_dir']
            symbols_path = self.kwargs['symbols_path']
            d_start = self.kwargs['d_start']
            d_end = self.kwargs['d_end']
            project_root = self.kwargs['project_root']
            ds_root = self.kwargs['ds_root']
            
            # Step 1: Tick Import
            self.log_signal.emit("正在执行：Tick数据导入...")
            tmp_dir = raw_dir / "_tmp_extract_gui"
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                instruments_path = None
                
            def worker_logger(msg: str):
                self.log_signal.emit(msg)
            
            def worker_progress(curr: int, total: int, msg: str):
                self.progress_signal.emit(curr, total, msg)

            tick_res = import_from_raw(
                root=project_root,
                rawdir=raw_dir,
                universe_file=symbols_path,
                dates=None,
                tmp_dir=tmp_dir,
                instruments_path=instruments_path,
                logger=worker_logger,
                progress_callback=worker_progress,
            )
            self.log_signal.emit(f"Tick导入完成: Written={tick_res.get('written', 0)}")
            
            # Step 2: Daily Download
            self.log_signal.emit("正在执行：日线数据下载...")
            
            years = set()
            curr = d_start
            while curr <= d_end:
                years.add(curr.year())
                curr = curr.addDays(365)
            years.add(d_end.year())
            
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                 self.log_signal.emit("跳过日线下载：未找到 instruments.parquet")
            else:
                total_daily_files = 0
                for y in sorted(years):
                    self.log_signal.emit(f"正在下载年份: {y}")
                    try:
                        daily_files = fetch_daily_year(
                            root=project_root, 
                            instruments_parquet=instruments_path, 
                            year=y,
                            logger=worker_logger,
                            progress_callback=worker_progress,
                        )
                        count = len(daily_files)
                        total_daily_files += count
                        self.log_signal.emit(f"年份 {y} 下载完成: {count} 文件")
                    except Exception as e:
                        self.log_signal.emit(f"年份 {y} 下载失败: {e}")
                self.log_signal.emit(f"日线下载完成，共更新 {total_daily_files} 个文件")
            
            self.log_signal.emit("=== 任务结束 ===")
            self.finished_signal.emit()

        except Exception as e:
            self.error_signal.emit(str(e))


class ImportWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent)
        self.kwargs = kwargs

    def run(self):
        try:
            self.log_signal.emit("=== 任务开始 ===")
            
            # Unpack args
            raw_dir = self.kwargs['raw_dir']
            symbols_path = self.kwargs['symbols_path']
            d_start = self.kwargs['d_start']
            d_end = self.kwargs['d_end']
            project_root = self.kwargs['project_root']
            ds_root = self.kwargs['ds_root']
            
            # Step 1: Tick Import
            self.log_signal.emit("正在执行：Tick数据导入...")
            tmp_dir = raw_dir / "_tmp_extract_gui"
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                instruments_path = None
                
            def worker_logger(msg: str):
                self.log_signal.emit(msg)

            tick_res = import_from_raw(
                root=project_root,
                rawdir=raw_dir,
                universe_file=symbols_path,
                dates=None,
                tmp_dir=tmp_dir,
                instruments_path=instruments_path,
                logger=worker_logger,
            )
            self.log_signal.emit(f"Tick导入完成: Written={tick_res.get('written', 0)}")
            
            # Step 2: Daily Download
            self.log_signal.emit("正在执行：日线数据下载...")
            
            years = set()
            curr = d_start
            while curr <= d_end:
                years.add(curr.year())
                curr = curr.addDays(365)
            years.add(d_end.year())
            
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                 self.log_signal.emit("跳过日线下载：未找到 instruments.parquet")
            else:
                total_daily_files = 0
                for y in sorted(years):
                    self.log_signal.emit(f"正在下载年份: {y}")
                    try:
                        daily_files = fetch_daily_year(
                            root=project_root, 
                            instruments_parquet=instruments_path, 
                            year=y,
                            logger=worker_logger
                        )
                        count = len(daily_files)
                        total_daily_files += count
                        self.log_signal.emit(f"年份 {y} 下载完成: {count} 文件")
                    except Exception as e:
                        self.log_signal.emit(f"年份 {y} 下载失败: {e}")
                self.log_signal.emit(f"日线下载完成，共更新 {total_daily_files} 个文件")
            
            self.log_signal.emit("=== 任务结束 ===")
            self.finished_signal.emit()

        except Exception as e:
            self.error_signal.emit(str(e))


class ImportWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent)
        self.kwargs = kwargs

    def run(self):
        try:
            self.log_signal.emit("=== 任务开始 ===")
            
            # Unpack args
            raw_dir = self.kwargs['raw_dir']
            symbols_path = self.kwargs['symbols_path']
            d_start = self.kwargs['d_start']
            d_end = self.kwargs['d_end']
            project_root = self.kwargs['project_root']
            ds_root = self.kwargs['ds_root']
            
            # Step 1: Tick Import
            self.log_signal.emit("正在执行：Tick数据导入...")
            tmp_dir = raw_dir / "_tmp_extract_gui"
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                instruments_path = None
                
            def worker_logger(msg: str):
                self.log_signal.emit(msg)

            tick_res = import_from_raw(
                root=project_root,
                rawdir=raw_dir,
                universe_file=symbols_path,
                dates=None,
                tmp_dir=tmp_dir,
                instruments_path=instruments_path,
                logger=worker_logger,
            )
            self.log_signal.emit(f"Tick导入完成: Written={tick_res.get('written', 0)}")
            
            # Step 2: Daily Download
            self.log_signal.emit("正在执行：日线数据下载...")
            
            years = set()
            curr = d_start
            while curr <= d_end:
                years.add(curr.year())
                curr = curr.addDays(365)
            years.add(d_end.year())
            
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                 self.log_signal.emit("跳过日线下载：未找到 instruments.parquet")
            else:
                total_daily_files = 0
                for y in sorted(years):
                    self.log_signal.emit(f"正在下载年份: {y}")
                    try:
                        daily_files = fetch_daily_year(
                            root=project_root, 
                            instruments_parquet=instruments_path, 
                            year=y,
                            logger=worker_logger
                        )
                        count = len(daily_files)
                        total_daily_files += count
                        self.log_signal.emit(f"年份 {y} 下载完成: {count} 文件")
                    except Exception as e:
                        self.log_signal.emit(f"年份 {y} 下载失败: {e}")
                self.log_signal.emit(f"日线下载完成，共更新 {total_daily_files} 个文件")
            
            self.log_signal.emit("=== 任务结束 ===")
            self.finished_signal.emit()

        except Exception as e:
            self.error_signal.emit(str(e))


class ImportWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent)
        self.kwargs = kwargs

    def run(self):
        try:
            self.log_signal.emit("=== 任务开始 ===")
            
            # Unpack args
            raw_dir = self.kwargs['raw_dir']
            symbols_path = self.kwargs['symbols_path']
            d_start = self.kwargs['d_start']
            d_end = self.kwargs['d_end']
            project_root = self.kwargs['project_root']
            ds_root = self.kwargs['ds_root']
            
            # Step 1: Tick Import
            self.log_signal.emit("正在执行：Tick数据导入...")
            tmp_dir = raw_dir / "_tmp_extract_gui"
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                instruments_path = None
                
            def worker_logger(msg: str):
                self.log_signal.emit(msg)

            tick_res = import_from_raw(
                root=project_root,
                rawdir=raw_dir,
                universe_file=symbols_path,
                dates=None,
                tmp_dir=tmp_dir,
                instruments_path=instruments_path,
                logger=worker_logger,
            )
            self.log_signal.emit(f"Tick导入完成: Written={tick_res.get('written', 0)}")
            
            # Step 2: Daily Download
            self.log_signal.emit("正在执行：日线数据下载...")
            
            years = set()
            curr = d_start
            while curr <= d_end:
                years.add(curr.year())
                curr = curr.addDays(365)
            years.add(d_end.year())
            
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                 self.log_signal.emit("跳过日线下载：未找到 instruments.parquet")
            else:
                total_daily_files = 0
                for y in sorted(years):
                    self.log_signal.emit(f"正在下载年份: {y}")
                    try:
                        daily_files = fetch_daily_year(
                            root=project_root, 
                            instruments_parquet=instruments_path, 
                            year=y,
                            logger=worker_logger
                        )
                        count = len(daily_files)
                        total_daily_files += count
                        self.log_signal.emit(f"年份 {y} 下载完成: {count} 文件")
                    except Exception as e:
                        self.log_signal.emit(f"年份 {y} 下载失败: {e}")
                self.log_signal.emit(f"日线下载完成，共更新 {total_daily_files} 个文件")
            
            self.log_signal.emit("=== 任务结束 ===")
            self.finished_signal.emit()

        except Exception as e:
            self.error_signal.emit(str(e))


class ImportWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()
    error_signal = pyqtSignal(str)

    def __init__(self, parent=None, **kwargs):
        super().__init__(parent)
        self.kwargs = kwargs

    def run(self):
        try:
            self.log_signal.emit("=== 任务开始 ===")
            
            # Unpack args
            raw_dir = self.kwargs['raw_dir']
            symbols_path = self.kwargs['symbols_path']
            d_start = self.kwargs['d_start']
            d_end = self.kwargs['d_end']
            project_root = self.kwargs['project_root']
            ds_root = self.kwargs['ds_root']
            
            # Step 1: Tick Import
            self.log_signal.emit("正在执行：Tick数据导入...")
            tmp_dir = raw_dir / "_tmp_extract_gui"
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                instruments_path = None
                
            def worker_logger(msg: str):
                self.log_signal.emit(msg)

            tick_res = import_from_raw(
                root=project_root,
                rawdir=raw_dir,
                universe_file=symbols_path,
                dates=None,
                tmp_dir=tmp_dir,
                instruments_path=instruments_path,
                logger=worker_logger,
            )
            self.log_signal.emit(f"Tick导入完成: Written={tick_res.get('written', 0)}")
            
            # Step 2: Daily Download
            self.log_signal.emit("正在执行：日线数据下载...")
            
            years = set()
            curr = d_start
            while curr <= d_end:
                years.add(curr.year())
                curr = curr.addDays(365)
            years.add(d_end.year())
            
            instruments_path = ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                 self.log_signal.emit("跳过日线下载：未找到 instruments.parquet")
            else:
                total_daily_files = 0
                for y in sorted(years):
                    self.log_signal.emit(f"正在下载年份: {y}")
                    try:
                        daily_files = fetch_daily_year(
                            root=project_root, 
                            instruments_parquet=instruments_path, 
                            year=y,
                            logger=worker_logger
                        )
                        count = len(daily_files)
                        total_daily_files += count
                        self.log_signal.emit(f"年份 {y} 下载完成: {count} 文件")
                    except Exception as e:
                        self.log_signal.emit(f"年份 {y} 下载失败: {e}")
                self.log_signal.emit(f"日线下载完成，共更新 {total_daily_files} 个文件")
            
            self.log_signal.emit("=== 任务结束 ===")
            self.finished_signal.emit()

        except Exception as e:
            self.error_signal.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self, dataset_root: Path | None = None) -> None:
        super().__init__()
        self.setWindowTitle("数据查看器")

        self.settings = QSettings("gui_viewer", "viewer")
        self.ds_root = dataset_root or default_dataset_root()
        self.instruments = load_instruments(self.ds_root)

        self.current_ts_code: str | None = None
        self.current_daily_year: int | None = None
        self.current_tick_date: str | None = None
        self.current_tick_range: TickRange | None = None
        self._plot_df: pd.DataFrame | None = None
        self._last_hover_idx: int | None = None
        self._plot_x: np.ndarray | None = None
        self._plot_y: np.ndarray | None = None
        self._plot_x_secondary: np.ndarray | None = None
        self._plot_y_secondary: np.ndarray | None = None

        self._init_actions()
        self._init_ui()
        self._load_initial()

    def _init_actions(self) -> None:
        self.action_select_root = QAction("选择数据目录", self)
        self.action_select_root.setShortcut(QKeySequence("Ctrl+O"))
        self.action_select_root.triggered.connect(self._on_select_dataset_root)

        self.action_export_table = QAction("导出表格CSV", self)
        self.action_export_table.setShortcut(QKeySequence("Ctrl+S"))
        self.action_export_table.triggered.connect(self._on_export_table)

        self.action_export_chart = QAction("导出图表PNG", self)
        self.action_export_chart.triggered.connect(self._on_export_chart)

        self.action_import_ticks = QAction("导入7z Tick数据", self)
        self.action_import_ticks.setShortcut(QKeySequence("Ctrl+I"))
        self.action_import_ticks.triggered.connect(self._on_import_ticks)
        self.addAction(self.action_import_ticks)

    def _init_ui(self) -> None:
        import pyqtgraph as pg
        import pyqtgraph.exporters
        from pyqtgraph.graphicsItems.DateAxisItem import DateAxisItem

        self.pg = pg

        root = QWidget()
        layout = QHBoxLayout(root)
        layout.setContentsMargins(8, 8, 8, 8)

        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)

        # --- Data Management Section ---
        group_data = QGroupBox("数据管理")
        data_layout = QVBoxLayout(group_data)
        data_layout.setContentsMargins(4, 8, 4, 8)

        # 1. Symbols File
        data_layout.addWidget(QLabel("关注股票名单文件:"))
        row_sym = QHBoxLayout()
        self.edit_symbols_path = QLineEdit()
        self.edit_symbols_path.setPlaceholderText("dataset/meta/symbols.txt")
        self.btn_browse_sym = QPushButton("...")
        self.btn_browse_sym.setFixedWidth(30)
        self.btn_browse_sym.clicked.connect(self._on_browse_symbols)
        row_sym.addWidget(self.edit_symbols_path)
        row_sym.addWidget(self.btn_browse_sym)
        data_layout.addLayout(row_sym)

        # 2. Source Directory
        data_layout.addWidget(QLabel("Tick 7z 源目录:"))
        row_src = QHBoxLayout()
        self.edit_raw_dir = QLineEdit()
        self.edit_raw_dir.setPlaceholderText("rawData")
        self.btn_browse_raw = QPushButton("...")
        self.btn_browse_raw.setFixedWidth(30)
        self.btn_browse_raw.clicked.connect(self._on_browse_raw)
        row_src.addWidget(self.edit_raw_dir)
        row_src.addWidget(self.btn_browse_raw)
        data_layout.addLayout(row_src)

        # 3. Daily Data Range
        data_layout.addWidget(QLabel("日线下载范围:"))
        row_dates = QHBoxLayout()
        self.date_daily_start = QDateEdit()
        self.date_daily_start.setCalendarPopup(True)
        self.date_daily_start.setDisplayFormat("yyyy-MM-dd")
        self.date_daily_start.setDate(QDate.currentDate().addDays(-30))
        self.date_daily_end = QDateEdit()
        self.date_daily_end.setCalendarPopup(True)
        self.date_daily_end.setDisplayFormat("yyyy-MM-dd")
        self.date_daily_end.setDate(QDate.currentDate())
        row_dates.addWidget(self.date_daily_start)
        row_dates.addWidget(QLabel("-"))
        row_dates.addWidget(self.date_daily_end)
        data_layout.addLayout(row_dates)

        # 4. Clear Buttons
        row_clear = QHBoxLayout()
        self.btn_clear_daily = QPushButton("清除日线数据")
        self.btn_clear_daily.clicked.connect(self._on_clear_daily)
        self.btn_clear_ticks = QPushButton("清除Tick数据")
        self.btn_clear_ticks.clicked.connect(self._on_clear_ticks)
        row_clear.addWidget(self.btn_clear_daily)
        row_clear.addWidget(self.btn_clear_ticks)
        data_layout.addLayout(row_clear)

        # 5. Start Import & Progress
        self.btn_start_import = QPushButton("开始导入 / 下载")
        self.btn_start_import.setFixedHeight(36)
        self.btn_start_import.setStyleSheet("font-weight: bold; background-color: #e0e0e0;")
        self.btn_start_import.clicked.connect(self._on_start_import)
        data_layout.addWidget(self.btn_start_import)
        
        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p% - %v/%m")
        data_layout.addWidget(self.progress_bar)

        # ETA Label
        self.lbl_eta = QLabel("")
        self.lbl_eta.setStyleSheet("color: #666;")
        self.lbl_eta.setVisible(False)
        self.lbl_eta.setAlignment(Qt.AlignCenter)
        data_layout.addWidget(self.lbl_eta)

        left_layout.addWidget(group_data)

        # Export Buttons (Moved below Data Management but kept accessible)
        row_export = QHBoxLayout()
        self.btn_export_table = QPushButton("导出表格CSV")
        self.btn_export_table.clicked.connect(self._on_export_table)
        self.btn_export_chart = QPushButton("导出图表PNG")
        self.btn_export_chart.clicked.connect(self._on_export_chart)
        row_export.addWidget(self.btn_export_table)
        row_export.addWidget(self.btn_export_chart)
        left_layout.addLayout(row_export)

        sep_top = QFrame()
        sep_top.setFrameShape(QFrame.HLine)
        sep_top.setFrameShadow(QFrame.Sunken)
        left_layout.addWidget(sep_top)

        # --- Visualization Controls ---
        self.combo_symbol = QComboBox()
        for it in self.instruments:
            self.combo_symbol.addItem(f"{it.ts_code}  {it.name}", it.ts_code)
        self.combo_symbol.currentIndexChanged.connect(self._on_symbol_changed)
        left_layout.addWidget(QLabel("标的"))
        left_layout.addWidget(self.combo_symbol)

        self.combo_mode = QComboBox()
        self.combo_mode.addItem("日线", "daily")
        self.combo_mode.addItem("Tick", "tick")
        self.combo_mode.currentIndexChanged.connect(self._on_mode_changed)
        left_layout.addWidget(QLabel("数据类型"))
        left_layout.addWidget(self.combo_mode)

        self.combo_tick_mode = QComboBox()
        self.combo_tick_mode.addItem("单日", "single")
        self.combo_tick_mode.addItem("范围", "range")
        self.combo_tick_mode.currentIndexChanged.connect(self._on_tick_mode_changed)
        self.label_tick_mode = QLabel("Tick显示范围")
        left_layout.addWidget(self.label_tick_mode)
        left_layout.addWidget(self.combo_tick_mode)

        self.combo_year = QComboBox()
        self.combo_year.currentIndexChanged.connect(self._on_year_changed)
        self.label_year = QLabel("年份（日线）")
        left_layout.addWidget(self.label_year)
        left_layout.addWidget(self.combo_year)

        self.combo_tick_date = QComboBox()
        self.combo_tick_date.currentIndexChanged.connect(self._on_tick_date_changed)
        self.label_tick_date = QLabel("日期（Tick表格）")
        left_layout.addWidget(self.label_tick_date)
        left_layout.addWidget(self.combo_tick_date)

        self.label_tick_range = QLabel("Tick日期范围（最多30天）")
        left_layout.addWidget(self.label_tick_range)
        range_row = QHBoxLayout()
        self.date_start = QDateEdit()
        self.date_start.setCalendarPopup(True)
        self.date_start.dateChanged.connect(self._on_tick_range_changed)
        self.date_end = QDateEdit()
        self.date_end.setCalendarPopup(True)
        self.date_end.dateChanged.connect(self._on_tick_range_changed)
        range_row.addWidget(self.date_start)
        range_row.addWidget(self.date_end)
        left_layout.addLayout(range_row)

        self.combo_y = QComboBox()
        self.combo_y.currentIndexChanged.connect(self._refresh_chart)
        self.label_y = QLabel("主线Y列（左轴）")
        left_layout.addWidget(self.label_y)
        left_layout.addWidget(self.combo_y)

        self.combo_y_secondary = QComboBox()
        self.combo_y_secondary.currentIndexChanged.connect(self._refresh_chart)
        self.label_y_secondary = QLabel("次线Y列（右轴）")
        left_layout.addWidget(self.label_y_secondary)
        left_layout.addWidget(self.combo_y_secondary)
        self.chk_secondary_independent = QCheckBox("次线独立坐标")
        self.chk_secondary_independent.setChecked(True)
        self.chk_secondary_independent.stateChanged.connect(self._on_secondary_axis_mode_changed)
        left_layout.addWidget(self.chk_secondary_independent)

        self.chk_show_points = QCheckBox("显示圆点")
        self.chk_show_points.setChecked(False)
        self.chk_show_points.stateChanged.connect(self._refresh_chart)
        left_layout.addWidget(self.chk_show_points)

        self.chk_drop_zeros = QCheckBox("过滤价格为0的点")
        self.chk_drop_zeros.setChecked(True)
        self.chk_drop_zeros.stateChanged.connect(self._refresh_chart)
        left_layout.addWidget(self.chk_drop_zeros)

        self.chk_break_gaps = QCheckBox("断开长时间空档")
        self.chk_break_gaps.setChecked(True)
        self.chk_break_gaps.stateChanged.connect(self._refresh_chart)
        left_layout.addWidget(self.chk_break_gaps)

        self.chk_auto_y = QCheckBox("总是Y自适应")
        self.chk_auto_y.setChecked(True)
        self.chk_auto_y.stateChanged.connect(self._apply_auto_y_if_needed)
        left_layout.addWidget(self.chk_auto_y)

        self.btn_fit_all = QPushButton("适应全图")
        self.btn_fit_all.clicked.connect(self._on_reset_zoom)
        left_layout.addWidget(self.btn_fit_all)

        gap_row = QHBoxLayout()
        self.label_gap_minutes = QLabel("Tick空档(分钟)")
        self.spin_gap_minutes = QSpinBox()
        self.spin_gap_minutes.setRange(1, 120)
        self.spin_gap_minutes.setValue(5)
        self.spin_gap_minutes.valueChanged.connect(self._refresh_chart)
        gap_row.addWidget(self.label_gap_minutes)
        gap_row.addWidget(self.spin_gap_minutes)
        left_layout.addLayout(gap_row)

        gap_row2 = QHBoxLayout()
        self.label_gap_days = QLabel("日线空档(天)")
        self.spin_gap_days = QSpinBox()
        self.spin_gap_days.setRange(1, 30)
        self.spin_gap_days.setValue(3)
        self.spin_gap_days.valueChanged.connect(self._refresh_chart)
        gap_row2.addWidget(self.label_gap_days)
        gap_row2.addWidget(self.spin_gap_days)
        left_layout.addLayout(gap_row2)

        left_layout.addStretch(1)
        sep_bottom = QFrame()
        sep_bottom.setFrameShape(QFrame.HLine)
        sep_bottom.setFrameShadow(QFrame.Sunken)
        left_layout.addWidget(sep_bottom)
        self.label_message_stream = QLabel("消息流")
        left_layout.addWidget(self.label_message_stream)
        self.message_stream = QPlainTextEdit()
        self.message_stream.setReadOnly(True)
        self.message_stream.setMinimumHeight(160)
        left_layout.addWidget(self.message_stream)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.table = QTableView()
        self.table.setSortingEnabled(True)
        self.table_model = DataFrameTableModel(pd.DataFrame())
        self.table.setModel(self.table_model)

        class AxisViewBox(pg.ViewBox):
            def __init__(self, outer):
                super().__init__()
                self.outer = outer

            def wheelEvent(self, ev):
                delta = ev.angleDelta().y() if hasattr(ev, "angleDelta") else ev.delta()
                if delta == 0:
                    return
                scale = 0.9 if delta > 0 else 1.1
                if ev.modifiers() & Qt.ControlModifier:
                    self.scaleBy((1.0, scale))
                    self.outer.chk_auto_y.setChecked(False)
                else:
                    self.scaleBy((scale, 1.0))
                    self.outer._apply_auto_y_if_needed()
                ev.accept()

        axis = DateAxisItem(orientation="bottom")
        axis.setStyle(tickTextOffset=10, autoExpandTextSpace=True)
        self.view_box = AxisViewBox(self)
        self.plot = pg.PlotWidget(axisItems={"bottom": axis}, viewBox=self.view_box)
        self.plot.setBackground("w")
        self.plot.showGrid(x=True, y=True, alpha=0.2)
        self.plot.getPlotItem().getViewBox().setDefaultPadding(0.06)
        self.plot.setContentsMargins(12, 8, 12, 12)
        self.plot_item = self.plot.plot([], [], pen=pg.mkPen(color=(20, 20, 20), width=1))
        self.plot_item_secondary_shared = self.plot.plot([], [], pen=pg.mkPen(color=(219, 90, 25), width=1))
        self.plot_item_secondary = pg.PlotCurveItem([], [], pen=pg.mkPen(color=(219, 90, 25), width=1))
        self.right_view = pg.ViewBox()
        self.plot.plotItem.showAxis("right")
        self.plot.plotItem.scene().addItem(self.right_view)
        self.plot.plotItem.getAxis("right").linkToView(self.right_view)
        self.right_view.setXLink(self.plot.plotItem.getViewBox())
        self.right_view.addItem(self.plot_item_secondary)
        self.plot.plotItem.getAxis("right").setPen(pg.mkPen(color=(219, 90, 25), width=1))
        self.plot.plotItem.getAxis("right").setTextPen(pg.mkPen(color=(219, 90, 25)))
        self.plot.getAxis("bottom").setPen(pg.mkPen(color=(30, 30, 30), width=1))
        self.plot.getAxis("left").setPen(pg.mkPen(color=(30, 30, 30), width=1))
        self.plot.getAxis("bottom").setTextPen(pg.mkPen(color=(30, 30, 30)))
        self.plot.getAxis("left").setTextPen(pg.mkPen(color=(30, 30, 30)))
        self.plot.getAxis("left").setLabel("")
        self.plot.getAxis("right").setLabel("")
        self.plot.plotItem.hideAxis("right")
        self.hover_popup = QLabel(self)
        self.hover_popup.setWindowFlags(Qt.ToolTip | Qt.FramelessWindowHint)
        self.hover_popup.setWordWrap(True)
        self.hover_popup.setStyleSheet(
            "background-color: #ffffff; color: #111111; border: 1px solid #999999; padding: 6px; "
            "font-family: Consolas, 'Courier New', monospace;"
        )
        self.hover_popup.hide()
        class HoverScatter(pg.ScatterPlotItem):
            def __init__(self, outer, **opts):
                super().__init__(**opts)
                self.outer = outer
                self.setAcceptHoverEvents(True)

            def hoverEvent(self, ev):
                if ev is None:
                    return
                try:
                    pos = ev.pos()
                except Exception:
                    self.outer._hide_tooltip()
                    return
                idx = self.outer._nearest_point_index(pos, radius_px=10)
                if idx is not None:
                    self.outer._show_tooltip(idx)
                else:
                    self.outer._hide_tooltip()
                if ev is not None and hasattr(ev, "accept"):
                    ev.accept()

        self.scatter = HoverScatter(
            self,
            size=9,
            pen=pg.mkPen(color=(20, 20, 20), width=1),
            brush=pg.mkBrush(79, 125, 255, 180),
        )
        self.plot.addItem(self.scatter)
        self.view_box.sigXRangeChanged.connect(self._on_view_xrange_changed)
        self.plot.plotItem.getViewBox().sigResized.connect(self._update_secondary_view_geometry)

        vsplit = QSplitter(Qt.Vertical)
        vsplit.addWidget(self.table)
        vsplit.addWidget(self.plot)
        vsplit.setSizes([600, 300])
        right_layout.addWidget(vsplit)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setSizes([360, 1000])

        self.setCentralWidget(root)

    def _append_message(self, text: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.message_stream.appendPlainText(f"[{ts}] {text}")
        sb = self.message_stream.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _load_initial(self) -> None:
        # Initialize default paths from settings or defaults
        last_sym = self.settings.value("import/symbols_path", "")
        if last_sym and Path(last_sym).exists():
            self.edit_symbols_path.setText(str(last_sym))
        else:
            default_sym = self._default_symbols_file()
            if default_sym.exists():
                self.edit_symbols_path.setText(str(default_sym))
        
        last_raw = self.settings.value("import/raw_dir", "")
        if last_raw and Path(last_raw).exists():
             self.edit_raw_dir.setText(str(last_raw))
        else:
            project_root = self._project_root()
            default_raw = project_root / "rawData"
            if default_raw.exists():
                self.edit_raw_dir.setText(str(default_raw))

        # Restore daily date range
        d_start_str = self.settings.value("import/daily_start", "")
        if d_start_str:
            self.date_daily_start.setDate(QDate.fromString(d_start_str, "yyyy-MM-dd"))
        d_end_str = self.settings.value("import/daily_end", "")
        if d_end_str:
            self.date_daily_end.setDate(QDate.fromString(d_end_str, "yyyy-MM-dd"))

        if not self.instruments:
            QMessageBox.critical(self, "错误", "未找到 instruments.parquet")
            return
        self.current_ts_code = self.instruments[0].ts_code
        self._reload_symbol_dependent()
        self._apply_mode_visibility()
        self._refresh_table()
        self._refresh_chart()
        self._load_settings()

    def _reload_symbol_dependent(self) -> None:
        if not self.current_ts_code:
            return
        years = available_daily_years(self.ds_root, self.current_ts_code)
        self.combo_year.blockSignals(True)
        self.combo_year.clear()
        for y in years:
            self.combo_year.addItem(str(y), y)
        self.combo_year.blockSignals(False)
        self.current_daily_year = years[-1] if years else None
        if self.current_daily_year is not None and years:
            self.combo_year.setCurrentIndex(len(years) - 1)

        dates = available_tick_dates(self.ds_root, self.current_ts_code)
        self.combo_tick_date.blockSignals(True)
        self.combo_tick_date.clear()
        for d in dates:
            self.combo_tick_date.addItem(d, d)
        self.combo_tick_date.blockSignals(False)
        self.current_tick_date = dates[0] if dates else None
        if self.current_tick_date is not None and dates:
            self.combo_tick_date.setCurrentIndex(0)

        if dates:
            d0 = parse_yyyymmdd(dates[0])
            d1 = parse_yyyymmdd(dates[-1])
            self.date_start.setDate(QDate(d0.year, d0.month, d0.day))
            self.date_end.setDate(QDate(d1.year, d1.month, d1.day))
            self.current_tick_range = TickRange(start=dates[0], end=dates[-1])
        else:
            self.current_tick_range = None

    def _apply_mode_visibility(self) -> None:
        mode = self.combo_mode.currentData()
        is_daily = mode == "daily"
        is_tick = mode == "tick"
        is_tick_range = is_tick and self.combo_tick_mode.currentData() == "range"
        is_tick_single = is_tick and self.combo_tick_mode.currentData() == "single"
        self.label_year.setVisible(is_daily)
        self.combo_year.setVisible(is_daily)
        self.label_tick_mode.setVisible(is_tick)
        self.combo_tick_mode.setVisible(is_tick)
        self.label_tick_date.setVisible(is_tick_single)
        self.combo_tick_date.setVisible(is_tick_single)
        self.label_tick_range.setVisible(is_tick_range)
        self.date_start.setVisible(is_tick_range)
        self.date_end.setVisible(is_tick_range)
        self.label_gap_minutes.setVisible(is_tick)
        self.spin_gap_minutes.setVisible(is_tick)
        self.label_gap_days.setVisible(is_daily)
        self.spin_gap_days.setVisible(is_daily)

    def _on_browse_symbols(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "选择证券列表文件", self.edit_symbols_path.text(), "Text Files (*.txt);;All Files (*)"
        )
        if path:
            self.edit_symbols_path.setText(path)

    def _on_browse_raw(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "选择Tick 7z源目录", self.edit_raw_dir.text())
        if path:
            self.edit_raw_dir.setText(path)

    def _on_clear_daily(self) -> None:
        daily_dir = self.ds_root / "daily"
        if not daily_dir.exists():
            self._append_message("清除日线：目录不存在，无需清除。")
            return
        ret = QMessageBox.question(
            self, "确认清除", "确定要删除所有已导入的日线数据吗？\n(dataset/daily/*)",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if ret != QMessageBox.Yes:
            return
        try:
            count = 0
            for p in daily_dir.iterdir():
                if p.is_file() or p.is_dir():
                    if p.is_dir():
                        shutil.rmtree(p)
                    else:
                        p.unlink()
                    count += 1
            self._append_message(f"已清除日线数据，共删除 {count} 个项。")
            self._reload_symbol_dependent()
            self._refresh_table()
            self._refresh_chart()
        except Exception as e:
            QMessageBox.critical(self, "清除失败", str(e))
            self._append_message(f"清除日线失败：{e}")

    def _on_clear_ticks(self) -> None:
        tick_dir = self.ds_root / "ticks"
        if not tick_dir.exists():
            self._append_message("清除Tick：目录不存在，无需清除。")
            return
        ret = QMessageBox.question(
            self, "确认清除", "确定要删除所有已导入的Tick数据吗？\n(dataset/ticks/*)",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if ret != QMessageBox.Yes:
            return
        try:
            count = 0
            for p in tick_dir.iterdir():
                if p.is_file() or p.is_dir():
                    if p.is_dir():
                        shutil.rmtree(p)
                    else:
                        p.unlink()
                    count += 1
            self._append_message(f"已清除Tick数据，共删除 {count} 个项。")
            self._reload_symbol_dependent()
            self._refresh_table()
            self._refresh_chart()
        except Exception as e:
            QMessageBox.critical(self, "清除失败", str(e))
            self._append_message(f"清除Tick失败：{e}")

    def _on_start_import(self) -> None:
        # 1. Gather Inputs
        symbols_path_str = self.edit_symbols_path.text().strip()
        raw_dir_str = self.edit_raw_dir.text().strip()
        
        if not symbols_path_str or not Path(symbols_path_str).exists():
            QMessageBox.warning(self, "参数错误", "请指定有效的证券列表文件 (symbols.txt)")
            return
        if not raw_dir_str or not Path(raw_dir_str).exists():
            QMessageBox.warning(self, "参数错误", "请指定有效的Tick源目录 (rawData)")
            return

        symbols_path = Path(symbols_path_str)
        raw_dir = Path(raw_dir_str)
        
        # Save inputs to settings
        self.settings.setValue("import/symbols_path", symbols_path_str)
        self.settings.setValue("import/raw_dir", raw_dir_str)
        
        # Date Range for Daily
        d_start = self.date_daily_start.date()
        d_end = self.date_daily_end.date()
        if d_start > d_end:
            d_start, d_end = d_end, d_start
        
        # Save dates to settings
        self.settings.setValue("import/daily_start", d_start.toString("yyyy-MM-dd"))
        self.settings.setValue("import/daily_end", d_end.toString("yyyy-MM-dd"))
        
        # Confirm
        msg = (
            f"准备开始数据导入任务：\n\n"
            f"1. 导入Tick数据 (Source: {raw_dir.name})\n"
            f"2. 下载日线数据 ({d_start.toString('yyyy-MM-dd')} - {d_end.toString('yyyy-MM-dd')})\n\n"
            "任务可能耗时较长，请保持程序运行。\n是否开始？"
        )
        if QMessageBox.question(self, "开始任务", msg, QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return

        self.setCursor(Qt.WaitCursor)
        self.btn_start_import.setEnabled(False)
        self._append_message("=== 任务开始 ===")
        
        try:
            # Step 1: Tick Import
            self._append_message("正在执行：Tick数据导入...")
            QApplication.processEvents()
            
            project_root = self._project_root()
            tmp_dir = raw_dir / "_tmp_extract_gui"
            instruments_path = self.ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                instruments_path = None
            
            # Use direct callback to append messages and process events immediately
            def tick_logger(msg: str):
                self._append_message(msg)
                QApplication.processEvents()

            tick_res = import_from_raw(
                root=project_root,
                rawdir=raw_dir,
                universe_file=symbols_path,
                dates=None,
                tmp_dir=tmp_dir,
                instruments_path=instruments_path,
                logger=tick_logger,
            )
            
            self._append_message(f"Tick导入完成: Written={tick_res.get('written', 0)}")

            # Step 2: Daily Download
            self._append_message("正在执行：日线数据下载...")
            QApplication.processEvents()
            
            years = set()
            curr = d_start
            while curr <= d_end:
                years.add(curr.year())
                curr = curr.addDays(365)
            years.add(d_end.year())
            
            instruments_path = self.ds_root / "meta" / "instruments.parquet"
            if not instruments_path.exists():
                 self._append_message("跳过日线下载：未找到 instruments.parquet")
            else:
                total_daily_files = 0
                
                def daily_logger(msg: str):
                    self._append_message(msg)
                    QApplication.processEvents()

                for y in sorted(years):
                    self._append_message(f"正在下载年份: {y}")
                    QApplication.processEvents()
                    try:
                        daily_files = fetch_daily_year(
                            root=project_root, 
                            instruments_parquet=instruments_path, 
                            year=y,
                            logger=daily_logger
                        )
                        count = len(daily_files)
                        total_daily_files += count
                        self._append_message(f"年份 {y} 下载完成: {count} 文件")
                    except Exception as e:
                        self._append_message(f"年份 {y} 下载失败: {e}")
                    QApplication.processEvents()
                self._append_message(f"日线下载完成，共更新 {total_daily_files} 个文件")

        except Exception as e:
            QMessageBox.critical(self, "任务出错", str(e))
            self._append_message(f"任务异常终止: {e}")
        finally:
            self.unsetCursor()
            self.btn_start_import.setEnabled(True)
            self._append_message("=== 任务结束 ===")
            self._reload_symbol_dependent()
            self._refresh_table()
            self._refresh_chart()

    def _on_select_dataset_root(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "选择dataset目录", str(self.ds_root))
        if not d:
            return
        p = Path(d)
        if p.name != "dataset":
            p = p / "dataset"
        if not (p / "meta" / "instruments.parquet").exists():
            QMessageBox.warning(self, "提示", "未在该目录找到 dataset/meta/instruments.parquet")
            self._append_message(f"选择数据目录失败：未找到 instruments.parquet -> {p}")
            return
        self.ds_root = p
        self._append_message(f"数据目录已切换：{self.ds_root}")
        self.instruments = load_instruments(self.ds_root)
        self.combo_symbol.blockSignals(True)
        self.combo_symbol.clear()
        for it in self.instruments:
            self.combo_symbol.addItem(f"{it.ts_code}  {it.name}", it.ts_code)
        self.combo_symbol.blockSignals(False)
        self.current_ts_code = self.instruments[0].ts_code if self.instruments else None
        self._reload_symbol_dependent()
        self._refresh_table()
        self._refresh_chart()

    def _project_root(self) -> Path:
        if self.ds_root.name == "dataset":
            return self.ds_root.parent
        return self.ds_root

    def _default_symbols_file(self) -> Path:
        p1 = self.ds_root / "meta" / "symbols.txt"
        if p1.exists():
            return p1
        return self._project_root() / "dataset" / "meta" / "symbols.txt"

    def _on_import_ticks(self) -> None:
        self._on_start_import()

    def _on_secondary_axis_mode_changed(self) -> None:
        self._refresh_chart()
        self._apply_auto_y_if_needed()

    def _on_symbol_changed(self) -> None:
        ts_code = self.combo_symbol.currentData()
        self.current_ts_code = ts_code
        self._reload_symbol_dependent()
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _on_mode_changed(self) -> None:
        self._apply_mode_visibility()
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _on_tick_mode_changed(self) -> None:
        self._apply_mode_visibility()
        self._sync_tick_range_from_ui()
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _on_year_changed(self) -> None:
        y = self.combo_year.currentData()
        self.current_daily_year = int(y) if y is not None else None
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _on_tick_date_changed(self) -> None:
        d = self.combo_tick_date.currentData()
        self.current_tick_date = str(d) if d else None
        if self.current_tick_date:
            self.current_tick_range = TickRange(start=self.current_tick_date, end=self.current_tick_date)
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _on_tick_range_changed(self) -> None:
        if self.combo_mode.currentData() != "tick":
            return
        if self.combo_tick_mode.currentData() != "range":
            return
        self._sync_tick_range_from_ui()
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _sync_tick_range_from_ui(self) -> None:
        s = self.date_start.date().toPyDate()
        e = self.date_end.date().toPyDate()
        if s > e:
            s, e = e, s
        if (e - s).days > 29:
            e = s + timedelta(days=29)
            self.date_end.blockSignals(True)
            self.date_end.setDate(QDate(e.year, e.month, e.day))
            self.date_end.blockSignals(False)
        self.current_tick_range = TickRange(start=format_yyyymmdd(s), end=format_yyyymmdd(e))

    def _refresh_table(self) -> None:
        mode = self.combo_mode.currentData()
        if not self.current_ts_code:
            self.table_model.set_dataframe(pd.DataFrame())
            return
        try:
            if mode == "daily":
                if self.current_daily_year is None:
                    self.table_model.set_dataframe(pd.DataFrame())
                    return
                df = load_daily(self.ds_root, self.current_ts_code, self.current_daily_year)
                self.table_model.set_dataframe(df)
                self._rebuild_y_columns(df, default="close")
            else:
                if self.combo_tick_mode.currentData() == "range":
                    if not self.current_tick_range:
                        self.table_model.set_dataframe(pd.DataFrame())
                        return
                    df = load_tick_range_full(self.ds_root, self.current_ts_code, self.current_tick_range.start, self.current_tick_range.end)
                    self.table_model.set_dataframe(df)
                    self._rebuild_y_columns(df, default="last_price")
                    return
                if not self.current_tick_date:
                    self.table_model.set_dataframe(pd.DataFrame())
                    return
                df = load_tick_day(self.ds_root, self.current_ts_code, self.current_tick_date)
                self.table_model.set_dataframe(df)
                self._rebuild_y_columns(df, default="last_price")
        except Exception as e:
            QMessageBox.critical(self, "加载失败", str(e))
            self.table_model.set_dataframe(pd.DataFrame())

    def _rebuild_y_columns(self, df: pd.DataFrame, default: str) -> None:
        prev_primary = self.combo_y.currentData()
        prev_secondary = self.combo_y_secondary.currentData()
        cols = []
        for c in df.columns:
            if c in ("datetime", "trade_date"):
                continue
            if pd.api.types.is_numeric_dtype(df[c]):
                cols.append(c)
        self.combo_y.blockSignals(True)
        self.combo_y.clear()
        for c in cols:
            self.combo_y.addItem(c, c)
        self.combo_y.blockSignals(False)
        if prev_primary in cols:
            self.combo_y.setCurrentIndex(self.combo_y.findData(prev_primary))
        elif default in cols:
            self.combo_y.setCurrentText(default)
        elif cols:
            self.combo_y.setCurrentIndex(0)
        self.combo_y_secondary.blockSignals(True)
        self.combo_y_secondary.clear()
        self.combo_y_secondary.addItem("无", "__none__")
        for c in cols:
            self.combo_y_secondary.addItem(c, c)
        self.combo_y_secondary.blockSignals(False)
        if prev_secondary in cols:
            self.combo_y_secondary.setCurrentIndex(self.combo_y_secondary.findData(prev_secondary))
        else:
            self.combo_y_secondary.setCurrentIndex(0)

    def _clear_plot(self) -> None:
        self.plot_item.setData([], [])
        self.plot_item_secondary_shared.setData([], [])
        self.plot_item_secondary.setData([], [])
        self.scatter.setData([])
        self._plot_x = None
        self._plot_y = None
        self._plot_x_secondary = None
        self._plot_y_secondary = None
        self.plot.getAxis("left").setLabel("")
        self.plot.getAxis("right").setLabel("")
        self.plot.plotItem.hideAxis("right")

    def _update_secondary_view_geometry(self) -> None:
        try:
            vb = self.plot.plotItem.getViewBox()
            self.right_view.setGeometry(vb.sceneBoundingRect())
            self.right_view.linkedViewChanged(vb, self.right_view.XAxis)
        except Exception:
            return

    def _apply_secondary_axis_mode(self, y2_name: str | None) -> None:
        independent = self.chk_secondary_independent.isChecked() and y2_name is not None
        if independent:
            self.plot.plotItem.showAxis("right")
            self.plot.getAxis("right").setLabel(y2_name)
        else:
            self.plot.plotItem.hideAxis("right")
            self.plot.getAxis("right").setLabel("")

    def _refresh_chart(self) -> None:
        mode = self.combo_mode.currentData()
        if not self.current_ts_code:
            self._clear_plot()
            return
        y_col = str(self.combo_y.currentData() or "")
        y2_raw = self.combo_y_secondary.currentData()
        y_col_secondary = None if y2_raw in (None, "__none__", y_col) else str(y2_raw)
        if not y_col:
            self._clear_plot()
            return
        try:
            if mode == "daily":
                if self.current_daily_year is None:
                    self._clear_plot()
                    return
                df = load_daily(self.ds_root, self.current_ts_code, self.current_daily_year)
                if "trade_date" not in df.columns or y_col not in df.columns:
                    self._clear_plot()
                    return
                dt = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
                df = df.assign(_dt=dt).dropna(subset=["_dt"])
                x = _to_epoch_seconds(df["_dt"])
                y = pd.to_numeric(df[y_col], errors="coerce").to_numpy(dtype=float)
                y_points = self._apply_plot_filters(x, y, mode="daily")
                idx = _downsample_indices(len(x), max_points=10000, extra=None)
                x_s = x[idx]
                y_points_s = y_points[idx]
                x_line, y_line = self._apply_line_breaks(x_s, y_points_s, mode="daily")
                self.plot_item.setData(x_line, y_line)
                self.plot.getAxis("left").setLabel(y_col)
                self.plot_item_secondary_shared.setData([], [])
                self.plot_item_secondary.setData([], [])
                self._plot_x_secondary = None
                self._plot_y_secondary = None
                if y_col_secondary and y_col_secondary in df.columns:
                    y2 = pd.to_numeric(df[y_col_secondary], errors="coerce").to_numpy(dtype=float)
                    y2_points = self._apply_plot_filters(x, y2, mode="daily")
                    y2_points_s = y2_points[idx]
                    x2_line, y2_line = self._apply_line_breaks(x_s, y2_points_s, mode="daily")
                    if self.chk_secondary_independent.isChecked():
                        self.plot_item_secondary.setData(x2_line, y2_line)
                        self.plot_item_secondary_shared.setData([], [])
                    else:
                        self.plot_item_secondary_shared.setData(x2_line, y2_line)
                        self.plot_item_secondary.setData([], [])
                    self._plot_x_secondary = x_s
                    self._plot_y_secondary = y2_points_s
                self._apply_secondary_axis_mode(y_col_secondary)
                self._update_scatter(df.iloc[idx].copy(), x_s, y_points_s)
                self._plot_x = x_s
                self._plot_y = y_points_s
                self._apply_auto_y_if_needed()
            else:
                if self.combo_tick_mode.currentData() == "single" and self.current_tick_date:
                    self.current_tick_range = TickRange(start=self.current_tick_date, end=self.current_tick_date)
                if not self.current_tick_range:
                    self._clear_plot()
                    return
                need_full = self.chk_show_points.isChecked() or bool(y_col_secondary)
                if need_full:
                    df = load_tick_range_full(
                        self.ds_root,
                        self.current_ts_code,
                        self.current_tick_range.start,
                        self.current_tick_range.end,
                    )
                else:
                    df = load_tick_series(
                        self.ds_root,
                        self.current_ts_code,
                        self.current_tick_range.start,
                        self.current_tick_range.end,
                        str(y_col),
                    )
                if df.empty or "datetime" not in df.columns or y_col not in df.columns:
                    self._clear_plot()
                    return
                x = _to_epoch_seconds(df["datetime"])
                y = pd.to_numeric(df[y_col], errors="coerce").to_numpy(dtype=float)
                y_points = self._apply_plot_filters(x, y, mode="tick")
                idx = _downsample_indices(len(x), max_points=20000, extra=None)
                x_s = x[idx]
                y_points_s = y_points[idx]
                x_line, y_line = self._apply_line_breaks(x_s, y_points_s, mode="tick")
                self.plot_item.setData(x_line, y_line)
                self.plot.getAxis("left").setLabel(y_col)
                self.plot_item_secondary_shared.setData([], [])
                self.plot_item_secondary.setData([], [])
                self._plot_x_secondary = None
                self._plot_y_secondary = None
                if y_col_secondary and y_col_secondary in df.columns:
                    y2 = pd.to_numeric(df[y_col_secondary], errors="coerce").to_numpy(dtype=float)
                    y2_points = self._apply_plot_filters(x, y2, mode="tick")
                    y2_points_s = y2_points[idx]
                    x2_line, y2_line = self._apply_line_breaks(x_s, y2_points_s, mode="tick")
                    if self.chk_secondary_independent.isChecked():
                        self.plot_item_secondary.setData(x2_line, y2_line)
                        self.plot_item_secondary_shared.setData([], [])
                    else:
                        self.plot_item_secondary_shared.setData(x2_line, y2_line)
                        self.plot_item_secondary.setData([], [])
                    self._plot_x_secondary = x_s
                    self._plot_y_secondary = y2_points_s
                self._apply_secondary_axis_mode(y_col_secondary)
                self._update_scatter(df.iloc[idx].copy(), x_s, y_points_s)
                self._plot_x = x_s
                self._plot_y = y_points_s
                self._apply_auto_y_if_needed()
            self._update_secondary_view_geometry()
        except Exception as e:
            QMessageBox.critical(self, "绘图失败", str(e))
            self._clear_plot()

    def _apply_plot_filters(self, x: np.ndarray, y: np.ndarray, mode: str) -> np.ndarray:
        if self.chk_drop_zeros.isChecked():
            y = y.astype(float)
            y[y <= 0] = np.nan
        return y

    def _apply_line_breaks(self, x: np.ndarray, y: np.ndarray, mode: str) -> tuple[np.ndarray, np.ndarray]:
        if not self.chk_break_gaps.isChecked() or len(x) <= 1:
            return x, y
        gaps = np.diff(x)
        if mode == "tick":
            threshold = self.spin_gap_minutes.value() * 60
        else:
            threshold = self.spin_gap_days.value() * 86400
        break_idx = np.where(gaps > threshold)[0]
        if len(break_idx) == 0:
            return x, y
        break_set = set(break_idx.tolist())
        x_line = []
        y_line = []
        for i in range(len(x)):
            x_line.append(x[i])
            y_line.append(y[i])
            if i in break_set:
                x_line.append(np.nan)
                y_line.append(np.nan)
        return np.asarray(x_line, dtype=float), np.asarray(y_line, dtype=float)

    def _update_scatter(self, df: pd.DataFrame, x: np.ndarray, y: np.ndarray) -> None:
        if not self.chk_show_points.isChecked():
            self.scatter.setData([])
            self._plot_df = None
            self._last_hover_idx = None
            return
        mask = ~np.isnan(y)
        if not np.any(mask):
            self.scatter.setData([])
            self._plot_df = None
            self._last_hover_idx = None
            return
        df = df.reset_index(drop=True)
        df = df.loc[mask].reset_index(drop=True)
        x = x[mask]
        y = y[mask]
        self._plot_df = df
        spots = [{"pos": (float(x[i]), float(y[i])), "data": i} for i in range(len(df))]
        self.scatter.setData(spots)

    def _nearest_point_index(self, pos, radius_px: float = 10) -> int | None:
        if self._plot_df is None or self._plot_x is None or self._plot_y is None:
            return None
        x = self._plot_x
        y = self._plot_y
        if len(x) == 0:
            return None
        try:
            px = self.plot.getViewBox().viewPixelSize()
            dx = max(px[0] * radius_px, 1e-12)
            dy = max(px[1] * radius_px, 1e-12)
        except Exception:
            return None
        x0 = pos.x()
        y0 = pos.y()
        mask = np.isfinite(y)
        if not np.any(mask):
            return None
        x_m = x[mask]
        y_m = y[mask]
        dxn = (x_m - x0) / dx
        dyn = (y_m - y0) / dy
        dist2 = dxn * dxn + dyn * dyn
        i = int(np.argmin(dist2))
        if dist2[i] <= 1.0:
            idxs = np.flatnonzero(mask)
            return int(idxs[i])
        return None

    def _show_tooltip(self, idx: int) -> None:
        if self._plot_df is None:
            return
        try:
            if self._last_hover_idx != idx:
                self._last_hover_idx = idx
            row = self._plot_df.iloc[idx].to_dict()
            text = self._format_tooltip(row, mode=self.combo_mode.currentData())
            self.hover_popup.setText(text)
            self.hover_popup.adjustSize()
            pos = QCursor.pos()
            self._position_popup(pos, self.hover_popup.sizeHint())
            self.hover_popup.show()
        except Exception:
            return

    def _hide_tooltip(self) -> None:
        try:
            self.hover_popup.hide()
        except Exception:
            return

    def _format_tooltip(self, row: dict, mode: str | None) -> str:
        time_text = None
        if "datetime" in row:
            try:
                time_text = pd.to_datetime(row["datetime"]).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                time_text = str(row["datetime"])
        if "trade_date" in row:
            try:
                time_text = pd.to_datetime(str(row["trade_date"]), format="%Y%m%d").strftime("%Y-%m-%d")
            except Exception:
                time_text = str(row["trade_date"])
        if mode == "daily":
            keys = [
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "volume",
                "turnover",
                "adj_factor",
            ]
        else:
            keys = [
                "datetime",
                "last_price",
                "open_price",
                "high_price",
                "low_price",
                "pre_close",
                "volume",
                "turnover",
                "cum_volume",
                "cum_turnover",
                "trades_count",
                "bs_flag",
                "trade_flag",
                "iopv",
            ]
        keys = [k for k in keys if k in row]
        extra = [k for k in sorted(row.keys()) if k not in keys]
        keys = keys + extra
        lines = []
        if time_text is not None:
            lines.append(f"时间={time_text}")
        used_keys = set()
        other_lines = []
        if mode != "daily":
            ob_lines, ob_keys = self._format_orderbook(row)
            lines.extend(ob_lines)
            used_keys |= ob_keys
        for k in keys:
            if k in used_keys:
                continue
            v = row[k]
            if isinstance(v, float):
                other_lines.append(f"{k}={v:.6g}")
            else:
                other_lines.append(f"{k}={v}")
        lines.extend(self._two_column_lines(other_lines))
        return "\n".join(lines)

    def _format_orderbook(self, row: dict) -> tuple[list[str], set[str]]:
        lines: list[str] = []
        used: set[str] = set()
        ask_pairs = [(f"ask_price_{i}", f"ask_volume_{i}") for i in range(10, 0, -1)]
        bid_pairs = [(f"bid_price_{i}", f"bid_volume_{i}") for i in range(1, 11)]
        if not any(k in row for pair in ask_pairs + bid_pairs for k in pair):
            return lines, used
        for p_key, v_key in ask_pairs:
            used.update([p_key, v_key])
            p = row.get(p_key)
            v = row.get(v_key)
            lines.append(f"{p_key}={p}, {v_key}={v}")
        for p_key, v_key in bid_pairs:
            used.update([p_key, v_key])
            p = row.get(p_key)
            v = row.get(v_key)
            lines.append(f"{p_key}={p}, {v_key}={v}")
        return lines, used

    def _two_column_lines(self, lines: list[str]) -> list[str]:
        if len(lines) <= 1:
            return lines
        max_len = max(len(s) for s in lines)
        col_width = max_len + 4
        merged = []
        i = 0
        while i < len(lines):
            left = lines[i]
            right = lines[i + 1] if i + 1 < len(lines) else ""
            merged.append(f"{left:<{col_width}}{right}")
            i += 2
        return merged

    def _position_popup(self, cursor_pos, popup_size) -> None:
        geo = self.frameGeometry()
        if geo.isNull():
            self.hover_popup.move(cursor_pos.x() + 12, cursor_pos.y() + 12)
            return
        w = popup_size.width()
        h = popup_size.height()
        padding = 8
        offsets = [
            (12, 12),
            (-w - 12, 12),
            (12, -h - 12),
            (-w - 12, -h - 12),
        ]
        for dx, dy in offsets:
            x = cursor_pos.x() + dx
            y = cursor_pos.y() + dy
            if geo.left() + padding <= x <= geo.right() - w - padding and geo.top() + padding <= y <= geo.bottom() - h - padding:
                self.hover_popup.move(x, y)
                return
        x = min(max(cursor_pos.x() + 12, geo.left() + padding), geo.right() - w - padding)
        y = min(max(cursor_pos.y() + 12, geo.top() + padding), geo.bottom() - h - padding)
        self.hover_popup.move(x, y)

    def _on_reset_zoom(self) -> None:
        try:
            self.plot.getViewBox().autoRange(padding=0.06)
            self._apply_auto_y_if_needed()
        except Exception:
            return

    def _on_view_xrange_changed(self, *_args) -> None:
        self._apply_auto_y_if_needed()

    def _apply_auto_y_if_needed(self, *_args) -> None:
        if not self.chk_auto_y.isChecked():
            return
        if self._plot_x is None or self._plot_y is None:
            return
        x = self._plot_x
        y = self._plot_y
        if len(x) == 0:
            return
        x_min, x_max = self.plot.getViewBox().viewRange()[0]
        mask = (x >= x_min) & (x <= x_max) & np.isfinite(y)
        if not np.any(mask):
            return
        y_sel = y[mask]
        ymin = float(np.nanmin(y_sel))
        ymax = float(np.nanmax(y_sel))
        if not np.isfinite(ymin) or not np.isfinite(ymax):
            return
        span = ymax - ymin
        pad = span * 0.05 if span > 0 else max(1e-6, abs(ymin) * 0.01)
        if self._plot_x_secondary is not None and self._plot_y_secondary is not None and not self.chk_secondary_independent.isChecked():
            x2 = self._plot_x_secondary
            y2 = self._plot_y_secondary
            mask2 = (x2 >= x_min) & (x2 <= x_max) & np.isfinite(y2)
            if np.any(mask2):
                y2_sel = y2[mask2]
                ymin = min(ymin, float(np.nanmin(y2_sel)))
                ymax = max(ymax, float(np.nanmax(y2_sel)))
                span = ymax - ymin
                pad = span * 0.05 if span > 0 else max(1e-6, abs(ymin) * 0.01)
        self.plot.getViewBox().setYRange(ymin - pad, ymax + pad, padding=0)
        if self._plot_x_secondary is None or self._plot_y_secondary is None or not self.chk_secondary_independent.isChecked():
            return
        x2 = self._plot_x_secondary
        y2 = self._plot_y_secondary
        mask2 = (x2 >= x_min) & (x2 <= x_max) & np.isfinite(y2)
        if not np.any(mask2):
            return
        y2_sel = y2[mask2]
        y2_min = float(np.nanmin(y2_sel))
        y2_max = float(np.nanmax(y2_sel))
        if not np.isfinite(y2_min) or not np.isfinite(y2_max):
            return
        span2 = y2_max - y2_min
        pad2 = span2 * 0.05 if span2 > 0 else max(1e-6, abs(y2_min) * 0.01)
        self.right_view.setYRange(y2_min - pad2, y2_max + pad2, padding=0)

    def _load_settings(self) -> None:
        ts_code = self.settings.value("symbol_ts_code")
        if ts_code:
            idx = self.combo_symbol.findData(ts_code)
            if idx >= 0:
                self.combo_symbol.setCurrentIndex(idx)
        mode = self.settings.value("mode")
        if mode:
            idx = self.combo_mode.findData(mode)
            if idx >= 0:
                self.combo_mode.setCurrentIndex(idx)
        tick_mode = self.settings.value("tick_mode")
        if tick_mode:
            idx = self.combo_tick_mode.findData(tick_mode)
            if idx >= 0:
                self.combo_tick_mode.setCurrentIndex(idx)
        year = self.settings.value("daily_year")
        if year:
            idx = self.combo_year.findData(int(year))
            if idx >= 0:
                self.combo_year.setCurrentIndex(idx)
        tick_date = self.settings.value("tick_date")
        if tick_date:
            idx = self.combo_tick_date.findData(tick_date)
            if idx >= 0:
                self.combo_tick_date.setCurrentIndex(idx)
        start = self.settings.value("tick_start")
        end = self.settings.value("tick_end")
        if start and end:
            try:
                d0 = parse_yyyymmdd(str(start))
                d1 = parse_yyyymmdd(str(end))
                self.date_start.setDate(QDate(d0.year, d0.month, d0.day))
                self.date_end.setDate(QDate(d1.year, d1.month, d1.day))
            except Exception:
                pass
        y_col = self.settings.value("y_col")
        if y_col:
            idx = self.combo_y.findData(y_col)
            if idx >= 0:
                self.combo_y.setCurrentIndex(idx)
        y_col_secondary = self.settings.value("y_col_secondary")
        if y_col_secondary:
            idx = self.combo_y_secondary.findData(y_col_secondary)
            if idx >= 0:
                self.combo_y_secondary.setCurrentIndex(idx)
        self.chk_show_points.setChecked(self.settings.value("show_points", False, type=bool))
        self.chk_drop_zeros.setChecked(self.settings.value("drop_zeros", True, type=bool))
        self.chk_break_gaps.setChecked(self.settings.value("break_gaps", True, type=bool))
        self.chk_auto_y.setChecked(self.settings.value("auto_y", True, type=bool))
        self.chk_secondary_independent.setChecked(self.settings.value("secondary_independent_axis", True, type=bool))
        self.spin_gap_minutes.setValue(int(self.settings.value("gap_minutes", 5)))
        self.spin_gap_days.setValue(int(self.settings.value("gap_days", 3)))
        self._apply_mode_visibility()
        self._refresh_table()
        self._refresh_chart()
        self._on_reset_zoom()

    def _save_settings(self) -> None:
        self.settings.setValue("symbol_ts_code", self.combo_symbol.currentData())
        self.settings.setValue("mode", self.combo_mode.currentData())
        self.settings.setValue("tick_mode", self.combo_tick_mode.currentData())
        self.settings.setValue("daily_year", self.combo_year.currentData())
        self.settings.setValue("tick_date", self.combo_tick_date.currentData())
        self.settings.setValue("tick_start", format_yyyymmdd(self.date_start.date().toPyDate()))
        self.settings.setValue("tick_end", format_yyyymmdd(self.date_end.date().toPyDate()))
        self.settings.setValue("y_col", self.combo_y.currentData())
        self.settings.setValue("y_col_secondary", self.combo_y_secondary.currentData())
        self.settings.setValue("show_points", self.chk_show_points.isChecked())
        self.settings.setValue("drop_zeros", self.chk_drop_zeros.isChecked())
        self.settings.setValue("break_gaps", self.chk_break_gaps.isChecked())
        self.settings.setValue("auto_y", self.chk_auto_y.isChecked())
        self.settings.setValue("secondary_independent_axis", self.chk_secondary_independent.isChecked())
        self.settings.setValue("gap_minutes", self.spin_gap_minutes.value())
        self.settings.setValue("gap_days", self.spin_gap_days.value())
        self.settings.sync()

    def closeEvent(self, event) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        super().closeEvent(event)

    def _on_export_table(self) -> None:
        df = self.table_model.dataframe()
        if df.empty:
            QMessageBox.information(self, "提示", "当前表格为空")
            return
        path, _ = QFileDialog.getSaveFileName(self, "导出CSV", "data.csv", "CSV Files (*.csv)")
        if not path:
            return
        df.to_csv(path, index=False)
        self._append_message(f"表格已导出：{path}")

    def _on_export_chart(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "导出PNG", "chart.png", "PNG Files (*.png)")
        if not path:
            return
        exporter = self.pg.exporters.ImageExporter(self.plot.plotItem)
        exporter.export(path)
        self._append_message(f"图表已导出：{path}")


if __name__ == "__main__":
    from gui_viewer.main import main

    main()

