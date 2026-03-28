"""
DS_DMTR 回测启动器 — 参数录入 + 一键运行。

布局模仿 mstr_gui/launcher.py：
  第一排: 候选股票池 | 配对标的+日期 | 布林通道参数
  第二排: 触发门限+比例+初始仓位 | 执行参数 | 引擎参数
  按钮区 + 进度 & 日志
"""

from __future__ import annotations

import json
import traceback
from pathlib import Path

from PyQt5.QtCore import QDate, Qt, QThread, pyqtSignal
from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDoubleSpinBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


# ────────────────────────────────────────────────
#  背景线程: 运行回测
# ────────────────────────────────────────────────


class _BacktestWorker(QThread):
    """在独立线程中运行 DS_DMTR 回测。"""

    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(object, object)  # (BacktestResult, strategy)
    error_signal = pyqtSignal(str)

    def __init__(
        self,
        engine_params: dict,
        strategy_params: dict,
        pct_a: int = 0,
        pct_b: int = 0,
        parent=None,
    ):
        super().__init__(parent)
        self._engine_params = engine_params
        self._strategy_params = strategy_params
        self._pct_a = pct_a
        self._pct_b = pct_b

    def run(self):
        try:
            from core.datatypes import MatchingMode
            from backtest.engine import BacktestEngine
            from strategies.ds_dmtr_strategy import DsDmtrStrategy

            ep = self._engine_params
            sp = self._strategy_params

            mode = MatchingMode(ep.get("matching_mode", "smart_tick_delay_fill"))
            self.log_signal.emit(
                f"创建引擎: capital={ep['initial_capital']:,.0f}, "
                f"mode={mode.name}"
            )
            engine = BacktestEngine(
                dataset_dir=ep["dataset_dir"],
                mode=mode,
                initial_capital=ep["initial_capital"],
                rate=ep["rate"],
                slippage=ep["slippage"],
                pricetick=ep["pricetick"],
                volume_limit_ratio=ep.get("volume_limit_ratio", 0.5),
                credit_ratio=ep.get("credit_ratio", 0.0),
                enable_t0=ep.get("enable_t0", False),
            )

            symbol_a = sp["symbol_a"]
            symbol_b = sp["symbol_b"]
            symbols = [symbol_a, symbol_b]

            setting = {
                k: v
                for k, v in sp.items()
                if k not in ("start_date", "end_date")
            }
            setting["dataset_dir"] = ep["dataset_dir"]

            self.log_signal.emit(
                f"创建策略: A={symbol_a}, B={symbol_b}, "
                f"bar_interval={setting.get('bar_interval_minutes')}min"
            )
            strategy = DsDmtrStrategy(
                engine=engine,
                strategy_name="DS_DMTR_Backtest",
                symbols=symbols,
                setting=setting,
            )

            # 根据百分比计算初始持仓
            init_pos = None
            if self._pct_a > 0 or self._pct_b > 0:
                init_pos = self._calc_initial_positions(
                    ep["dataset_dir"], symbol_a, symbol_b,
                    ep["initial_capital"], sp["start_date"],
                    self._pct_a, self._pct_b,
                )
                if init_pos:
                    self.log_signal.emit(f"初始持仓: {init_pos}")

            # on_init (加载日线)
            self.log_signal.emit("正在加载日线数据...")
            strategy.on_init()

            # 预热 (加载预热期 tick 构建分钟级统计量)
            self.log_signal.emit("正在预热分钟级 K 线...")
            strategy.warmup(sp["start_date"])

            self.log_signal.emit(
                f"开始回测: {sp['start_date']} ~ {sp['end_date']}"
            )
            result = engine.run(
                strategy,
                sp["start_date"],
                sp["end_date"],
                initial_positions=init_pos,
                progress_callback=self.log_signal.emit,
            )
            self.log_signal.emit("回测完成!")
            self.finished_signal.emit(result, strategy)

        except Exception:
            self.error_signal.emit(traceback.format_exc())

    @staticmethod
    def _calc_initial_positions(
        dataset_dir: str,
        symbol_a: str,
        symbol_b: str,
        capital: float,
        start_date: str,
        pct_a: int,
        pct_b: int,
    ) -> dict[str, tuple[int, float]] | None:
        """根据百分比和回测前一交易日收盘价计算初始持仓。"""
        from core.data_feed import ParquetBarFeed

        bar_feed = ParquetBarFeed(dataset_dir)
        result = {}

        for sym, pct in [(symbol_a, pct_a), (symbol_b, pct_b)]:
            if pct <= 0:
                continue
            # 加载 start_date 之前的日线，取最后一条作为前一交易日
            bars = bar_feed.load(sym, end_date=start_date)
            # 找到严格 < start_date 的最后一根 bar
            prev_bars = [
                b for b in bars
                if b.datetime.strftime("%Y%m%d") < start_date
            ]
            if not prev_bars:
                continue
            close = prev_bars[-1].close_price
            if close <= 0:
                continue
            alloc = capital * pct / 100.0
            vol = int(alloc / close / 100) * 100
            if vol >= 100:
                result[sym] = (vol, close)

        return result if result else None


# ────────────────────────────────────────────────
#  ETF 库存 (与 mstr_gui 一致)
# ────────────────────────────────────────────────

_ALL_ETFS: list[tuple[str, str]] = [
    ("159300.SZ", "300ETF"),
    ("159330.SZ", "沪深300ETF东财"),
    ("159513.SZ", "纳斯达克100ETF大成"),
    ("159612.SZ", "标普500ETF"),
    ("159632.SZ", "纳斯达克ETF"),
    ("159655.SZ", "标普ETF"),
    ("159696.SZ", "纳指ETF易方达"),
    ("159919.SZ", "沪深300ETF"),
    ("159925.SZ", "沪深300ETF南方"),
    ("159941.SZ", "纳指ETF"),
    ("510300.SH", "沪深300ETF华泰柏瑞"),
    ("510310.SH", "沪深300ETF易方达"),
    ("510330.SH", "沪深300ETF华夏"),
    ("510350.SH", "沪深300ETF工银"),
    ("510360.SH", "沪深300ETF广发"),
    ("513100.SH", "纳指ETF"),
    ("513300.SH", "纳斯达克ETF"),
    ("513500.SH", "标普500ETF"),
    ("513650.SH", "标普500ETF南方"),
    ("513870.SH", "纳指ETF富国"),
    ("515330.SH", "沪深300ETF天弘"),
]


# ────────────────────────────────────────────────
#  参数 Tooltip 定义
# ────────────────────────────────────────────────

_TOOLTIPS = {
    # 布林通道
    "bar_interval_minutes": "分钟 K 线聚合周期。\n30 = 30分钟线。",
    "window_minutes": "分钟级布林均线窗口长度。\n取最近 N 根 K 线计算 SMA/STD。",
    "window_days": "日线级布林均线窗口长度。\n取最近 N 个交易日计算 SMA/STD。",
    "k_sigma_minutes": "分钟级布林 σ 倍数 (仅用于绘图)。\n上下轨 = 均值 ± k × σ。",
    "k_sigma_days": "日线级布林 σ 倍数 (仅用于绘图)。\n上下轨 = 均值 ± k × σ。",
    # 触发门限
    "thresh_sigma_min": "|delta_sigma_minutes| 基础触发门限。\n超过此值才考虑交易。",
    "thresh_sigma_min_high": "|delta_sigma_minutes| 加码触发门限。\n超过时使用加码比例。",
    "thresh_sigma_day": "|delta_sigma_days| 加码触发门限。\n如果日线偏离也大，使用加码比例。",
    "thresh_delta_min": "|delta_minutes| 触发门限。\n相对偏离率的最小过滤。",
    # 执行参数
    "cooldown_seconds": "同方向交易冷却时间 (秒)。\n上次同方向交易后需等待此时长。",
    "trading_cutoff_str": "日内禁止新开 Block 的截止时间。\n格式 HH:MM:SS。",
    "block_timeout_minutes": "单个 Block 的实现期上限（分钟）。\n超时后触发 timeout 分支处理。",
    "timeout_recover_minutes": "超时后强制激进阶段的额外时长（分钟）。",
    "timeout_recover_policy": "超时后的处理策略：\nfull=优先补齐双边目标量\nbalance=优先收敛对冲差\nabort=放弃补单直接结束",
    "chase_wait_ticks": "激进单追价等待 tick 数。\n达到该 tick 数仍未完成则撤单改价重发。",
    "max_chase_rounds": "激进改单最大轮数。\n超过后标记为 CRITICAL。",
    "passive_slice_count": "初始被动拆单数量 n。\n用于最小撤单改追价，减少排队优势损失。",
    "cancel_priority": "撤单优先级。\nnewest_unfilled_first=优先撤最新且未成交子单。",
    "base_pct": "基础轮动比例。\n满足基础条件时交易净值的此比例。",
    "high_pct": "加码轮动比例。\n满足加码条件时交易净值的此比例。",
    "min_order_ratio": "最低发单比例。\n买卖两腿实际发单量都低于期望量的此比例时放弃 block。\n0 = 不过滤。",
    "open_wait_minutes": "开盘等待分钟数。\n开盘后此时间内不发出新信号，避免开盘波动导致大滑价。\n0 = 不等待。",
    "enable_signal_check": "执行中信号失效检测。\n启用后，block 执行中若信号回归则提前终止并再平衡。",
    # 引擎参数
    "initial_capital": "回测初始资金 (元)。",
    "rate": "委托手续费率。\nETF 一般为万 0.5 = 0.00005。",
    "slippage": "滑点 (元)。模拟成交价偏移。\nA股 ETF 默认 0。",
    "pricetick": "最小价格变动单位。\nA股 ETF 为 0.001 元。",
    "vol_limit": "盘口量比例限制 (0~1)。\n单笔限价单最多匹配盘口挂单量的此比例。",
    "credit_ratio": "信用额度比例 (0~1)。资金不足时，\n可额外使用 总净值×此比例 作为信用额度。",
    "dataset_dir": "Tick/日线数据集的目录路径。\n相对于项目根目录。",
}


# ────────────────────────────────────────────────
#  启动器主面板
# ────────────────────────────────────────────────


class LauncherWidget(QWidget):
    """DS_DMTR 回测参数录入 + 一键启动面板。多排紧凑布局。"""

    backtest_finished = pyqtSignal(object, object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: _BacktestWorker | None = None
        self._json_path = (
            Path(__file__).resolve().parent.parent / "ds_dmtr_params.json"
        )
        self._symbol_a: str = ""
        self._symbol_b: str = ""
        self._init_ui()
        self._load_from_json()

    # ════════════════════════════════════════════════
    #  UI 构建
    # ════════════════════════════════════════════════

    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(6)

        # ── 第一排: 候选股票池 | 已选标的+日期 | 布林通道参数 ──
        row1 = QHBoxLayout()
        row1.setSpacing(6)
        row1.addLayout(self._build_col_candidates(), stretch=3)
        row1.addLayout(self._build_col_selected(), stretch=2)
        row1.addWidget(self._build_grp_bollinger(), stretch=2)
        root.addLayout(row1, stretch=1)

        # ── 第二排: 触发门限+比例+初始仓位 | 执行参数 | 引擎参数 ──
        row2 = QHBoxLayout()
        row2.setSpacing(6)
        row2.addLayout(self._build_col_threshold_and_pos(), stretch=3)
        row2.addLayout(self._build_col_exec_and_pos(), stretch=3)
        row2.addWidget(self._build_grp_engine(), stretch=3)
        root.addLayout(row2, stretch=0)

        # ── 按钮区 ──
        btn_row = QHBoxLayout()
        self._btn_save = QPushButton("保存参数")
        self._btn_load = QPushButton("加载参数")
        self._btn_run = QPushButton("▶ 运行回测")
        self._btn_run.setMinimumHeight(34)
        self._btn_run.setStyleSheet(
            "QPushButton { font-size: 14px; font-weight: bold; }"
        )
        btn_row.addWidget(self._btn_save)
        btn_row.addWidget(self._btn_load)
        btn_row.addStretch()
        btn_row.addWidget(self._btn_run)
        root.addLayout(btn_row)

        # ── 进度 & 日志 ──
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setVisible(False)
        root.addWidget(self._progress)

        self._log = QPlainTextEdit()
        self._log.setReadOnly(True)
        self._log.setMaximumHeight(110)
        root.addWidget(self._log)

        # ── 信号连接 ──
        self._btn_run.clicked.connect(self._on_run)
        self._btn_save.clicked.connect(self._on_save)
        self._btn_load.clicked.connect(self._on_load)

    # ──────────────────────────
    #  左列: 候选股票池
    # ──────────────────────────

    def _build_col_candidates(self) -> QVBoxLayout:
        col = QVBoxLayout()
        grp = QGroupBox("候选股票池")
        lay = QVBoxLayout(grp)
        lay.setContentsMargins(4, 8, 4, 4)
        lay.setSpacing(4)

        # 搜索栏
        self._le_filter = QLineEdit()
        self._le_filter.setPlaceholderText("输入关键字过滤 (代码/名称)...")
        self._le_filter.textChanged.connect(self._on_filter_changed)
        lay.addWidget(self._le_filter)

        # 候选列表 (单选)
        self._candidate_list = QListWidget()
        self._candidate_list.setSelectionMode(QListWidget.SingleSelection)
        for ts_code, name in _ALL_ETFS:
            item = QListWidgetItem(f"{ts_code}  {name}")
            item.setData(Qt.UserRole, ts_code)
            self._candidate_list.addItem(item)
        lay.addWidget(self._candidate_list, stretch=1)

        # 直接设为 A / B 按钮
        ab_row = QHBoxLayout()
        btn_set_a = QPushButton("设为 A")
        btn_set_a.clicked.connect(self._on_set_a)
        btn_set_b = QPushButton("设为 B")
        btn_set_b.clicked.connect(self._on_set_b)
        ab_row.addWidget(btn_set_a)
        ab_row.addWidget(btn_set_b)
        lay.addLayout(ab_row)

        col.addWidget(grp)
        return col

    def _on_filter_changed(self, text: str):
        keyword = text.strip().lower()
        for i in range(self._candidate_list.count()):
            item = self._candidate_list.item(i)
            visible = keyword in item.text().lower() if keyword else True
            item.setHidden(not visible)

    def _on_set_a(self):
        item = self._candidate_list.currentItem()
        if item:
            self._symbol_a = item.data(Qt.UserRole)
            self._lbl_sym_a.setText(f"Symbol A:  {self._symbol_a}")

    def _on_set_b(self):
        item = self._candidate_list.currentItem()
        if item:
            self._symbol_b = item.data(Qt.UserRole)
            self._lbl_sym_b.setText(f"Symbol B:  {self._symbol_b}")

    # ──────────────────────────
    #  中列: 已选标的 + 日期
    # ──────────────────────────

    def _build_col_selected(self) -> QVBoxLayout:
        col = QVBoxLayout()

        # Symbol A / B 显示
        grp_ab = QGroupBox("配对标的")
        lay_ab = QVBoxLayout(grp_ab)
        lay_ab.setContentsMargins(4, 8, 4, 4)
        self._lbl_sym_a = QLabel("Symbol A:  (未设定)")
        self._lbl_sym_b = QLabel("Symbol B:  (未设定)")
        self._lbl_sym_a.setStyleSheet("font-weight: bold; color: #1976D2;")
        self._lbl_sym_b.setStyleSheet("font-weight: bold; color: #E65100;")
        lay_ab.addWidget(self._lbl_sym_a)
        lay_ab.addWidget(self._lbl_sym_b)
        btn_clear_ab = QPushButton("清空 A / B")
        btn_clear_ab.clicked.connect(self._on_clear_ab)
        lay_ab.addWidget(btn_clear_ab)
        col.addWidget(grp_ab)

        # 仿真起止时间
        grp_date = QGroupBox("仿真起止时间")
        lay_date = QFormLayout(grp_date)
        lay_date.setContentsMargins(4, 8, 4, 4)
        self._date_start = QDateEdit()
        self._date_start.setCalendarPopup(True)
        self._date_start.setDisplayFormat("yyyyMMdd")
        self._date_end = QDateEdit()
        self._date_end.setCalendarPopup(True)
        self._date_end.setDisplayFormat("yyyyMMdd")
        lay_date.addRow("起始:", self._date_start)
        lay_date.addRow("结束:", self._date_end)
        col.addWidget(grp_date)

        return col

    def _on_clear_ab(self):
        self._symbol_a = ""
        self._symbol_b = ""
        self._lbl_sym_a.setText("Symbol A:  (未设定)")
        self._lbl_sym_b.setText("Symbol B:  (未设定)")

    # ──────────────────────────
    #  第一排右列: 布林通道参数
    # ──────────────────────────

    def _build_grp_bollinger(self) -> QGroupBox:
        grp = QGroupBox("布林通道参数")
        f = QFormLayout(grp)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_bar_interval = self._spin_i(1, 240, 30, "bar_interval_minutes")
        self._sp_window_min = self._spin_i(5, 200, 20, "window_minutes")
        self._sp_window_day = self._spin_i(5, 200, 20, "window_days")
        self._sp_k_sigma_min = self._spin_d(0.5, 5.0, 2.0, 1, 0.1, "k_sigma_minutes")
        self._sp_k_sigma_day = self._spin_d(0.5, 5.0, 2.0, 1, 0.1, "k_sigma_days")
        f.addRow("K线周期(min):", self._sp_bar_interval)
        f.addRow("分钟窗口长度:", self._sp_window_min)
        f.addRow("日线窗口长度:", self._sp_window_day)
        f.addRow("分钟σ倍数(绘图):", self._sp_k_sigma_min)
        f.addRow("日线σ倍数(绘图):", self._sp_k_sigma_day)
        return grp

    # ──────────────────────────
    #  第二排左列: 触发门限 + 比例 + 初始仓位
    # ──────────────────────────

    def _build_col_threshold_and_pos(self) -> QVBoxLayout:
        col = QVBoxLayout()
        col.setSpacing(4)
        col.addWidget(self._build_grp_threshold())
        col.addWidget(self._build_grp_initial_positions())
        return col

    def _build_grp_threshold(self) -> QGroupBox:
        grp = QGroupBox("触发门限与比例")
        f = QFormLayout(grp)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_thresh_sigma_min = self._spin_d(
            0.0, 5.0, 0.5, 2, 0.05, "thresh_sigma_min"
        )
        self._sp_thresh_sigma_min_high = self._spin_d(
            0.0, 5.0, 1.0, 2, 0.05, "thresh_sigma_min_high"
        )
        self._sp_thresh_sigma_day = self._spin_d(
            0.0, 5.0, 1.5, 2, 0.05, "thresh_sigma_day"
        )
        self._sp_thresh_delta_min = self._spin_d(
            0.0, 0.1, 0.005, 4, 0.001, "thresh_delta_min"
        )
        f.addRow("sigma基础门限:", self._sp_thresh_sigma_min)
        f.addRow("sigma加码门限:", self._sp_thresh_sigma_min_high)
        f.addRow("日线sigma门限:", self._sp_thresh_sigma_day)
        f.addRow("delta偏离门限:", self._sp_thresh_delta_min)
        self._sp_base_pct = self._spin_d(0.01, 1.0, 0.1, 2, 0.01, "base_pct")
        self._sp_high_pct = self._spin_d(0.01, 1.0, 0.3, 2, 0.01, "high_pct")
        self._sp_min_order_ratio = self._spin_d(0.0, 1.0, 0.1, 2, 0.01, "min_order_ratio")
        f.addRow("基础比例:", self._sp_base_pct)
        f.addRow("加码比例:", self._sp_high_pct)
        f.addRow("最低发单比例:", self._sp_min_order_ratio)
        return grp

    def _build_grp_initial_positions(self) -> QGroupBox:
        grp_ipos = QGroupBox("初始持仓比例 (可选)")
        lay_ipos = QFormLayout(grp_ipos)
        lay_ipos.setContentsMargins(4, 8, 4, 4)
        self._sp_init_pct_a = QSpinBox()
        self._sp_init_pct_a.setRange(0, 100)
        self._sp_init_pct_a.setValue(50)
        self._sp_init_pct_a.setSuffix("%")
        self._sp_init_pct_a.setToolTip(
            "初始持有 A 股票的资金比例。\n"
            "按回测起始日前一交易日收盘价计算持仓量（向下取整到100股）。"
        )
        self._sp_init_pct_b = QSpinBox()
        self._sp_init_pct_b.setRange(0, 100)
        self._sp_init_pct_b.setValue(50)
        self._sp_init_pct_b.setSuffix("%")
        self._sp_init_pct_b.setToolTip(
            "初始持有 B 股票的资金比例。\n"
            "A% + B% 不得超过 100%。"
        )
        lay_ipos.addRow("A 股票比例:", self._sp_init_pct_a)
        lay_ipos.addRow("B 股票比例:", self._sp_init_pct_b)
        return grp_ipos

    # ──────────────────────────
    #  第二排中列: 执行参数
    # ──────────────────────────

    def _build_col_exec_and_pos(self) -> QVBoxLayout:
        col = QVBoxLayout()
        col.setSpacing(4)

        # 执行参数
        grp_exec = QGroupBox("执行参数")
        f = QFormLayout(grp_exec)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_cooldown = self._spin_i(0, 7200, 1800, "cooldown_seconds")
        self._le_trading_cutoff = QLineEdit("14:55:00")
        self._le_trading_cutoff.setToolTip(_TOOLTIPS["trading_cutoff_str"])
        self._sp_block_timeout = self._spin_i(
            1, 180, 20, "block_timeout_minutes"
        )
        self._sp_timeout_recover_minutes = self._spin_i(
            1, 30, 2, "timeout_recover_minutes"
        )
        self._combo_timeout_policy = QComboBox()
        self._combo_timeout_policy.addItem("FULL(补齐目标量)", "full")
        self._combo_timeout_policy.addItem("BALANCE(收敛对冲差)", "balance")
        self._combo_timeout_policy.addItem("ABORT(放弃补单)", "abort")
        self._combo_timeout_policy.setToolTip(_TOOLTIPS["timeout_recover_policy"])
        self._sp_chase_wait_ticks = self._spin_i(
            1, 100, 3, "chase_wait_ticks"
        )
        self._sp_max_chase_rounds = self._spin_i(
            1, 200, 20, "max_chase_rounds"
        )
        self._sp_passive_slice_count = self._spin_i(
            1, 20, 3, "passive_slice_count"
        )
        self._combo_cancel_priority = QComboBox()
        self._combo_cancel_priority.addItem(
            "NEWEST_UNFILLED_FIRST", "newest_unfilled_first"
        )
        self._combo_cancel_priority.setToolTip(_TOOLTIPS["cancel_priority"])
        self._cb_strategy_t0 = QCheckBox("策略启用 T+0")
        self._cb_strategy_t0.setToolTip(
            "策略层 T+0 开关。需引擎层也勾选 T+0，\n"
            "双方都开启时当日买入的仓位可当日卖出。"
        )
        self._sp_open_wait = self._spin_i(0, 60, 5, "open_wait_minutes")
        f.addRow("开盘等待(分钟):", self._sp_open_wait)
        f.addRow("冷却时间(秒):", self._sp_cooldown)
        f.addRow("截止时间(HH:MM:SS):", self._le_trading_cutoff)
        f.addRow("实现期(分钟):", self._sp_block_timeout)
        f.addRow("二阶段时长(分钟):", self._sp_timeout_recover_minutes)
        f.addRow("超时处理策略:", self._combo_timeout_policy)
        f.addRow("追价等待(ticks):", self._sp_chase_wait_ticks)
        f.addRow("追价最大轮数:", self._sp_max_chase_rounds)
        f.addRow("被动拆单数:", self._sp_passive_slice_count)
        f.addRow("撤单优先级:", self._combo_cancel_priority)
        self._cb_signal_check = QCheckBox("执行中信号失效检测")
        if "enable_signal_check" in _TOOLTIPS:
            self._cb_signal_check.setToolTip(_TOOLTIPS["enable_signal_check"])
        self._cb_signal_check.setChecked(True)
        f.addRow("", self._cb_strategy_t0)
        f.addRow("", self._cb_signal_check)
        col.addWidget(grp_exec)

        return col

    # ──────────────────────────
    #  第二排右列: 引擎参数
    # ──────────────────────────

    def _build_grp_engine(self) -> QGroupBox:
        grp = QGroupBox("引擎参数")
        f = QFormLayout(grp)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_capital = self._spin_d(
            10_000, 100_000_000, 1_000_000, 0, 10_000, "initial_capital"
        )
        self._sp_rate = self._spin_d(0.0, 0.01, 0.00005, 5, 0.00001, "rate")
        self._sp_slippage = self._spin_d(0.0, 0.1, 0.0, 4, 0.0001, "slippage")
        self._sp_pricetick = self._spin_d(
            0.0001, 1.0, 0.001, 4, 0.0001, "pricetick"
        )
        self._sp_vol_limit = self._spin_d(0.0, 1.0, 0.5, 2, 0.05, "vol_limit")
        self._sp_credit_ratio = self._spin_d(
            0.0, 1.0, 0.0, 2, 0.05, "credit_ratio"
        )
        self._cb_engine_t0 = QCheckBox("允许 T+0")
        self._cb_engine_t0.setToolTip(
            "引擎层 T+0 开关。勾选后当日买入不锁仓，\n"
            "策略层也需勾选才能实际利用 T+0 交易。"
        )
        self._le_dataset = QLineEdit("dataset")
        self._le_dataset.setToolTip(_TOOLTIPS["dataset_dir"])
        self._combo_mode = QComboBox()
        self._combo_mode.addItem(
            "SMART_TICK_DELAY_FILL", "smart_tick_delay_fill"
        )
        self._combo_mode.addItem("TICK_FILL", "tick_fill")
        self._combo_mode.addItem("CLOSE_FILL", "close_fill")
        self._combo_mode.setToolTip(
            "撮合模式:\n"
            "SMART_TICK_DELAY_FILL — 延迟深度撮合（推荐）\n"
            "TICK_FILL — Tick 即时全量\n"
            "CLOSE_FILL — 日线收盘价"
        )
        f.addRow("撮合模式:", self._combo_mode)
        f.addRow("初始资金:", self._sp_capital)
        f.addRow("手续费率:", self._sp_rate)
        f.addRow("滑点:", self._sp_slippage)
        f.addRow("最小价格变动:", self._sp_pricetick)
        f.addRow("盘口量限比:", self._sp_vol_limit)
        f.addRow("信用额度比:", self._sp_credit_ratio)
        f.addRow("", self._cb_engine_t0)
        f.addRow("数据目录:", self._le_dataset)
        return grp

    # ════════════════════════════════════════════════
    #  SpinBox 工厂
    # ════════════════════════════════════════════════

    def _spin_i(self, mn, mx, val, tip_key: str = "") -> QSpinBox:
        sp = QSpinBox()
        sp.setRange(mn, mx)
        sp.setValue(val)
        if tip_key in _TOOLTIPS:
            sp.setToolTip(_TOOLTIPS[tip_key])
        return sp

    def _spin_d(
        self, mn, mx, val, dec, step, tip_key: str = ""
    ) -> QDoubleSpinBox:
        sp = QDoubleSpinBox()
        sp.setRange(mn, mx)
        sp.setDecimals(dec)
        sp.setSingleStep(step)
        sp.setValue(val)
        if tip_key in _TOOLTIPS:
            sp.setToolTip(_TOOLTIPS[tip_key])
        return sp

    # ════════════════════════════════════════════════
    #  初始仓位表
    # ════════════════════════════════════════════════

    def _collect_init_pct(self) -> tuple[int, int]:
        """返回 (pct_a, pct_b)。"""
        return self._sp_init_pct_a.value(), self._sp_init_pct_b.value()

    # ════════════════════════════════════════════════
    #  参数收集
    # ════════════════════════════════════════════════

    def _collect_params(self) -> tuple[dict, dict] | None:
        if not self._symbol_a or not self._symbol_b:
            QMessageBox.warning(
                self, "参数错误", "请设定 Symbol A 和 Symbol B!"
            )
            return None
        if self._symbol_a == self._symbol_b:
            QMessageBox.warning(
                self, "参数错误", "Symbol A 和 Symbol B 不能相同!"
            )
            return None

        engine_params = {
            "dataset_dir": self._le_dataset.text().strip(),
            "matching_mode": self._combo_mode.currentData(),
            "initial_capital": self._sp_capital.value(),
            "rate": self._sp_rate.value(),
            "slippage": self._sp_slippage.value(),
            "pricetick": self._sp_pricetick.value(),
            "volume_limit_ratio": self._sp_vol_limit.value(),
            "credit_ratio": self._sp_credit_ratio.value(),
            "enable_t0": self._cb_engine_t0.isChecked(),
        }
        strategy_params = {
            "symbol_a": self._symbol_a,
            "symbol_b": self._symbol_b,
            "start_date": self._date_start.date().toString("yyyyMMdd"),
            "end_date": self._date_end.date().toString("yyyyMMdd"),
            "bar_interval_minutes": self._sp_bar_interval.value(),
            "window_minutes": self._sp_window_min.value(),
            "window_days": self._sp_window_day.value(),
            "k_sigma_minutes": self._sp_k_sigma_min.value(),
            "k_sigma_days": self._sp_k_sigma_day.value(),
            "thresh_sigma_min": self._sp_thresh_sigma_min.value(),
            "thresh_sigma_min_high": self._sp_thresh_sigma_min_high.value(),
            "thresh_sigma_day": self._sp_thresh_sigma_day.value(),
            "thresh_delta_min": self._sp_thresh_delta_min.value(),
            "cooldown_seconds": self._sp_cooldown.value(),
            "trading_cutoff_str": self._le_trading_cutoff.text().strip() or "14:55:00",
            "block_timeout_minutes": self._sp_block_timeout.value(),
            "timeout_recover_minutes": self._sp_timeout_recover_minutes.value(),
            "timeout_recover_policy": self._combo_timeout_policy.currentData(),
            "chase_wait_ticks": self._sp_chase_wait_ticks.value(),
            "max_chase_rounds": self._sp_max_chase_rounds.value(),
            "passive_slice_count": self._sp_passive_slice_count.value(),
            "cancel_priority": self._combo_cancel_priority.currentData(),
            "base_pct": self._sp_base_pct.value(),
            "high_pct": self._sp_high_pct.value(),
            "min_order_ratio": self._sp_min_order_ratio.value(),
            "open_wait_minutes": self._sp_open_wait.value(),
            "enable_signal_check": self._cb_signal_check.isChecked(),
            "enable_t0": self._cb_strategy_t0.isChecked(),
        }
        return engine_params, strategy_params

    # ════════════════════════════════════════════════
    #  运行回测
    # ════════════════════════════════════════════════

    def _on_run(self):
        params = self._collect_params()
        if params is None:
            return
        engine_params, strategy_params = params
        pct_a, pct_b = self._collect_init_pct()
        if pct_a + pct_b > 100:
            self._append_log("ERROR: A% + B% 不得超过 100%")
            return

        self._btn_run.setEnabled(False)
        self._progress.setVisible(True)
        self._log.clear()
        self._append_log("准备启动回测...")

        self._worker = _BacktestWorker(
            engine_params, strategy_params, pct_a=pct_a, pct_b=pct_b
        )
        self._worker.log_signal.connect(self._append_log)
        self._worker.finished_signal.connect(self._on_finished)
        self._worker.error_signal.connect(self._on_error)
        self._worker.start()

    def _on_finished(self, result, strategy):
        self._progress.setVisible(False)
        self._btn_run.setEnabled(True)
        self._append_log("回测完成。可切换到 [回测结果] 标签页查看详情。")

        from backtest.report import BacktestReport

        report = BacktestReport(result)
        stats = report.stats
        summary_lines = [
            f"总收益率:  {stats.get('total_return', 0):.2%}",
            f"年化收益:  {stats.get('annual_return', 0):.2%}",
            f"最大回撤:  {stats.get('max_drawdown', 0):.2%}",
            f"Sharpe:    {stats.get('sharpe', 0):.3f}",
            f"总成交笔:  {len(result.trades)}",
        ]
        self._append_log("\n".join(summary_lines))
        self.backtest_finished.emit(result, strategy)

    def _on_error(self, msg):
        self._progress.setVisible(False)
        self._btn_run.setEnabled(True)
        self._append_log(f"回测出错:\n{msg}")

    def _append_log(self, text: str):
        self._log.appendPlainText(text)

    # ════════════════════════════════════════════════
    #  JSON 持久化 (ds_dmtr_params.json)
    # ════════════════════════════════════════════════

    def _save_to_json(self, path: Path | None = None):
        params = self._collect_params()
        if params is None:
            return
        target = path or self._json_path
        engine_params, strategy_params = params
        pct_a, pct_b = self._collect_init_pct()
        data = {
            "engine": engine_params,
            "strategy": strategy_params,
            "initial_positions": {"pct_a": pct_a, "pct_b": pct_b},
        }
        target.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def _load_from_json(self, path: Path | None = None):
        target = path or self._json_path
        if not target.exists():
            # 默认值
            return

        data = json.loads(target.read_text(encoding="utf-8"))
        ep = data.get("engine", {})
        sp = data.get("strategy", {})

        # 引擎参数
        if "dataset_dir" in ep:
            self._le_dataset.setText(ep["dataset_dir"])
        if "initial_capital" in ep:
            self._sp_capital.setValue(ep["initial_capital"])
        if "rate" in ep:
            self._sp_rate.setValue(ep["rate"])
        if "slippage" in ep:
            self._sp_slippage.setValue(ep["slippage"])
        if "pricetick" in ep:
            self._sp_pricetick.setValue(ep["pricetick"])
        if "volume_limit_ratio" in ep:
            self._sp_vol_limit.setValue(ep["volume_limit_ratio"])
        if "credit_ratio" in ep:
            self._sp_credit_ratio.setValue(ep["credit_ratio"])
        if "matching_mode" in ep:
            idx = self._combo_mode.findData(ep["matching_mode"])
            if idx >= 0:
                self._combo_mode.setCurrentIndex(idx)
        if "enable_t0" in ep:
            self._cb_engine_t0.setChecked(ep["enable_t0"])

        # 策略参数
        if "symbol_a" in sp:
            self._symbol_a = sp["symbol_a"]
            self._lbl_sym_a.setText(f"Symbol A:  {self._symbol_a}")
        if "symbol_b" in sp:
            self._symbol_b = sp["symbol_b"]
            self._lbl_sym_b.setText(f"Symbol B:  {self._symbol_b}")

        if "start_date" in sp:
            self._date_start.setDate(
                QDate.fromString(sp["start_date"], "yyyyMMdd")
            )
        if "end_date" in sp:
            self._date_end.setDate(
                QDate.fromString(sp["end_date"], "yyyyMMdd")
            )
        if "bar_interval_minutes" in sp:
            self._sp_bar_interval.setValue(sp["bar_interval_minutes"])
        if "window_minutes" in sp:
            self._sp_window_min.setValue(sp["window_minutes"])
        if "window_days" in sp:
            self._sp_window_day.setValue(sp["window_days"])
        if "k_sigma_minutes" in sp:
            self._sp_k_sigma_min.setValue(sp["k_sigma_minutes"])
        if "k_sigma_days" in sp:
            self._sp_k_sigma_day.setValue(sp["k_sigma_days"])
        if "thresh_sigma_min" in sp:
            self._sp_thresh_sigma_min.setValue(sp["thresh_sigma_min"])
        if "thresh_sigma_min_high" in sp:
            self._sp_thresh_sigma_min_high.setValue(sp["thresh_sigma_min_high"])
        if "thresh_sigma_day" in sp:
            self._sp_thresh_sigma_day.setValue(sp["thresh_sigma_day"])
        if "thresh_delta_min" in sp:
            self._sp_thresh_delta_min.setValue(sp["thresh_delta_min"])
        if "cooldown_seconds" in sp:
            self._sp_cooldown.setValue(sp["cooldown_seconds"])
        if "trading_cutoff_str" in sp:
            self._le_trading_cutoff.setText(sp["trading_cutoff_str"])
        if "block_timeout_minutes" in sp:
            self._sp_block_timeout.setValue(sp["block_timeout_minutes"])
        if "timeout_recover_minutes" in sp:
            self._sp_timeout_recover_minutes.setValue(sp["timeout_recover_minutes"])
        if "timeout_recover_policy" in sp:
            policy = str(sp["timeout_recover_policy"]).strip().lower()
            if policy == "recover":
                policy = "balance"
            idx = self._combo_timeout_policy.findData(policy)
            if idx >= 0:
                self._combo_timeout_policy.setCurrentIndex(idx)
        if "chase_wait_ticks" in sp:
            self._sp_chase_wait_ticks.setValue(sp["chase_wait_ticks"])
        if "max_chase_rounds" in sp:
            self._sp_max_chase_rounds.setValue(sp["max_chase_rounds"])
        if "passive_slice_count" in sp:
            self._sp_passive_slice_count.setValue(sp["passive_slice_count"])
        if "cancel_priority" in sp:
            idx = self._combo_cancel_priority.findData(sp["cancel_priority"])
            if idx >= 0:
                self._combo_cancel_priority.setCurrentIndex(idx)
        if "base_pct" in sp:
            self._sp_base_pct.setValue(sp["base_pct"])
        if "high_pct" in sp:
            self._sp_high_pct.setValue(sp["high_pct"])
        if "min_order_ratio" in sp:
            self._sp_min_order_ratio.setValue(sp["min_order_ratio"])
        if "open_wait_minutes" in sp:
            self._sp_open_wait.setValue(sp["open_wait_minutes"])
        if "enable_signal_check" in sp:
            self._cb_signal_check.setChecked(sp["enable_signal_check"])
        if "enable_t0" in sp:
            self._cb_strategy_t0.setChecked(sp["enable_t0"])

        # 初始持仓比例
        ipos_data = data.get("initial_positions", {})
        if "pct_a" in ipos_data:
            self._sp_init_pct_a.setValue(ipos_data["pct_a"])
        if "pct_b" in ipos_data:
            self._sp_init_pct_b.setValue(ipos_data["pct_b"])

    def hideEvent(self, event):
        """窗口隐藏/关闭时自动保存。"""
        self._save_to_json()
        super().hideEvent(event)

    # ════════════════════════════════════════════════
    #  参数文件 保存/加载
    # ════════════════════════════════════════════════

    def _on_save(self):
        from PyQt5.QtWidgets import QFileDialog

        path, _ = QFileDialog.getSaveFileName(
            self, "保存参数", str(self._json_path), "JSON (*.json)"
        )
        if not path:
            return
        self._save_to_json(Path(path))
        self._append_log(f"参数已保存到 {path}")

    def _on_load(self):
        from PyQt5.QtWidgets import QFileDialog

        path, _ = QFileDialog.getOpenFileName(
            self, "加载参数", str(self._json_path.parent), "JSON (*.json)"
        )
        if not path:
            return
        self._load_from_json(Path(path))
        self._append_log(f"参数已从 {path} 加载")
