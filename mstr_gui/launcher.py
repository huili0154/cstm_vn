"""
MSTR 回测启动器 — 参数录入 + 一键运行。

布局: 三列紧凑排列
  左列: 候选股票池 (搜索过滤 + 勾选 + 添加)
  中列: 已选股票组 + 日期范围
  右列: 信号/执行/仓位/引擎参数 + 初始仓位
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
    """在独立线程中运行 MSTR 回测。"""

    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(object, object)  # (BacktestResult, strategy)
    error_signal = pyqtSignal(str)

    def __init__(
        self,
        engine_params: dict,
        strategy_params: dict,
        initial_positions: dict | None = None,
        parent=None,
    ):
        super().__init__(parent)
        self._engine_params = engine_params
        self._strategy_params = strategy_params
        self._initial_positions = initial_positions

    def run(self):
        try:
            from core.datatypes import MatchingMode
            from backtest.engine import BacktestEngine
            from strategies.mstr_strategy import MstrStrategy

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
                volume_limit_ratio=ep["volume_limit_ratio"],
                credit_ratio=ep.get("credit_ratio", 0.0),
                enable_t0=ep.get("enable_t0", False),
            )

            setting = {
                k: v
                for k, v in sp.items()
                if k not in ("symbols", "start_date", "end_date")
            }
            setting["dataset_dir"] = ep["dataset_dir"]

            symbols = sp["symbols"]
            self.log_signal.emit(
                f"创建策略: {len(symbols)} 品种, "
                f"window={setting.get('window')}, "
                f"num_blocks={setting.get('num_blocks')}"
            )
            strategy = MstrStrategy(
                engine=engine,
                strategy_name="MSTR_Backtest",
                symbols=symbols,
                setting=setting,
            )

            init_pos = self._initial_positions
            if init_pos:
                self.log_signal.emit(f"初始仓位: {init_pos}")
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


# ────────────────────────────────────────────────
#  ETF 库存 (来源: dataset/meta/instruments.json)
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
    # 信号参数
    "window": "Ratio 均值/标准差的滚动计算窗口天数。\n越大越平滑，但信号越滞后。",
    "k_threshold": "Score 触发阈值。Score = |dev| / σ，\n超过此值才产生换仓信号。越小越灵敏。",
    "least_bias": "最小偏差过滤。|dev| < least_bias 的信号\n将被忽略，防止在波动极小时频繁换仓。",
    # 执行参数
    "num_blocks": "总资金分块数。资金被等分为 N 份，\n每次换仓占用 1 个 block 的资金。",
    "sub_lots": "每个 block 内拆分的子单数。\n子单数越多，被动挂单越分散，冲击越小。",
    "cooldown_1": "首次冷却 (分钟)。当日第 1 个 Block\n完成后的等待时长，确认首笔交易后的市场反应。",
    "cooldown_2": "后续冷却 (分钟)。当日第 2 个及之后\n每个 Block 完成后的等待时长，持续控制下单节奏。",
    "chase_wait": "追单等待 tick 数。被动单未成交时，\n等待几个 tick 后才开始追单。",
    "block_timeout": "Block 超时 (分钟)。一个 block\n从开始到强制结束的最大时长。",
    "near_optimal_delta": "近优差值。当 Score 接近最优\nceil 的差值在此范围内，视为近优信号。",
    "cutoff": "交易截止时间 (HH:MM:SS)。\n超过此时间不再发起新的 block 换仓。",
    # 仓位参数
    "max_positions": "最大同时持仓品种数量。\n超过则不会买入新品种。",
    "max_single_weight": "单个品种最大资金权重 (0~1)。\n防止单一品种仓位过于集中。",
    "min_cash_reserve": "最低现金保留 (元)。\n账户现金低于此值时不再开新 block。",
    # 引擎参数
    "initial_capital": "回测初始资金 (元)。",
    "rate": "委托手续费率。\nETF 一般为万 0.5 = 0.00005。",
    "slippage": "滑点 (元)。模拟成交价偏移。\nA股 ETF 默认 0。",
    "pricetick": "最小价格变动单位。\nA股 ETF 为 0.001 元。报价精度。",
    "vol_limit": "盘口量比例限制 (0~1)。\n单笔限价单最多匹配盘口挂单量的此比例。",
    "credit_ratio": "信用额度比例 (0~1)。资金不足时，\n可额外使用 总净值×此比例 作为信用额度。",
    "dataset_dir": "Tick/日线数据集的目录路径。\n相对于项目根目录。",
}


# ────────────────────────────────────────────────
#  启动器主面板
# ────────────────────────────────────────────────


class LauncherWidget(QWidget):
    """MSTR 回测参数录入 + 一键启动面板。三列紧凑布局。"""

    backtest_finished = pyqtSignal(object, object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._worker: _BacktestWorker | None = None
        self._json_path = Path(__file__).resolve().parent.parent / "mstr_params.json"
        self._init_ui()
        self._load_from_json()

    # ════════════════════════════════════════════════
    #  UI 构建
    # ════════════════════════════════════════════════

    def _init_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(6)

        # ── 第一排: 候选股票池 | 已选股票+日期 | 信号参数 ──
        row1 = QHBoxLayout()
        row1.setSpacing(6)
        row1.addLayout(self._build_col_candidates(), stretch=3)
        row1.addLayout(self._build_col_selected(), stretch=2)
        row1.addWidget(self._build_grp_signal(), stretch=2)
        root.addLayout(row1, stretch=1)

        # ── 第二排: 执行参数 | 仓位参数+初始仓位 | 引擎参数 ──
        row2 = QHBoxLayout()
        row2.setSpacing(6)
        row2.addWidget(self._build_grp_exec(), stretch=3)
        row2.addLayout(self._build_col_position(), stretch=3)
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

        # 候选列表 (多选)
        self._candidate_list = QListWidget()
        self._candidate_list.setSelectionMode(QListWidget.MultiSelection)
        for ts_code, name in _ALL_ETFS:
            item = QListWidgetItem(f"{ts_code}  {name}")
            item.setData(Qt.UserRole, ts_code)
            self._candidate_list.addItem(item)
        lay.addWidget(self._candidate_list, stretch=1)

        # 添加按钮
        btn_row = QHBoxLayout()
        btn_add = QPushButton("添加选中 →")
        btn_add.clicked.connect(self._on_add_symbols)
        btn_add_all = QPushButton("全部添加 →")
        btn_add_all.clicked.connect(self._on_add_all)
        btn_row.addWidget(btn_add)
        btn_row.addWidget(btn_add_all)
        lay.addLayout(btn_row)

        col.addWidget(grp)
        return col

    def _on_filter_changed(self, text: str):
        keyword = text.strip().lower()
        for i in range(self._candidate_list.count()):
            item = self._candidate_list.item(i)
            visible = keyword in item.text().lower() if keyword else True
            item.setHidden(not visible)

    def _on_add_symbols(self):
        existing = set(self._get_selected_symbols())
        for item in self._candidate_list.selectedItems():
            ts = item.data(Qt.UserRole)
            if ts not in existing:
                self._selected_list.addItem(ts)
                existing.add(ts)
        self._candidate_list.clearSelection()

    def _on_add_all(self):
        existing = set(self._get_selected_symbols())
        for i in range(self._candidate_list.count()):
            item = self._candidate_list.item(i)
            if not item.isHidden():
                ts = item.data(Qt.UserRole)
                if ts not in existing:
                    self._selected_list.addItem(ts)
                    existing.add(ts)

    # ──────────────────────────
    #  中列: 已选股票 + 日期
    # ──────────────────────────

    def _build_col_selected(self) -> QVBoxLayout:
        col = QVBoxLayout()

        # 已选股票组
        grp_sym = QGroupBox("已选股票组")
        lay_sym = QVBoxLayout(grp_sym)
        lay_sym.setContentsMargins(4, 8, 4, 4)
        lay_sym.setSpacing(4)

        self._selected_list = QListWidget()
        lay_sym.addWidget(self._selected_list, stretch=1)

        btn_clear = QPushButton("清除全部")
        btn_clear.clicked.connect(self._selected_list.clear)
        lay_sym.addWidget(btn_clear)

        col.addWidget(grp_sym, stretch=1)

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

    def _get_selected_symbols(self) -> list[str]:
        return [
            self._selected_list.item(i).text()
            for i in range(self._selected_list.count())
        ]

    # ──────────────────────────
    #  第一排右列: 信号参数
    # ──────────────────────────

    def _build_grp_signal(self) -> QGroupBox:
        grp = QGroupBox("信号参数")
        f = QFormLayout(grp)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_window = self._spin_i(5, 500, 20, "window")
        self._sp_k_threshold = self._spin_d(0.0, 5.0, 0.8, 2, 0.05, "k_threshold")
        self._sp_least_bias = self._spin_d(0.0, 0.5, 0.01, 4, 0.001, "least_bias")
        f.addRow("滚动窗口:", self._sp_window)
        f.addRow("偏离阈值:", self._sp_k_threshold)
        f.addRow("最小偏差:", self._sp_least_bias)
        return grp

    # ──────────────────────────
    #  第二排左列: 执行参数
    # ──────────────────────────

    def _build_grp_exec(self) -> QGroupBox:
        grp = QGroupBox("执行参数")
        f = QFormLayout(grp)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_num_blocks = self._spin_i(1, 50, 5, "num_blocks")
        self._sp_sub_lots = self._spin_i(1, 20, 5, "sub_lots")
        self._sp_cooldown_1 = self._spin_i(0, 120, 10, "cooldown_1")
        self._sp_cooldown_2 = self._spin_i(0, 120, 15, "cooldown_2")
        self._sp_chase_wait = self._spin_i(1, 20, 2, "chase_wait")
        self._sp_block_timeout = self._spin_i(1, 120, 20, "block_timeout")
        self._sp_near_optimal = self._spin_d(0.0, 1.0, 0.1, 2, 0.01, "near_optimal_delta")
        self._le_cutoff = QLineEdit("14:55:00")
        self._le_cutoff.setToolTip(_TOOLTIPS["cutoff"])
        f.addRow("资金分块:", self._sp_num_blocks)
        f.addRow("子单批次:", self._sp_sub_lots)
        f.addRow("首次冷却/分:", self._sp_cooldown_1)
        f.addRow("后续冷却/分:", self._sp_cooldown_2)
        f.addRow("追单等待tick:", self._sp_chase_wait)
        f.addRow("Block超时/分:", self._sp_block_timeout)
        f.addRow("近优差值:", self._sp_near_optimal)
        f.addRow("截止时间:", self._le_cutoff)
        return grp

    # ──────────────────────────
    #  第二排中列: 仓位参数 + 初始仓位
    # ──────────────────────────

    def _build_col_position(self) -> QVBoxLayout:
        col = QVBoxLayout()
        col.setSpacing(4)

        # 仓位参数
        grp_pos = QGroupBox("仓位参数")
        f = QFormLayout(grp_pos)
        f.setContentsMargins(4, 8, 4, 4)
        self._sp_max_positions = self._spin_i(1, 20, 3, "max_positions")
        self._sp_max_single_wt = self._spin_d(0.1, 1.0, 0.5, 2, 0.05, "max_single_weight")
        self._sp_min_cash = self._spin_d(0.0, 100000.0, 100.0, 0, 100.0, "min_cash_reserve")
        self._cb_strategy_t0 = QCheckBox("策略启用 T+0")
        self._cb_strategy_t0.setToolTip(
            "策略层 T+0 开关。需引擎层也勾选 T+0，\n"
            "双方都开启时当日买入的仓位可当日卖出。"
        )
        f.addRow("最大持仓品种:", self._sp_max_positions)
        f.addRow("单品种最大权重:", self._sp_max_single_wt)
        f.addRow("最低现金保留:", self._sp_min_cash)
        f.addRow("", self._cb_strategy_t0)
        col.addWidget(grp_pos)

        # 初始仓位
        grp_ipos = QGroupBox("初始仓位 (可选)")
        lay_ipos = QVBoxLayout(grp_ipos)
        lay_ipos.setContentsMargins(4, 8, 4, 4)
        self._ipos_table = QTableWidget(0, 3)
        self._ipos_table.setHorizontalHeaderLabels(["品种代码", "持仓量", "成本价"])
        self._ipos_table.horizontalHeader().setStretchLastSection(True)
        self._ipos_table.setMaximumHeight(80)
        lay_ipos.addWidget(self._ipos_table)
        ipos_btn = QHBoxLayout()
        btn_add_pos = QPushButton("+")
        btn_del_pos = QPushButton("-")
        btn_add_pos.setFixedWidth(32)
        btn_del_pos.setFixedWidth(32)
        btn_add_pos.clicked.connect(self._add_position_row)
        btn_del_pos.clicked.connect(self._del_position_row)
        ipos_btn.addWidget(btn_add_pos)
        ipos_btn.addWidget(btn_del_pos)
        ipos_btn.addStretch()
        lay_ipos.addLayout(ipos_btn)
        col.addWidget(grp_ipos)

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
        self._sp_pricetick = self._spin_d(0.0001, 1.0, 0.001, 4, 0.0001, "pricetick")
        self._sp_vol_limit = self._spin_d(0.0, 1.0, 0.5, 2, 0.05, "vol_limit")
        self._sp_credit_ratio = self._spin_d(0.0, 1.0, 0.0, 2, 0.05, "credit_ratio")
        self._cb_engine_t0 = QCheckBox("允许 T+0")
        self._cb_engine_t0.setToolTip(
            "引擎层 T+0 开关。勾选后当日买入不锁仓，\n"
            "策略层也需勾选才能实际利用 T+0 交易。"
        )
        self._le_dataset = QLineEdit("dataset")
        self._le_dataset.setToolTip(_TOOLTIPS["dataset_dir"])
        self._combo_mode = QComboBox()
        self._combo_mode.addItem("SMART_TICK_DELAY_FILL", "smart_tick_delay_fill")
        self._combo_mode.addItem("TICK_FILL", "tick_fill")
        self._combo_mode.addItem("CLOSE_FILL", "close_fill")
        self._combo_mode.setToolTip(
            "撮合模式:\n"
            "SMART_TICK_DELAY_FILL — 延迟深度撮合（推荐，最真实）\n"
            "TICK_FILL — Tick 即时全量成交（更快但偏乐观）\n"
            "CLOSE_FILL — 日线收盘价模式"
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
    #  SpinBox 工厂 (自动附加 tooltip)
    # ════════════════════════════════════════════════

    def _spin_i(self, mn, mx, val, tip_key: str = "") -> QSpinBox:
        sp = QSpinBox()
        sp.setRange(mn, mx)
        sp.setValue(val)
        if tip_key in _TOOLTIPS:
            sp.setToolTip(_TOOLTIPS[tip_key])
        return sp

    def _spin_d(self, mn, mx, val, dec, step, tip_key: str = "") -> QDoubleSpinBox:
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

    def _add_position_row(self):
        row = self._ipos_table.rowCount()
        self._ipos_table.insertRow(row)
        self._ipos_table.setItem(row, 0, QTableWidgetItem(""))
        self._ipos_table.setItem(row, 1, QTableWidgetItem("0"))
        self._ipos_table.setItem(row, 2, QTableWidgetItem("0.0"))

    def _del_position_row(self):
        row = self._ipos_table.currentRow()
        if row >= 0:
            self._ipos_table.removeRow(row)

    def _collect_initial_positions(self) -> dict[str, tuple[int, float]] | None:
        result = {}
        for row in range(self._ipos_table.rowCount()):
            sym_item = self._ipos_table.item(row, 0)
            vol_item = self._ipos_table.item(row, 1)
            price_item = self._ipos_table.item(row, 2)
            if not sym_item:
                continue
            sym = sym_item.text().strip()
            if not sym:
                continue
            try:
                vol = int(vol_item.text().strip()) if vol_item else 0
                price = float(price_item.text().strip()) if price_item else 0.0
            except ValueError:
                continue
            if vol > 0:
                result[sym] = (vol, price)
        return result if result else None

    # ════════════════════════════════════════════════
    #  参数收集
    # ════════════════════════════════════════════════

    def _collect_params(self) -> tuple[dict, dict] | None:
        symbols = self._get_selected_symbols()
        if not symbols:
            QMessageBox.warning(self, "参数错误", "请至少选择一个品种!")
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
            "symbols": symbols,
            "start_date": self._date_start.date().toString("yyyyMMdd"),
            "end_date": self._date_end.date().toString("yyyyMMdd"),
            "window": self._sp_window.value(),
            "k_threshold": self._sp_k_threshold.value(),
            "least_bias": self._sp_least_bias.value(),
            "num_blocks": self._sp_num_blocks.value(),
            "sub_lots": self._sp_sub_lots.value(),
            "cooldown_1": self._sp_cooldown_1.value(),
            "cooldown_2": self._sp_cooldown_2.value(),
            "chase_wait_ticks": self._sp_chase_wait.value(),
            "block_timeout": self._sp_block_timeout.value(),
            "near_optimal_delta": self._sp_near_optimal.value(),
            "trading_cutoff_str": self._le_cutoff.text().strip(),
            "max_positions": self._sp_max_positions.value(),
            "max_single_weight": self._sp_max_single_wt.value(),
            "min_cash_reserve": self._sp_min_cash.value(),
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
        initial_positions = self._collect_initial_positions()

        self._btn_run.setEnabled(False)
        self._progress.setVisible(True)
        self._log.clear()
        self._append_log("准备启动回测...")

        self._worker = _BacktestWorker(
            engine_params, strategy_params, initial_positions
        )
        self._worker.log_signal.connect(self._append_log)
        self._worker.finished_signal.connect(self._on_finished)
        self._worker.error_signal.connect(self._on_error)
        self._worker.start()

    def _on_finished(self, result, strategy):
        self._progress.setVisible(False)
        self._btn_run.setEnabled(True)
        self._append_log("回测完成。可切换到 [日志查看器] 标签页查看详情。")

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
        block_logs = getattr(strategy, "_block_logs", [])
        summary_lines.append(f"Block总数: {len(block_logs)}")
        self._append_log("\n".join(summary_lines))
        self.backtest_finished.emit(result, strategy)

    def _on_error(self, msg):
        self._progress.setVisible(False)
        self._btn_run.setEnabled(True)
        self._append_log(f"回测出错:\n{msg}")

    def _append_log(self, text: str):
        self._log.appendPlainText(text)

    # ════════════════════════════════════════════════
    #  JSON 持久化 (mstr_params.json)
    # ════════════════════════════════════════════════

    def _save_to_json(self, path: Path | None = None):
        """将当前全部参数写入指定 JSON 文件，默认 mstr_params.json。"""
        params = self._collect_params()
        if params is None:
            return
        target = path or self._json_path
        engine_params, strategy_params = params
        ipos = self._collect_initial_positions()
        data = {
            "engine": engine_params,
            "strategy": strategy_params,
            "initial_positions": {
                sym: {"volume": v, "cost_price": p}
                for sym, (v, p) in ipos.items()
            }
            if ipos
            else {},
        }
        target.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def _load_from_json(self, path: Path | None = None):
        """从指定 JSON 文件恢复界面，默认 mstr_params.json。文件不存在则用默认值。"""
        target = path or self._json_path
        if not target.exists():
            for sym in [
                "510300.SH", "159300.SZ", "510330.SH",
                "159919.SZ", "159925.SZ",
            ]:
                self._selected_list.addItem(sym)
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
        if "enable_t0" in ep:
            self._cb_engine_t0.setChecked(ep["enable_t0"])
        if "matching_mode" in ep:
            idx = self._combo_mode.findData(ep["matching_mode"])
            if idx >= 0:
                self._combo_mode.setCurrentIndex(idx)

        # 策略参数
        if "symbols" in sp:
            self._selected_list.clear()
            for sym in sp["symbols"]:
                self._selected_list.addItem(sym)
        else:
            for sym in [
                "510300.SH", "159300.SZ", "510330.SH",
                "159919.SZ", "159925.SZ",
            ]:
                self._selected_list.addItem(sym)
        if "start_date" in sp:
            self._date_start.setDate(
                QDate.fromString(sp["start_date"], "yyyyMMdd")
            )
        if "end_date" in sp:
            self._date_end.setDate(QDate.fromString(sp["end_date"], "yyyyMMdd"))
        if "window" in sp:
            self._sp_window.setValue(sp["window"])
        if "k_threshold" in sp:
            self._sp_k_threshold.setValue(sp["k_threshold"])
        if "least_bias" in sp:
            self._sp_least_bias.setValue(sp["least_bias"])
        if "num_blocks" in sp:
            self._sp_num_blocks.setValue(sp["num_blocks"])
        if "sub_lots" in sp:
            self._sp_sub_lots.setValue(sp["sub_lots"])
        if "cooldown_1" in sp:
            self._sp_cooldown_1.setValue(sp["cooldown_1"])
        if "cooldown_2" in sp:
            self._sp_cooldown_2.setValue(sp["cooldown_2"])
        if "chase_wait_ticks" in sp:
            self._sp_chase_wait.setValue(sp["chase_wait_ticks"])
        if "block_timeout" in sp:
            self._sp_block_timeout.setValue(sp["block_timeout"])
        if "near_optimal_delta" in sp:
            self._sp_near_optimal.setValue(sp["near_optimal_delta"])
        if "trading_cutoff_str" in sp:
            self._le_cutoff.setText(sp["trading_cutoff_str"])
        if "max_positions" in sp:
            self._sp_max_positions.setValue(sp["max_positions"])
        if "max_single_weight" in sp:
            self._sp_max_single_wt.setValue(sp["max_single_weight"])
        if "min_cash_reserve" in sp:
            self._sp_min_cash.setValue(sp["min_cash_reserve"])
        if "enable_t0" in sp:
            self._cb_strategy_t0.setChecked(sp["enable_t0"])

        # 初始仓位
        ipos_data = data.get("initial_positions", {})
        self._ipos_table.setRowCount(0)
        for sym, info in ipos_data.items():
            row = self._ipos_table.rowCount()
            self._ipos_table.insertRow(row)
            self._ipos_table.setItem(row, 0, QTableWidgetItem(sym))
            self._ipos_table.setItem(
                row, 1, QTableWidgetItem(str(info.get("volume", 0)))
            )
            self._ipos_table.setItem(
                row, 2, QTableWidgetItem(str(info.get("cost_price", 0.0)))
            )

    def hideEvent(self, event):
        """窗口隐藏/关闭时自动保存到 mstr_params.json。"""
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
