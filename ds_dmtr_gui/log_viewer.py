"""
DS_DMTR 回测日志查看器（对齐 MSTR 风格）。
"""

from __future__ import annotations

from datetime import datetime, time as dt_time

import matplotlib
matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg as FigureCanvas,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


def _setup_chinese_font() -> None:
    candidates = ["Microsoft YaHei", "SimHei", "STSong", "SimSun"]
    import matplotlib.font_manager as fm

    available = {f.name for f in fm.fontManager.ttflist}
    for font in candidates:
        if font in available:
            matplotlib.rcParams["font.family"] = font
            break
    matplotlib.rcParams["axes.unicode_minus"] = False


_setup_chinese_font()


class _MplCanvas(FigureCanvas):
    def __init__(self, width=8, height=4, dpi=100, parent=None):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.setParent(parent)


class LogViewerWidget(QWidget):
    """DS_DMTR 回测日志图形化查看器（增强版）。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._result = None
        self._strategy = None
        self._block_logs = []
        self._equity_plot_data = None
        self._equity_zoom_ctx = None
        self._equity_pan_state = {"active": False, "press_px": None, "start_xlim": None}
        self._ratio_plot_data = None
        self._ratio_zoom_ctx = None
        self._ratio_pan_state = {
            "active": False,
            "press_px": None,
            "press_py": None,
            "start_xlim": None,
            "start_ylim": None,
        }
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        self._lbl_status = QLabel("等待回测完成...")
        self._lbl_status.setStyleSheet("font-size: 14px; color: #666;")
        layout.addWidget(self._lbl_status)

        self._tabs = QTabWidget()
        self._tabs.setStyleSheet(
            "QTabBar::tab { font-size: 11px; padding: 5px 10px; }"
        )
        self._tabs.addTab(self._build_summary_tab(), "总览")
        self._tabs.addTab(self._build_block_table_tab(), "Block 跟踪表")
        self._tabs.addTab(self._build_block_detail_tab(), "Block 详情")
        self._tabs.addTab(self._build_equity_tab(), "净值曲线")
        self._tabs.addTab(self._build_ratio_tab(), "ABratio 布林")
        self._tabs.addTab(self._build_trade_table_tab(), "交易明细")
        self._tabs.addTab(self._build_daily_tab(), "日交易可视化")
        layout.addWidget(self._tabs, stretch=1)

    def _build_summary_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        self._summary_text = QTextEdit()
        self._summary_text.setReadOnly(True)
        self._summary_text.setStyleSheet("font-size: 13px;")
        layout.addWidget(self._summary_text)
        return w

    def _build_block_table_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.addWidget(QLabel("提示：双击行可跳转到 Block 详情"))
        self._block_table = QTableWidget()
        self._block_table.setStyleSheet("font-size: 12px;")
        self._block_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._block_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._block_table.cellDoubleClicked.connect(self._on_block_row_clicked)
        layout.addWidget(self._block_table)
        return w

    def _build_block_detail_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        sel_row = QHBoxLayout()
        sel_row.addWidget(QLabel("选择 Block:"))
        self._combo_block = QComboBox()
        self._combo_block.currentIndexChanged.connect(self._on_block_selected)
        sel_row.addWidget(self._combo_block, stretch=1)
        layout.addLayout(sel_row)

        splitter = QSplitter(Qt.Vertical)
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        splitter.addWidget(self._detail_text)

        self._canvas_detail = _MplCanvas(width=10, height=4)
        self._toolbar_detail = NavigationToolbar(self._canvas_detail, w)
        chart_w = QWidget()
        chart_l = QVBoxLayout(chart_w)
        chart_l.setContentsMargins(0, 0, 0, 0)
        chart_l.addWidget(self._toolbar_detail)
        chart_l.addWidget(self._canvas_detail)
        splitter.addWidget(chart_w)

        splitter.setSizes([240, 420])
        layout.addWidget(splitter, stretch=1)
        return w

    def _build_equity_tab(self) -> QWidget:
        w = QWidget()
        layout = QHBoxLayout(w)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        self._canvas_equity = _MplCanvas(width=10, height=6)
        self._canvas_equity.mpl_connect("scroll_event", self._on_equity_scroll)
        self._canvas_equity.mpl_connect("button_press_event", self._on_equity_press)
        self._canvas_equity.mpl_connect("button_release_event", self._on_equity_release)
        self._canvas_equity.mpl_connect("motion_notify_event", self._on_equity_motion)
        self._toolbar_equity = NavigationToolbar(self._canvas_equity, w)
        left_layout.addWidget(self._toolbar_equity)
        left_layout.addWidget(self._canvas_equity, stretch=1)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.addWidget(QLabel("显隐控制 / 图例"))

        self._cb_eq_strategy = QCheckBox("■ 策略净值")
        self._cb_eq_strategy.setChecked(True)
        self._cb_eq_strategy.setStyleSheet("color:#1976D2;")
        self._cb_eq_hold_a = QCheckBox("■ 持有A")
        self._cb_eq_hold_a.setChecked(True)
        self._cb_eq_hold_a.setStyleSheet("color:#FF9800;")
        self._cb_eq_hold_b = QCheckBox("■ 持有B")
        self._cb_eq_hold_b.setChecked(True)
        self._cb_eq_hold_b.setStyleSheet("color:#9C27B0;")
        self._cb_eq_excess_max = QCheckBox("■ 超额收益 max")
        self._cb_eq_excess_max.setChecked(True)
        self._cb_eq_excess_max.setStyleSheet("color:#E91E63;")
        self._cb_eq_excess_mean = QCheckBox("■ 超额收益 mean")
        self._cb_eq_excess_mean.setChecked(True)
        self._cb_eq_excess_mean.setStyleSheet("color:#1976D2;")
        self._cb_eq_dd_max = QCheckBox("■ 超额回撤 max")
        self._cb_eq_dd_max.setChecked(True)
        self._cb_eq_dd_max.setStyleSheet("color:#AD1457;")
        self._cb_eq_dd_mean = QCheckBox("■ 超额回撤 mean")
        self._cb_eq_dd_mean.setChecked(True)
        self._cb_eq_dd_mean.setStyleSheet("color:#0D47A1;")
        self._cb_eq_ysync = QCheckBox("Y-Sync")
        self._cb_eq_ysync.setChecked(True)
        self._btn_eq_fit = QPushButton("适应全图")
        self._btn_eq_fit.clicked.connect(self._fit_equity_full)

        for cb in [
            self._cb_eq_strategy,
            self._cb_eq_hold_a,
            self._cb_eq_hold_b,
            self._cb_eq_excess_max,
            self._cb_eq_excess_mean,
            self._cb_eq_dd_max,
            self._cb_eq_dd_mean,
        ]:
            cb.stateChanged.connect(self._redraw_equity)
            right_layout.addWidget(cb)
        right_layout.addWidget(self._cb_eq_ysync)
        right_layout.addWidget(self._btn_eq_fit)
        right_layout.addStretch(1)

        layout.addWidget(left, stretch=1)
        layout.addWidget(right)
        return w

    def _build_ratio_tab(self) -> QWidget:
        w = QWidget()
        layout = QHBoxLayout(w)

        left = QWidget()
        left_layout = QVBoxLayout(left)
        self._canvas_ratio = _MplCanvas(width=10, height=6)
        self._canvas_ratio.mpl_connect("scroll_event", self._on_ratio_scroll)
        self._canvas_ratio.mpl_connect("button_press_event", self._on_ratio_press)
        self._canvas_ratio.mpl_connect("button_release_event", self._on_ratio_release)
        self._canvas_ratio.mpl_connect("motion_notify_event", self._on_ratio_motion)
        self._toolbar_ratio = NavigationToolbar(self._canvas_ratio, w)
        left_layout.addWidget(self._toolbar_ratio)
        left_layout.addWidget(self._canvas_ratio, stretch=1)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.addWidget(QLabel("显隐控制"))

        self._cb_ratio_daily_price = QCheckBox("■ 日线比值")
        self._cb_ratio_daily_price.setChecked(True)
        self._cb_ratio_daily_price.setStyleSheet("color:#0D47A1;")
        self._cb_ratio_min_price = QCheckBox("■ 分钟比值")
        self._cb_ratio_min_price.setChecked(True)
        self._cb_ratio_min_price.setStyleSheet("color:#90CAF9;")
        self._cb_ratio_day_mid = QCheckBox("■ 日线中轨")
        self._cb_ratio_day_mid.setChecked(True)
        self._cb_ratio_day_mid.setStyleSheet("color:#E65100;")
        self._cb_ratio_day_band = QCheckBox("■ 日线上下轨")
        self._cb_ratio_day_band.setChecked(True)
        self._cb_ratio_day_band.setStyleSheet("color:#FF8A65;")
        self._cb_ratio_min_mid = QCheckBox("■ 分钟中轨")
        self._cb_ratio_min_mid.setChecked(True)
        self._cb_ratio_min_mid.setStyleSheet("color:#2E7D32;")
        self._cb_ratio_min_band = QCheckBox("■ 分钟上下轨")
        self._cb_ratio_min_band.setChecked(True)
        self._cb_ratio_min_band.setStyleSheet("color:#81C784;")
        self._cb_ratio_trades = QCheckBox("◎ 交易点(B/S)")
        self._cb_ratio_trades.setChecked(True)
        self._cb_ratio_trades.setStyleSheet("color:#6A1B9A;")
        self._cb_ratio_ysync = QCheckBox("Y-Sync")
        self._cb_ratio_ysync.setChecked(True)
        self._btn_ratio_fit = QPushButton("适应全图")
        self._btn_ratio_fit.clicked.connect(self._fit_ratio_full)

        for cb in [
            self._cb_ratio_daily_price,
            self._cb_ratio_min_price,
            self._cb_ratio_day_mid,
            self._cb_ratio_day_band,
            self._cb_ratio_min_mid,
            self._cb_ratio_min_band,
            self._cb_ratio_trades,
        ]:
            cb.stateChanged.connect(self._redraw_ratio)
            right_layout.addWidget(cb)
        right_layout.addWidget(self._cb_ratio_ysync)
        right_layout.addWidget(self._btn_ratio_fit)
        right_layout.addStretch(1)

        layout.addWidget(left, stretch=1)
        layout.addWidget(right)
        return w

    def _build_trade_table_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        self._trade_table = QTableWidget()
        self._trade_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._trade_table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self._trade_table)
        return w

    def _build_daily_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        sel_row = QHBoxLayout()
        sel_row.addWidget(QLabel("选择交易日:"))
        self._combo_daily = QComboBox()
        self._combo_daily.currentIndexChanged.connect(self._on_daily_changed)
        sel_row.addWidget(self._combo_daily, stretch=1)
        layout.addLayout(sel_row)

        self._canvas_daily = _MplCanvas(width=12, height=8)
        self._toolbar_daily = NavigationToolbar(self._canvas_daily, w)
        layout.addWidget(self._toolbar_daily)
        layout.addWidget(self._canvas_daily, stretch=1)
        return w

    def load_result(self, result, strategy):
        self._result = result
        self._strategy = strategy
        self._block_logs = getattr(strategy, "_block_logs", [])

        self._lbl_status.setText(
            f"已加载回测结果: {len(result.trades)} 笔成交, {len(self._block_logs)} 个 Block"
        )
        self._lbl_status.setStyleSheet("font-size: 13px; color: #080;")

        self._refresh_summary()
        self._refresh_block_table()
        self._refresh_block_combo()
        self._refresh_equity()
        self._refresh_ratio()
        self._refresh_trade_table()
        self._refresh_daily_combo()

    def _refresh_summary(self):
        from backtest.report import BacktestReport

        report = BacktestReport(self._result)
        stats = report.stats
        strategy = self._strategy
        blocks = self._block_logs
        total_turnover = sum(t.price * t.volume for t in self._result.trades)
        start_balance = float(stats.get("start_balance", 0.0) or 0.0)
        turnover_multiple = (
            total_turnover / start_balance if start_balance > 0 else 0.0
        )

        done = sum(1 for b in blocks if b.state.value == "DONE")
        partial = sum(1 for b in blocks if b.state.value == "PARTIAL")
        timeout = sum(1 for b in blocks if b.state.value == "TIMEOUT")
        critical = sum(1 for b in blocks if b.state.value == "CRITICAL")
        rejected = sum(1 for b in blocks if b.state.value == "REJECTED")
        other = max(0, len(blocks) - done - partial - timeout - critical - rejected)
        done_rate = (done / len(blocks)) if blocks else 0.0

        lines = [
            "═══════ DS_DMTR 回测总览 ═══════",
            "",
            f"  Symbol A:      {getattr(strategy, 'symbol_a', '')}",
            f"  Symbol B:      {getattr(strategy, 'symbol_b', '')}",
            "",
            f"  初始资金:      {stats.get('start_balance', 0):>14,.2f}",
            f"  期末资金:      {stats.get('end_balance', 0):>14,.2f}",
            f"  总收益率:      {stats.get('total_return', 0):>14.2%}",
            f"  年化收益率:    {stats.get('annual_return', 0):>14.2%}",
            f"  最大回撤:      {stats.get('max_drawdown', 0):>14.2%}",
            f"  Sharpe Ratio:  {stats.get('sharpe', 0):>14.3f}",
            "",
            f"  总成交笔数:    {len(self._result.trades):>14d}",
            f"  总交易额:      {total_turnover:>14,.2f}",
            f"  总换手倍数:    {turnover_multiple:>14.2f}x",
            f"  总手续费:      {self._result.total_commission:>14.2f}",
            f"  Block 总数:    {len(blocks):>14d}",
            f"  DONE:          {done:>14d}",
            f"  DONE占比:      {done_rate:>14.2%}",
            f"  PARTIAL:       {partial:>14d}",
            f"  TIMEOUT:       {timeout:>14d}",
            f"  CRITICAL:      {critical:>14d}",
            f"  REJECTED:      {rejected:>14d}",
            f"  OTHER:         {other:>14d}",
        ]
        self._summary_text.setPlainText("\n".join(lines))

    def _refresh_block_table(self):
        headers = [
            "Block ID", "方向", "状态", "卖品种", "买品种", "触发时刻",
            "触发dσ(min)", "触发dσ(day)", "触发δ(min)", "比例",
            "卖信号价", "卖成交均价", "卖期望量", "卖发单量", "卖成交量",
            "买信号价", "买成交均价", "买期望量", "买发单量", "买成交量",
            "信号现金", "结束现金", "信号净值", "结束净值", "耗时(秒)",
        ]
        self._block_table.setColumnCount(len(headers))
        self._block_table.setHorizontalHeaderLabels(headers)
        self._block_table.setRowCount(len(self._block_logs))

        for r, b in enumerate(self._block_logs):
            vals = [
                b.block_id,
                b.direction,
                b.state.value,
                b.sell_symbol,
                b.buy_symbol,
                b.signal_time.strftime("%Y-%m-%d %H:%M:%S") if b.signal_time else "-",
                f"{b.delta_sigma_minutes:.3f}",
                f"{b.delta_sigma_days:.3f}",
                f"{b.delta_minutes:.4%}",
                f"{b.trade_pct:.2%}",
                f"{b.sell_signal_price:.4f}" if b.sell_signal_price > 0 else "-",
                f"{b.sell_avg_price:.4f}" if b.sell_filled > 0 else "-",
                str(b.desired_sell_volume),
                str(b.sell_order_volume),
                str(b.sell_filled),
                f"{b.buy_signal_price:.4f}" if b.buy_signal_price > 0 else "-",
                f"{b.buy_avg_price:.4f}" if b.buy_filled > 0 else "-",
                str(b.desired_buy_volume),
                str(b.buy_order_volume),
                str(b.buy_filled),
                f"{b.signal_cash:,.2f}",
                f"{b.end_cash:,.2f}",
                f"{b.signal_nav:,.2f}",
                f"{b.end_nav:,.2f}",
                f"{b.total_duration:.1f}",
            ]
            for c, txt in enumerate(vals):
                item = QTableWidgetItem(txt)
                if c == 2:
                    if txt == "DONE":
                        item.setBackground(Qt.green)
                    elif txt == "PARTIAL":
                        item.setBackground(Qt.yellow)
                    elif txt == "REJECTED":
                        item.setBackground(Qt.red)
                self._block_table.setItem(r, c, item)

        self._block_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

    def _refresh_block_combo(self):
        self._combo_block.blockSignals(True)
        self._combo_block.clear()
        for b in self._block_logs:
            self._combo_block.addItem(
                f"{b.block_id} [{b.state.value}] {b.sell_symbol} → {b.buy_symbol}"
            )
        self._combo_block.blockSignals(False)
        if self._block_logs:
            self._combo_block.setCurrentIndex(0)
            self._on_block_selected(0)

    def _on_block_row_clicked(self, row, _col):
        if 0 <= row < len(self._block_logs):
            self._combo_block.setCurrentIndex(row)
            self._tabs.setCurrentIndex(2)

    def _on_block_selected(self, idx):
        if idx < 0 or idx >= len(self._block_logs):
            return
        b = self._block_logs[idx]
        lines = [
            f"Block: {b.block_id}",
            f"状态: {b.state.value}  方向: {b.direction}",
            f"时间: {b.signal_time} -> {b.end_time}",
            f"卖: {b.sell_symbol}  信号价={b.sell_signal_price:.4f}  成交均价={b.sell_avg_price:.4f}",
            f"买: {b.buy_symbol}  信号价={b.buy_signal_price:.4f}  成交均价={b.buy_avg_price:.4f}",
            f"卖期望/发单/成交: {b.desired_sell_volume} / {b.sell_order_volume} / {b.sell_filled}",
            f"买期望/发单/成交: {b.desired_buy_volume} / {b.buy_order_volume} / {b.buy_filled}",
            f"卖发单完成率: {(b.sell_filled / b.sell_order_volume):.2%}" if b.sell_order_volume > 0 else "卖发单完成率: -",
            f"买发单完成率: {(b.buy_filled / b.buy_order_volume):.2%}" if b.buy_order_volume > 0 else "买发单完成率: -",
            f"触发指标: dσ_min={b.delta_sigma_minutes:.3f}  dσ_day={b.delta_sigma_days:.3f}  δ_min={b.delta_minutes:.4%}",
            f"账户: signal_cash={b.signal_cash:,.2f}  signal_nav={b.signal_nav:,.2f}",
            f"      end_cash={b.end_cash:,.2f}  end_nav={b.end_nav:,.2f}",
            f"滑点: sell={b.slippage_sell:.4%}  buy={b.slippage_buy:.4%}",
        ]
        self._detail_text.setPlainText("\n".join(lines))

        fig = self._canvas_detail.fig
        fig.clear()
        ax = fig.add_subplot(111)
        labels = ["卖信号", "卖成交", "买信号", "买成交"]
        values = [
            b.sell_signal_price,
            b.sell_avg_price if b.sell_filled > 0 else np.nan,
            b.buy_signal_price,
            b.buy_avg_price if b.buy_filled > 0 else np.nan,
        ]
        colors = ["#F44336", "#FF9800", "#4CAF50", "#1B5E20"]
        ax.bar(labels, values, color=colors, alpha=0.85)
        ax.set_title(f"Block {b.block_id} 价格对比")
        ax.grid(True, alpha=0.25, axis="y")
        fig.tight_layout()
        self._canvas_detail.draw()

    def _refresh_equity(self):
        if not self._result or not self._result.daily_nav:
            fig = self._canvas_equity.fig
            fig.clear()
            self._equity_plot_data = None
            self._equity_zoom_ctx = None
            fig.text(0.5, 0.5, "无净值数据", ha="center", va="center", fontsize=14)
            self._canvas_equity.draw()
            return

        nav = self._result.daily_nav
        dates = [pd.Timestamp(d) for d, _ in nav]
        date_strs = [d for d, _ in nav]
        values = np.array([v for _, v in nav], dtype=float)
        initial = self._result.start_balance

        bh_curves = {}
        if self._strategy and hasattr(self._strategy, "_all_daily"):
            all_daily = self._strategy._all_daily
            for symbol in [self._strategy.symbol_a, self._strategy.symbol_b]:
                daily = all_daily.get(symbol, {})
                if not daily:
                    continue
                first_close = None
                for ds in date_strs:
                    if ds in daily:
                        first_close = daily[ds][0]
                        break
                if first_close is None or first_close <= 0:
                    continue
                curve = []
                for ds in date_strs:
                    if ds in daily:
                        close_adj = daily[ds][0]
                        curve.append(initial * close_adj / first_close)
                    else:
                        curve.append(curve[-1] if curve else initial)
                bh_curves[symbol] = curve

        hold_symbols = list(bh_curves.keys())
        hold_a = np.array(bh_curves.get(self._strategy.symbol_a, []), dtype=float)
        hold_b = np.array(bh_curves.get(self._strategy.symbol_b, []), dtype=float)
        excess = {}
        if bh_curves:
            bh_matrix = np.array([np.array(v, dtype=float) for v in bh_curves.values()])
            bh_max = bh_matrix.max(axis=0)
            bh_mean = bh_matrix.mean(axis=0)
            excess_max = (values - bh_max) / bh_max
            excess_mean = (values - bh_mean) / bh_mean
            excess_mean = excess_mean - excess_mean[0]
            peak_max = np.maximum.accumulate(excess_max)
            peak_mean = np.maximum.accumulate(excess_mean)
            dd_max = excess_max - peak_max
            dd_mean = excess_mean - peak_mean
            excess = {
                "excess_max": np.array(excess_max, dtype=float),
                "excess_mean": np.array(excess_mean, dtype=float),
                "dd_max": np.array(dd_max, dtype=float),
                "dd_mean": np.array(dd_mean, dtype=float),
            }
        else:
            peak = np.maximum.accumulate(values)
            dd = (values - peak) / peak
            excess = {"dd_only": np.array(dd, dtype=float)}

        if self._strategy:
            a_short = self._strategy.symbol_a.split(".")[0]
            b_short = self._strategy.symbol_b.split(".")[0]
            self._cb_eq_hold_a.setText(f"■ 持有 {a_short}")
            self._cb_eq_hold_b.setText(f"■ 持有 {b_short}")

        self._cb_eq_hold_a.setEnabled(len(hold_a) == len(values) and self._strategy.symbol_a in hold_symbols)
        self._cb_eq_hold_b.setEnabled(len(hold_b) == len(values) and self._strategy.symbol_b in hold_symbols)
        has_excess = "excess_mean" in excess
        self._cb_eq_excess_max.setEnabled(has_excess)
        self._cb_eq_excess_mean.setEnabled(has_excess)
        self._cb_eq_dd_max.setEnabled(has_excess)
        self._cb_eq_dd_mean.setEnabled(has_excess)

        self._equity_plot_data = {
            "dates": dates,
            "x": np.asarray(mdates.date2num(dates), dtype=float),
            "initial": float(initial),
            "strategy_nav": values,
            "hold_a": hold_a if len(hold_a) == len(values) else None,
            "hold_b": hold_b if len(hold_b) == len(values) else None,
            "excess": excess,
        }
        self._redraw_equity()

    def _redraw_equity(self):
        old_xlim = None
        if self._equity_zoom_ctx and self._equity_zoom_ctx.get("ax_top"):
            old_xlim = self._equity_zoom_ctx["ax_top"].get_xlim()
        fig = self._canvas_equity.fig
        fig.clear()
        data = self._equity_plot_data
        if not data:
            fig.text(0.5, 0.5, "无净值数据", ha="center", va="center", fontsize=14)
            self._canvas_equity.draw()
            return

        dates = data["dates"]
        x = data["x"]
        ax1 = fig.add_subplot(2, 1, 1)
        top_series = []
        if self._cb_eq_strategy.isChecked():
            ax1.plot(dates, data["strategy_nav"], color="#1976D2", linewidth=1.6)
            top_series.append(np.asarray(data["strategy_nav"], dtype=float))
        if self._cb_eq_hold_a.isChecked() and data["hold_a"] is not None:
            ax1.plot(dates, data["hold_a"], color="#FF9800", linewidth=1.0, linestyle="--", alpha=0.85)
            top_series.append(np.asarray(data["hold_a"], dtype=float))
        if self._cb_eq_hold_b.isChecked() and data["hold_b"] is not None:
            ax1.plot(dates, data["hold_b"], color="#9C27B0", linewidth=1.0, linestyle="--", alpha=0.85)
            top_series.append(np.asarray(data["hold_b"], dtype=float))
        ax1.axhline(data["initial"], color="#9E9E9E", linestyle=":", linewidth=0.6)
        ax1.set_title("净值曲线 vs 买入并持有")
        ax1.set_ylabel("净值")
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

        ax2 = fig.add_subplot(2, 1, 2, sharex=ax1)
        bottom_series = []
        ex = data["excess"]
        if "excess_mean" in ex:
            if self._cb_eq_excess_max.isChecked():
                ax2.plot(dates, ex["excess_max"], color="#E91E63", linewidth=1.1)
                bottom_series.append(np.asarray(ex["excess_max"], dtype=float))
            if self._cb_eq_excess_mean.isChecked():
                ax2.plot(dates, ex["excess_mean"], color="#1976D2", linewidth=1.1)
                bottom_series.append(np.asarray(ex["excess_mean"], dtype=float))
            if self._cb_eq_dd_max.isChecked():
                ax2.plot(dates, ex["dd_max"], color="#AD1457", linewidth=0.9, linestyle="--")
                ax2.fill_between(dates, ex["dd_max"], color="#E91E63", alpha=0.12)
                bottom_series.append(np.asarray(ex["dd_max"], dtype=float))
            if self._cb_eq_dd_mean.isChecked():
                ax2.plot(dates, ex["dd_mean"], color="#0D47A1", linewidth=0.9, linestyle="--")
                ax2.fill_between(dates, ex["dd_mean"], color="#1976D2", alpha=0.12)
                bottom_series.append(np.asarray(ex["dd_mean"], dtype=float))
        else:
            dd = ex["dd_only"]
            ax2.plot(dates, dd, color="#F44336", linewidth=1.0)
            ax2.fill_between(dates, dd, color="#F44336", alpha=0.2)
            bottom_series.append(np.asarray(dd, dtype=float))

        ax2.axhline(0, color="#9E9E9E", linestyle=":", linewidth=0.6)
        ax2.set_title("超额收益 & 超额回撤")
        ax2.set_ylabel("比例")
        ax2.grid(True, alpha=0.3)
        ax2.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))

        self._equity_zoom_ctx = {
            "ax_top": ax1,
            "ax_bottom": ax2,
            "x": x,
            "x_min_bound": float(x.min()),
            "x_max_bound": float(x.max()),
            "top_series": top_series if top_series else [np.asarray(data["strategy_nav"], dtype=float)],
            "bottom_series": bottom_series if bottom_series else [np.zeros_like(x)],
            "min_span": max(2.0, float(len(x)) * 0.03),
        }

        if old_xlim is not None:
            left, right = self._clamp_xlim(
                old_xlim,
                self._equity_zoom_ctx["x_min_bound"],
                self._equity_zoom_ctx["x_max_bound"],
                self._equity_zoom_ctx["min_span"],
            )
            self._equity_zoom_ctx["ax_top"].set_xlim(left, right)
            if self._cb_eq_ysync.isChecked():
                self._rescale_equity_y(left, right)
        else:
            self._fit_equity_full()

        fig.autofmt_xdate()
        fig.tight_layout()
        self._canvas_equity.draw()

    def _fit_equity_full(self):
        ctx = self._equity_zoom_ctx
        if not ctx:
            return
        ctx["ax_top"].set_xlim(ctx["x_min_bound"], ctx["x_max_bound"])
        self._rescale_equity_y(ctx["x_min_bound"], ctx["x_max_bound"])
        self._canvas_equity.draw_idle()

    def _on_equity_scroll(self, event):
        ctx = self._equity_zoom_ctx
        if not ctx:
            return
        if event.inaxes not in {ctx["ax_top"], ctx["ax_bottom"]}:
            return
        scale = 0.8 if event.button == "up" else 1.25
        if self._event_has_ctrl(event):
            center_y = float(event.ydata) if event.ydata is not None else float(np.mean(event.inaxes.get_ylim()))
            self._zoom_axis_y(event.inaxes, center_y, scale)
            self._canvas_equity.draw_idle()
            return
        if event.xdata is None:
            return
        left, right = self._zoom_x_range(
            ctx["ax_top"].get_xlim(),
            float(event.xdata),
            scale,
            ctx["x_min_bound"],
            ctx["x_max_bound"],
            ctx["min_span"],
        )
        ctx["ax_top"].set_xlim(left, right)
        if self._cb_eq_ysync.isChecked():
            self._rescale_equity_y(left, right)
        self._canvas_equity.draw_idle()

    def _on_equity_press(self, event):
        if self._toolbar_equity.mode:
            return
        ctx = self._equity_zoom_ctx
        if not ctx or event.inaxes not in {ctx["ax_top"], ctx["ax_bottom"]}:
            return
        if event.button != 1 or event.xdata is None:
            return
        self._equity_pan_state["active"] = True
        self._equity_pan_state["press_px"] = float(event.x)
        self._equity_pan_state["start_xlim"] = ctx["ax_top"].get_xlim()

    def _on_equity_release(self, _event):
        self._equity_pan_state["active"] = False
        self._equity_pan_state["press_px"] = None
        self._equity_pan_state["start_xlim"] = None
        if self._cb_eq_ysync.isChecked() and self._equity_zoom_ctx:
            left, right = self._equity_zoom_ctx["ax_top"].get_xlim()
            self._rescale_equity_y(left, right)
            self._canvas_equity.draw_idle()

    def _on_equity_motion(self, event):
        if not self._equity_pan_state["active"]:
            return
        ctx = self._equity_zoom_ctx
        if not ctx or event.inaxes not in {ctx["ax_top"], ctx["ax_bottom"]}:
            return
        if event.x is None:
            return
        press_px = self._equity_pan_state["press_px"]
        start_xlim = self._equity_pan_state["start_xlim"]
        if press_px is None or start_xlim is None:
            return
        ax = ctx["ax_top"]
        p0 = ax.transData.transform((start_xlim[0], 0.0))[0]
        p1 = ax.transData.transform((start_xlim[1], 0.0))[0]
        shift_px = float(event.x) - float(press_px)
        new_left = ax.transData.inverted().transform((p0 - shift_px, 0.0))[0]
        new_right = ax.transData.inverted().transform((p1 - shift_px, 0.0))[0]
        left, right = self._shift_x_range(
            (new_left, new_right), 0.0, ctx["x_min_bound"], ctx["x_max_bound"]
        )
        ctx["ax_top"].set_xlim(left, right)
        self._canvas_equity.draw_idle()

    def _rescale_equity_y(self, left: float, right: float):
        ctx = self._equity_zoom_ctx
        if not ctx:
            return
        x = ctx["x"]
        mask = (x >= left) & (x <= right)
        if not mask.any():
            return

        def _apply_ylim(ax, series_list):
            vals = []
            for s in series_list:
                part = s[mask]
                part = part[np.isfinite(part)]
                if part.size:
                    vals.append(part)
            if not vals:
                return
            merged = np.concatenate(vals)
            vmin = float(np.min(merged))
            vmax = float(np.max(merged))
            if np.isclose(vmin, vmax):
                pad = max(abs(vmax) * 0.02, 1e-6)
            else:
                pad = (vmax - vmin) * 0.08
            ax.set_ylim(vmin - pad, vmax + pad)

        _apply_ylim(ctx["ax_top"], ctx["top_series"])
        _apply_ylim(ctx["ax_bottom"], ctx["bottom_series"])

    def _refresh_ratio(self):
        strategy = self._strategy
        if not strategy or not self._result:
            fig = self._canvas_ratio.fig
            fig.clear()
            self._ratio_plot_data = None
            self._ratio_zoom_ctx = None
            fig.text(0.5, 0.5, "无数据", ha="center", va="center", fontsize=14)
            self._canvas_ratio.draw()
            return

        symbol_a = getattr(strategy, "symbol_a", "")
        symbol_b = getattr(strategy, "symbol_b", "")
        all_daily_a = strategy._all_daily.get(symbol_a, {})
        all_daily_b = strategy._all_daily.get(symbol_b, {})
        if not all_daily_a or not all_daily_b:
            fig = self._canvas_ratio.fig
            fig.clear()
            self._ratio_plot_data = None
            self._ratio_zoom_ctx = None
            fig.text(0.5, 0.5, "无日线数据", ha="center", va="center", fontsize=14)
            self._canvas_ratio.draw()
            return

        nav_dates = [d for d, _ in self._result.daily_nav]
        if not nav_dates:
            fig = self._canvas_ratio.fig
            fig.clear()
            self._ratio_plot_data = None
            self._ratio_zoom_ctx = None
            fig.text(0.5, 0.5, "无净值日期范围", ha="center", va="center", fontsize=14)
            self._canvas_ratio.draw()
            return
        start_date = nav_dates[0]
        end_date = nav_dates[-1]

        all_common_dates = sorted(set(all_daily_a.keys()) & set(all_daily_b.keys()))
        common_dates = [d for d in all_common_dates if start_date <= d <= end_date]
        day_ratios = []
        day_dates = []
        for d in common_dates:
            ca, _ = all_daily_a[d]
            cb, _ = all_daily_b[d]
            if cb > 0:
                day_ratios.append(ca / cb)
                day_dates.append(pd.Timestamp(datetime.strptime(d, "%Y%m%d")))

        window = getattr(strategy, "window_days", 20)
        k_sigma = getattr(strategy, "k_sigma_days", 2.0)
        min_window = getattr(strategy, "window_minutes", 20)
        min_k_sigma = getattr(strategy, "k_sigma_minutes", 2.0)

        day_arr = np.array(day_ratios, dtype=float)
        if len(day_arr) > 0:
            pre_dates = [d for d in all_common_dates if d < start_date]
            pre_keep = max(window * 3, window)
            pre_dates = pre_dates[-pre_keep:]
            pre_ratios = []
            for d in pre_dates:
                ca, _ = all_daily_a[d]
                cb, _ = all_daily_b[d]
                if cb > 0:
                    pre_ratios.append(ca / cb)
            ext = np.array(pre_ratios + day_ratios, dtype=float)
            ext_mid = pd.Series(ext).rolling(window, min_periods=1).mean().to_numpy()
            ext_std = (
                pd.Series(ext).rolling(window, min_periods=1).std(ddof=0).fillna(0.0).to_numpy()
            )
            day_mid = ext_mid[-len(day_arr):]
            day_std = ext_std[-len(day_arr):]
            day_upper = day_mid + k_sigma * day_std
            day_lower = day_mid - k_sigma * day_std
        else:
            day_mid = np.array([])
            day_upper = np.array([])
            day_lower = np.array([])

        pre_dates_min = [d for d in all_common_dates if d < start_date]
        pre_keep_min = max(min_window * 3, min_window)
        pre_dates_min = pre_dates_min[-pre_keep_min:]
        ext_dates_min = pre_dates_min + common_dates
        ext_min_dates, ext_min_arr = self._build_minute_ratio_series(
            symbol_a, symbol_b, ext_dates_min, getattr(strategy, "bar_interval_minutes", 30)
        )
        if len(ext_min_arr) > 0:
            ext_mid = (
                pd.Series(ext_min_arr).rolling(min_window, min_periods=1).mean().to_numpy()
            )
            ext_std = (
                pd.Series(ext_min_arr).rolling(min_window, min_periods=1).std(ddof=0).fillna(0.0).to_numpy()
            )
            min_dates = []
            min_vals = []
            min_mid_vals = []
            min_upper_vals = []
            min_lower_vals = []
            for i, ts in enumerate(ext_min_dates):
                ds = ts.strftime("%Y%m%d")
                if start_date <= ds <= end_date:
                    v = float(ext_min_arr[i])
                    m = float(ext_mid[i])
                    s = float(ext_std[i])
                    min_dates.append(ts)
                    min_vals.append(v)
                    min_mid_vals.append(m)
                    min_upper_vals.append(m + min_k_sigma * s)
                    min_lower_vals.append(m - min_k_sigma * s)
            min_arr = np.array(min_vals, dtype=float)
            min_mid = np.array(min_mid_vals, dtype=float)
            min_upper = np.array(min_upper_vals, dtype=float)
            min_lower = np.array(min_lower_vals, dtype=float)
        else:
            min_dates = []
            min_arr = np.array([])
            min_mid = np.array([])
            min_upper = np.array([])
            min_lower = np.array([])

        trade_buy_sig_x = []
        trade_buy_sig_y = []
        trade_sell_sig_x = []
        trade_sell_sig_y = []
        trade_buy_fill_x = []
        trade_buy_fill_y = []
        trade_sell_fill_x = []
        trade_sell_fill_y = []
        trade_link_x0 = []
        trade_link_y0 = []
        trade_link_x1 = []
        trade_link_y1 = []
        trade_link_alert = []
        trade_fill_alert_x = []
        trade_fill_alert_y = []
        trade_fill_norm_x = []
        trade_fill_norm_y = []
        if len(min_dates) > 0:
            x_min = np.array(mdates.date2num(min_dates), dtype=float)
            for b in self._block_logs:
                signal_time = b.signal_time
                if not signal_time:
                    continue
                ds = signal_time.strftime("%Y%m%d")
                if ds < start_date or ds > end_date:
                    continue
                x_sig = mdates.date2num(pd.Timestamp(signal_time))
                idx_sig = int(np.argmin(np.abs(x_min - x_sig)))
                y_sig = float(b.ab_ratio) if float(b.ab_ratio) > 0 else min_arr[idx_sig]
                if not np.isfinite(y_sig) or y_sig <= 0:
                    continue
                ratio_fill = np.nan
                if b.sell_filled > 0 and b.buy_filled > 0 and b.sell_avg_price > 0 and b.buy_avg_price > 0:
                    if b.direction == "SELL_A_BUY_B":
                        ratio_fill = b.sell_avg_price / b.buy_avg_price
                    elif b.direction == "SELL_B_BUY_A":
                        ratio_fill = b.buy_avg_price / b.sell_avg_price
                sell_notional = float(b.sell_filled) * float(b.sell_avg_price) if b.sell_filled > 0 and b.sell_avg_price > 0 else 0.0
                buy_notional = float(b.buy_filled) * float(b.buy_avg_price) if b.buy_filled > 0 and b.buy_avg_price > 0 else 0.0
                notional_gap = abs(sell_notional - buy_notional)
                max_ref_price = max(float(b.sell_avg_price), float(b.buy_avg_price), 0.0)
                is_unbalanced = (
                    sell_notional > 0
                    and buy_notional > 0
                    and max_ref_price > 0
                    and notional_gap > max_ref_price * 100
                )
                if b.direction == "SELL_B_BUY_A":
                    trade_buy_sig_x.append(pd.Timestamp(signal_time))
                    trade_buy_sig_y.append(float(y_sig))
                    if np.isfinite(ratio_fill) and ratio_fill > 0 and b.end_time:
                        is_alert = (
                            b.state.value in {"TIMEOUT", "CRITICAL"}
                            or is_unbalanced
                        )
                        trade_buy_fill_x.append(pd.Timestamp(b.end_time))
                        trade_buy_fill_y.append(float(ratio_fill))
                        if is_alert:
                            trade_fill_alert_x.append(pd.Timestamp(b.end_time))
                            trade_fill_alert_y.append(float(ratio_fill))
                        else:
                            trade_fill_norm_x.append(pd.Timestamp(b.end_time))
                            trade_fill_norm_y.append(float(ratio_fill))
                        trade_link_x0.append(pd.Timestamp(signal_time))
                        trade_link_y0.append(float(y_sig))
                        trade_link_x1.append(pd.Timestamp(b.end_time))
                        trade_link_y1.append(float(ratio_fill))
                        trade_link_alert.append(is_alert)
                elif b.direction == "SELL_A_BUY_B":
                    trade_sell_sig_x.append(pd.Timestamp(signal_time))
                    trade_sell_sig_y.append(float(y_sig))
                    if np.isfinite(ratio_fill) and ratio_fill > 0 and b.end_time:
                        is_alert = (
                            b.state.value in {"TIMEOUT", "CRITICAL"}
                            or is_unbalanced
                        )
                        trade_sell_fill_x.append(pd.Timestamp(b.end_time))
                        trade_sell_fill_y.append(float(ratio_fill))
                        if is_alert:
                            trade_fill_alert_x.append(pd.Timestamp(b.end_time))
                            trade_fill_alert_y.append(float(ratio_fill))
                        else:
                            trade_fill_norm_x.append(pd.Timestamp(b.end_time))
                            trade_fill_norm_y.append(float(ratio_fill))
                        trade_link_x0.append(pd.Timestamp(signal_time))
                        trade_link_y0.append(float(y_sig))
                        trade_link_x1.append(pd.Timestamp(b.end_time))
                        trade_link_y1.append(float(ratio_fill))
                        trade_link_alert.append(is_alert)

        self._ratio_plot_data = {
            "symbol_a": symbol_a,
            "symbol_b": symbol_b,
            "day_dates": day_dates,
            "day_price": day_arr,
            "day_mid": day_mid,
            "day_upper": day_upper,
            "day_lower": day_lower,
            "min_dates": min_dates,
            "min_price": min_arr,
            "min_mid": min_mid,
            "min_upper": min_upper,
            "min_lower": min_lower,
            "trade_buy_sig_x": trade_buy_sig_x,
            "trade_buy_sig_y": np.array(trade_buy_sig_y, dtype=float),
            "trade_sell_sig_x": trade_sell_sig_x,
            "trade_sell_sig_y": np.array(trade_sell_sig_y, dtype=float),
            "trade_buy_fill_x": trade_buy_fill_x,
            "trade_buy_fill_y": np.array(trade_buy_fill_y, dtype=float),
            "trade_sell_fill_x": trade_sell_fill_x,
            "trade_sell_fill_y": np.array(trade_sell_fill_y, dtype=float),
            "trade_link_x0": trade_link_x0,
            "trade_link_y0": np.array(trade_link_y0, dtype=float),
            "trade_link_x1": trade_link_x1,
            "trade_link_y1": np.array(trade_link_y1, dtype=float),
            "trade_link_alert": np.array(trade_link_alert, dtype=bool),
            "trade_fill_alert_x": trade_fill_alert_x,
            "trade_fill_alert_y": np.array(trade_fill_alert_y, dtype=float),
            "trade_fill_norm_x": trade_fill_norm_x,
            "trade_fill_norm_y": np.array(trade_fill_norm_y, dtype=float),
        }
        self._redraw_ratio()

    def _build_minute_ratio_series(
        self,
        symbol_a: str,
        symbol_b: str,
        dates: list[str],
        interval: int,
    ) -> tuple[list[pd.Timestamp], np.ndarray]:
        if not dates:
            return [], np.array([])
        dataset_dir = getattr(self._strategy, "dataset_dir", "")
        if not dataset_dir:
            return [], np.array([])

        from core.data_feed import ParquetTickFeed
        feed = ParquetTickFeed(dataset_dir)
        out_dates: list[pd.Timestamp] = []
        out_values: list[float] = []

        for ds in dates:
            ticks_a = feed.load_day(symbol_a, ds)
            ticks_b = feed.load_day(symbol_b, ds)
            if not ticks_a or not ticks_b:
                continue
            bi = 0
            latest_b = None
            cur_slot = None
            cur_close = 0.0
            cur_dt = None

            for ta in ticks_a:
                if ta.last_price <= 0 or ta.cum_volume <= 0:
                    continue
                while bi < len(ticks_b) and ticks_b[bi].datetime <= ta.datetime:
                    tb = ticks_b[bi]
                    if tb.last_price > 0 and tb.cum_volume > 0:
                        latest_b = tb
                    bi += 1
                if latest_b is None or latest_b.last_price <= 0:
                    continue
                slot = self._bar_slot_for_ratio(ta.datetime, interval)
                if slot is None:
                    continue
                ratio = ta.last_price / latest_b.last_price
                if cur_slot is None:
                    cur_slot = slot
                    cur_close = ratio
                    cur_dt = ta.datetime
                    continue
                if slot == cur_slot:
                    cur_close = ratio
                    continue
                out_dates.append(pd.Timestamp(cur_dt))
                out_values.append(cur_close)
                cur_slot = slot
                cur_close = ratio
                cur_dt = ta.datetime

            if cur_slot is not None and cur_dt is not None:
                out_dates.append(pd.Timestamp(cur_dt))
                out_values.append(cur_close)

        return out_dates, np.array(out_values, dtype=float)

    def _bar_slot_for_ratio(self, dt: datetime, interval: int):
        t = dt.time()
        if t < dt_time(9, 30) or t > dt_time(15, 0):
            return None
        m = t.hour * 60 + t.minute
        am_start = 9 * 60 + 30
        am_end = 11 * 60 + 30
        pm_start = 13 * 60
        if m < am_end:
            trading_minutes = m - am_start
        elif m < pm_start:
            trading_minutes = am_end - am_start - 1
        else:
            trading_minutes = (am_end - am_start) + (m - pm_start)
        day_offset = dt.toordinal() * 240
        return day_offset + trading_minutes // interval

    def _redraw_ratio(self):
        old_xlim = None
        if self._ratio_zoom_ctx and self._ratio_zoom_ctx.get("ax"):
            old_xlim = self._ratio_zoom_ctx["ax"].get_xlim()
        fig = self._canvas_ratio.fig
        fig.clear()
        data = self._ratio_plot_data
        if not data:
            fig.text(0.5, 0.5, "无数据", ha="center", va="center", fontsize=14)
            self._canvas_ratio.draw()
            return

        ax = fig.add_subplot(111)
        draw_series = []

        if self._cb_ratio_daily_price.isChecked() and len(data["day_dates"]) > 0:
            ax.plot(data["day_dates"], data["day_price"], color="#0D47A1", linewidth=1.0, label="日线比值")
            draw_series.append((np.array(mdates.date2num(data["day_dates"]), dtype=float), np.array(data["day_price"], dtype=float)))
        if self._cb_ratio_min_price.isChecked() and len(data["min_dates"]) > 0:
            ax.plot(data["min_dates"], data["min_price"], color="#90CAF9", linewidth=0.9, alpha=0.9, label="分钟比值")
            draw_series.append((np.array(mdates.date2num(data["min_dates"]), dtype=float), np.array(data["min_price"], dtype=float)))
        if self._cb_ratio_day_mid.isChecked() and len(data["day_dates"]) > 0:
            ax.plot(data["day_dates"], data["day_mid"], color="#E65100", linewidth=1.1, linestyle="--", label="日线中轨")
            draw_series.append((np.array(mdates.date2num(data["day_dates"]), dtype=float), np.array(data["day_mid"], dtype=float)))
        if self._cb_ratio_day_band.isChecked() and len(data["day_dates"]) > 0:
            ax.plot(data["day_dates"], data["day_upper"], color="#FF8A65", linewidth=0.9, linestyle=":", label="日线上轨")
            ax.plot(data["day_dates"], data["day_lower"], color="#FF8A65", linewidth=0.9, linestyle=":", label="日线下轨")
            draw_series.append((np.array(mdates.date2num(data["day_dates"]), dtype=float), np.array(data["day_upper"], dtype=float)))
            draw_series.append((np.array(mdates.date2num(data["day_dates"]), dtype=float), np.array(data["day_lower"], dtype=float)))
        if self._cb_ratio_min_mid.isChecked() and len(data["min_dates"]) > 0:
            ax.plot(data["min_dates"], data["min_mid"], color="#2E7D32", linewidth=1.0, linestyle="--", label="分钟中轨")
            draw_series.append((np.array(mdates.date2num(data["min_dates"]), dtype=float), np.array(data["min_mid"], dtype=float)))
        if self._cb_ratio_min_band.isChecked() and len(data["min_dates"]) > 0:
            ax.plot(data["min_dates"], data["min_upper"], color="#81C784", linewidth=0.8, linestyle=":", label="分钟上轨")
            ax.plot(data["min_dates"], data["min_lower"], color="#81C784", linewidth=0.8, linestyle=":", label="分钟下轨")
            draw_series.append((np.array(mdates.date2num(data["min_dates"]), dtype=float), np.array(data["min_upper"], dtype=float)))
            draw_series.append((np.array(mdates.date2num(data["min_dates"]), dtype=float), np.array(data["min_lower"], dtype=float)))

        if self._cb_ratio_trades.isChecked():
            if len(data["trade_buy_sig_x"]) > 0:
                ax.scatter(
                    data["trade_buy_sig_x"], data["trade_buy_sig_y"],
                    s=58, facecolors="none", edgecolors="#D32F2F", linewidths=1.2, zorder=6
                )
                for x0, y0 in zip(data["trade_buy_sig_x"], data["trade_buy_sig_y"]):
                    ax.text(x0, y0, "B", color="#D32F2F", fontsize=6, ha="center", va="center", zorder=7)
                draw_series.append((np.array(mdates.date2num(data["trade_buy_sig_x"]), dtype=float), np.array(data["trade_buy_sig_y"], dtype=float)))
            if len(data["trade_sell_sig_x"]) > 0:
                ax.scatter(
                    data["trade_sell_sig_x"], data["trade_sell_sig_y"],
                    s=58, facecolors="none", edgecolors="#2E7D32", linewidths=1.2, zorder=6
                )
                for x0, y0 in zip(data["trade_sell_sig_x"], data["trade_sell_sig_y"]):
                    ax.text(x0, y0, "S", color="#2E7D32", fontsize=6, ha="center", va="center", zorder=7)
                draw_series.append((np.array(mdates.date2num(data["trade_sell_sig_x"]), dtype=float), np.array(data["trade_sell_sig_y"], dtype=float)))
            if len(data["trade_link_x0"]) > 0:
                for x0, y0, x1, y1, is_alert in zip(
                    data["trade_link_x0"],
                    data["trade_link_y0"],
                    data["trade_link_x1"],
                    data["trade_link_y1"],
                    data["trade_link_alert"],
                ):
                    c = "#EC407A" if bool(is_alert) else "#424242"
                    ax.plot([x0, x1], [y0, y1], color=c, linewidth=0.8, alpha=0.85, zorder=5)
                draw_series.append((np.array(mdates.date2num(data["trade_link_x1"]), dtype=float), np.array(data["trade_link_y1"], dtype=float)))
            if len(data["trade_fill_norm_x"]) > 0:
                ax.scatter(
                    data["trade_fill_norm_x"], data["trade_fill_norm_y"],
                    s=12, c="#000000", linewidths=0.0, zorder=8
                )
                draw_series.append((np.array(mdates.date2num(data["trade_fill_norm_x"]), dtype=float), np.array(data["trade_fill_norm_y"], dtype=float)))
            if len(data["trade_fill_alert_x"]) > 0:
                ax.scatter(
                    data["trade_fill_alert_x"], data["trade_fill_alert_y"],
                    s=14, c="#EC407A", linewidths=0.0, zorder=9
                )
                draw_series.append((np.array(mdates.date2num(data["trade_fill_alert_x"]), dtype=float), np.array(data["trade_fill_alert_y"], dtype=float)))

        short_a = data["symbol_a"].split(".")[0]
        short_b = data["symbol_b"].split(".")[0]
        ax.set_title(f"ABratio 日线+分钟布林 ({short_a} / {short_b})")
        ax.set_ylabel("ABratio")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

        x_all = []
        for x, _ in draw_series:
            if len(x) > 0:
                x_all.append(x)
        if x_all:
            x_cat = np.concatenate(x_all)
            x_min = float(np.min(x_cat))
            x_max = float(np.max(x_cat))
        else:
            x_min = x_max = 0.0
        self._ratio_zoom_ctx = {
            "ax": ax,
            "series": draw_series,
            "x_min_bound": x_min,
            "x_max_bound": x_max,
            "min_span": max(5.0 / 86400.0, (x_max - x_min) * 0.00001 if x_max > x_min else 5.0 / 86400.0),
        }

        if old_xlim is not None:
            left, right = self._clamp_xlim(
                old_xlim,
                self._ratio_zoom_ctx["x_min_bound"],
                self._ratio_zoom_ctx["x_max_bound"],
                self._ratio_zoom_ctx["min_span"],
            )
            self._ratio_zoom_ctx["ax"].set_xlim(left, right)
            if self._cb_ratio_ysync.isChecked():
                self._rescale_ratio_y(left, right)
        else:
            self._fit_ratio_full(draw=False)
        fig.autofmt_xdate()
        fig.tight_layout()
        self._canvas_ratio.draw()

    def _on_ratio_scroll(self, event):
        ctx = self._ratio_zoom_ctx
        if not ctx:
            return
        if event.inaxes != ctx["ax"]:
            return
        if ctx["x_max_bound"] <= ctx["x_min_bound"]:
            return
        scale = 0.8 if event.button == "up" else 1.25
        if self._event_has_ctrl(event):
            if self._cb_ratio_ysync.isChecked():
                self._cb_ratio_ysync.setChecked(False)
            center_y = float(event.ydata) if event.ydata is not None else float(np.mean(ctx["ax"].get_ylim()))
            self._zoom_axis_y(ctx["ax"], center_y, scale)
            self._canvas_ratio.draw_idle()
            return
        if event.xdata is None:
            return
        left, right = self._zoom_x_range(
            ctx["ax"].get_xlim(),
            float(event.xdata),
            scale,
            ctx["x_min_bound"],
            ctx["x_max_bound"],
            ctx["min_span"],
        )
        ctx["ax"].set_xlim(left, right)
        if self._cb_ratio_ysync.isChecked():
            self._rescale_ratio_y(left, right)
        self._canvas_ratio.draw_idle()

    def _on_ratio_press(self, event):
        if self._toolbar_ratio.mode:
            return
        ctx = self._ratio_zoom_ctx
        if not ctx or event.inaxes != ctx["ax"]:
            return
        if event.button != 1 or event.xdata is None:
            return
        self._ratio_pan_state["active"] = True
        self._ratio_pan_state["press_px"] = float(event.x)
        self._ratio_pan_state["press_py"] = float(event.y) if event.y is not None else None
        self._ratio_pan_state["start_xlim"] = ctx["ax"].get_xlim()
        self._ratio_pan_state["start_ylim"] = ctx["ax"].get_ylim()

    def _on_ratio_release(self, _event):
        self._ratio_pan_state["active"] = False
        self._ratio_pan_state["press_px"] = None
        self._ratio_pan_state["press_py"] = None
        self._ratio_pan_state["start_xlim"] = None
        self._ratio_pan_state["start_ylim"] = None
        if self._cb_ratio_ysync.isChecked() and self._ratio_zoom_ctx:
            left, right = self._ratio_zoom_ctx["ax"].get_xlim()
            self._rescale_ratio_y(left, right)
            self._canvas_ratio.draw_idle()

    def _on_ratio_motion(self, event):
        if not self._ratio_pan_state["active"]:
            return
        ctx = self._ratio_zoom_ctx
        if not ctx or event.inaxes != ctx["ax"]:
            return
        if event.x is None:
            return
        press_px = self._ratio_pan_state["press_px"]
        press_py = self._ratio_pan_state["press_py"]
        start_xlim = self._ratio_pan_state["start_xlim"]
        start_ylim = self._ratio_pan_state["start_ylim"]
        if press_px is None or start_xlim is None:
            return
        ax = ctx["ax"]
        p0 = ax.transData.transform((start_xlim[0], 0.0))[0]
        p1 = ax.transData.transform((start_xlim[1], 0.0))[0]
        shift_px = float(event.x) - float(press_px)
        new_left = ax.transData.inverted().transform((p0 - shift_px, 0.0))[0]
        new_right = ax.transData.inverted().transform((p1 - shift_px, 0.0))[0]
        left, right = self._shift_x_range(
            (new_left, new_right), 0.0, ctx["x_min_bound"], ctx["x_max_bound"]
        )
        ctx["ax"].set_xlim(left, right)
        if not self._cb_ratio_ysync.isChecked() and press_py is not None and start_ylim is not None and event.y is not None:
            q0 = ax.transData.transform((0.0, start_ylim[0]))[1]
            q1 = ax.transData.transform((0.0, start_ylim[1]))[1]
            shift_py = float(event.y) - float(press_py)
            new_bottom = ax.transData.inverted().transform((0.0, q0 - shift_py))[1]
            new_top = ax.transData.inverted().transform((0.0, q1 - shift_py))[1]
            if np.isfinite(new_bottom) and np.isfinite(new_top):
                if new_bottom > new_top:
                    new_bottom, new_top = new_top, new_bottom
                if np.isclose(new_bottom, new_top):
                    pad = max(abs(new_top) * 0.02, 1e-6)
                    new_bottom, new_top = new_bottom - pad, new_top + pad
                ax.set_ylim(new_bottom, new_top)
        self._canvas_ratio.draw_idle()

    def _rescale_ratio_y(self, left: float, right: float):
        ctx = self._ratio_zoom_ctx
        if not ctx:
            return
        vals = []
        for x, y in ctx["series"]:
            if len(x) == 0 or len(y) == 0:
                continue
            mask = (x >= left) & (x <= right)
            if not mask.any():
                continue
            part = y[mask]
            part = part[np.isfinite(part)]
            if part.size:
                vals.append(part)
        if not vals:
            return
        merged = np.concatenate(vals)
        vmin = float(np.min(merged))
        vmax = float(np.max(merged))
        if np.isclose(vmin, vmax):
            pad = max(abs(vmax) * 0.02, 1e-6)
        else:
            pad = (vmax - vmin) * 0.08
        ctx["ax"].set_ylim(vmin - pad, vmax + pad)

    def _fit_ratio_full(self, *_args, draw=True):
        ctx = self._ratio_zoom_ctx
        if not ctx:
            return
        if ctx["x_max_bound"] <= ctx["x_min_bound"]:
            return
        ctx["ax"].set_xlim(ctx["x_min_bound"], ctx["x_max_bound"])
        self._rescale_ratio_y(ctx["x_min_bound"], ctx["x_max_bound"])
        if draw:
            self._canvas_ratio.draw_idle()

    def _event_has_ctrl(self, event) -> bool:
        try:
            if QApplication.keyboardModifiers() & Qt.ControlModifier:
                return True
        except Exception:
            pass
        gui_event = getattr(event, "guiEvent", None)
        if gui_event is not None:
            try:
                if gui_event.modifiers() & Qt.ControlModifier:
                    return True
            except Exception:
                pass
        key = getattr(event, "key", None)
        if not key:
            return False
        key = str(key).lower()
        return "control" in key or "ctrl" in key

    def _zoom_axis_y(self, ax, center_y: float, scale: float):
        y0, y1 = ax.get_ylim()
        new_y0 = center_y - (center_y - y0) * scale
        new_y1 = center_y + (y1 - center_y) * scale
        if np.isclose(new_y0, new_y1):
            pad = max(abs(center_y) * 0.02, 1e-6)
            new_y0, new_y1 = center_y - pad, center_y + pad
        ax.set_ylim(new_y0, new_y1)

    def _zoom_x_range(
        self,
        current_xlim,
        center: float,
        scale: float,
        min_bound: float,
        max_bound: float,
        min_span: float,
    ) -> tuple[float, float]:
        cur_left, cur_right = current_xlim
        left = center - (center - cur_left) * scale
        right = center + (cur_right - center) * scale
        span = right - left
        full_span = max_bound - min_bound
        min_span = min(min_span, full_span)
        if span < min_span:
            half = min_span / 2.0
            left, right = center - half, center + half
        if span > full_span:
            left, right = min_bound, max_bound
        if left < min_bound:
            right += min_bound - left
            left = min_bound
        if right > max_bound:
            left -= right - max_bound
            right = max_bound
        left = max(left, min_bound)
        right = min(right, max_bound)
        return left, right

    def _shift_x_range(
        self,
        current_xlim,
        dx: float,
        min_bound: float,
        max_bound: float,
    ) -> tuple[float, float]:
        left, right = current_xlim
        span = right - left
        left -= dx
        right -= dx
        if left < min_bound:
            left = min_bound
            right = left + span
        if right > max_bound:
            right = max_bound
            left = right - span
        if left < min_bound:
            left, right = min_bound, max_bound
        return left, right

    def _clamp_xlim(
        self,
        xlim,
        min_bound: float,
        max_bound: float,
        min_span: float,
    ) -> tuple[float, float]:
        left, right = float(xlim[0]), float(xlim[1])
        if max_bound <= min_bound:
            return min_bound, max_bound
        if left > right:
            left, right = right, left
        full_span = max_bound - min_bound
        span = right - left
        if span <= 0:
            span = min_span
            center = (left + right) / 2.0
            left = center - span / 2.0
            right = center + span / 2.0
        min_span = min(min_span, full_span)
        if span < min_span:
            center = (left + right) / 2.0
            left = center - min_span / 2.0
            right = center + min_span / 2.0
        if span > full_span:
            return min_bound, max_bound
        if left < min_bound:
            right += min_bound - left
            left = min_bound
        if right > max_bound:
            left -= right - max_bound
            right = max_bound
        left = max(left, min_bound)
        right = min(right, max_bound)
        return left, right

    def _refresh_trade_table(self):
        trades = self._result.trades if self._result else []
        headers = ["时间", "方向", "品种", "价格", "数量", "金额", "佣金"]
        self._trade_table.setColumnCount(len(headers))
        self._trade_table.setHorizontalHeaderLabels(headers)
        self._trade_table.setRowCount(len(trades))

        for row, t in enumerate(trades):
            dt_str = t.datetime.strftime("%Y-%m-%d %H:%M:%S") if t.datetime else ""
            amount = t.price * t.volume
            items = [
                dt_str,
                t.direction.value,
                t.symbol,
                f"{t.price:.4f}",
                str(t.volume),
                f"{amount:,.2f}",
                f"{t.commission:.2f}",
            ]
            for col, txt in enumerate(items):
                item = QTableWidgetItem(txt)
                if col == 1:
                    if t.direction.value == "BUY":
                        item.setBackground(Qt.green)
                    else:
                        item.setBackground(Qt.red)
                self._trade_table.setItem(row, col, item)

        self._trade_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

    def _refresh_daily_combo(self):
        self._combo_daily.blockSignals(True)
        self._combo_daily.clear()

        dates = sorted({b.trade_date for b in self._block_logs if b.trade_date})
        for d in dates:
            n = sum(1 for b in self._block_logs if b.trade_date == d)
            self._combo_daily.addItem(f"{d} ({n} blocks)", d)

        self._combo_daily.blockSignals(False)
        if self._combo_daily.count() > 0:
            self._combo_daily.setCurrentIndex(0)
            self._on_daily_changed(0)

    def _on_daily_changed(self, idx):
        if idx < 0:
            return
        date_str = self._combo_daily.itemData(idx)
        if date_str:
            self._draw_daily(date_str)

    def _draw_daily(self, date_str: str):
        fig = self._canvas_daily.fig
        fig.clear()

        strategy = self._strategy
        if not strategy:
            fig.text(0.5, 0.5, "无策略数据", ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        symbol_a = getattr(strategy, "symbol_a", "")
        symbol_b = getattr(strategy, "symbol_b", "")
        dataset_dir = getattr(strategy, "dataset_dir", "")
        if not dataset_dir or not symbol_a or not symbol_b:
            fig.text(0.5, 0.5, "数据不足", ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        from core.data_feed import ParquetTickFeed

        feed = ParquetTickFeed(dataset_dir)
        ticks_a = feed.load_day(symbol_a, date_str)
        ticks_b = feed.load_day(symbol_b, date_str)
        if not ticks_a and not ticks_b:
            fig.text(0.5, 0.5, f"{date_str} 无 Tick 数据", ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        times_a, prices_a = [], []
        for t in ticks_a:
            tt = t.datetime.time()
            if dt_time(9, 30) <= tt < dt_time(15, 0) and t.last_price > 0:
                times_a.append(t.datetime)
                prices_a.append(t.last_price)

        times_b, prices_b = [], []
        for t in ticks_b:
            tt = t.datetime.time()
            if dt_time(9, 30) <= tt < dt_time(15, 0) and t.last_price > 0:
                times_b.append(t.datetime)
                prices_b.append(t.last_price)

        blocks = [b for b in self._block_logs if b.trade_date == date_str]

        short_a = symbol_a.split(".")[0]
        short_b = symbol_b.split(".")[0]
        c_a, c_b = "#1976D2", "#E65100"

        ax_price = fig.add_subplot(2, 1, 1)
        if prices_a:
            ax_price.plot(times_a, prices_a, color=c_a, linewidth=0.8, alpha=0.85, label=short_a)
        ax2_price = ax_price.twinx()
        if prices_b:
            ax2_price.plot(times_b, prices_b, color=c_b, linewidth=0.8, alpha=0.85, linestyle="--", label=short_b)

        ax_price.set_ylabel(f"{short_a} 价格", color=c_a)
        ax_price.tick_params(axis="y", labelcolor=c_a)
        ax2_price.set_ylabel(f"{short_b} 价格", color=c_b)
        ax2_price.tick_params(axis="y", labelcolor=c_b)

        state_colors = {
            "DONE": "#4CAF50",
            "PARTIAL": "#FFC107",
            "BUY_ONLY": "#2196F3",
            "REJECTED": "#F44336",
        }

        for b in blocks:
            col = state_colors.get(b.state.value, "#9E9E9E")
            if b.signal_time:
                ax_price.axvline(b.signal_time, color=col, linestyle="--", linewidth=0.6, alpha=0.5)

            if b.sell_signal_price > 0:
                target_ax = ax_price if b.sell_symbol == symbol_a else ax2_price
                target_ax.scatter([b.signal_time], [b.sell_signal_price], marker="*", s=90,
                                  c=col, edgecolors="black", linewidths=0.4, zorder=12)
            if b.buy_signal_price > 0:
                target_ax = ax_price if b.buy_symbol == symbol_a else ax2_price
                target_ax.scatter([b.signal_time], [b.buy_signal_price], marker="*", s=90,
                                  c=col, edgecolors="black", linewidths=0.4, zorder=12)

            if b.sell_filled > 0 and b.sell_avg_price > 0 and b.end_time:
                target_ax = ax_price if b.sell_symbol == symbol_a else ax2_price
                target_ax.scatter([b.end_time], [b.sell_avg_price], marker="v", s=35,
                                  c="#D32F2F", edgecolors="black", linewidths=0.3, zorder=12)
            if b.buy_filled > 0 and b.buy_avg_price > 0 and b.end_time:
                target_ax = ax_price if b.buy_symbol == symbol_a else ax2_price
                target_ax.scatter([b.end_time], [b.buy_avg_price], marker="^", s=35,
                                  c="#2E7D32", edgecolors="black", linewidths=0.3, zorder=12)

        ax_ratio = fig.add_subplot(2, 1, 2, sharex=ax_price)
        all_times = sorted(set([t.datetime for t in ticks_a if t.last_price > 0] +
                               [t.datetime for t in ticks_b if t.last_price > 0]))
        map_a = {t.datetime: t.last_price for t in ticks_a if t.last_price > 0}
        map_b = {t.datetime: t.last_price for t in ticks_b if t.last_price > 0}
        ratio_t, ratio_v = [], []
        pa, pb = 0.0, 0.0
        for t in all_times:
            tt = t.time()
            if not (dt_time(9, 30) <= tt < dt_time(15, 0)):
                continue
            if t in map_a:
                pa = map_a[t]
            if t in map_b:
                pb = map_b[t]
            if pa > 0 and pb > 0:
                ratio_t.append(t)
                ratio_v.append(pa / pb)
        if ratio_v:
            ax_ratio.plot(ratio_t, ratio_v, color="#9C27B0", linewidth=0.75, label="ABratio")

        for b in blocks:
            col = state_colors.get(b.state.value, "#9E9E9E")
            if b.signal_time:
                ax_ratio.axvline(b.signal_time, color=col, linestyle="--", linewidth=0.6, alpha=0.4)

        ax_price.set_title(f"{date_str} 日交易总览 | {short_a} vs {short_b} | Blocks={len(blocks)}")
        ax_price.grid(True, alpha=0.2)

        ax_ratio.set_ylabel("ABratio")
        ax_ratio.set_xlabel("时间")
        ax_ratio.grid(True, alpha=0.3)
        ax_ratio.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

        legend_items = [
            Line2D([0], [0], color=c_a, linewidth=1.0, label=short_a),
            Line2D([0], [0], color=c_b, linewidth=1.0, linestyle="--", label=short_b),
            Line2D([0], [0], marker="*", color="w", markerfacecolor="#4CAF50", markersize=10, label="信号点"),
            Line2D([0], [0], marker="^", color="w", markerfacecolor="#2E7D32", markersize=8, label="买成交"),
            Line2D([0], [0], marker="v", color="w", markerfacecolor="#D32F2F", markersize=8, label="卖成交"),
            Patch(facecolor="#4CAF50", alpha=0.6, label="DONE"),
            Patch(facecolor="#FFC107", alpha=0.6, label="PARTIAL"),
            Patch(facecolor="#2196F3", alpha=0.6, label="BUY_ONLY"),
            Patch(facecolor="#F44336", alpha=0.6, label="REJECTED"),
        ]
        ax_price.legend(handles=legend_items, loc="upper left", fontsize=7, ncol=4, framealpha=0.85)

        fig.autofmt_xdate(rotation=30)
        fig.tight_layout()
        self._canvas_daily.draw()
