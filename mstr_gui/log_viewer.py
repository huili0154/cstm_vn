"""
MSTR 回测日志查看器 — Block 交易事件图形化分析。
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.backends.backend_qt5agg import (
    FigureCanvasQTAgg as FigureCanvas,
    NavigationToolbar2QT as NavigationToolbar,
)
from matplotlib.figure import Figure

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
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


# ────────────────────────────────────────────────
#  内嵌 Matplotlib 画布
# ────────────────────────────────────────────────


class _MplCanvas(FigureCanvas):
    def __init__(self, width=8, height=4, dpi=100, parent=None):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.setParent(parent)


# ────────────────────────────────────────────────
#  日志查看器主面板
# ────────────────────────────────────────────────


class LogViewerWidget(QWidget):
    """MSTR 回测日志图形化查看器。"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._result = None
        self._strategy = None
        self._block_logs = []
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        # 顶部提示
        self._lbl_status = QLabel("等待回测完成...")
        self._lbl_status.setStyleSheet("font-size: 13px; color: #666;")
        layout.addWidget(self._lbl_status)

        # Tab 页
        self._tabs = QTabWidget()

        # Tab 1: 总览
        self._tab_summary = self._build_summary_tab()
        self._tabs.addTab(self._tab_summary, "总览")

        # Tab 2: Block 列表
        self._tab_blocks = self._build_block_table_tab()
        self._tabs.addTab(self._tab_blocks, "Block 列表")

        # Tab 3: Block 详情
        self._tab_detail = self._build_block_detail_tab()
        self._tabs.addTab(self._tab_detail, "Block 详情")

        # Tab 4: 净值曲线
        self._tab_equity = self._build_equity_tab()
        self._tabs.addTab(self._tab_equity, "净值曲线")

        # Tab 5: Block 时间线
        self._tab_timeline = self._build_timeline_tab()
        self._tabs.addTab(self._tab_timeline, "Block 时间线")

        # Tab 6: 日交易可视化
        self._tab_daily = self._build_daily_trade_tab()
        self._tabs.addTab(self._tab_daily, "日交易可视化")

        layout.addWidget(self._tabs, stretch=1)

    # ════════════════════════════════════════
    #  Tab builders
    # ════════════════════════════════════════

    def _build_summary_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        self._canvas_summary = _MplCanvas(width=10, height=6)
        self._toolbar_summary = NavigationToolbar(self._canvas_summary, w)
        layout.addWidget(self._toolbar_summary)
        layout.addWidget(self._canvas_summary, stretch=1)
        return w

    def _build_block_table_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        self._block_table = QTableWidget()
        self._block_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._block_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._block_table.cellClicked.connect(self._on_block_row_clicked)
        layout.addWidget(self._block_table)
        return w

    def _build_block_detail_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        # 选择器
        sel_row = QHBoxLayout()
        sel_row.addWidget(QLabel("选择 Block:"))
        self._combo_block = QComboBox()
        self._combo_block.currentIndexChanged.connect(self._on_block_selected)
        sel_row.addWidget(self._combo_block, stretch=1)
        layout.addLayout(sel_row)

        # 分割: 上方图表, 下方事件表
        splitter = QSplitter(Qt.Vertical)

        self._canvas_detail = _MplCanvas(width=10, height=4)
        self._toolbar_detail = NavigationToolbar(self._canvas_detail, w)
        chart_w = QWidget()
        chart_l = QVBoxLayout(chart_w)
        chart_l.setContentsMargins(0, 0, 0, 0)
        chart_l.addWidget(self._toolbar_detail)
        chart_l.addWidget(self._canvas_detail)
        splitter.addWidget(chart_w)

        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        splitter.addWidget(self._detail_text)

        splitter.setSizes([400, 200])
        layout.addWidget(splitter, stretch=1)
        return w

    def _build_equity_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        self._canvas_equity = _MplCanvas(width=10, height=5)
        self._toolbar_equity = NavigationToolbar(self._canvas_equity, w)
        layout.addWidget(self._toolbar_equity)
        layout.addWidget(self._canvas_equity, stretch=1)
        return w

    def _build_timeline_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)
        self._canvas_timeline = _MplCanvas(width=10, height=5)
        self._toolbar_timeline = NavigationToolbar(self._canvas_timeline, w)
        layout.addWidget(self._toolbar_timeline)
        layout.addWidget(self._canvas_timeline, stretch=1)
        return w

    # ════════════════════════════════════════
    #  加载回测结果
    # ════════════════════════════════════════

    def load_result(self, result, strategy):
        """回测完成后加载数据并刷新所有图表。"""
        self._result = result
        self._strategy = strategy
        self._block_logs = getattr(strategy, "_block_logs", [])

        n = len(self._block_logs)
        self._lbl_status.setText(
            f"已加载回测结果: {len(result.trades)} 笔成交, {n} 个 Block"
        )
        self._lbl_status.setStyleSheet("font-size: 13px; color: #080;")

        self._refresh_summary()
        self._refresh_block_table()
        self._refresh_combo()
        self._refresh_equity()
        self._refresh_timeline()
        self._refresh_daily_combo()

    # ════════════════════════════════════════
    #  Tab 1: 总览 (状态分布 + 被动/激进比例)
    # ════════════════════════════════════════

    def _refresh_summary(self):
        fig = self._canvas_summary.fig
        fig.clear()

        if not self._block_logs:
            fig.text(0.5, 0.5, "无 Block 数据", ha="center", va="center", fontsize=14)
            self._canvas_summary.draw()
            return

        logs = self._block_logs

        # 左上: Block 状态分布
        ax1 = fig.add_subplot(2, 2, 1)
        state_counter = Counter(b.state.value for b in logs)
        labels = list(state_counter.keys())
        sizes = list(state_counter.values())
        colors_map = {
            "DONE": "#4CAF50",
            "TIMEOUT": "#FF9800",
            "CRITICAL": "#F44336",
            "FILLING": "#2196F3",
            "PENDING": "#9E9E9E",
            "PENDING_BUY": "#03A9F4",
            "MATCHING": "#FFC107",
            "CHASING": "#FF5722",
        }
        colors = [colors_map.get(l, "#9E9E9E") for l in labels]
        ax1.pie(sizes, labels=labels, colors=colors, autopct="%1.0f%%", startangle=90)
        ax1.set_title("Block 状态分布")

        # 右上: 被动/激进 成交量
        ax2 = fig.add_subplot(2, 2, 2)
        total_passive_buy = 0
        total_aggressive_buy = 0
        total_passive_sell = 0
        total_aggressive_sell = 0
        for b in logs:
            total_aggressive_buy += b.buy_aggressive
            total_passive_buy += max(0, b.buy_filled - b.buy_aggressive)
            total_aggressive_sell += b.sell_aggressive
            total_passive_sell += max(0, b.sell_filled - b.sell_aggressive)

        x = np.arange(2)
        passive = [total_passive_buy, total_passive_sell]
        aggressive = [total_aggressive_buy, total_aggressive_sell]
        w = 0.35
        ax2.bar(x - w / 2, passive, w, label="被动", color="#4CAF50")
        ax2.bar(x + w / 2, aggressive, w, label="激进", color="#F44336")
        ax2.set_xticks(x)
        ax2.set_xticklabels(["买入", "卖出"])
        ax2.set_title("被动 vs 激进 成交量")
        ax2.legend()
        ax2.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))

        # 左下: Block 持续时间分布
        ax3 = fig.add_subplot(2, 2, 3)
        durations = [b.total_duration for b in logs if b.total_duration > 0]
        if durations:
            ax3.hist(durations, bins=min(30, len(durations)), color="#2196F3", alpha=0.8)
        ax3.set_title("Block 持续时间分布 (秒)")
        ax3.set_xlabel("秒")
        ax3.set_ylabel("频次")

        # 右下: 按品种统计 Block 数量
        ax4 = fig.add_subplot(2, 2, 4)
        symbol_counter: Counter = Counter()
        for b in logs:
            if b.mode == "rotation":
                symbol_counter[b.sell_symbol] += 1
                symbol_counter[b.buy_symbol] += 1
            else:
                symbol_counter[b.buy_symbol] += 1
        if symbol_counter:
            syms = sorted(symbol_counter.keys())
            counts = [symbol_counter[s] for s in syms]
            # 缩短 label
            short = [s.split(".")[0] for s in syms]
            ax4.barh(short, counts, color="#9C27B0", alpha=0.8)
            ax4.set_title("品种参与 Block 次数")
            ax4.set_xlabel("次数")

        fig.tight_layout()
        self._canvas_summary.draw()

    # ════════════════════════════════════════
    #  Tab 2: Block 列表表格
    # ════════════════════════════════════════

    def _refresh_block_table(self):
        headers = [
            "Block ID",
            "模式",
            "卖出品种",
            "买入品种",
            "状态",
            "触发Score",
            "卖信号价",
            "卖成交价",
            "买信号价",
            "买成交价",
            "买入成交",
            "卖出成交",
            "现金",
            "账户市值",
            "持续(秒)",
            "事件数",
        ]
        self._block_table.setColumnCount(len(headers))
        self._block_table.setHorizontalHeaderLabels(headers)
        self._block_table.setRowCount(len(self._block_logs))

        for row, b in enumerate(self._block_logs):
            items = [
                b.block_id,
                b.mode,
                b.sell_symbol or "-",
                b.buy_symbol,
                b.state.value,
                f"{b.trigger_score:.4f}",
                f"{b.sell_signal_price:.4f}" if b.sell_signal_price else "-",
                f"{b.sell_avg_price:.4f}" if b.sell_filled > 0 else "-",
                f"{b.buy_signal_price:.4f}" if b.buy_signal_price else "-",
                f"{b.buy_avg_price:.4f}" if b.buy_filled > 0 else "-",
                str(b.buy_filled),
                str(b.sell_filled),
                f"{b.end_cash:,.0f}",
                f"{b.end_nav:,.0f}",
                f"{b.total_duration:.1f}",
                str(len(b.events)),
            ]
            for col, txt in enumerate(items):
                item = QTableWidgetItem(txt)
                # 颜色标记状态
                if col == 4:
                    if txt == "DONE":
                        item.setBackground(Qt.green)
                    elif txt == "TIMEOUT":
                        item.setBackground(Qt.yellow)
                    elif txt == "CRITICAL":
                        item.setBackground(Qt.red)
                self._block_table.setItem(row, col, item)

        self._block_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeToContents
        )

    def _on_block_row_clicked(self, row, _col):
        if 0 <= row < len(self._block_logs):
            self._combo_block.setCurrentIndex(row)
            self._tabs.setCurrentIndex(2)  # 切到 Block 详情

    # ════════════════════════════════════════
    #  Tab 3: Block 详情
    # ════════════════════════════════════════

    def _refresh_combo(self):
        self._combo_block.blockSignals(True)
        self._combo_block.clear()
        for b in self._block_logs:
            label = (
                f"{b.block_id} [{b.state.value}] "
                f"{b.sell_symbol or ''} → {b.buy_symbol}"
            )
            self._combo_block.addItem(label)
        self._combo_block.blockSignals(False)
        if self._block_logs:
            self._combo_block.setCurrentIndex(0)
            self._on_block_selected(0)

    def _on_block_selected(self, idx):
        if idx < 0 or idx >= len(self._block_logs):
            return
        block = self._block_logs[idx]
        self._draw_block_detail(block)
        self._show_block_events(block)

    def _draw_block_detail(self, block):
        """绘制单个 Block 的事件驱动时间线 — 价格坐标版。

        X 轴: 事件驱动步进 (同 tick_seq 内纵向堆叠, 不同 tick_seq 横向推进)
        Y 轴: 股价 (左轴=卖出品种, 右轴=买入品种)
        背景: 彩色 last_price 折线
        前景: 彩色标记标注每个事件，悬停显示详情
        """
        fig = self._canvas_detail.fig
        fig.clear()

        events = block.events
        if not events:
            fig.text(0.5, 0.5, "无事件", ha="center", va="center")
            self._canvas_detail.draw()
            return

        dataset_dir = getattr(self._strategy, "dataset_dir", "")

        # ── 分组: 按 tick_seq 聚合事件 ──
        from collections import OrderedDict
        tick_groups: OrderedDict[int, list] = OrderedDict()
        for e in events:
            tick_groups.setdefault(e.tick_seq, []).append(e)

        # ── 构建 X 轴映射: tick_seq → x_slot ──
        tick_seq_list = list(tick_groups.keys())
        tick_x: dict[int, float] = {}
        for i, ts in enumerate(tick_seq_list):
            tick_x[ts] = float(i)

        # ── 事件分类颜色和标记 ──
        _event_style = {
            # (color, marker, label_zh)
            "PASSIVE_FILL":      ("#4CAF50", "o", "被动成交"),
            "AGGRESSIVE_FILL":   ("#FF5722", "o", "激进成交"),
            "PASSIVE_SUBMIT":    ("#2196F3", "^", "挂被动单"),
            "AGGRESSIVE_SUBMIT": ("#E91E63", "v", "挂激进单"),
            "CHASE_SUBMIT":      ("#E91E63", "v", "挂追单"),
            "PASSIVE_CANCEL":    ("#795548", "x", "撤被动单"),
            "CANCEL_FOR_CHASE":  ("#795548", "x", "撤单→转追"),
            "CANCEL_ALL":        ("#795548", "X", "全部撤单"),
            "CANCEL_CONFIRMED":  ("#9E9E9E", "x", "撤单确认"),
            "PASSIVE_RESUBMIT":  ("#42A5F5", "^", "补挂被动单"),
            "ORDER_REJECTED":    ("#B71C1C", "X", "下单被拒"),
            "EXCESS_DETECT":     ("#FF9800", "D", "检测差额"),
            "CHASE_CANCEL":      ("#9C27B0", "x", "撤旧追单"),
            "SIGNAL":            ("#00BCD4", "*", "触发信号"),
            "RECOVERY_SIGNAL":   ("#00BCD4", "*", "恢复信号"),
            "IMMEDIATE_CHASE":   ("#E91E63", "P", "立即追单"),
            "CHASE_ABORT":       ("#B71C1C", "8", "追单终止"),
            "BUDGET_DONE":       ("#4CAF50", "s", "预算完成"),
            "BLOCK_DONE":        ("#4CAF50", "s", "完成"),
            "BLOCK_TIMEOUT":     ("#FF9800", "s", "超时"),
            "BLOCK_CRITICAL":    ("#F44336", "s", "严重"),
        }
        _default_style = ("#9E9E9E", ".", "其他")

        # ── 辅助: 推断事件所属 side + 价格 ──
        sp0 = block.sell_signal_price or 0
        bp0 = block.buy_signal_price or 0

        def _resolve_side_price(e):
            """返回 (side, price)。side='sell'|'buy'|'meta', price 可能为 0。"""
            d = e.detail
            et = e.event_type
            # 1) 信号 / 终态事件 → meta
            if et in ("SIGNAL", "RECOVERY_SIGNAL", "BLOCK_DONE",
                       "BLOCK_TIMEOUT", "BLOCK_CRITICAL", "CANCEL_ALL"):
                return "meta", 0
            # 2) PASSIVE_SUBMIT 有双边价格
            if et == "PASSIVE_SUBMIT":
                return "dual", 0
            # 3) 有明确 side 字段
            side = d.get("side", "")
            price = d.get("fill_price") or d.get("price") or d.get("new_price") or d.get("old_price", 0)
            if side in ("sell", "buy"):
                return side, price
            # 4) EXCESS_DETECT 有 side
            if et == "EXCESS_DETECT":
                return d.get("side", "meta"), price
            # 5) 通过价格距离推断
            if price and price > 0 and sp0 > 0 and bp0 > 0:
                return ("sell" if abs(price - sp0) < abs(price - bp0) else "buy"), price
            # 6) 撤单/拒绝/确认类事件
            if et in ("PASSIVE_CANCEL", "ORDER_REJECTED", "CHASE_ABORT",
                       "CANCEL_CONFIRMED", "CANCEL_FOR_CHASE",
                       "CHASE_CANCEL", "PASSIVE_RESUBMIT"):
                return side or "meta", price
            if price and price > 0:
                if sp0 > 0:
                    return "sell", price
                return "buy", price
            return "meta", 0

        # ── 收集事件价格 (用于 Y 轴范围) ──
        sell_prices = []
        buy_prices = []
        for e in events:
            side, price = _resolve_side_price(e)
            if side == "sell" and price > 0:
                sell_prices.append(price)
            elif side == "buy" and price > 0:
                buy_prices.append(price)
            elif side == "dual":
                sp = e.detail.get("sell_price", 0)
                bp = e.detail.get("buy_price", 0)
                if sp and sp > 0:
                    sell_prices.append(sp)
                if bp and bp > 0:
                    buy_prices.append(bp)

        if sp0 > 0:
            sell_prices.append(sp0)
        if bp0 > 0:
            buy_prices.append(bp0)

        has_sell = bool(block.sell_symbol and sell_prices)
        has_buy = bool(block.buy_symbol and buy_prices)

        # ── Create axes ──
        ax = fig.add_subplot(111)
        ax2 = None

        if has_sell and has_buy:
            ax2 = ax.twinx()
        elif not has_sell and not has_buy:
            fig.text(0.5, 0.5, "无价格数据", ha="center", va="center")
            self._canvas_detail.draw()
            return

        # ── 背景 tick 价格曲线 (明显且不同的颜色 + 图例) ──
        SELL_LINE_COLOR = "#1976D2"   # 深蓝
        BUY_LINE_COLOR  = "#E65100"   # 深橙

        if dataset_dir and block.trade_date:
            from core.data_feed import ParquetTickFeed
            feed = ParquetTickFeed(dataset_dir)
            min_time = block.signal_time
            max_time = block.end_time or events[-1].time
            min_seq = tick_seq_list[0]
            max_seq = tick_seq_list[-1]

            seq_to_time_bg = {}
            for e in events:
                if e.tick_seq not in seq_to_time_bg:
                    seq_to_time_bg[e.tick_seq] = e.time

            def _time_to_x(dt):
                """Map a real datetime to x position by interpolation."""
                if len(seq_to_time_bg) < 2:
                    return tick_x.get(min_seq, 0.0)
                prev_seq = min_seq
                prev_time = seq_to_time_bg.get(min_seq, min_time)
                for s in tick_seq_list:
                    t = seq_to_time_bg.get(s, None)
                    if t is None:
                        continue
                    if t > dt:
                        span = (t - prev_time).total_seconds()
                        frac = ((dt - prev_time).total_seconds() / span) if span > 0 else 0.0
                        return tick_x[prev_seq] + frac * (tick_x[s] - tick_x[prev_seq])
                    prev_seq = s
                    prev_time = t
                return tick_x.get(max_seq, float(len(tick_seq_list) - 1))

            if has_sell and block.sell_symbol:
                raw = feed.load_day(block.sell_symbol, block.trade_date)
                bg_x, bg_y = [], []
                for t in raw:
                    if min_time and t.datetime < min_time:
                        continue
                    if max_time and t.datetime > max_time:
                        continue
                    if t.last_price > 0:
                        bg_x.append(_time_to_x(t.datetime))
                        bg_y.append(t.last_price)
                if bg_x:
                    ax.plot(bg_x, bg_y, color=SELL_LINE_COLOR, linewidth=1.0,
                            alpha=0.35, zorder=1, label=f"{block.sell_symbol} 价格")

            if has_buy and block.buy_symbol:
                raw = feed.load_day(block.buy_symbol, block.trade_date)
                bg_x, bg_y = [], []
                for t in raw:
                    if min_time and t.datetime < min_time:
                        continue
                    if max_time and t.datetime > max_time:
                        continue
                    if t.last_price > 0:
                        bg_x.append(_time_to_x(t.datetime))
                        bg_y.append(t.last_price)
                if bg_x:
                    target_ax = ax2 if ax2 else ax
                    target_ax.plot(bg_x, bg_y, color=BUY_LINE_COLOR, linewidth=1.0,
                                   alpha=0.35, zorder=1, linestyle="--",
                                   label=f"{block.buy_symbol} 价格")

        # ── 预计算 Y 范围 (用于 meta 事件定位 & 同 tick 散开) ──
        if has_sell and sell_prices:
            sell_mn, sell_mx = min(sell_prices), max(sell_prices)
            sell_margin = max((sell_mx - sell_mn) * 0.15, 0.001)
        else:
            sell_mn, sell_mx, sell_margin = 0, 0, 0
        if has_buy and buy_prices:
            buy_mn, buy_mx = min(buy_prices), max(buy_prices)
            buy_margin = max((buy_mx - buy_mn) * 0.15, 0.001)
        else:
            buy_mn, buy_mx, buy_margin = 0, 0, 0

        # ── 同 tick 事件散开: 计算 y 偏移量 ──
        # 每个 tick_seq 内按 side 分组，同 side 事件之间拉开 y_step
        sell_range = (sell_mx - sell_mn) if sell_mn < sell_mx else 0.01
        buy_range = (buy_mx - buy_mn) if buy_mn < buy_mx else 0.01
        y_step_sell = sell_range * 0.08   # 每个事件间隔 = 价格范围 8%
        y_step_buy = buy_range * 0.08

        # 为每个事件预计算: (x_pos, y_pos, target_ax, side, style, tooltip)
        plot_items = []
        # 按 tick_seq 分组计数器 (side → count_within_tick)
        tick_side_count: dict[int, dict[str, int]] = {}

        for e in events:
            d = e.detail
            et = e.event_type
            style = _event_style.get(et, _default_style)
            color, marker, label_zh = style
            side, price = _resolve_side_price(e)
            x_pos = tick_x.get(e.tick_seq, 0.0)

            # tooltip 以关键信息拼接
            tip_parts = [f"{label_zh} ({et})", f"时间: {e.time.strftime('%H:%M:%S')}"]
            for k, v in d.items():
                if isinstance(v, float):
                    tip_parts.append(f"{k}: {v:.4f}")
                else:
                    tip_parts.append(f"{k}: {v}")
            tooltip = "\n".join(tip_parts)

            ts_key = e.tick_seq
            if ts_key not in tick_side_count:
                tick_side_count[ts_key] = {}

            if et in ("SIGNAL", "RECOVERY_SIGNAL"):
                # 双边信号点
                if sp0 > 0 and has_sell:
                    cnt_s = tick_side_count[ts_key]
                    n = cnt_s.get("sell", 0)
                    cnt_s["sell"] = n + 1
                    y = sp0 + n * y_step_sell
                    plot_items.append((x_pos, y, ax, "sell", style, tooltip, 80))
                if bp0 > 0 and has_buy:
                    cnt_s = tick_side_count[ts_key]
                    n = cnt_s.get("buy", 0)
                    cnt_s["buy"] = n + 1
                    y = bp0 + n * y_step_buy
                    tgt = ax2 if ax2 else ax
                    plot_items.append((x_pos, y, tgt, "buy", style, tooltip, 80))
                continue

            if side == "dual":
                # PASSIVE_SUBMIT: 卖价 + 买价
                sp = d.get("sell_price", 0)
                bp_val = d.get("buy_price", 0)
                if sp and sp > 0 and has_sell:
                    cnt_s = tick_side_count[ts_key]
                    n = cnt_s.get("sell", 0)
                    cnt_s["sell"] = n + 1
                    y = sp + n * y_step_sell
                    plot_items.append((x_pos, y, ax, "sell", style, tooltip, 60))
                if bp_val and bp_val > 0 and has_buy:
                    cnt_s = tick_side_count[ts_key]
                    n = cnt_s.get("buy", 0)
                    cnt_s["buy"] = n + 1
                    y = bp_val + n * y_step_buy
                    tgt = ax2 if ax2 else ax
                    plot_items.append((x_pos, y, tgt, "buy", style, tooltip, 60))
                continue

            if side == "meta":
                # 终态/元事件 — 放在 sell 轴信号价处
                cnt_s = tick_side_count[ts_key]
                n = cnt_s.get("meta", 0)
                cnt_s["meta"] = n + 1
                ref_price = sp0 or bp0 or (sum(sell_prices) / len(sell_prices) if sell_prices else 0)
                y = ref_price + n * y_step_sell if ref_price else 0
                if y > 0:
                    plot_items.append((x_pos, y, ax, "meta", style, tooltip, 100))
                continue

            # 有明确 side (sell / buy) 的事件
            if side == "sell":
                ref_price = price if price > 0 else sp0
                if ref_price > 0 and has_sell:
                    cnt_s = tick_side_count[ts_key]
                    n = cnt_s.get("sell", 0)
                    cnt_s["sell"] = n + 1
                    y = ref_price + n * y_step_sell
                    plot_items.append((x_pos, y, ax, "sell", style, tooltip, 60))
            elif side == "buy":
                ref_price = price if price > 0 else bp0
                if ref_price > 0 and has_buy:
                    cnt_s = tick_side_count[ts_key]
                    n = cnt_s.get("buy", 0)
                    cnt_s["buy"] = n + 1
                    y = ref_price + n * y_step_buy
                    tgt = ax2 if ax2 else ax
                    plot_items.append((x_pos, y, tgt, "buy", style, tooltip, 60))

        # ── 实际绘制所有事件标记 ──
        legend_entries = {}
        all_artists = []   # (scatter_handle, tooltip_text)

        for x_pos, y_pos, target_ax, side, style, tooltip, sz in plot_items:
            color, marker, label_zh = style
            edge_w = 1.0 if sz >= 100 else 0.5
            h = target_ax.scatter(
                [x_pos], [y_pos],
                c=color, marker=marker, s=sz, zorder=10 if sz < 100 else 12,
                edgecolors="black", linewidths=edge_w,
            )
            all_artists.append((h, tooltip))
            if label_zh not in legend_entries:
                legend_entries[label_zh] = h

        # ── 鼠标悬停 tooltip ──
        self._detail_annot = None
        if all_artists:
            import mplcursors
            artists_only = [a for a, _ in all_artists]
            tip_map = {id(a): tip for a, tip in all_artists}
            cursor = mplcursors.cursor(artists_only, hover=True)

            @cursor.connect("add")
            def _on_add(sel):
                sel.annotation.set_text(tip_map.get(id(sel.artist), ""))
                sel.annotation.get_bbox_patch().set(
                    fc="#FFFFDD", alpha=0.95, boxstyle="round,pad=0.4",
                )
                sel.annotation.set_fontsize(8)

        # ── Axis labels & styling ──
        ax.set_xlabel("事件步进 (Tick)")
        if has_sell:
            ax.set_ylabel(f"{block.sell_symbol} 价格", color=SELL_LINE_COLOR)
            ax.tick_params(axis="y", labelcolor=SELL_LINE_COLOR)
        elif has_buy and not ax2:
            ax.set_ylabel(f"{block.buy_symbol} 价格", color=BUY_LINE_COLOR)

        if ax2 and has_buy:
            ax2.set_ylabel(f"{block.buy_symbol} 价格", color=BUY_LINE_COLOR)
            ax2.tick_params(axis="y", labelcolor=BUY_LINE_COLOR)

        # Y 范围 (包括散开后的偏移)
        if has_sell and sell_prices:
            ax.set_ylim(sell_mn - sell_margin, sell_mx + sell_margin)
        if ax2 and has_buy and buy_prices:
            ax2.set_ylim(buy_mn - buy_margin, buy_mx + buy_margin)

        # ── Vertical dashed time annotations (after Y limits set) ──
        seq_to_time = {}
        for e in events:
            if e.tick_seq not in seq_to_time:
                seq_to_time[e.tick_seq] = e.time

        n_ticks = len(tick_seq_list)
        step = max(1, n_ticks // 12)
        y_bottom = ax.get_ylim()[0]
        for i, ts in enumerate(tick_seq_list):
            x_pos = tick_x[ts]
            if i % step == 0 or i == n_ticks - 1:
                t = seq_to_time.get(ts)
                if t:
                    ax.axvline(x_pos, color="#E0E0E0", linestyle=":", linewidth=0.5, zorder=0)
                    ax.text(
                        x_pos, y_bottom,
                        t.strftime("%H:%M:%S"), fontsize=6, ha="center", va="top",
                        rotation=45, color="#888",
                    )

        ax.set_xticks([])

        ax.set_title(
            f"Block {block.block_id}  [{block.state.value}]  "
            f"{block.sell_symbol or ''} → {block.buy_symbol}  "
            f"买={block.buy_filled}  卖={block.sell_filled}  "
            f"耗时={block.total_duration:.1f}s",
            fontsize=10,
        )
        ax.grid(True, alpha=0.15, axis="y")

        # ── Legend (事件类型 + 价格线) ──
        if legend_entries:
            handles = list(legend_entries.values())
            labels = list(legend_entries.keys())
            # 合并两个 axes 的图例
            if ax2:
                h2, l2 = ax2.get_legend_handles_labels()
                h1, l1 = ax.get_legend_handles_labels()
                all_h = h1 + h2 + handles
                all_l = l1 + l2 + labels
            else:
                h1, l1 = ax.get_legend_handles_labels()
                all_h = h1 + handles
                all_l = l1 + labels
            # 去重
            seen = set()
            final_h, final_l = [], []
            for h, lb in zip(all_h, all_l):
                if lb not in seen:
                    seen.add(lb)
                    final_h.append(h)
                    final_l.append(lb)
            ax.legend(
                final_h, final_l,
                loc="upper left", fontsize=9, ncol=2,
                framealpha=0.8, borderpad=0.3,
            )

        fig.tight_layout()
        self._canvas_detail.draw()

    def _show_block_events(self, block):
        """在文本框中展示 Block 完整事件日志。"""
        lines = []
        lines.append(f"═══ Block {block.block_id} ═══")
        lines.append(f"日期: {block.trade_date}  模式: {block.mode}")
        lines.append(f"卖出: {block.sell_symbol or '-'}  买入: {block.buy_symbol}")
        lines.append(
            f"Score: {block.trigger_score:.4f}  Dev: {block.trigger_dev:.6f}  "
            f"MaxDev: {block.trigger_max_dev:.6f}"
        )
        lines.append(
            f"Ratio: {block.trigger_ratio:.6f}  Mu: {block.mu:.6f}"
        )
        lines.append(
            f"买信号价: {block.buy_signal_price:.4f}  "
            f"卖信号价: {block.sell_signal_price:.4f}"
        )
        lines.append(
            f"结束现金: {block.end_cash:,.2f}  "
            f"结束市值: {block.end_nav:,.2f}"
        )
        lines.append(
            f"信号现金: {block.signal_cash:,.2f}  "
            f"信号市值: {block.signal_nav:,.2f}"
        )
        lines.append(
            f"Block Size: {block.block_size:,.0f}  "
            f"目标买: {block.buy_target}  目标卖: {block.sell_target}"
        )
        lines.append(f"候选列表: {block.candidate_list}")
        lines.append(f"选择原因: {block.chosen_reason}")
        lines.append("")
        lines.append(f"── 执行结果 ──")
        lines.append(f"状态: {block.state.value}")
        lines.append(
            f"买入: {block.buy_filled} 股 @ {block.buy_avg_price:.4f}  "
            f"佣金: {block.buy_commission:.2f}"
        )
        lines.append(
            f"卖出: {block.sell_filled} 股 @ {block.sell_avg_price:.4f}  "
            f"佣金: {block.sell_commission:.2f}"
        )
        lines.append(
            f"被动买: {max(0, block.buy_filled - block.buy_aggressive)}  "
            f"激进买: {block.buy_aggressive}"
        )
        lines.append(
            f"被动卖: {max(0, block.sell_filled - block.sell_aggressive)}  "
            f"激进卖: {block.sell_aggressive}"
        )
        lines.append(f"持续: {block.total_duration:.1f} 秒")
        lines.append("")
        lines.append(f"── 事件时间线 ({len(block.events)} 条) ──")
        for i, e in enumerate(block.events):
            ts = e.time.strftime("%H:%M:%S.%f")[:12]
            detail_str = "  ".join(f"{k}={v}" for k, v in e.detail.items())
            lines.append(f"  [{i:02d}] {ts}  {e.event_type:20s}  {detail_str}")

        self._detail_text.setPlainText("\n".join(lines))

    # ════════════════════════════════════════
    #  Tab 4: 净值曲线
    # ════════════════════════════════════════

    def _refresh_equity(self):
        fig = self._canvas_equity.fig
        fig.clear()

        if not self._result or not self._result.daily_nav:
            fig.text(0.5, 0.5, "无净值数据", ha="center", va="center", fontsize=14)
            self._canvas_equity.draw()
            return

        nav = self._result.daily_nav
        dates = [pd.Timestamp(d) for d, _ in nav]
        date_strs = [d for d, _ in nav]
        values = [v for _, v in nav]
        initial = self._result.start_balance

        # ── 买入并持有基准曲线 ──
        bh_curves: dict[str, list[float]] = {}  # symbol -> [market_value_per_day]
        if self._strategy and hasattr(self._strategy, "_all_daily"):
            all_daily = self._strategy._all_daily
            for symbol in self._strategy.symbols:
                daily = all_daily.get(symbol, {})
                if not daily:
                    continue
                # 取回测第一天的收盘价作为基准
                first_close = None
                for ds in date_strs:
                    if ds in daily:
                        first_close = daily[ds][0]  # close_adj
                        break
                if first_close is None or first_close <= 0:
                    continue
                curve = []
                for ds in date_strs:
                    if ds in daily:
                        close_adj = daily[ds][0]
                        curve.append(initial * close_adj / first_close)
                    else:
                        # 该日无数据，沿用前值
                        curve.append(curve[-1] if curve else initial)
                bh_curves[symbol] = curve

        # 买入并持有配色
        bh_colors = [
            "#FF9800", "#9C27B0", "#009688", "#E91E63",
            "#3F51B5", "#795548", "#607D8B", "#CDDC39",
        ]

        # 净值曲线
        ax1 = fig.add_subplot(2, 1, 1)
        ax1.plot(dates, values, color="#1976D2", linewidth=1.5,
                 label="策略净值", zorder=10)
        for i, (symbol, curve) in enumerate(bh_curves.items()):
            color = bh_colors[i % len(bh_colors)]
            short_name = symbol.split(".")[0]
            ax1.plot(dates, curve, color=color, linewidth=0.9,
                     linestyle="--", alpha=0.7,
                     label=f"持有 {short_name}")
        ax1.axhline(initial, color="#9E9E9E", linestyle=":", linewidth=0.6)
        ax1.set_title("净值曲线 vs 买入并持有")
        ax1.set_ylabel("净值")
        ax1.legend(fontsize=8, loc="upper left")
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

        # ── 超额收益 & 超额回撤 ──
        ax2 = fig.add_subplot(2, 1, 2, sharex=ax1)
        if bh_curves:
            nav_arr = np.array(values)
            bh_matrix = np.array(list(bh_curves.values()))  # (n_symbols, n_days)
            bh_max = bh_matrix.max(axis=0)   # 每日最优股票市值
            bh_mean = bh_matrix.mean(axis=0)  # 每日平均市值

            # 超额收益 = (策略 - 基准) / 基准
            excess_max = (nav_arr - bh_max) / bh_max
            excess_mean = (nav_arr - bh_mean) / bh_mean

            # 超额回撤 = (当前超额 - 历史最高超额) — 绝对值差
            excess_max_peak = np.maximum.accumulate(excess_max)
            excess_max_dd = excess_max - excess_max_peak
            excess_mean_peak = np.maximum.accumulate(excess_mean)
            excess_mean_dd = excess_mean - excess_mean_peak

            ax2.plot(dates, excess_max, color="#E91E63", linewidth=1.2,
                     label="超额收益 max")
            ax2.plot(dates, excess_mean, color="#1976D2", linewidth=1.2,
                     label="超额收益 mean")
            ax2.fill_between(dates, excess_max_dd, color="#E91E63",
                             alpha=0.15, label="超额回撤 max")
            ax2.fill_between(dates, excess_mean_dd, color="#1976D2",
                             alpha=0.15, label="超额回撤 mean")
            ax2.axhline(0, color="#9E9E9E", linestyle=":", linewidth=0.6)
        ax2.set_title("超额收益 & 超额回撤")
        ax2.set_ylabel("超额比例")
        ax2.legend(fontsize=7, loc="upper left")
        ax2.grid(True, alpha=0.3)
        ax2.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))

        fig.autofmt_xdate()
        fig.tight_layout()
        self._canvas_equity.draw()

    # ════════════════════════════════════════
    #  Tab 5: Block 时间线 (甘特图)
    # ════════════════════════════════════════

    def _refresh_timeline(self):
        fig = self._canvas_timeline.fig
        fig.clear()

        if not self._block_logs:
            fig.text(0.5, 0.5, "无 Block 数据", ha="center", va="center", fontsize=14)
            self._canvas_timeline.draw()
            return

        logs = [b for b in self._block_logs if b.signal_time and b.end_time]
        if not logs:
            fig.text(
                0.5, 0.5, "无有效时间范围的 Block",
                ha="center", va="center", fontsize=14,
            )
            self._canvas_timeline.draw()
            return

        # 按日期分组
        by_date: dict[str, list] = {}
        for b in logs:
            by_date.setdefault(b.trade_date, []).append(b)

        dates = sorted(by_date.keys())
        ax = fig.add_subplot(111)

        state_colors = {
            "DONE": "#4CAF50",
            "TIMEOUT": "#FF9800",
            "CRITICAL": "#F44336",
        }

        y_labels = []
        for i, date in enumerate(dates):
            blocks = by_date[date]
            for b in blocks:
                start = mdates.date2num(b.signal_time)
                end = mdates.date2num(b.end_time)
                width = end - start
                color = state_colors.get(b.state.value, "#2196F3")
                ax.barh(
                    i,
                    width,
                    left=start,
                    height=0.6,
                    color=color,
                    edgecolor="black",
                    linewidth=0.5,
                    alpha=0.8,
                )
            y_labels.append(date)

        ax.set_yticks(range(len(dates)))
        ax.set_yticklabels(y_labels, fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.set_title("Block 时间线 (按日)")
        ax.set_xlabel("时间")
        ax.grid(True, alpha=0.3, axis="x")

        # 图例
        from matplotlib.patches import Patch
        legend_elems = [
            Patch(facecolor="#4CAF50", label="DONE"),
            Patch(facecolor="#FF9800", label="TIMEOUT"),
            Patch(facecolor="#F44336", label="CRITICAL"),
            Patch(facecolor="#2196F3", label="其他"),
        ]
        ax.legend(handles=legend_elems, loc="upper right", fontsize=8)

        fig.tight_layout()
        self._canvas_timeline.draw()

    # ════════════════════════════════════════
    #  Tab 6: 日交易可视化
    # ════════════════════════════════════════

    def _build_daily_trade_tab(self) -> QWidget:
        w = QWidget()
        layout = QVBoxLayout(w)

        # 日期选择器
        sel_row = QHBoxLayout()
        sel_row.addWidget(QLabel("选择交易日:"))
        self._combo_daily_date = QComboBox()
        self._combo_daily_date.currentIndexChanged.connect(self._on_daily_date_changed)
        sel_row.addWidget(self._combo_daily_date, stretch=1)
        layout.addLayout(sel_row)

        # Matplotlib 画布 + 工具栏
        self._canvas_daily = _MplCanvas(width=12, height=8)
        self._toolbar_daily = NavigationToolbar(self._canvas_daily, w)
        layout.addWidget(self._toolbar_daily)
        layout.addWidget(self._canvas_daily, stretch=1)
        return w

    def _refresh_daily_combo(self):
        """填充日期选择下拉框。"""
        self._combo_daily_date.blockSignals(True)
        self._combo_daily_date.clear()

        # 收集所有有 block 或有日参数的交易日
        dates_set: set[str] = set()
        for b in self._block_logs:
            if b.trade_date:
                dates_set.add(b.trade_date)
        if self._strategy:
            dp = getattr(self._strategy, "_daily_params", {})
            dates_set.update(dp.keys())

        dates = sorted(dates_set)
        for d in dates:
            # 统计当天 block 数
            n_blocks = sum(1 for b in self._block_logs if b.trade_date == d)
            label = f"{d}  ({n_blocks} blocks)"
            self._combo_daily_date.addItem(label, d)

        self._combo_daily_date.blockSignals(False)
        if dates:
            self._combo_daily_date.setCurrentIndex(0)
            self._on_daily_date_changed(0)

    def _on_daily_date_changed(self, idx):
        if idx < 0:
            return
        date_str = self._combo_daily_date.itemData(idx)
        if date_str:
            self._draw_daily_trade(date_str)

    def _draw_daily_trade(self, date_str: str):
        """绘制单日交易可视化: 价格 + Score + Block 事件标注。"""
        fig = self._canvas_daily.fig
        fig.clear()

        strategy = self._strategy
        if not strategy:
            fig.text(0.5, 0.5, "无策略数据", ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        symbols = list(strategy.symbols)
        dataset_dir = getattr(strategy, "dataset_dir", "")
        if not dataset_dir or len(symbols) < 2:
            fig.text(0.5, 0.5, "数据不足", ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        # ── 加载 Tick 数据 ──
        from core.data_feed import ParquetTickFeed
        feed = ParquetTickFeed(dataset_dir)

        ticks_by_sym: dict[str, list] = {}
        for sym in symbols:
            ticks_by_sym[sym] = feed.load_day(sym, date_str)

        if not any(ticks_by_sym.values()):
            fig.text(0.5, 0.5, f"{date_str} 无 Tick 数据",
                     ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        # ── 获取当日参数 ──
        daily_params = getattr(strategy, "_daily_params", {}).get(date_str)
        k_threshold = getattr(strategy, "k_threshold", 0.3)
        sym_a, sym_b = symbols[0], symbols[1]
        pair_ab = (sym_a, sym_b)
        pair_ba = (sym_b, sym_a)

        mu_ab = daily_params["mu"].get(pair_ab) if daily_params else None
        max_dev_ab = daily_params["max_dev"].get(pair_ab) if daily_params else None
        mu_ba = daily_params["mu"].get(pair_ba) if daily_params else None
        max_dev_ba = daily_params["max_dev"].get(pair_ba) if daily_params else None
        adj_factors = daily_params.get("adj", {}) if daily_params else {}

        # ── 构建时间对齐的价格序列 ──
        # 使用 sym_a 的 tick 时间轴为主轴
        from datetime import time as dt_time

        # 合并所有 tick 时间并排序
        all_times: list[datetime] = []
        tick_price: dict[str, dict[datetime, float]] = {s: {} for s in symbols}
        for sym in symbols:
            for t in ticks_by_sym[sym]:
                tt = t.datetime.time()
                if tt < dt_time(9, 30) or tt >= dt_time(15, 0):
                    continue
                if t.last_price > 0:
                    tick_price[sym][t.datetime] = t.last_price
                    all_times.append(t.datetime)

        if not all_times:
            fig.text(0.5, 0.5, f"{date_str} 无有效 Tick",
                     ha="center", va="center", fontsize=14)
            self._canvas_daily.draw()
            return

        all_times = sorted(set(all_times))

        # 前向填充价格
        prices_aligned: dict[str, list[float]] = {s: [] for s in symbols}
        last_p: dict[str, float] = {s: 0.0 for s in symbols}
        for t in all_times:
            for sym in symbols:
                if t in tick_price[sym]:
                    last_p[sym] = tick_price[sym][t]
                prices_aligned[sym].append(last_p[sym])

        # ── 计算 Score 序列 ──
        scores_ab: list[float] = []  # sym_a / sym_b (sym_a 高估 → sell sym_a)
        scores_ba: list[float] = []  # sym_b / sym_a (sym_b 高估 → sell sym_b)

        adj_a = adj_factors.get(sym_a, 1.0)
        adj_b = adj_factors.get(sym_b, 1.0)

        for i, t in enumerate(all_times):
            p_a = prices_aligned[sym_a][i] * adj_a
            p_b = prices_aligned[sym_b][i] * adj_b
            if p_a > 0 and p_b > 0:
                # Score for pair (A, B): A is overvalued vs B
                if mu_ab and max_dev_ab and max_dev_ab > 0:
                    ratio = p_a / p_b
                    dev = ratio - mu_ab
                    scores_ab.append(dev / max_dev_ab)
                else:
                    scores_ab.append(0.0)
                # Score for pair (B, A): B is overvalued vs A
                if mu_ba and max_dev_ba and max_dev_ba > 0:
                    ratio = p_b / p_a
                    dev = ratio - mu_ba
                    scores_ba.append(dev / max_dev_ba)
                else:
                    scores_ba.append(0.0)
            else:
                scores_ab.append(0.0)
                scores_ba.append(0.0)

        # ── 当日 Block 列表 ──
        day_blocks = [b for b in self._block_logs if b.trade_date == date_str]

        # ── 绘图: 上方=价格, 下方=Score ──
        ax_price = fig.add_subplot(2, 1, 1)
        ax_score = fig.add_subplot(2, 1, 2, sharex=ax_price)

        COLOR_A = "#1976D2"  # 深蓝
        COLOR_B = "#E65100"  # 深橙

        short_a = sym_a.split(".")[0]
        short_b = sym_b.split(".")[0]

        # ── 价格子图 ──
        valid_a = [(t, p) for t, p in zip(all_times, prices_aligned[sym_a]) if p > 0]
        valid_b = [(t, p) for t, p in zip(all_times, prices_aligned[sym_b]) if p > 0]

        if valid_a:
            ax_price.plot(
                [x[0] for x in valid_a], [x[1] for x in valid_a],
                color=COLOR_A, linewidth=0.8, alpha=0.8, label=short_a,
            )
        ax2_price = ax_price.twinx()
        if valid_b:
            ax2_price.plot(
                [x[0] for x in valid_b], [x[1] for x in valid_b],
                color=COLOR_B, linewidth=0.8, alpha=0.8, label=short_b,
                linestyle="--",
            )

        ax_price.set_ylabel(f"{short_a} 价格", color=COLOR_A)
        ax_price.tick_params(axis="y", labelcolor=COLOR_A)
        ax2_price.set_ylabel(f"{short_b} 价格", color=COLOR_B)
        ax2_price.tick_params(axis="y", labelcolor=COLOR_B)

        # ── Score 子图 ──
        ax_score.plot(all_times, scores_ab, color=COLOR_A, linewidth=0.7,
                      alpha=0.8, label=f"Score({short_a}/{short_b})")
        ax_score.plot(all_times, scores_ba, color=COLOR_B, linewidth=0.7,
                      alpha=0.8, label=f"Score({short_b}/{short_a})",
                      linestyle="--")
        ax_score.axhline(k_threshold, color="#F44336", linestyle=":",
                         linewidth=1.0, alpha=0.7, label=f"k={k_threshold}")
        ax_score.axhline(0, color="#9E9E9E", linestyle="-",
                         linewidth=0.5, alpha=0.5)
        ax_score.set_ylabel("Score")
        ax_score.set_xlabel("时间")
        ax_score.legend(loc="upper left", fontsize=8, ncol=3)

        # ── Block 事件标注 ──
        _state_colors = {
            "DONE": "#4CAF50",
            "TIMEOUT": "#FF9800",
            "CRITICAL": "#F44336",
        }

        for b in day_blocks:
            block_color = _state_colors.get(b.state.value, "#2196F3")

            # 1) 信号触发点 (星号)
            if b.signal_time:
                # 在价格图上标注
                ax_price.axvline(b.signal_time, color=block_color,
                                 linestyle="--", linewidth=0.6, alpha=0.5)
                # 在 Score 图上标三角
                ax_score.axvline(b.signal_time, color=block_color,
                                 linestyle="--", linewidth=0.6, alpha=0.5)

                # 信号点标注
                if b.sell_signal_price and b.sell_signal_price > 0:
                    sell_ax = ax_price if b.sell_symbol == sym_a else ax2_price
                    sell_ax.scatter(
                        [b.signal_time], [b.sell_signal_price],
                        marker="*", s=120, c=block_color, zorder=15,
                        edgecolors="black", linewidths=0.5,
                    )
                if b.buy_signal_price and b.buy_signal_price > 0:
                    buy_ax = ax_price if b.buy_symbol == sym_a else ax2_price
                    buy_ax.scatter(
                        [b.signal_time], [b.buy_signal_price],
                        marker="*", s=120, c=block_color, zorder=15,
                        edgecolors="black", linewidths=0.5,
                    )

            # 2) 成交事件 — 从 events 提取 FILL 时间和价格
            for ev in b.events:
                if ev.event_type not in ("PASSIVE_FILL", "AGGRESSIVE_FILL"):
                    continue
                d = ev.detail
                side = d.get("side", "")
                price = d.get("fill_price", 0)
                if price <= 0:
                    continue

                if side == "sell":
                    marker = "v"  # 卖出向下三角
                    target_ax = ax_price if b.sell_symbol == sym_a else ax2_price
                    color = "#D32F2F" if ev.event_type == "AGGRESSIVE_FILL" else "#FF9800"
                elif side == "buy":
                    marker = "^"  # 买入向上三角
                    target_ax = ax_price if b.buy_symbol == sym_a else ax2_price
                    color = "#1B5E20" if ev.event_type == "AGGRESSIVE_FILL" else "#4CAF50"
                else:
                    continue

                target_ax.scatter(
                    [ev.time], [price],
                    marker=marker, s=30, c=color, zorder=12,
                    alpha=0.7, edgecolors="black", linewidths=0.3,
                )

            # 3) Block 完成点
            if b.end_time:
                ax_score.axvline(b.end_time, color=block_color,
                                 linestyle=":", linewidth=0.5, alpha=0.4)

            # 4) Block ID 标注 (在 score 图上)
            if b.signal_time:
                score_val = b.trigger_score
                ax_score.annotate(
                    b.block_id.split("-")[-1],
                    xy=(b.signal_time, min(score_val, k_threshold + 0.5)),
                    fontsize=7, color=block_color, alpha=0.8,
                    ha="center", va="bottom",
                )

        # ── 标题 & 格式 ──
        n_done = sum(1 for b in day_blocks if b.state.value == "DONE")
        n_timeout = sum(1 for b in day_blocks if b.state.value == "TIMEOUT")
        n_critical = sum(1 for b in day_blocks if b.state.value == "CRITICAL")

        status_parts = []
        if n_done:
            status_parts.append(f"DONE={n_done}")
        if n_timeout:
            status_parts.append(f"TIMEOUT={n_timeout}")
        if n_critical:
            status_parts.append(f"CRITICAL={n_critical}")
        status_str = ", ".join(status_parts) if status_parts else "无 Block"

        ax_price.set_title(
            f"{date_str}  日交易总览  |  {short_a} vs {short_b}  |  "
            f"Blocks: {len(day_blocks)} ({status_str})",
            fontsize=11,
        )
        ax_price.grid(True, alpha=0.2)
        ax_score.grid(True, alpha=0.2)

        # X 轴格式
        ax_score.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        fig.autofmt_xdate(rotation=30)

        # 图例 — 价格图
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch

        legend_items = [
            Line2D([0], [0], color=COLOR_A, linewidth=1.2, label=short_a),
            Line2D([0], [0], color=COLOR_B, linewidth=1.2, linestyle="--", label=short_b),
            Line2D([0], [0], marker="*", color="w", markerfacecolor="#4CAF50",
                   markersize=10, label="信号触发"),
            Line2D([0], [0], marker="^", color="w", markerfacecolor="#4CAF50",
                   markersize=8, label="买入成交"),
            Line2D([0], [0], marker="v", color="w", markerfacecolor="#FF9800",
                   markersize=8, label="卖出成交"),
        ]
        for state_name, state_color in _state_colors.items():
            legend_items.append(
                Patch(facecolor=state_color, alpha=0.5, label=state_name)
            )
        ax_price.legend(
            handles=legend_items, loc="upper left", fontsize=7,
            ncol=4, framealpha=0.8, borderpad=0.3,
        )

        fig.tight_layout()
        self._canvas_daily.draw()
