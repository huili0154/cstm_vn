"""
实盘交易 GUI — PyQt5 界面。

功能:
  - 连接/断开 QMT
  - 选择策略、配置参数、启动/停止
  - 实时显示: 行情、持仓、委托、成交
  - 手动下单面板
  - 日志窗口
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from typing import Any

from PyQt5.QtCore import QTimer, Qt, pyqtSignal, QObject
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QApplication,
    QComboBox,
    QDoubleSpinBox,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from core.datatypes import (
    Direction,
    Order,
    OrderType,
    TickData,
    Trade,
)

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════
#  信号桥 — 用于跨线程安全更新 GUI
# ════════════════════════════════════════════════════

class _SignalBridge(QObject):
    sig_log = pyqtSignal(str)
    sig_tick = pyqtSignal(object)
    sig_order = pyqtSignal(object)
    sig_trade = pyqtSignal(object)


# ════════════════════════════════════════════════════
#  主窗口
# ════════════════════════════════════════════════════

class TraderWindow(QMainWindow):
    """实盘交易主窗口。"""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("cstm_vn — 实盘交易")
        self.resize(1280, 800)

        self._engine = None  # LiveEngine
        self._gateway = None  # QmtGateway
        self._strategy = None  # StrategyBase instance

        self._bridge = _SignalBridge()
        self._bridge.sig_log.connect(self._on_log)
        self._bridge.sig_tick.connect(self._on_tick_update)
        self._bridge.sig_order.connect(self._on_order_update)
        self._bridge.sig_trade.connect(self._on_trade_update)

        self._init_ui()

        # 定时刷新持仓/账户 (1秒)
        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._refresh_account_positions)
        self._refresh_timer.start(1000)

    # ────────────── UI 构建 ──────────────

    def _init_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # 顶部: 连接 + 策略控制
        top = QHBoxLayout()
        top.addWidget(self._build_connection_group())
        top.addWidget(self._build_strategy_group())
        top.addWidget(self._build_manual_order_group())
        layout.addLayout(top)

        # 中部: 数据表格
        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_account_group())
        splitter.addWidget(self._build_tick_table_group())
        splitter.addWidget(self._build_position_table_group())
        splitter.addWidget(self._build_order_table_group())
        splitter.addWidget(self._build_trade_table_group())
        layout.addWidget(splitter, stretch=1)

        # 底部: 日志
        layout.addWidget(self._build_log_group())

    # ── 连接面板 ──

    def _build_connection_group(self) -> QGroupBox:
        grp = QGroupBox("QMT 连接")
        lay = QGridLayout(grp)

        lay.addWidget(QLabel("QMT路径:"), 0, 0)
        self._edit_qmt_path = QLineEdit()
        self._edit_qmt_path.setPlaceholderText("userdata_mini 目录路径")
        lay.addWidget(self._edit_qmt_path, 0, 1, 1, 2)

        lay.addWidget(QLabel("账号:"), 1, 0)
        self._edit_account = QLineEdit()
        self._edit_account.setPlaceholderText("资金账号")
        lay.addWidget(self._edit_account, 1, 1)

        self._btn_connect = QPushButton("连接")
        self._btn_connect.clicked.connect(self._on_connect)
        lay.addWidget(self._btn_connect, 1, 2)

        self._lbl_conn_status = QLabel("未连接")
        self._lbl_conn_status.setStyleSheet("color: red; font-weight: bold;")
        lay.addWidget(self._lbl_conn_status, 2, 0, 1, 3)

        return grp

    # ── 策略面板 ──

    def _build_strategy_group(self) -> QGroupBox:
        grp = QGroupBox("策略控制")
        lay = QGridLayout(grp)

        lay.addWidget(QLabel("策略类:"), 0, 0)
        self._combo_strategy = QComboBox()
        lay.addWidget(self._combo_strategy, 0, 1)

        lay.addWidget(QLabel("品种:"), 1, 0)
        self._edit_symbols = QLineEdit()
        self._edit_symbols.setPlaceholderText("逗号分隔, e.g. 510300.SH,159919.SZ")
        lay.addWidget(self._edit_symbols, 1, 1)

        lay.addWidget(QLabel("参数 JSON:"), 2, 0)
        self._edit_params = QLineEdit()
        self._edit_params.setPlaceholderText('{"fast":5, "slow":20}')
        lay.addWidget(self._edit_params, 2, 1)

        btn_row = QHBoxLayout()
        self._btn_start = QPushButton("启动策略")
        self._btn_start.clicked.connect(self._on_start_strategy)
        self._btn_start.setEnabled(False)
        btn_row.addWidget(self._btn_start)

        self._btn_stop = QPushButton("停止策略")
        self._btn_stop.clicked.connect(self._on_stop_strategy)
        self._btn_stop.setEnabled(False)
        btn_row.addWidget(self._btn_stop)

        lay.addLayout(btn_row, 3, 0, 1, 2)
        return grp

    # ── 手动下单面板 ──

    def _build_manual_order_group(self) -> QGroupBox:
        grp = QGroupBox("手动下单")
        lay = QGridLayout(grp)

        lay.addWidget(QLabel("品种:"), 0, 0)
        self._edit_order_symbol = QLineEdit()
        self._edit_order_symbol.setPlaceholderText("510300.SH")
        lay.addWidget(self._edit_order_symbol, 0, 1)

        lay.addWidget(QLabel("方向:"), 1, 0)
        self._combo_direction = QComboBox()
        self._combo_direction.addItems(["BUY", "SELL"])
        lay.addWidget(self._combo_direction, 1, 1)

        lay.addWidget(QLabel("价格:"), 2, 0)
        self._spin_price = QDoubleSpinBox()
        self._spin_price.setDecimals(3)
        self._spin_price.setMaximum(999999.0)
        self._spin_price.setSingleStep(0.001)
        lay.addWidget(self._spin_price, 2, 1)

        lay.addWidget(QLabel("数量:"), 3, 0)
        self._spin_volume = QSpinBox()
        self._spin_volume.setMaximum(10_000_000)
        self._spin_volume.setSingleStep(100)
        self._spin_volume.setValue(100)
        lay.addWidget(self._spin_volume, 3, 1)

        btn_row = QHBoxLayout()
        self._btn_limit_order = QPushButton("限价下单")
        self._btn_limit_order.clicked.connect(self._on_manual_limit_order)
        self._btn_limit_order.setEnabled(False)
        btn_row.addWidget(self._btn_limit_order)

        self._btn_market_order = QPushButton("市价下单")
        self._btn_market_order.clicked.connect(self._on_manual_market_order)
        self._btn_market_order.setEnabled(False)
        btn_row.addWidget(self._btn_market_order)

        lay.addLayout(btn_row, 4, 0, 1, 2)
        return grp

    # ── 账户信息 ──

    def _build_account_group(self) -> QGroupBox:
        grp = QGroupBox("账户信息")
        lay = QHBoxLayout(grp)
        self._lbl_total_asset = QLabel("总资产: --")
        self._lbl_available = QLabel("可用: --")
        self._lbl_frozen = QLabel("冻结: --")
        self._lbl_market_value = QLabel("市值: --")
        for lbl in (self._lbl_total_asset, self._lbl_available,
                     self._lbl_frozen, self._lbl_market_value):
            lbl.setFont(QFont("Consolas", 10))
            lay.addWidget(lbl)
        return grp

    # ── 行情表 ──

    def _build_tick_table_group(self) -> QGroupBox:
        grp = QGroupBox("实时行情")
        lay = QVBoxLayout(grp)
        self._tbl_tick = QTableWidget(0, 8)
        self._tbl_tick.setHorizontalHeaderLabels([
            "品种", "最新价", "涨跌%", "买一价", "买一量",
            "卖一价", "卖一量", "时间",
        ])
        self._tbl_tick.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        self._tbl_tick.setEditTriggers(QTableWidget.NoEditTriggers)
        lay.addWidget(self._tbl_tick)
        self._tick_row_map: dict[str, int] = {}
        return grp

    # ── 持仓表 ──

    def _build_position_table_group(self) -> QGroupBox:
        grp = QGroupBox("持仓")
        lay = QVBoxLayout(grp)
        self._tbl_pos = QTableWidget(0, 6)
        self._tbl_pos.setHorizontalHeaderLabels([
            "品种", "持仓量", "可用量", "成本价", "市价", "浮动盈亏",
        ])
        self._tbl_pos.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        self._tbl_pos.setEditTriggers(QTableWidget.NoEditTriggers)
        lay.addWidget(self._tbl_pos)
        return grp

    # ── 委托表 ──

    def _build_order_table_group(self) -> QGroupBox:
        grp = QGroupBox("委托")
        lay = QVBoxLayout(grp)

        self._tbl_order = QTableWidget(0, 8)
        self._tbl_order.setHorizontalHeaderLabels([
            "委托号", "品种", "方向", "类型", "价格", "委托量",
            "成交量", "状态",
        ])
        self._tbl_order.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        self._tbl_order.setEditTriggers(QTableWidget.NoEditTriggers)

        btn_row = QHBoxLayout()
        self._btn_cancel_selected = QPushButton("撤选中单")
        self._btn_cancel_selected.clicked.connect(self._on_cancel_selected_order)
        btn_row.addWidget(self._btn_cancel_selected)
        self._btn_cancel_all = QPushButton("全撤")
        self._btn_cancel_all.clicked.connect(self._on_cancel_all_orders)
        btn_row.addWidget(self._btn_cancel_all)

        lay.addWidget(self._tbl_order)
        lay.addLayout(btn_row)
        return grp

    # ── 成交表 ──

    def _build_trade_table_group(self) -> QGroupBox:
        grp = QGroupBox("成交")
        lay = QVBoxLayout(grp)
        self._tbl_trade = QTableWidget(0, 7)
        self._tbl_trade.setHorizontalHeaderLabels([
            "成交号", "委托号", "品种", "方向", "价格", "成交量", "时间",
        ])
        self._tbl_trade.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        self._tbl_trade.setEditTriggers(QTableWidget.NoEditTriggers)
        lay.addWidget(self._tbl_trade)
        return grp

    # ── 日志 ──

    def _build_log_group(self) -> QGroupBox:
        grp = QGroupBox("日志")
        lay = QVBoxLayout(grp)
        self._txt_log = QTextEdit()
        self._txt_log.setReadOnly(True)
        self._txt_log.setMaximumHeight(150)
        self._txt_log.setFont(QFont("Consolas", 9))
        lay.addWidget(self._txt_log)
        return grp

    # ════════════════════════════════════════════════
    #  槽函数
    # ════════════════════════════════════════════════

    def _on_connect(self) -> None:
        """连接/断开 QMT。"""
        if self._gateway and self._gateway.connected:
            # 断开
            if self._engine and self._engine._running:
                self._on_stop_strategy()
            self._gateway.disconnect()
            self._gateway = None
            self._engine = None
            self._btn_connect.setText("连接")
            self._lbl_conn_status.setText("未连接")
            self._lbl_conn_status.setStyleSheet("color: red; font-weight: bold;")
            self._btn_start.setEnabled(False)
            self._btn_limit_order.setEnabled(False)
            self._btn_market_order.setEnabled(False)
            self._log("QMT 已断开")
            return

        qmt_path = self._edit_qmt_path.text().strip()
        account_id = self._edit_account.text().strip()
        if not qmt_path or not account_id:
            QMessageBox.warning(self, "参数缺失", "请填写 QMT 路径和账号。")
            return

        from live.qmt_gateway import QmtGateway
        from live.engine import LiveEngine

        self._gateway = QmtGateway(qmt_path, account_id)
        if not self._gateway.connect():
            QMessageBox.critical(self, "连接失败", "无法连接 QMT，请检查路径和客户端。")
            self._gateway = None
            return

        self._engine = LiveEngine(self._gateway)
        # 挂接信号
        self._engine.on_log_callback = lambda msg: self._bridge.sig_log.emit(msg)
        self._engine.on_tick_callback = lambda t: self._bridge.sig_tick.emit(t)
        self._engine.on_order_callback = lambda o: self._bridge.sig_order.emit(o)
        self._engine.on_trade_callback = lambda t: self._bridge.sig_trade.emit(t)

        self._btn_connect.setText("断开")
        self._lbl_conn_status.setText(f"已连接 ({account_id})")
        self._lbl_conn_status.setStyleSheet("color: green; font-weight: bold;")
        self._btn_start.setEnabled(True)
        self._btn_limit_order.setEnabled(True)
        self._btn_market_order.setEnabled(True)
        self._log("QMT 已连接")

    def _on_start_strategy(self) -> None:
        """启动策略。"""
        if not self._engine:
            return

        symbols_text = self._edit_symbols.text().strip()
        if not symbols_text:
            QMessageBox.warning(self, "参数缺失", "请填写品种列表。")
            return
        symbols = [s.strip() for s in symbols_text.split(",") if s.strip()]

        # 解析参数
        import json
        params_text = self._edit_params.text().strip()
        setting = {}
        if params_text:
            try:
                setting = json.loads(params_text)
            except json.JSONDecodeError as e:
                QMessageBox.warning(self, "参数错误", f"JSON 解析失败: {e}")
                return

        # 获取策略类
        strategy_cls = self._get_selected_strategy_class()
        if strategy_cls is None:
            QMessageBox.warning(self, "策略缺失", "请先注册策略类。")
            return

        strategy = strategy_cls(
            engine=self._engine,
            strategy_name=strategy_cls.__name__,
            symbols=symbols,
            setting=setting,
        )
        self._strategy = strategy

        if self._engine.start(strategy):
            self._btn_start.setEnabled(False)
            self._btn_stop.setEnabled(True)
            self._log(f"策略 {strategy_cls.__name__} 已启动")
        else:
            QMessageBox.critical(self, "启动失败", "策略启动失败，请查看日志。")

    def _on_stop_strategy(self) -> None:
        """停止策略。"""
        if self._engine:
            self._engine.stop()
        self._btn_start.setEnabled(True)
        self._btn_stop.setEnabled(False)
        self._log("策略已停止")

    def _on_manual_limit_order(self) -> None:
        """手动限价下单。"""
        self._send_manual_order(OrderType.LIMIT)

    def _on_manual_market_order(self) -> None:
        """手动市价下单。"""
        self._send_manual_order(OrderType.MARKET)

    def _send_manual_order(self, order_type: OrderType) -> None:
        if not self._gateway or not self._gateway.connected:
            return
        symbol = self._edit_order_symbol.text().strip()
        if not symbol:
            QMessageBox.warning(self, "参数缺失", "请填写品种代码。")
            return
        direction = (
            Direction.BUY
            if self._combo_direction.currentText() == "BUY"
            else Direction.SELL
        )
        price = self._spin_price.value()
        volume = self._spin_volume.value()

        order_id = self._gateway.send_order(
            symbol=symbol,
            direction=direction,
            order_type=order_type,
            price=price,
            volume=volume,
            strategy_name="manual",
        )
        if order_id:
            self._log(
                f"手动下单: {symbol} {direction.value} "
                f"{order_type.value} price={price} vol={volume} "
                f"→ order_id={order_id}"
            )

    def _on_cancel_selected_order(self) -> None:
        """撤选中的委托。"""
        row = self._tbl_order.currentRow()
        if row < 0:
            return
        order_id = self._tbl_order.item(row, 0)
        if order_id and self._gateway:
            self._gateway.cancel_order(order_id.text())

    def _on_cancel_all_orders(self) -> None:
        """全撤。"""
        if self._engine and self._strategy:
            self._engine.cancel_all(self._strategy)
        elif self._gateway:
            orders = self._gateway.query_orders()
            for o in orders:
                if o.is_active:
                    self._gateway.cancel_order(o.order_id)

    # ════════════════════════════════════════════════
    #  信号槽 — GUI 更新 (主线程)
    # ════════════════════════════════════════════════

    def _on_log(self, msg: str) -> None:
        self._txt_log.append(msg)

    def _on_tick_update(self, tick: TickData) -> None:
        sym = tick.symbol
        if sym not in self._tick_row_map:
            row = self._tbl_tick.rowCount()
            self._tbl_tick.insertRow(row)
            self._tick_row_map[sym] = row
        row = self._tick_row_map[sym]

        change_pct = 0.0
        if tick.pre_close > 0:
            change_pct = (tick.last_price - tick.pre_close) / tick.pre_close * 100

        values = [
            sym,
            f"{tick.last_price:.3f}",
            f"{change_pct:+.2f}%",
            f"{tick.bid_price_1:.3f}",
            str(tick.bid_volume_1),
            f"{tick.ask_price_1:.3f}",
            str(tick.ask_volume_1),
            tick.datetime.strftime("%H:%M:%S") if tick.datetime else "",
        ]
        for col, val in enumerate(values):
            item = QTableWidgetItem(val)
            item.setTextAlignment(Qt.AlignCenter)
            if col == 2:  # 涨跌%
                if change_pct > 0:
                    item.setForeground(QColor("red"))
                elif change_pct < 0:
                    item.setForeground(QColor("green"))
            self._tbl_tick.setItem(row, col, item)

    def _on_order_update(self, order: Order) -> None:
        # 查找已有行或新增
        target_row = -1
        for r in range(self._tbl_order.rowCount()):
            item = self._tbl_order.item(r, 0)
            if item and item.text() == order.order_id:
                target_row = r
                break
        if target_row < 0:
            target_row = self._tbl_order.rowCount()
            self._tbl_order.insertRow(target_row)

        values = [
            order.order_id,
            order.symbol,
            order.direction.value,
            order.order_type.value,
            f"{order.price:.3f}",
            str(order.volume),
            str(order.traded),
            order.status.value,
        ]
        for col, val in enumerate(values):
            item = QTableWidgetItem(val)
            item.setTextAlignment(Qt.AlignCenter)
            self._tbl_order.setItem(target_row, col, item)

    def _on_trade_update(self, trade: Trade) -> None:
        row = self._tbl_trade.rowCount()
        self._tbl_trade.insertRow(row)
        values = [
            trade.trade_id,
            trade.order_id,
            trade.symbol,
            trade.direction.value,
            f"{trade.price:.3f}",
            str(trade.volume),
            trade.datetime.strftime("%H:%M:%S") if trade.datetime else "",
        ]
        for col, val in enumerate(values):
            item = QTableWidgetItem(val)
            item.setTextAlignment(Qt.AlignCenter)
            self._tbl_trade.setItem(row, col, item)

    def _refresh_account_positions(self) -> None:
        """定时刷新账户和持仓。"""
        if not self._gateway or not self._gateway.connected:
            return

        # 账户
        acct = self._gateway.query_account()
        self._lbl_total_asset.setText(f"总资产: {acct.balance:,.2f}")
        self._lbl_available.setText(f"可用: {acct.available:,.2f}")
        self._lbl_frozen.setText(f"冻结: {acct.frozen:,.2f}")

        # 持仓
        positions = self._gateway.query_positions()
        self._tbl_pos.setRowCount(0)
        total_mv = 0.0
        for sym, pos in positions.items():
            row = self._tbl_pos.rowCount()
            self._tbl_pos.insertRow(row)
            pnl = pos.pnl
            total_mv += pos.market_value
            values = [
                sym,
                str(pos.volume),
                str(pos.available),
                f"{pos.cost_price:.3f}",
                f"{pos.market_price:.3f}",
                f"{pnl:+,.2f}",
            ]
            for col, val in enumerate(values):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignCenter)
                if col == 5:  # 浮动盈亏
                    if pnl > 0:
                        item.setForeground(QColor("red"))
                    elif pnl < 0:
                        item.setForeground(QColor("green"))
                self._tbl_pos.setItem(row, col, item)
        self._lbl_market_value.setText(f"市值: {total_mv:,.2f}")

    # ════════════════════════════════════════════════
    #  策略注册
    # ════════════════════════════════════════════════

    _strategy_classes: dict[str, type] = {}

    def register_strategy(self, cls: type) -> None:
        """注册策略类到下拉列表。"""
        name = cls.__name__
        self._strategy_classes[name] = cls
        self._combo_strategy.addItem(name)

    def _get_selected_strategy_class(self) -> type | None:
        name = self._combo_strategy.currentText()
        return self._strategy_classes.get(name)

    # ──────────── 日志辅助 ──────────────

    def _log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self._txt_log.append(f"[{ts}] {msg}")
