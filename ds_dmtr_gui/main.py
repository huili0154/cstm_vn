"""
DS_DMTR 回测工具 — 启动器 + 日志查看器。

用法:
    python -m ds_dmtr_gui
"""

from __future__ import annotations

import sys
from pathlib import Path

# 确保项目根目录在 sys.path 中
_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QApplication, QMainWindow, QTabWidget


class DsDmtrMainWindow(QMainWindow):
    """DS_DMTR 回测工具主窗口 — 双标签页。"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("DS_DMTR 回测工具 — 双股双均线 Tick 级回归")
        self.resize(1100, 800)
        self.setStyleSheet(
            "QTabBar::tab { font-size: 12px; padding: 6px 12px; }"
        )

        tabs = QTabWidget()
        self.setCentralWidget(tabs)

        from ds_dmtr_gui.launcher import LauncherWidget
        from ds_dmtr_gui.log_viewer import LogViewerWidget

        self._launcher = LauncherWidget()
        self._viewer = LogViewerWidget()

        tabs.addTab(self._launcher, "🚀 回测启动器")
        tabs.addTab(self._viewer, "📊 回测结果")

        self._launcher.backtest_finished.connect(self._on_backtest_done)
        self._tabs = tabs

    def _on_backtest_done(self, result, strategy):
        self._viewer.load_result(result, strategy)
        self._tabs.setCurrentIndex(1)


def main() -> None:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Microsoft YaHei", 11))
    w = DsDmtrMainWindow()
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
