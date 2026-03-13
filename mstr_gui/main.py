"""
MSTR 回测工具 — 启动器 + 日志查看器。

用法:
    python -m mstr_gui
"""

from __future__ import annotations

import sys
from pathlib import Path

# 确保项目根目录在 sys.path 中
_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from PyQt5.QtWidgets import QApplication, QMainWindow, QTabWidget
from PyQt5.QtCore import Qt


class MstrMainWindow(QMainWindow):
    """MSTR 回测工具主窗口 — 双标签页。"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("MSTR 回测工具")
        self.resize(1100, 800)

        tabs = QTabWidget()
        self.setCentralWidget(tabs)

        # 延迟导入避免循环
        from mstr_gui.launcher import LauncherWidget
        from mstr_gui.log_viewer import LogViewerWidget

        self._launcher = LauncherWidget()
        self._viewer = LogViewerWidget()

        tabs.addTab(self._launcher, "🚀 回测启动器")
        tabs.addTab(self._viewer, "📊 日志查看器")

        # 连接: 回测完成 → 日志查看器自动加载
        self._launcher.backtest_finished.connect(self._on_backtest_done)

        self._tabs = tabs

    def _on_backtest_done(self, result, strategy):
        self._viewer.load_result(result, strategy)
        self._tabs.setCurrentIndex(1)  # 自动跳转到查看器


def main() -> None:
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = MstrMainWindow()
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
