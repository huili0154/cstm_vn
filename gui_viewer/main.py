from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PyQt5.QtWidgets import QApplication


def main() -> None:
    parser = argparse.ArgumentParser(prog="gui_viewer")
    parser.add_argument("--dataset-root", default=None)
    args = parser.parse_args()

    app = QApplication(sys.argv)
    from gui_viewer.ui_main import MainWindow
    ds_root = Path(args.dataset_root) if args.dataset_root else None
    w = MainWindow(dataset_root=ds_root)
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

