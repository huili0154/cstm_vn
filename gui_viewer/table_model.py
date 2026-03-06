from __future__ import annotations

from typing import Any

import pandas as pd
from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt


class DataFrameTableModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame | None = None) -> None:
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()

    def set_dataframe(self, df: pd.DataFrame) -> None:
        self.beginResetModel()
        self._df = df
        self.endResetModel()

    def dataframe(self) -> pd.DataFrame:
        return self._df

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return int(len(self._df))

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return int(len(self._df.columns))

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            if 0 <= section < len(self._df.columns):
                return str(self._df.columns[section])
            return None
        return str(section + 1)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid():
            return None
        if role not in (Qt.DisplayRole, Qt.ToolTipRole):
            return None
        r = index.row()
        c = index.column()
        try:
            v = self._df.iat[r, c]
        except Exception:
            return None
        if pd.isna(v):
            return ""
        if isinstance(v, float):
            return f"{v:.6g}"
        return str(v)

