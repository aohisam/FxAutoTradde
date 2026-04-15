"""Qt table models."""

from __future__ import annotations

import pandas as pd


def load_dataframe_model_class():  # pragma: no cover - UI helper
    from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

    class DataFrameTableModel(QAbstractTableModel):
        def __init__(self, frame: pd.DataFrame | None = None) -> None:
            super().__init__()
            self._frame = frame if frame is not None else pd.DataFrame()

        def set_frame(self, frame: pd.DataFrame | None) -> None:
            self.beginResetModel()
            self._frame = frame if frame is not None else pd.DataFrame()
            self.endResetModel()

        def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
            _ = parent
            return len(self._frame.index)

        def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
            _ = parent
            return len(self._frame.columns)

        def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # noqa: ANN001
            if not index.isValid() or role != Qt.DisplayRole:
                return None
            value = self._frame.iat[index.row(), index.column()]
            if isinstance(value, float):
                return f"{value:.4f}"
            return "" if pd.isna(value) else str(value)

        def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):  # noqa: ANN001,N802
            if role != Qt.DisplayRole:
                return None
            if orientation == Qt.Horizontal:
                return str(self._frame.columns[section]) if section < len(self._frame.columns) else ""
            return str(section + 1)

    return DataFrameTableModel
