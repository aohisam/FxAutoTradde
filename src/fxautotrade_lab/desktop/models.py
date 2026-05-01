"""Qt table models."""

from __future__ import annotations

import pandas as pd


def load_dataframe_model_class():  # pragma: no cover - UI helper
    from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

    class DataFrameTableModel(QAbstractTableModel):
        def __init__(self, frame: pd.DataFrame | None = None) -> None:
            super().__init__()
            self._frame = pd.DataFrame()
            self._headers: list[str] = []
            self._rows: list[list[str]] = []
            self.set_frame(frame)

        def set_frame(self, frame: pd.DataFrame | None) -> None:
            self.beginResetModel()
            self._frame = frame if frame is not None else pd.DataFrame()
            self._headers = [str(column) for column in self._frame.columns]
            self._rows = [
                [self._format_value(value) for value in row]
                for row in self._frame.itertuples(index=False, name=None)
            ]
            self.endResetModel()

        def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
            _ = parent
            return len(self._rows)

        def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
            _ = parent
            return len(self._headers)

        def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # noqa: ANN001
            if not index.isValid() or role != Qt.DisplayRole:
                return None
            row = index.row()
            column = index.column()
            if row >= len(self._rows) or column >= len(self._headers):
                return None
            return self._rows[row][column]

        def headerData(
            self, section: int, orientation, role: int = Qt.DisplayRole
        ):  # noqa: ANN001,N802
            if role != Qt.DisplayRole:
                return None
            if orientation == Qt.Horizontal:
                return self._headers[section] if section < len(self._headers) else ""
            return str(section + 1)

        @staticmethod
        def _format_value(value) -> str:  # noqa: ANN001
            if isinstance(value, float):
                return f"{value:.4f}"
            return "" if pd.isna(value) else str(value)

    return DataFrameTableModel
