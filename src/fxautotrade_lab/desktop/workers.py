"""Background worker helpers for PySide6."""

from __future__ import annotations


def load_worker_classes():  # pragma: no cover - UI helper
    from PySide6.QtCore import QObject, QRunnable, Signal, Slot

    class WorkerSignals(QObject):
        finished = Signal(object)
        error = Signal(str)

    class FunctionWorker(QRunnable):
        def __init__(self, fn, *args, **kwargs) -> None:
            super().__init__()
            self.setAutoDelete(False)
            self.fn = fn
            self.args = args
            self.kwargs = kwargs
            self.signals = WorkerSignals()

        @Slot()
        def run(self) -> None:
            try:
                result = self.fn(*self.args, **self.kwargs)
            except Exception as exc:
                self.signals.error.emit(str(exc))
            else:
                self.signals.finished.emit(result)

        def dispose(self) -> None:
            self.fn = None
            self.args = ()
            self.kwargs = {}
            if self.signals is not None:
                self.signals.deleteLater()

    return FunctionWorker
