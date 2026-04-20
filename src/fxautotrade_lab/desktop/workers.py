"""Background worker helpers for PySide6."""

from __future__ import annotations

from fxautotrade_lab.desktop.runtime import log_runtime_exception


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
                log_runtime_exception("background_worker")
                self.signals.error.emit(str(exc) or exc.__class__.__name__)
            else:
                self.signals.finished.emit(result)

        def dispose(self) -> None:
            self.fn = None
            self.args = ()
            self.kwargs = {}
            if self.signals is not None:
                self.signals.deleteLater()

    return FunctionWorker
