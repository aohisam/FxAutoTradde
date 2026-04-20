"""Background worker helpers for PySide6."""

from __future__ import annotations

import inspect

from fxautotrade_lab.desktop.runtime import log_runtime_exception


def load_worker_classes():  # pragma: no cover - UI helper
    from PySide6.QtCore import QObject, QRunnable, Signal, Slot

    class WorkerSignals(QObject):
        finished = Signal(object)
        error = Signal(str)
        progress = Signal(object)

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
                kwargs = dict(self.kwargs)
                if self._supports_progress_callback():
                    kwargs.setdefault("progress_callback", self.signals.progress.emit)
                result = self.fn(*self.args, **kwargs)
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

        def _supports_progress_callback(self) -> bool:
            try:
                signature = inspect.signature(self.fn)
            except (TypeError, ValueError):
                return False
            if "progress_callback" in signature.parameters:
                return True
            return any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in signature.parameters.values()
            )

    return FunctionWorker
