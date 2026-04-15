from __future__ import annotations

import os
from pathlib import Path

import pytest

from fxautotrade_lab.application import LabApplication
from fxautotrade_lab.config.loader import load_environment


@pytest.mark.skipif(
    os.getenv("RUN_ALPACA_PAPER_INTEGRATION") != "1",
    reason="Alpaca Paper integration is opt-in.",
)
def test_alpaca_paper_verify_runtime_optional():
    env = load_environment()
    if not env.has_credentials("paper"):
        pytest.skip("Alpaca credentials are not configured.")
    app = LabApplication(Path("configs/paper_alpaca_free.yaml"))
    summary = app.verify_broker_runtime()
    assert "account_summary" in summary


@pytest.mark.skipif(
    os.getenv("RUN_ALPACA_LIVE_READONLY_INTEGRATION") != "1",
    reason="Alpaca Live integration is opt-in and requires explicit safety gates.",
)
def test_alpaca_live_verify_runtime_optional():
    env = load_environment()
    if not env.has_credentials("live"):
        pytest.skip("Alpaca credentials are not configured.")
    app = LabApplication(Path("configs/live_alpaca_disabled.yaml"))
    summary = app.verify_broker_runtime()
    assert "account_summary" in summary
