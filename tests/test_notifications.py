from __future__ import annotations

import json

from fxautotrade_lab.automation.notifications import MultiChannelNotifier
from fxautotrade_lab.config.models import NotificationChannelConfig


def test_multichannel_notifier_writes_log_channel(tmp_path):
    log_path = tmp_path / "notifications.log"
    notifier = MultiChannelNotifier(
        enabled=True,
        config=NotificationChannelConfig(channels=["log"], log_path=log_path),
    )
    result = notifier.notify(
        title="FXAutoTrade Lab", message="注文を送信しました。", subtitle="gmo_sim"
    )
    assert result["log"] is True
    payload = json.loads(log_path.read_text(encoding="utf-8").strip())
    assert payload["title"] == "FXAutoTrade Lab"
    assert payload["message"] == "注文を送信しました。"
    assert payload["subtitle"] == "gmo_sim"
