"""Automation state helpers."""

from __future__ import annotations

from dataclasses import dataclass, field

from fxautotrade_lab.core.enums import AutomationStatus, BrokerMode


@dataclass(slots=True)
class AutomationState:
    status: AutomationStatus = AutomationStatus.STOPPED
    mode: BrokerMode = BrokerMode.LOCAL_SIM
    current_cycle: int = 0
    heartbeat_message_ja: str = "停止中"
    last_error: str = ""
    recent_logs: list[str] = field(default_factory=list)
