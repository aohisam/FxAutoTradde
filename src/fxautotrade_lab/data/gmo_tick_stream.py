"""GMO public WebSocket ticker recorder for scalping shadow data."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO, UTC
from fxautotrade_lab.core.symbols import normalize_fx_symbol
from fxautotrade_lab.data.ticks import ParquetTickCache, validate_tick_frame


@dataclass(slots=True)
class GmoWebSocketTick:
    symbol: str
    timestamp: pd.Timestamp
    bid: float
    ask: float
    status: str = ""

    def to_frame(self) -> pd.DataFrame:
        frame = pd.DataFrame(
            [
                {
                    "bid": self.bid,
                    "ask": self.ask,
                    "bid_volume": 0.0,
                    "ask_volume": 0.0,
                    "symbol": self.symbol,
                }
            ],
            index=pd.DatetimeIndex([self.timestamp]),
        )
        return validate_tick_frame(frame, symbol=self.symbol)


class GmoPublicWebSocketTickRecorder:
    """Record GMO public ticker messages into the tick cache.

    This is intentionally read-only and is meant for shadow validation before
    any private broker implementation is enabled.
    """

    def __init__(
        self,
        env: EnvironmentConfig,
        cache: ParquetTickCache,
        *,
        symbol: str,
    ) -> None:
        self.env = env
        self.cache = cache
        self.symbol = normalize_fx_symbol(symbol)

    def run(
        self,
        *,
        max_ticks: int | None = None,
        on_tick: Callable[[GmoWebSocketTick], None] | None = None,
    ) -> dict[str, object]:
        try:
            import websocket
        except ImportError as exc:  # pragma: no cover - depends on optional runtime package
            raise RuntimeError(
                "GMO WebSocket ticker を使うには websocket-client が必要です。"
                " 依存関係をインストールしてから再実行してください。"
            ) from exc

        ws = websocket.create_connection(self.env.gmo_public_ws_url, timeout=15)
        count = 0
        cache_paths: set[str] = set()
        try:
            ws.send(
                json.dumps(
                    {
                        "command": "subscribe",
                        "channel": "ticker",
                        "symbol": self.symbol,
                    }
                )
            )
            while max_ticks is None or count < max_ticks:
                raw = ws.recv()
                payload = json.loads(raw)
                tick = self._parse_tick(payload)
                if tick is None:
                    continue
                for path in self.cache.upsert(self.symbol, tick.to_frame()):
                    cache_paths.add(str(path))
                count += 1
                if on_tick is not None:
                    on_tick(tick)
        finally:
            ws.close()
        return {
            "symbol": self.symbol,
            "recorded_ticks": count,
            "cache_paths": sorted(cache_paths),
        }

    def _parse_tick(self, payload: dict[str, object]) -> GmoWebSocketTick | None:
        if str(payload.get("channel", "")).lower() not in {"", "ticker"}:
            return None
        raw_symbol = str(payload.get("symbol") or self.symbol)
        symbol = normalize_fx_symbol(raw_symbol)
        if symbol != self.symbol:
            return None
        bid = float(payload.get("bid", 0) or 0)
        ask = float(payload.get("ask", 0) or 0)
        if bid <= 0 or ask <= bid:
            return None
        timestamp = pd.Timestamp(str(payload.get("timestamp", "")))
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize(UTC)
        else:
            timestamp = timestamp.tz_convert(UTC)
        return GmoWebSocketTick(
            symbol=symbol,
            timestamp=timestamp.tz_convert(ASIA_TOKYO),
            bid=bid,
            ask=ask,
            status=str(payload.get("status", "")),
        )
