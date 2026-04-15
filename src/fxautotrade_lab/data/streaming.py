"""Alpaca streaming adapter with reconnect support."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from fxautotrade_lab.config.models import EnvironmentConfig


def _coerce_trade_update(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    event = getattr(payload, "event", "")
    order = getattr(payload, "order", None)
    return {
        "event": getattr(event, "value", event),
        "order_id": getattr(order, "id", ""),
        "client_order_id": getattr(order, "client_order_id", ""),
        "symbol": getattr(order, "symbol", ""),
        "side": getattr(getattr(order, "side", ""), "value", getattr(order, "side", "")),
        "status": getattr(getattr(order, "status", ""), "value", getattr(order, "status", "")),
        "qty": getattr(order, "qty", ""),
        "filled_qty": getattr(order, "filled_qty", ""),
        "filled_avg_price": getattr(order, "filled_avg_price", ""),
        "submitted_at": getattr(order, "submitted_at", ""),
        "filled_at": getattr(order, "filled_at", ""),
    }


@dataclass(slots=True)
class AlpacaStreamingClient:
    """Optional websocket runtime for paper/live monitoring."""

    env: EnvironmentConfig
    on_bar: Callable[[dict[str, Any]], None] | None = None
    on_trade_update: Callable[[dict[str, Any]], None] | None = None
    is_connected: bool = field(default=False, init=False)
    last_message_at: pd.Timestamp | None = field(default=None, init=False)
    last_error: str = field(default="", init=False)
    reconnect_count: int = field(default=0, init=False)
    _market_stream: Any | None = field(default=None, init=False, repr=False)
    _trading_stream: Any | None = field(default=None, init=False, repr=False)
    _threads: list[threading.Thread] = field(default_factory=list, init=False, repr=False)
    _symbols: tuple[str, ...] = field(default_factory=tuple, init=False, repr=False)
    _feed: str = field(default="iex", init=False, repr=False)
    _paper: bool = field(default=True, init=False, repr=False)
    _base_url: str | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def connect(self, symbols: list[str], feed: str = "iex", paper: bool = True, base_url: str | None = None) -> None:
        self.disconnect()
        self._symbols = tuple(symbols)
        self._feed = feed
        self._paper = paper
        self._base_url = base_url
        if not symbols:
            return
        try:
            from alpaca.data.enums import DataFeed
            from alpaca.data.live.stock import StockDataStream
            from alpaca.trading.stream import TradingStream
        except ImportError as exc:  # pragma: no cover - optional dependency path
            raise RuntimeError("alpaca-py がインストールされていません。") from exc
        api_key, api_secret = self.env.credentials_for_profile("paper" if paper else "live")
        data_feed = DataFeed.SIP if str(feed).lower() == "sip" else DataFeed.IEX
        self._market_stream = StockDataStream(
            api_key=api_key,
            secret_key=api_secret,
            feed=data_feed,
        )
        self._market_stream.subscribe_bars(self._handle_bar, *symbols)
        self._trading_stream = TradingStream(
            api_key=api_key,
            secret_key=api_secret,
            paper=paper,
            url_override=base_url,
        )
        self._trading_stream.subscribe_trade_updates(self._handle_trade_update)
        self._threads = [
            threading.Thread(target=self._run_stream, args=(self._market_stream, "market"), daemon=True),
            threading.Thread(target=self._run_stream, args=(self._trading_stream, "trading"), daemon=True),
        ]
        self.last_error = ""
        self.last_message_at = pd.Timestamp.now(tz="US/Eastern")
        for thread in self._threads:
            thread.start()
        self.is_connected = True

    def reconnect(self) -> None:
        self.reconnect_count += 1
        self.connect(list(self._symbols), feed=self._feed, paper=self._paper, base_url=self._base_url)

    def disconnect(self) -> None:
        for stream in (self._market_stream, self._trading_stream):
            if stream is None:
                continue
            for method_name in ("stop_ws", "stop"):
                method = getattr(stream, method_name, None)
                if callable(method):
                    try:
                        method()
                    except Exception:
                        pass
        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        self._market_stream = None
        self._trading_stream = None
        self._threads = []
        self.is_connected = False

    def emit_bar(self, payload: dict[str, Any]) -> None:
        self._touch()
        if self.on_bar is not None:
            self.on_bar(payload)

    def healthy(self) -> bool:
        if not self.is_connected:
            return False
        return all(thread.is_alive() for thread in self._threads)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": bool(self._symbols),
            "connected": self.is_connected,
            "healthy": self.healthy(),
            "reconnect_count": self.reconnect_count,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at is not None else "",
            "last_error": self.last_error,
        }

    def _run_stream(self, stream: Any, name: str) -> None:
        try:
            stream.run()
        except Exception as exc:  # pragma: no cover - depends on websocket runtime
            self.last_error = f"{name}: {exc}"
            self.is_connected = False

    def _handle_bar(self, payload: Any) -> None:
        frame = payload if isinstance(payload, dict) else payload.__dict__
        self.emit_bar(frame)

    def _handle_trade_update(self, payload: Any) -> None:
        self._touch()
        if self.on_trade_update is not None:
            self.on_trade_update(_coerce_trade_update(payload))

    def _touch(self) -> None:
        with self._lock:
            self.last_message_at = pd.Timestamp.now(tz="US/Eastern")

