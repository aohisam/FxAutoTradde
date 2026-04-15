"""Alpaca market data and trading adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import time
from typing import Any

import pandas as pd

from fxautotrade_lab.config.models import BrokerConfig, EnvironmentConfig
from fxautotrade_lab.core.enums import BrokerMode, OrderSide, TimeFrame
from fxautotrade_lab.data.quality import validate_bar_frame


TIMEFRAME_MAP = {
    TimeFrame.MIN_1: ("1Min", "minute"),
    TimeFrame.MIN_15: ("15Min", "minute"),
    TimeFrame.HOUR_1: ("1Hour", "hour"),
    TimeFrame.DAY_1: ("1Day", "day"),
}

FETCH_WINDOW_MAP = {
    TimeFrame.MIN_1: timedelta(days=14),
    TimeFrame.MIN_15: timedelta(days=120),
    TimeFrame.HOUR_1: timedelta(days=365),
}

RETRYABLE_MARKET_DATA_MARKERS = (
    "connection aborted",
    "connection reset by peer",
    "connection reset",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "server disconnected",
    "remote end closed connection",
    "too many requests",
    "429",
    "502",
    "503",
    "504",
)

DEFAULT_MAX_FETCH_ATTEMPTS = 3


def _require_alpaca() -> None:
    try:
        import alpaca  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on optional dependency
        raise RuntimeError(
            "alpaca-py がインストールされていません。`pip install alpaca-py` を実行してください。"
        ) from exc


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _iso_or_empty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _empty_bar_frame(tzinfo) -> pd.DataFrame:  # noqa: ANN001
    return validate_bar_frame(
        pd.DataFrame(
            columns=["open", "high", "low", "close", "volume"],
            index=pd.DatetimeIndex([], tz=tzinfo),
        )
    )


@dataclass(slots=True)
class AlpacaHistoricalDataClient:
    env: EnvironmentConfig
    broker_config: BrokerConfig
    _client: Any | None = field(default=None, init=False, repr=False)

    def _credentials(self) -> tuple[str, str]:
        profile = "live" if self.broker_config.mode == BrokerMode.ALPACA_LIVE else "paper"
        return self.env.credentials_for_profile(profile)

    def fetch_bars(self, symbol: str, timeframe: TimeFrame, start: datetime, end: datetime) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for window_start, window_end in self._request_windows(timeframe, start, end):
            frames.append(self._fetch_bars_window(symbol, timeframe, window_start, window_end))
        if not frames:
            return _empty_bar_frame(getattr(start, "tzinfo", None))
        merged = pd.concat(frames).sort_index()
        merged = merged.loc[~merged.index.duplicated(keep="last")]
        return validate_bar_frame(merged)

    def _fetch_bars_window(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        _require_alpaca()
        from alpaca.data.enums import Adjustment
        from alpaca.data.requests import StockBarsRequest

        request = StockBarsRequest(
            symbol_or_symbols=symbol.upper(),
            timeframe=self._alpaca_timeframe(timeframe),
            start=start,
            end=end,
            adjustment=Adjustment.SPLIT,
            feed=self.broker_config.feed,
        )
        for attempt in range(1, DEFAULT_MAX_FETCH_ATTEMPTS + 1):
            try:
                response = self._historical_client().get_stock_bars(request)
                return self._normalize_bar_response(response, symbol, start)
            except Exception as exc:  # noqa: BLE001
                self._reset_client()
                if attempt < DEFAULT_MAX_FETCH_ATTEMPTS and self._is_retryable_market_data_error(exc):
                    time.sleep(self._retry_delay_seconds(attempt))
                    continue
                raise RuntimeError(
                    self._format_market_data_error(symbol, timeframe, start, end, exc)
                ) from exc
        raise RuntimeError("Alpaca 市場データ取得で予期しない制御に到達しました。")

    def _historical_client(self):
        _require_alpaca()
        from alpaca.data.historical import StockHistoricalDataClient

        if self._client is None:
            api_key, api_secret = self._credentials()
            self._client = StockHistoricalDataClient(
                api_key=api_key,
                secret_key=api_secret,
            )
        return self._client

    def _reset_client(self) -> None:
        self._client = None

    def _alpaca_timeframe(self, timeframe: TimeFrame):
        from alpaca.data.timeframe import TimeFrame as AlpacaTimeFrame, TimeFrameUnit

        amount, unit = TIMEFRAME_MAP[timeframe]
        value = int(amount.replace("Min", "").replace("Hour", "").replace("Day", ""))
        return (
            AlpacaTimeFrame(value, TimeFrameUnit.Minute)
            if unit == "minute"
            else AlpacaTimeFrame(value, TimeFrameUnit.Hour)
            if unit == "hour"
            else AlpacaTimeFrame(value, TimeFrameUnit.Day)
        )

    def _request_windows(
        self,
        timeframe: TimeFrame,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        span = FETCH_WINDOW_MAP.get(timeframe)
        if span is None or start + span >= end:
            return [(start, end)]
        windows: list[tuple[datetime, datetime]] = []
        cursor = start
        while cursor < end:
            window_end = min(cursor + span, end)
            windows.append((cursor, window_end))
            cursor = window_end
        return windows

    def _normalize_bar_response(self, response, symbol: str, start: datetime) -> pd.DataFrame:  # noqa: ANN001
        frame = response.df
        if frame.empty:
            return _empty_bar_frame(getattr(start, "tzinfo", None))
        working = frame.reset_index()
        if "symbol" in working.columns:
            working = working[working["symbol"] == symbol.upper()]
        if working.empty:
            return _empty_bar_frame(getattr(start, "tzinfo", None))
        working = working.set_index("timestamp")
        working = working[["open", "high", "low", "close", "volume"]]
        return validate_bar_frame(working)

    def _is_retryable_market_data_error(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return any(marker in message for marker in RETRYABLE_MARKET_DATA_MARKERS)

    def _retry_delay_seconds(self, attempt: int) -> float:
        return min(0.75 * (2 ** (attempt - 1)), 3.0)

    def _format_market_data_error(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: datetime,
        end: datetime,
        exc: Exception,
    ) -> str:
        if self._is_retryable_market_data_error(exc):
            return (
                "Alpaca との通信が途中で切断されました。"
                f" 銘柄: {symbol.upper()} / 時間足: {timeframe.value}"
                f" / 期間: {start.date()} - {end.date()}。"
                " しばらく待って再実行するか、期間を短くして再同期してください。"
                f" 詳細: {exc}"
            )
        return (
            f"Alpaca から {symbol.upper()} の {timeframe.value} データ取得に失敗しました。"
            f" 詳細: {exc}"
        )


@dataclass(slots=True)
class AlpacaTradingGateway:
    env: EnvironmentConfig
    base_url: str

    def _credentials(self) -> tuple[str, str]:
        profile = "paper" if "paper" in self.base_url else "live"
        return self.env.credentials_for_profile(profile)

    def _client(self):
        _require_alpaca()
        from alpaca.trading.client import TradingClient

        api_key, api_secret = self._credentials()
        return TradingClient(
            api_key=api_key,
            secret_key=api_secret,
            paper="paper" in self.base_url,
            url_override=self.base_url,
        )

    def submit_market_order(self, symbol: str, qty: int, side: OrderSide) -> dict[str, Any]:
        from alpaca.trading.enums import OrderSide as AlpacaOrderSide
        from alpaca.trading.enums import OrderType, TimeInForce
        from alpaca.trading.requests import MarketOrderRequest

        request = MarketOrderRequest(
            symbol=symbol.upper(),
            qty=qty,
            side=AlpacaOrderSide.BUY if side == OrderSide.BUY else AlpacaOrderSide.SELL,
            type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
        )
        order = self._client().submit_order(order_data=request)
        return self._normalize_order(order)

    def get_account_summary(self) -> dict[str, Any]:
        account = self._client().get_account()
        equity = float(account.equity)
        last_equity = float(account.last_equity)
        daily_pl = equity - last_equity
        daily_return = daily_pl / last_equity if last_equity else 0.0
        return {
            "account_number": _stringify(account.account_number),
            "equity": _stringify(account.equity),
            "last_equity": _stringify(account.last_equity),
            "portfolio_value": _stringify(account.portfolio_value),
            "cash": _stringify(account.cash),
            "buying_power": _stringify(account.buying_power),
            "status": _stringify(account.status),
            "trading_blocked": bool(account.trading_blocked),
            "account_blocked": bool(account.account_blocked),
            "trade_suspended_by_user": bool(account.trade_suspended_by_user),
            "daytrade_count": _stringify(account.daytrade_count),
            "daily_pl": f"{daily_pl:.2f}",
            "daily_return": f"{daily_return:.6f}",
        }

    def list_open_positions(self) -> list[dict[str, Any]]:
        positions = self._client().get_all_positions()
        return [self._normalize_position(position) for position in positions]

    def list_recent_orders(self, limit: int = 50) -> list[dict[str, Any]]:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest

        request = GetOrdersRequest(status=QueryOrderStatus.ALL, limit=limit)
        orders = self._client().get_orders(filter=request)
        return [self._normalize_order(order) for order in orders]

    def list_recent_fills(self, limit: int = 50) -> list[dict[str, Any]]:
        fills: list[dict[str, Any]] = []
        for order in self.list_recent_orders(limit=limit):
            status = str(order.get("status", "")).lower()
            if "filled" not in status:
                continue
            fills.append(
                {
                    "fill_id": order.get("order_id", ""),
                    "order_id": order.get("order_id", ""),
                    "symbol": order.get("symbol", ""),
                    "qty": order.get("filled_qty", order.get("qty", "")),
                    "side": order.get("side", ""),
                    "price": order.get("filled_avg_price", ""),
                    "filled_at": order.get("filled_at", ""),
                }
            )
        return fills

    def cancel_all_orders(self) -> dict[str, Any]:
        responses = self._client().cancel_orders()
        return {"cancelled_orders": len(responses)}

    def close_all_positions(self) -> dict[str, Any]:
        responses = self._client().close_all_positions(cancel_orders=True)
        return {"closed_positions": len(responses)}

    def verify_runtime(self, order_limit: int = 20) -> dict[str, Any]:
        return {
            "account_summary": self.get_account_summary(),
            "positions": self.list_open_positions(),
            "orders": self.list_recent_orders(limit=order_limit),
            "fills": self.list_recent_fills(limit=order_limit),
        }

    def _normalize_order(self, order) -> dict[str, Any]:  # noqa: ANN001
        return {
            "order_id": _stringify(getattr(order, "id", "")),
            "client_order_id": _stringify(getattr(order, "client_order_id", "")),
            "symbol": _stringify(getattr(order, "symbol", "")),
            "qty": _stringify(getattr(order, "qty", "")),
            "filled_qty": _stringify(getattr(order, "filled_qty", "")),
            "side": _stringify(_enum_value(getattr(order, "side", ""))),
            "status": _stringify(_enum_value(getattr(order, "status", ""))),
            "submitted_at": _iso_or_empty(getattr(order, "submitted_at", None)),
            "filled_at": _iso_or_empty(getattr(order, "filled_at", None)),
            "filled_avg_price": _stringify(getattr(order, "filled_avg_price", "")),
            "limit_price": _stringify(getattr(order, "limit_price", "")),
            "stop_price": _stringify(getattr(order, "stop_price", "")),
        }

    def _normalize_position(self, position) -> dict[str, Any]:  # noqa: ANN001
        return {
            "symbol": _stringify(getattr(position, "symbol", "")),
            "qty": _stringify(getattr(position, "qty", "")),
            "side": _stringify(_enum_value(getattr(position, "side", ""))),
            "avg_entry_price": _stringify(getattr(position, "avg_entry_price", "")),
            "market_value": _stringify(getattr(position, "market_value", "")),
            "unrealized_pl": _stringify(getattr(position, "unrealized_pl", "")),
            "unrealized_plpc": _stringify(getattr(position, "unrealized_plpc", "")),
            "current_price": _stringify(getattr(position, "current_price", "")),
        }
