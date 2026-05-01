"""GMO Coin FX public market data client."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

from fxautotrade_lab.config.models import EnvironmentConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO, UTC
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.core.symbols import normalize_fx_symbol
from fxautotrade_lab.data.quality import validate_bar_frame

INTRADAY_INTERVALS: dict[TimeFrame, str] = {
    TimeFrame.MIN_1: "1min",
    TimeFrame.MIN_5: "5min",
    TimeFrame.MIN_10: "10min",
    TimeFrame.MIN_15: "15min",
    TimeFrame.MIN_30: "30min",
    TimeFrame.HOUR_1: "1hour",
}

LONG_INTERVALS: dict[TimeFrame, str] = {
    TimeFrame.HOUR_4: "4hour",
    TimeFrame.HOUR_8: "8hour",
    TimeFrame.HOUR_12: "12hour",
    TimeFrame.DAY_1: "1day",
    TimeFrame.WEEK_1: "1week",
    TimeFrame.MONTH_1: "1month",
}


@dataclass(slots=True)
class GmoTickerQuote:
    symbol: str
    ask: float
    bid: float
    timestamp: pd.Timestamp
    status: str

    @property
    def mid(self) -> float:
        return (self.ask + self.bid) / 2


class GmoForexPublicClient:
    def __init__(self, env: EnvironmentConfig) -> None:
        self.env = env
        self.base_url = env.gmo_public_base_url.rstrip("/")

    def list_symbols(self) -> list[dict[str, Any]]:
        payload = self._get_json("/v1/symbols")
        return list(payload.get("data", []))

    def symbol_rules(self) -> dict[str, dict[str, Any]]:
        return {
            normalize_fx_symbol(str(item.get("symbol", ""))): dict(item)
            for item in self.list_symbols()
            if item.get("symbol")
        }

    def fetch_ticker_quotes(self) -> dict[str, GmoTickerQuote]:
        payload = self._get_json("/v1/ticker")
        quotes: dict[str, GmoTickerQuote] = {}
        for item in payload.get("data", []):
            raw_symbol = str(item.get("symbol", "")).strip()
            if not raw_symbol:
                continue
            symbol = normalize_fx_symbol(raw_symbol)
            timestamp = pd.Timestamp(str(item.get("timestamp", "")))
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize(UTC)
            else:
                timestamp = timestamp.tz_convert(UTC)
            quotes[symbol] = GmoTickerQuote(
                symbol=symbol,
                ask=float(item.get("ask", 0) or 0),
                bid=float(item.get("bid", 0) or 0),
                timestamp=timestamp.tz_convert(ASIA_TOKYO),
                status=str(item.get("status", "")),
            )
        return quotes

    def fetch_bars(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: datetime,
        end: datetime,
        *,
        price_type: str = "ASK",
    ) -> pd.DataFrame:
        normalized_symbol = normalize_fx_symbol(symbol)
        start_ts = self._to_tokyo(start)
        end_ts = self._to_tokyo(end)
        if start_ts >= end_ts:
            return self._empty_frame()
        frames: list[pd.DataFrame] = []
        inclusive_end = self._inclusive_fetch_end(end_ts)
        if timeframe in INTRADAY_INTERVALS:
            interval = INTRADAY_INTERVALS[timeframe]
            for current_date in self._date_range(start_ts.date(), inclusive_end.date()):
                if current_date < date(2023, 10, 28):
                    continue
                payload = self._get_json(
                    "/v1/klines",
                    {
                        "symbol": normalized_symbol,
                        "priceType": price_type.upper(),
                        "interval": interval,
                        "date": current_date.strftime("%Y%m%d"),
                    },
                )
                frames.append(self._normalize_klines(payload, normalized_symbol))
        else:
            interval = LONG_INTERVALS[timeframe]
            for year in range(start_ts.year, inclusive_end.year + 1):
                if year < 2023:
                    continue
                payload = self._get_json(
                    "/v1/klines",
                    {
                        "symbol": normalized_symbol,
                        "priceType": price_type.upper(),
                        "interval": interval,
                        "date": f"{year}",
                    },
                )
                frames.append(self._normalize_klines(payload, normalized_symbol))
        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return self._empty_frame()
        merged = pd.concat(non_empty).sort_index()
        merged = merged.loc[~merged.index.duplicated(keep="last")]
        selection = merged.loc[(merged.index >= start_ts) & (merged.index < end_ts)].copy()
        return validate_bar_frame(selection if not selection.empty else self._empty_frame())

    def _get_json(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        with urlopen(url, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if int(payload.get("status", -1)) != 0:
            raise RuntimeError(f"GMO API request failed: {payload}")
        return payload

    def _normalize_klines(self, payload: dict[str, Any], symbol: str) -> pd.DataFrame:
        rows = list(payload.get("data", []))
        if not rows:
            return self._empty_frame()
        frame = pd.DataFrame(rows).copy()
        open_times = pd.to_numeric(frame["openTime"], errors="coerce")
        invalid_rows = int(open_times.isna().sum())
        if invalid_rows:
            raise ValueError(f"GMO の kline openTime に不正な値が {invalid_rows} 件あります。")
        timestamps = pd.to_datetime(open_times.astype("int64"), unit="ms", utc=True).dt.tz_convert(
            ASIA_TOKYO
        )
        normalized = pd.DataFrame(
            {
                "open": frame["open"].astype(float).to_numpy(),
                "high": frame["high"].astype(float).to_numpy(),
                "low": frame["low"].astype(float).to_numpy(),
                "close": frame["close"].astype(float).to_numpy(),
                "volume": 0.0,
                "symbol": symbol,
            },
            index=pd.DatetimeIndex(timestamps),
        )
        return validate_bar_frame(normalized)

    @staticmethod
    def _date_range(start: date, end: date) -> list[date]:
        cursor = start
        values: list[date] = []
        while cursor <= end:
            values.append(cursor)
            cursor += timedelta(days=1)
        return values

    @staticmethod
    def _inclusive_fetch_end(end: pd.Timestamp) -> pd.Timestamp:
        # The sync window is half-open [start, end). When end lands exactly on a
        # day/year boundary, subtract a tick so we do not request the next period.
        return end - pd.Timedelta(nanoseconds=1)

    @staticmethod
    def _to_tokyo(value: datetime | pd.Timestamp) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            return ts.tz_localize(ASIA_TOKYO)
        return ts.tz_convert(ASIA_TOKYO)

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        return validate_bar_frame(
            pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"],
                index=pd.DatetimeIndex([], tz=ASIA_TOKYO),
            )
        )
