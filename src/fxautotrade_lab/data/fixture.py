"""Deterministic fixture market data for offline demo."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from fxautotrade_lab.config.models import DataConfig
from fxautotrade_lab.core.constants import ASIA_TOKYO
from fxautotrade_lab.core.enums import TimeFrame
from fxautotrade_lab.data.cache import ParquetBarCache
from fxautotrade_lab.data.quality import validate_bar_frame
from fxautotrade_lab.data.resample import resample_ohlcv
from fxautotrade_lab.data.session import trading_days

TIMEFRAME_MINUTES = {
    TimeFrame.MIN_1: 1,
    TimeFrame.MIN_5: 5,
    TimeFrame.MIN_10: 10,
    TimeFrame.MIN_15: 15,
    TimeFrame.MIN_30: 30,
    TimeFrame.HOUR_1: 60,
    TimeFrame.HOUR_4: 240,
    TimeFrame.HOUR_8: 480,
    TimeFrame.HOUR_12: 720,
}

SYMBOL_BIASES = {
    "USD_JPY": 0.00005,
    "EUR_JPY": 0.00004,
    "AUD_JPY": 0.00003,
    "GBP_JPY": 0.00005,
    "EUR_USD": 0.00002,
}


@dataclass(slots=True)
class FixtureDataLoader:
    config: DataConfig
    cache: ParquetBarCache

    def load_bars(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        expected_metadata = self._cache_metadata(timeframe)
        if not self._metadata_matches(
            self.cache.load_metadata(symbol, timeframe), expected_metadata
        ):
            self.cache.clear(symbol, timeframe)
        cached = self.cache.load(symbol, timeframe)
        if cached is not None:
            start_ts = pd.Timestamp(start or self.config.start_date, tz=ASIA_TOKYO)
            end_ts = pd.Timestamp(end or self.config.end_date, tz=ASIA_TOKYO) + pd.Timedelta(days=1)
            selection = cached.loc[(cached.index >= start_ts) & (cached.index <= end_ts)]
            if not selection.empty:
                return selection.copy()
        frame = self._generate_bars(
            symbol, timeframe, start or self.config.start_date, end or self.config.end_date
        )
        self.cache.upsert(symbol, timeframe, frame)
        self.cache.save_metadata(symbol, timeframe, expected_metadata)
        return frame

    def _cache_metadata(self, timeframe: TimeFrame) -> dict[str, object]:
        return {
            "source": "fixture",
            "timeframe": timeframe.value,
            "fixture_seed": int(self.config.fixture_seed),
            "version": 1,
        }

    def _metadata_matches(self, actual: dict[str, object], expected: dict[str, object]) -> bool:
        return bool(actual) and all(actual.get(key) == value for key, value in expected.items())

    def _generate_bars(
        self,
        symbol: str,
        timeframe: TimeFrame,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        seed = self.config.fixture_seed + sum(ord(char) for char in f"{symbol}-{timeframe.value}")
        rng = np.random.default_rng(seed)
        if timeframe == TimeFrame.WEEK_1:
            daily = self._generate_bars(symbol, TimeFrame.DAY_1, start, end)
            weekly = resample_ohlcv(daily[["open", "high", "low", "close", "volume"]], "W-FRI")
            weekly["symbol"] = symbol.upper()
            return validate_bar_frame(weekly)
        if timeframe == TimeFrame.MONTH_1:
            daily = self._generate_bars(symbol, TimeFrame.DAY_1, start, end)
            monthly = resample_ohlcv(daily[["open", "high", "low", "close", "volume"]], "ME")
            monthly["symbol"] = symbol.upper()
            return validate_bar_frame(monthly)
        if timeframe == TimeFrame.DAY_1:
            index = pd.DatetimeIndex(
                [day.replace(hour=23, minute=59, second=0) for day in trading_days(start, end)]
            )
        else:
            index = self._intraday_index(start, end, TIMEFRAME_MINUTES[timeframe])
        base_price = 90 + (sum(ord(char) for char in symbol) % 80)
        drift = SYMBOL_BIASES.get(symbol.upper(), 0.0005)
        seasonal = np.sin(np.linspace(0, 18, len(index))) * 0.0025
        noise = rng.normal(0, 0.006 if timeframe == TimeFrame.DAY_1 else 0.0018, len(index))
        regime = np.where(np.arange(len(index)) % 120 < 80, 1.0, -0.35)
        returns = drift * regime + seasonal + noise
        close = base_price * np.exp(np.cumsum(returns))
        open_ = np.concatenate(([close[0]], close[:-1])) * (1 + rng.normal(0, 0.0006, len(index)))
        spread = np.maximum(close * np.abs(rng.normal(0.0007, 0.0002, len(index))), 0.001)
        high = np.maximum(open_, close) + spread
        low = np.minimum(open_, close) - spread
        volume_base = 180_000 if timeframe == TimeFrame.DAY_1 else 30_000
        volume = np.maximum(
            (
                volume_base * (1 + rng.normal(0, 0.25, len(index))) * (1 + np.abs(returns) * 18)
            ).astype(int),
            100,
        )
        frame = pd.DataFrame(
            {
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            },
            index=index.tz_convert(ASIA_TOKYO),
        )
        frame["symbol"] = symbol.upper()
        return validate_bar_frame(frame)

    def _intraday_index(self, start: str, end: str, minutes: int) -> pd.DatetimeIndex:
        days = trading_days(start, end)
        timestamps: list[pd.Timestamp] = []
        for day in days:
            open_time = day.replace(hour=0, minute=0, second=0)
            close_time = day.replace(hour=23, minute=59, second=0)
            current = open_time + pd.Timedelta(minutes=minutes)
            while current <= close_time:
                timestamps.append(current)
                current += pd.Timedelta(minutes=minutes)
        return pd.DatetimeIndex(timestamps, tz=ASIA_TOKYO)


def build_fixture_loader(config: DataConfig, cache_dir: Path) -> FixtureDataLoader:
    return FixtureDataLoader(config=config, cache=ParquetBarCache(cache_dir))
