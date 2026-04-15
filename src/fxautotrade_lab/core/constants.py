"""Shared constants."""

from zoneinfo import ZoneInfo

APP_NAME = "FXAutoTrade Lab"
APP_DISPLAY_NAME_JA = "FXAutoTrade Lab"
ASIA_TOKYO = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")
US_EASTERN = ZoneInfo("US/Eastern")
DEFAULT_CURRENCY = "JPY"
DEFAULT_BENCHMARK_SYMBOLS = ["USD_JPY"]
DEFAULT_SECTOR_SYMBOLS: list[str] = []
FX_WEEKDAY_OPEN_LABEL_JA = "取引時間"
FX_WEEKEND_CLOSED_LABEL_JA = "週末休場"
