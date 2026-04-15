from __future__ import annotations

from pathlib import Path

import yaml


def make_config_dict(tmp_path: Path, strategy_name: str = "multi_timeframe_pattern_scoring") -> dict:
    return {
        "app_name": "FXAutoTrade Lab Test",
        "watchlist": {
            "symbols": ["USD_JPY", "EUR_JPY"],
            "benchmark_symbols": ["USD_JPY"],
            "sector_symbols": [],
        },
        "data": {
            "source": "fixture",
            "cache_dir": str(tmp_path / "cache"),
            "start_date": "2024-01-01",
            "end_date": "2024-03-31",
            "timeframes": ["1Day", "1Hour", "15Min", "1Min"],
            "preferred_entry_timeframe": "15Min",
        },
        "strategy": {
            "name": strategy_name,
            "entry_timeframe": "15Min",
            "scoring": {"entry_score_threshold": 0.55},
        },
        "broker": {"mode": "local_sim"},
        "automation": {"enabled": True, "max_cycles_for_demo": 4},
        "reporting": {"output_dir": str(tmp_path / "reports")},
        "persistence": {"sqlite_path": str(tmp_path / "runtime" / "trading_lab.sqlite")},
        "ui": {"default_page": "概要"},
    }


def write_config(tmp_path: Path, strategy_name: str = "multi_timeframe_pattern_scoring") -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(make_config_dict(tmp_path, strategy_name), sort_keys=False), encoding="utf-8")
    return path
