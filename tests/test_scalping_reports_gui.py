from __future__ import annotations

import json
from pathlib import Path

from fxautotrade_lab.desktop.pages.reports import load_scalping_report_rows


def test_load_scalping_report_rows_reads_exported_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "scalping" / "20260203_090000_scalping"
    run_dir.mkdir(parents=True)
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "20260203_090000_scalping",
                "symbol": "USD_JPY",
                "test_start": "2026-02-03T09:00:00+09:00",
                "test_end": "2026-02-03T10:00:00+09:00",
                "metrics": {
                    "starting_equity": 100_000.0,
                    "total_return": -0.02,
                    "max_drawdown": -3_000.0,
                    "number_of_trades": 4,
                    "accepted_signal_count": 4,
                    "rejected_signal_count": 7,
                },
                "model_summary": {"threshold_selected_on": "validation"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    rows = load_scalping_report_rows(tmp_path)

    assert len(rows) == 1
    row = rows[0]
    assert row["report_kind"] == "scalping"
    assert row["strategy_name"] == "fx_scalping"
    assert row["output_dir"] == str(run_dir)
    assert row["metrics"]["number_of_trades"] == 4
    assert row["metrics"]["max_drawdown"] == -0.03
