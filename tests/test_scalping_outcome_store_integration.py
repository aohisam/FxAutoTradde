from __future__ import annotations

import pandas as pd

from fxautotrade_lab.persistence.scalping_outcomes import ScalpingOutcomeStore


def test_outcome_store_appends_backtest_and_paper_without_paper_future_outcomes(tmp_path) -> None:
    store = ScalpingOutcomeStore(tmp_path / "outcomes", storage_format="csv")
    backtest_signals = pd.DataFrame(
        {
            "signal_id": ["s1"],
            "timestamp": ["2026-02-02T09:00:00+09:00"],
            "symbol": ["USD_JPY"],
            "probability": [0.8],
            "chosen_side": ["long"],
            "accepted": [True],
            "reject_reason": ["accepted"],
            "future_long_net_pips": [1.0],
            "future_short_net_pips": [-1.0],
        }
    )
    trades = pd.DataFrame(
        {
            "trade_id": ["t1"],
            "signal_id": ["s1"],
            "symbol": ["USD_JPY"],
            "net_pnl": [100.0],
            "realized_net_pips": [1.0],
        }
    )
    paper_signals = pd.DataFrame(
        {
            "signal_id": ["s2"],
            "timestamp": ["2026-02-02T09:01:00+09:00"],
            "symbol": ["USD_JPY"],
            "probability": [0.7],
            "chosen_side": ["short"],
            "accepted": [False],
            "reject_reason": ["threshold_not_met"],
        }
    )

    store.append_backtest(
        run_id="run1",
        model_id="model1",
        model_path="candidate.json",
        model_promoted=False,
        symbol="USD_JPY",
        signals=backtest_signals,
        trades=trades,
    )
    store.append_paper(
        run_id="run2",
        model_id="model1",
        model_path="latest.json",
        model_promoted=True,
        symbol="USD_JPY",
        signals=paper_signals,
        trades=pd.DataFrame(),
    )

    outcomes = store.load_outcomes()
    summary = store.load_summary()

    assert set(outcomes["source"]) == {"backtest", "paper"}
    paper = outcomes.loc[outcomes["source"] == "paper"].iloc[0]
    assert "future_long_net_pips" not in paper.index or pd.isna(paper["future_long_net_pips"])
    assert summary["outcome_count"] == 2
    assert summary["total_runs"] == 2
    assert summary["total_signals"] == 2
    assert summary["total_trades"] == 1
    assert summary["by_model_id"]


def test_outcome_store_summarizes_multiple_paper_runs(tmp_path) -> None:
    store = ScalpingOutcomeStore(tmp_path / "outcomes", storage_format="csv")
    for index in range(2):
        store.append_paper(
            run_id=f"paper{index}",
            model_id="model-paper",
            model_path="latest.json",
            model_promoted=True,
            symbol="USD_JPY",
            signals=pd.DataFrame(
                {
                    "signal_id": [f"s{index}"],
                    "timestamp": [f"2026-02-02T09:0{index}:00+09:00"],
                    "symbol": ["USD_JPY"],
                    "probability": [0.6 + index * 0.1],
                    "chosen_side": ["long"],
                    "accepted": [True],
                    "reject_reason": ["accepted"],
                }
            ),
            trades=pd.DataFrame(
                {
                    "trade_id": [f"t{index}"],
                    "signal_id": [f"s{index}"],
                    "symbol": ["USD_JPY"],
                    "net_pnl": [100.0 + index],
                    "realized_net_pips": [1.0],
                }
            ),
        )

    outcomes = store.load_outcomes()
    summary = store.load_summary()

    assert "future_long_net_pips" not in outcomes.columns
    assert summary["total_runs"] == 2
    assert summary["total_trades"] == 2
    assert summary["model_id_summary"][0]["model_id"] == "model-paper"
