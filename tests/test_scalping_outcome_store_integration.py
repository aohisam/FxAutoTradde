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


def test_outcome_store_drops_future_columns_for_paper_and_live_sim_only(tmp_path) -> None:
    store = ScalpingOutcomeStore(tmp_path / "outcomes", storage_format="csv")
    signals = pd.DataFrame(
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
            "future_long_exit_reason": ["take_profit"],
            "future_short_exit_reason": ["stop_loss"],
        }
    )

    store.append_backtest(
        run_id="backtest",
        model_id="model1",
        symbol="USD_JPY",
        signals=signals,
        trades=pd.DataFrame(),
    )
    store.append_paper(
        run_id="paper",
        model_id="model1",
        symbol="USD_JPY",
        signals=signals,
        trades=pd.DataFrame(),
    )
    store.append_paper(
        run_id="live-sim",
        model_id="model1",
        symbol="USD_JPY",
        signals=signals,
        trades=pd.DataFrame(),
        source="live_sim",
    )

    stored_signals = store.load_signals()
    outcomes = store.load_outcomes()

    for frame in (stored_signals, outcomes):
        assert "future_long_net_pips" in frame.columns
        assert frame.loc[frame["source"] == "backtest", "future_long_net_pips"].notna().all()
        assert frame.loc[frame["source"] == "paper", "future_long_net_pips"].isna().all()
        assert frame.loc[frame["source"] == "live_sim", "future_long_net_pips"].isna().all()
        assert frame.loc[frame["source"] == "paper", "future_short_net_pips"].isna().all()
        assert frame.loc[frame["source"] == "live_sim", "future_short_net_pips"].isna().all()


def test_outcome_store_paper_only_future_columns_are_not_written(tmp_path) -> None:
    store = ScalpingOutcomeStore(tmp_path / "paper_only", storage_format="csv")
    store.append_paper(
        run_id="paper",
        model_id="model1",
        symbol="USD_JPY",
        signals=pd.DataFrame(
            {
                "signal_id": ["s1"],
                "timestamp": ["2026-02-02T09:00:00+09:00"],
                "symbol": ["USD_JPY"],
                "probability": [0.8],
                "accepted": [True],
                "future_long_net_pips": [1.0],
                "future_short_net_pips": [-1.0],
            }
        ),
        trades=pd.DataFrame(),
    )

    assert not any(column.startswith("future_") for column in store.load_signals().columns)
    assert not any(column.startswith("future_") for column in store.load_outcomes().columns)


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


def test_outcome_store_summary_counts_breakeven_trade_and_excludes_rejected(tmp_path) -> None:
    store = ScalpingOutcomeStore(tmp_path / "outcomes", storage_format="csv")
    signals = pd.DataFrame(
        {
            "signal_id": ["accepted-zero", "rejected"],
            "timestamp": ["2026-02-02T09:00:00+09:00", "2026-02-02T09:01:00+09:00"],
            "symbol": ["USD_JPY", "USD_JPY"],
            "probability": [0.8, 0.7],
            "chosen_side": ["long", "short"],
            "accepted": [True, False],
            "reject_reason": ["accepted", "threshold_not_met"],
        }
    )
    trades = pd.DataFrame(
        {
            "trade_id": ["breakeven"],
            "signal_id": ["accepted-zero"],
            "symbol": ["USD_JPY"],
            "entry_time": ["2026-02-02T09:00:01+09:00"],
            "exit_time": ["2026-02-02T09:00:02+09:00"],
            "net_pnl": [0.0],
            "realized_net_pips": [0.0],
        }
    )

    store.append_paper(
        run_id="paper-zero",
        model_id="model-zero",
        model_path="latest.json",
        model_promoted=True,
        symbol="USD_JPY",
        signals=signals,
        trades=trades,
    )

    summary = store.load_summary()
    model_summary = summary["model_id_summary"][0]
    decile_summary = summary["probability_decile_summary"]

    assert summary["total_trades"] == 1
    assert model_summary["trades"] == 1
    assert sum(row["trades"] for row in decile_summary) == 1
    assert sum(row["signals"] for row in decile_summary) == 2
