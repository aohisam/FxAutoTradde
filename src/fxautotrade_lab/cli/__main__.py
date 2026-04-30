"""Command line entry point."""

from __future__ import annotations

import argparse
from pathlib import Path

from fxautotrade_lab.application import LabApplication


def _typer_main() -> None:
    import typer

    app = typer.Typer(help="FXAutoTrade Lab CLI")

    @app.command("sync-data")
    def sync_data(config: str = typer.Option(..., "--config", help="設定ファイル")) -> None:
        summary = LabApplication(Path(config)).sync_data()
        typer.echo(f"データ同期完了: {summary}")

    @app.command("backtest")
    def backtest(config: str = typer.Option(..., "--config", help="設定ファイル")) -> None:
        result = LabApplication(Path(config)).run_backtest()
        typer.echo(f"バックテスト完了: {result.output_dir}")

    @app.command("train-fx-model")
    def train_fx_model(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        as_of: str = typer.Option("", "--as-of", help="学習終了時刻。省略時は config の end_date"),
    ) -> None:
        summary = LabApplication(Path(config)).train_fx_model(as_of=as_of or None)
        typer.echo(f"FX ML 学習完了: {summary}")

    @app.command("research-run")
    def research_run(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        mode: str = typer.Option("", "--mode", help="quick / standard / exhaustive"),
    ) -> None:
        summary = LabApplication(Path(config)).run_research(mode=mode or None)
        typer.echo(f"research_run 完了: {summary['output_dir']}")

    @app.command("import-csv")
    def import_csv(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        file_path: str = typer.Option(
            ...,
            "--file",
            help="現在は未対応。Bid / Ask の 2 ファイルを import-bidask-csv で指定してください",
        ),
        symbol: str = typer.Option("", "--symbol", help="通貨ペア。省略時はファイル名から推定"),
    ) -> None:
        _ = config, file_path, symbol
        raise typer.BadParameter(
            "単一 CSV のインポートは無効です。"
            " import-bidask-csv で Bid / Ask の 2 ファイルを指定してください。"
        )

    @app.command("import-bidask-csv")
    def import_bidask_csv(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        bid_file: str = typer.Option(..., "--bid-file", help="Bid CSV"),
        ask_file: str = typer.Option(..., "--ask-file", help="Ask CSV"),
        symbol: str = typer.Option("", "--symbol", help="通貨ペア。省略時はファイル名から推定"),
    ) -> None:
        summary = LabApplication(Path(config)).import_jforex_bid_ask_csv(
            bid_file_path=bid_file,
            ask_file_path=ask_file,
            symbol=symbol or None,
        )
        typer.echo(f"Bid/Ask CSV 取込完了: {summary}")

    @app.command("import-tick-csv")
    def import_tick_csv(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        file_path: str = typer.Option(..., "--file", help="JForex tick CSV"),
        symbol: str = typer.Option("", "--symbol", help="通貨ペア。省略時はファイル名から推定"),
    ) -> None:
        summary = LabApplication(Path(config)).import_jforex_tick_csv(
            file_path=file_path,
            symbol=symbol or None,
        )
        typer.echo(f"JForex tick CSV 取込完了: {summary}")

    @app.command("scalping-backtest")
    def scalping_backtest(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        tick_file: str = typer.Option(
            "", "--tick-file", help="取り込みも同時に行う JForex tick CSV"
        ),
        symbol: str = typer.Option("", "--symbol", help="通貨ペア。省略時は watchlist 先頭"),
        start: str = typer.Option("", "--start", help="検証開始日時"),
        end: str = typer.Option("", "--end", help="検証終了日時"),
    ) -> None:
        summary = LabApplication(Path(config)).run_scalping_backtest(
            tick_file_path=tick_file or None,
            symbol=symbol or None,
            start=start or None,
            end=end or None,
        )
        typer.echo(f"スキャルピングバックテスト完了: {summary}")

    @app.command("scalping-realtime-sim")
    def scalping_realtime_sim(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        symbol: str = typer.Option("", "--symbol", help="通貨ペア。省略時は watchlist 先頭"),
        max_ticks: int = typer.Option(120, "--max-ticks", help="取得tick数"),
        poll_seconds: float = typer.Option(1.0, "--poll-seconds", help="REST ticker の取得間隔"),
    ) -> None:
        summary = LabApplication(Path(config)).run_scalping_realtime_sim(
            symbol=symbol or None,
            max_ticks=max_ticks,
            poll_seconds=poll_seconds,
        )
        typer.echo(f"スキャルピング実時間paperシミュレーション完了: {summary}")

    @app.command("record-gmo-ticks")
    def record_gmo_ticks(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        symbol: str = typer.Option("", "--symbol", help="通貨ペア。省略時は watchlist 先頭"),
        max_ticks: int = typer.Option(0, "--max-ticks", help="記録tick数。0 は停止するまで記録"),
    ) -> None:
        summary = LabApplication(Path(config)).record_gmo_scalping_ticks(
            symbol=symbol or None,
            max_ticks=max_ticks or None,
        )
        typer.echo(f"GMO WebSocket tick 記録完了: {summary}")

    @app.command("scalping-outcomes-summary")
    def scalping_outcomes_summary(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
    ) -> None:
        summary = LabApplication(Path(config)).load_scalping_outcome_summary()
        typer.echo(f"スキャルピングOutcomeStore集計: {summary}")

    @app.command("realtime-sim")
    def realtime_sim(
        config: str = typer.Option(..., "--config", help="設定ファイル"),
        max_cycles: int = typer.Option(0, "--max-cycles", help="実行サイクル数。0 は既定値"),
    ) -> None:
        logs = LabApplication(Path(config)).run_realtime_sim(max_cycles=max_cycles or None)
        typer.echo(f"実時間シミュレーション完了: {len(logs)} 件のイベント")

    @app.command("demo-run")
    def demo_run(config: str = typer.Option(..., "--config", help="設定ファイル")) -> None:
        summary = LabApplication(Path(config)).run_demo()
        result = summary["result"]
        typer.echo(f"デモ完了: {result.output_dir}")

    @app.command("export-report")
    def export_report(run_id: str = typer.Option(..., "--run-id", help="実行ID")) -> None:
        app_state = LabApplication(Path("configs/mac_desktop_default.yaml"))
        path = app_state.locate_report(run_id)
        if path is None:
            raise typer.BadParameter("指定 run_id のレポートが見つかりません。")
        html_path = path / "report.html"
        typer.echo(str(html_path if html_path.exists() else path))

    @app.command("launch-desktop")
    def launch_desktop(
        config: str = typer.Option("configs/mac_desktop_default.yaml", "--config")
    ) -> None:
        from fxautotrade_lab.desktop.app import launch_desktop_app

        launch_desktop_app(Path(config))

    @app.command("verify-broker")
    def verify_broker(config: str = typer.Option(..., "--config", help="設定ファイル")) -> None:
        summary = LabApplication(Path(config)).verify_broker_runtime()
        typer.echo(
            "ブローカー確認完了: "
            f"positions={len(summary.get('positions', []))}, "
            f"orders={len(summary.get('orders', []))}, "
            f"fills={len(summary.get('fills', []))}"
        )

    app()


def _argparse_main() -> None:
    parser = argparse.ArgumentParser(description="FXAutoTrade Lab CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument("--config", required=True)

    subparsers.add_parser("sync-data", parents=[config_parser])
    subparsers.add_parser("backtest", parents=[config_parser])
    train_model = subparsers.add_parser("train-fx-model", parents=[config_parser])
    train_model.add_argument("--as-of", default="")
    research = subparsers.add_parser("research-run", parents=[config_parser])
    research.add_argument("--mode", default="")
    import_csv = subparsers.add_parser("import-csv", parents=[config_parser])
    import_csv.add_argument("--file", required=True)
    import_csv.add_argument("--symbol", default="")
    import_bidask = subparsers.add_parser("import-bidask-csv", parents=[config_parser])
    import_bidask.add_argument("--bid-file", required=True)
    import_bidask.add_argument("--ask-file", required=True)
    import_bidask.add_argument("--symbol", default="")
    import_tick = subparsers.add_parser("import-tick-csv", parents=[config_parser])
    import_tick.add_argument("--file", required=True)
    import_tick.add_argument("--symbol", default="")
    scalping = subparsers.add_parser("scalping-backtest", parents=[config_parser])
    scalping.add_argument("--tick-file", default="")
    scalping.add_argument("--symbol", default="")
    scalping.add_argument("--start", default="")
    scalping.add_argument("--end", default="")
    scalping_rt = subparsers.add_parser("scalping-realtime-sim", parents=[config_parser])
    scalping_rt.add_argument("--symbol", default="")
    scalping_rt.add_argument("--max-ticks", type=int, default=120)
    scalping_rt.add_argument("--poll-seconds", type=float, default=1.0)
    record_gmo = subparsers.add_parser("record-gmo-ticks", parents=[config_parser])
    record_gmo.add_argument("--symbol", default="")
    record_gmo.add_argument("--max-ticks", type=int, default=0)
    subparsers.add_parser("scalping-outcomes-summary", parents=[config_parser])
    realtime = subparsers.add_parser("realtime-sim", parents=[config_parser])
    realtime.add_argument("--max-cycles", type=int, default=None)
    subparsers.add_parser("demo-run", parents=[config_parser])
    export = subparsers.add_parser("export-report")
    export.add_argument("--run-id", required=True)
    launch = subparsers.add_parser("launch-desktop")
    launch.add_argument("--config", default="configs/mac_desktop_default.yaml")
    subparsers.add_parser("verify-broker", parents=[config_parser])

    args = parser.parse_args()
    if args.command == "sync-data":
        print(LabApplication(Path(args.config)).sync_data())
    elif args.command == "backtest":
        result = LabApplication(Path(args.config)).run_backtest()
        print(result.output_dir)
    elif args.command == "train-fx-model":
        summary = LabApplication(Path(args.config)).train_fx_model(as_of=args.as_of or None)
        print(summary)
    elif args.command == "research-run":
        summary = LabApplication(Path(args.config)).run_research(mode=args.mode or None)
        print(summary["output_dir"])
    elif args.command == "import-csv":
        raise SystemExit(
            "単一 CSV のインポートは無効です。"
            " import-bidask-csv で Bid / Ask の 2 ファイルを指定してください。"
        )
    elif args.command == "import-bidask-csv":
        summary = LabApplication(Path(args.config)).import_jforex_bid_ask_csv(
            bid_file_path=args.bid_file,
            ask_file_path=args.ask_file,
            symbol=args.symbol or None,
        )
        print(summary)
    elif args.command == "import-tick-csv":
        summary = LabApplication(Path(args.config)).import_jforex_tick_csv(
            file_path=args.file,
            symbol=args.symbol or None,
        )
        print(summary)
    elif args.command == "scalping-backtest":
        summary = LabApplication(Path(args.config)).run_scalping_backtest(
            tick_file_path=args.tick_file or None,
            symbol=args.symbol or None,
            start=args.start or None,
            end=args.end or None,
        )
        print(summary)
    elif args.command == "scalping-realtime-sim":
        summary = LabApplication(Path(args.config)).run_scalping_realtime_sim(
            symbol=args.symbol or None,
            max_ticks=args.max_ticks,
            poll_seconds=args.poll_seconds,
        )
        print(summary)
    elif args.command == "record-gmo-ticks":
        summary = LabApplication(Path(args.config)).record_gmo_scalping_ticks(
            symbol=args.symbol or None,
            max_ticks=args.max_ticks or None,
        )
        print(summary)
    elif args.command == "scalping-outcomes-summary":
        print(LabApplication(Path(args.config)).load_scalping_outcome_summary())
    elif args.command == "realtime-sim":
        logs = LabApplication(Path(args.config)).run_realtime_sim(max_cycles=args.max_cycles)
        print(len(logs))
    elif args.command == "demo-run":
        result = LabApplication(Path(args.config)).run_demo()["result"]
        print(result.output_dir)
    elif args.command == "export-report":
        app_state = LabApplication(Path("configs/mac_desktop_default.yaml"))
        path = app_state.locate_report(args.run_id)
        if path is None:
            print(None)
        else:
            html_path = path / "report.html"
            print(html_path if html_path.exists() else path)
    elif args.command == "launch-desktop":
        from fxautotrade_lab.desktop.app import launch_desktop_app

        launch_desktop_app(Path(args.config))
    elif args.command == "verify-broker":
        summary = LabApplication(Path(args.config)).verify_broker_runtime()
        print(summary)


def main() -> None:
    try:
        import typer  # noqa: F401
    except ImportError:
        _argparse_main()
    else:
        _typer_main()


if __name__ == "__main__":
    main()
