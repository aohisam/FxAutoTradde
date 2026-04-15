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
    def launch_desktop(config: str = typer.Option("configs/mac_desktop_default.yaml", "--config")) -> None:
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
    import_bidask = subparsers.add_parser("import-bidask-csv", parents=[config_parser])
    import_bidask.add_argument("--bid-file", required=True)
    import_bidask.add_argument("--ask-file", required=True)
    import_bidask.add_argument("--symbol", default="")
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
    elif args.command == "import-bidask-csv":
        summary = LabApplication(Path(args.config)).import_jforex_bid_ask_csv(
            bid_file_path=args.bid_file,
            ask_file_path=args.ask_file,
            symbol=args.symbol or None,
        )
        print(summary)
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
