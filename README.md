# FXAutoTrade Lab

FXAutoTrade Lab は、米国株を対象にしたローカル完結型の定量売買リサーチ/自動売買デスクトップアプリです。PySide6 によるネイティブ UI、マルチタイムフレーム戦略、ヒストリカルバックテスト、ローカル自動シミュレーション、Alpaca ペーパー取引への接続経路を 1 つのコードベースで提供します。

本アプリは、単なる分析ダッシュボードではありません。ユーザーが自動売買を開始すると、アプリ自身が監視銘柄を巡回し、シグナル生成、リスク確認、シミュレーション注文または Alpaca ペーパー注文、ポジション管理、手仕舞い判断まで自動で実行します。

重要:

- 本アプリは利益や市場タイミングの正確性を保証しません。
- バックテスト、シミュレーション、ペーパー取引は実運用と異なる前提を含みます。
- ライブ取引は既定で無効化されています。
- 投資助言ではありません。

## 主な機能

- PySide6 による macOS 向けデスクトップ UI
- 日本語 UI
- Alpaca 市場データ/ペーパー注文アダプタ
- オフラインデモモード
- マルチ銘柄ポートフォリオバックテスト
- ローカル自動シミュレーション
- Alpaca ペーパー自動売買ループ
- 口座/注文/ポジションの再同期、paper 再接続復元、`verify-broker` 確認
- 日次損失制限、キルスイッチ、macOS 通知、通知ログ、任意 Webhook
- マルチタイムフレームの説明可能なスコア戦略
- HTML/CSV/JSON レポート出力
- SQLite による runs/orders/fills/positions/signal/event 履歴保持
- macOS キーチェーン保存による Alpaca 資格情報管理
- 将来のライブ移行を見据えた安全ゲート付き `AlpacaLiveBroker`

## アーキテクチャ概要

- `src/fxautotrade_lab/core`: ドメインモデル、enum、時間関連
- `src/fxautotrade_lab/config`: YAML/.env 設定ロード
- `src/fxautotrade_lab/data`: Fixture/Alpaca/Parquet キャッシュ
- `src/fxautotrade_lab/features`: EMA/RSI/ATR/パターン/相対強度
- `src/fxautotrade_lab/context`: ベンチマーク・流動性・セッション文脈
- `src/fxautotrade_lab/strategies`: `BaselineTrendPullbackStrategy` / `MultiTimeframePatternScoringStrategy`
- `src/fxautotrade_lab/simulation`: ポートフォリオシミュレータ
- `src/fxautotrade_lab/execution`: リスク/サイズ/重複注文防止
- `src/fxautotrade_lab/brokers`: `LocalSimBroker` / `AlpacaPaperBroker` / `AlpacaLiveBroker`
- `src/fxautotrade_lab/backtest`: バックテスト、指標、walk-forward 要約
- `src/fxautotrade_lab/reporting`: HTML/CSV/JSON/Markdown レポート
- `src/fxautotrade_lab/automation`: 自動売買ループ
- `src/fxautotrade_lab/persistence`: SQLite 永続化
- `src/fxautotrade_lab/desktop`: デスクトップ UI
- `src/fxautotrade_lab/cli`: CLI エントリポイント

## macOS Apple Silicon セットアップ

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
cp .env.example .env
```

`.env` には必要に応じて Alpaca の API キーを設定します。オフラインデモだけならキーは不要です。デスクトップ版では、設定画面から paper/live の API キーとシークレットを macOS キーチェーンに保存できます。`.env` は手動運用や CI 用の代替経路として残しています。

推奨:

- ペーパー用: 設定画面から `ペーパー資格情報をキーチェーン保存`
- ライブ用: まだ既定では無効だが、将来の read-only 検証や安全確認向けに `ライブ資格情報をキーチェーン保存`
- `.env`: ローカル CLI や一時的な検証時のみ

PySide6 のデスクトップ起動は、macOS では Python 配布形態の影響を受けます。Anaconda 系 Python で `cocoa` plugin 初期化に失敗する場合は、Homebrew Python 3.11 で GUI 用 venv を作成し、次のスクリプトから起動してください。

```bash
/opt/homebrew/bin/python3.11 -m venv .venv_gui
./.venv_gui/bin/pip install PySide6 alpaca-py click==8.1.7 numpy pandas plotly pyarrow pydantic pydantic-settings PyYAML typer
./scripts/launch_desktop_macos.sh
```

## オフラインデモ

Alpaca キーなしで動作確認できます。

```bash
python -m fxautotrade_lab.cli demo-run --config configs/demo_local.yaml
python -m fxautotrade_lab.cli launch-desktop --config configs/mac_desktop_default.yaml
./scripts/launch_desktop_macos.sh
```

## バックテスト

```bash
python -m fxautotrade_lab.cli sync-data --config configs/backtest_multitimeframe_scoring.yaml
python -m fxautotrade_lab.cli backtest --config configs/backtest_multitimeframe_scoring.yaml
python -m fxautotrade_lab.cli backtest --config configs/backtest_baseline.yaml
```

## Alpaca ペーパー取引

```bash
python -m fxautotrade_lab.cli paper-run --config configs/paper_alpaca_free.yaml
python -m fxautotrade_lab.cli verify-broker --config configs/paper_alpaca_free.yaml
```

`alpaca_paper` モードでは、UI/ログ上で次の注意表示を維持します。

- 実市場データ連動
- 実売買は行いません
- 約定はシミュレーションです

補足:

- paper 運転では起動時と各サイクルで口座/注文/約定/ポジション再同期を行います。
- WebSocket が不安定でも、ポーリングへフォールバックしつつ再接続を試みます。
- 資格情報は `.env` か macOS キーチェーンから読み込みます。キーチェーン保存がある場合は UI から状態を確認できます。

## レポート出力

バックテスト完了時に `reports/<timestamp>_<run_id>/` 配下へ以下を出力します。

- `report.html`
- `metrics.json`
- `trades.csv`
- `orders.csv`
- `fills.csv`
- `positions.csv`
- `equity_curve.csv`
- `drawdown.csv`
- `signal_log.csv`
- `config_snapshot.yaml`
- `summary.md`

既存レポートの場所確認:

```bash
python -m fxautotrade_lab.cli export-report --run-id <id>
```

## 無料でできる範囲

- Alpaca Paper Only の構成で市場データ取得とペーパー注文検証ができます。
- オフラインデモ、ローカルシミュレーション、バックテストは無料で使えます。
- 無料プランの制限を考慮し、既定のウォッチリストは少数銘柄です。

## 無料プランの制限

- IEX リアルタイムデータが中心です。
- SIP やより広いリアルタイム範囲は有料データが必要な場合があります。
- WebSocket 購読数やカバレッジに制限があるため、既定設定では銘柄数を控えめにしています。

## ペーパー取引とライブ取引の違い

- ペーパー取引は検証用で、実売買は行いません。
- スリッページ、約定、流動性、遅延は実運用と異なる場合があります。
- ライブ取引は v1 では hard-disabled です。

## 本番移行時に必要な変更

本番移行は API キー差し替えだけではありません。最低でも以下が必要です。

- `broker.mode=alpaca_live`
- ライブ用 API キー/シークレット
- ライブ用 base URL
- ライブ口座セットアップ
- 明示的な安全フラグ
- 運用ルール/監視手順/キルスイッチ整備

## ライブ安全ゲート

ライブ注文送信には次の設定がすべて必要です。

- `LIVE_TRADING_ENABLED=true`
- `I_UNDERSTAND_REAL_MONEY_RISK=true`
- `CONFIRM_BROKER_MODE=alpaca_live`
- `CONFIRM_LIVE_BROKER_CLASS=AlpacaLiveBroker`

read-only の接続確認でも、誤設定を避けるため `verify-broker` は live safety gate を通した構成でのみ有効です。

## 運用安全

- 口座・注文・ポジションは automation 開始時と各サイクルで再同期されます。
- paper/live のストリーミング接続が不安定な場合は再接続を試み、失敗時はポーリングのみで継続します。
- 日次損失制限に達するとキルスイッチが発動し、新規注文を止め、未完了注文キャンセルとポジションクローズを試みます。
- macOS では注文、エラー、日次損失停止、再接続復旧を通知できます。
- 通知チャネルは `desktop / sound / log / webhook` を設定画面から切り替えられます。
- デスクトップ起動時には古い `launch-desktop` 系プロセスを整理し、終了時も pid 状態を掃除します。

## macOS .app パッケージング

第 1 選択:

```bash
./scripts/package_macos_app.sh
```

生成された `.app` は Finder から開けて、Dock に追加して起動できます。

フォールバック:

```bash
pyinstaller --name FXAutoTradeLab --windowed --noconfirm scripts/desktop_entry.py
```

詳細は `docs/mac_packaging.md` を参照してください。
