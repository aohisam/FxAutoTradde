# FXAutoTrade Lab

FXAutoTrade Lab は、USD/JPY を主対象にしたローカル完結型の FX リサーチ/自動売買デスクトップアプリです。PySide6 による日本語ネイティブ UI、Bid/Ask 前提のバックテスト、GMO public API を使った実時間シミュレーション、JForex CSV 取込、walk-forward 学習対応の FX 参加許可フィルタを 1 つのコードベースで扱います。

本アプリの主目的は「値動きを当てること」ではなく、「参加してよい局面だけを選び、実売買に近い条件で一貫した期待値を狙うこと」です。現行版の主戦略は、1時間足トレンド判定と 15 分足ブレイク、1分足押し目再参加を組み合わせた `fx_breakout_pullback` です。

重要:

- 本アプリは利益や市場タイミングの正確性を保証しません。
- バックテスト、ローカルシミュレーション、実時間シミュレーションは実運用と異なる前提を含みます。
- 実売買は既定で無効化されています。
- 投資助言ではありません。

## 主な機能

- PySide6 による macOS 向けデスクトップ UI
- 日本語 UI
- GMO public API と JForex CSV の両対応データ導線
- Bid/Ask 分離、mid 系列生成、contextual spread filter
- `fx_breakout_pullback` 戦略
- ルールベース単体で稼働可能な FX ML 参加許可フィルタ
- walk-forward バックテスト / research_run
- ローカル自動シミュレーション / GMO 実時間シミュレーション
- SQLite による run / order / fill / position / signal / event の履歴保存
- HTML / CSV / JSON レポート出力
- macOS 通知、通知ログ、任意 Webhook

## アーキテクチャ概要

- `src/fxautotrade_lab/core`: ドメインモデル、enum、時刻・通貨ペアヘルパ
- `src/fxautotrade_lab/config`: YAML / `.env` 設定ロード
- `src/fxautotrade_lab/data`: fixture / GMO / JForex CSV / Parquet キャッシュ
- `src/fxautotrade_lab/features`: FX 指標、マルチタイムフレーム特徴量
- `src/fxautotrade_lab/strategies`: `fx_breakout_pullback` と補助戦略
- `src/fxautotrade_lab/simulation`: Bid/Ask 前提の FX バックテスト
- `src/fxautotrade_lab/execution`: リスク、サイズ、重複注文防止
- `src/fxautotrade_lab/backtest`: バックテスト、walk-forward、学習実行
- `src/fxautotrade_lab/research`: research_run パイプライン
- `src/fxautotrade_lab/automation`: 実時間シミュレーションループ
- `src/fxautotrade_lab/persistence`: SQLite 永続化
- `src/fxautotrade_lab/desktop`: デスクトップ UI
- `src/fxautotrade_lab/cli`: CLI エントリポイント

## セットアップ

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
cp .env.example .env
```

`.env` には必要に応じて GMO private API のキーを設定できます。現行版の主系は GMO public API とローカル約定なので、バックテストと実時間シミュレーションだけなら必須ではありません。

PySide6 の GUI 起動は macOS の Python 配布形態の影響を受けます。Anaconda 系 Python で `cocoa` plugin 初期化に失敗する場合は、Homebrew Python 3.11 で GUI 専用 venv を作成してから次を実行してください。

```bash
/opt/homebrew/bin/python3.11 -m venv .venv_gui
./.venv_gui/bin/pip install PySide6 click==8.1.7 numpy pandas plotly pyarrow pydantic pydantic-settings PyYAML typer pillow
./scripts/launch_desktop_macos.sh
```

## 代表コマンド

```bash
python -m fxautotrade_lab.cli sync-data --config configs/backtest_fx_breakout.yaml
python -m fxautotrade_lab.cli backtest --config configs/backtest_fx_breakout.yaml
python -m fxautotrade_lab.cli train-fx-model --config configs/backtest_fx_breakout.yaml
python -m fxautotrade_lab.cli research-run --config configs/backtest_fx_breakout.yaml --mode standard
python -m fxautotrade_lab.cli realtime-sim --config configs/realtime_gmo_public.yaml --max-cycles 5
python -m fxautotrade_lab.cli verify-broker --config configs/realtime_gmo_public.yaml
python -m fxautotrade_lab.cli launch-desktop --config configs/mac_desktop_default.yaml
```

## JForex CSV 取込

```bash
python -m fxautotrade_lab.cli import-bidask-csv \
  --config configs/backtest_fx_breakout.yaml \
  --bid-file /path/to/USDJPY_1\ Min_Bid.csv \
  --ask-file /path/to/USDJPY_1\ Min_Ask.csv \
  --symbol USD_JPY
```

1分足の Bid/Ask を取り込むと、1分足から 15 分足 / 1 時間足 / 日足などを再生成し、バックテストと research_run で再利用できます。

## 研究パイプライン

`research-run` は次を一括で実行します。

- データ検証
- 学習用データ生成
- FX ML 学習
- rule-only baseline と walk-forward 学習版の比較
- spread 悪化 / entry delay の頑健性チェック
- 感度表とレポート出力

## 実時間シミュレーション

`configs/realtime_gmo_public.yaml` は GMO public API でレートを取得しつつ、約定自体はローカルで行う構成です。

- 実市場データを監視します
- 実売買は行いません
- 約定は Bid/Ask 前提のローカルシミュレーションです

## GMO public API の制約

- public API は read-only です
- 通貨ペア一覧やティッカー取得は認証不要です
- 実売買や private API 連携は将来拡張扱いです

## 実時間シミュレーションと実売買の違い

- 実時間シミュレーションは検証用で、実売買は行いません
- 実運用ではスリッページ、流動性、注文制約、通信断の影響が変わります
- 実売買は v1 では既定無効です

## 将来の実売買移行

将来の GMO private API 連携は見据えていますが、最低でも以下が必要です。

- 実売買ブローカー実装
- 明示的な安全フラグ
- ロット / 損失制限 / 緊急停止
- 監視手順とアラート
- 実運用前の再検証

## 運用安全

- 口座・注文・ポジションは automation 開始時と各サイクルで再同期します
- 日次損失制限に達するとキルスイッチが発動し、新規注文を止めます
- macOS では注文、エラー、日次損失停止、再接続復旧を通知できます
- `.app` 起動時には stale process を整理します

## macOS .app パッケージング

第 1 選択:

```bash
./scripts/package_macos_app.sh
```

生成された `.app` は Finder から開けて、Dock に追加して起動できます。アプリアイコンはリポジトリ直下の `icon.png` から自動反映されます。
