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
単一 CSV のインポートは無効で、ファイル名に `Bid` / `Ask` と通貨ペア名を含む 2 ファイルが必須です。Bid / Ask の期間がずれる場合は、短い側にそろえた共通期間だけを取り込みます。
運用上は、長期履歴をまず CSV で取り込み、その後に GMO 同期で現在時刻までの空白期間だけを補完する形を推奨します。

## 研究パイプライン

`research-run` は次を一括で実行します。

- データ検証
- 学習用データ生成
- FX ML 学習
- rule-only baseline と walk-forward 学習版の比較
- spread 悪化 / entry delay の頑健性チェック
- 感度表とレポート出力

## スキャルピングML検証

`scalping-backtest` は tick / 秒足向けの研究用パイプラインです。利益を保証するものではなく、未知期間で成績が崩れる前提を見つけるための検証基盤です。

- ラベルは `max_hold_seconds` を実時間として扱い、不規則barや欠損barでも「行数=秒数」とみなしません。
- `label_source: tick` では、entry latency 後の最初の tick、Bid/Ask による TP/SL 判定、round-trip fee を含む tick replay と同じ前提でラベルを作ります。`label_source: bar` は過去互換のfallbackです。
- train / validation / test は purged split で分け、`max_hold_seconds`、entry latency、cooldown を含む境界purgeを入れます。
- モデル係数は train で学習し、`decision_threshold` は validation tick replay で選びます。cooldown、日次停止、spread filter、blackout、entry latency、最大取引回数まで含めた同じ売買エンジンで閾値を評価し、replay が使えない場合だけ label 集計へfallbackします。
- validation gate は `min_validation_net_pips`、`min_validation_profit_factor`、`min_validation_trade_count` で最低条件を確認します。`fail_closed_on_bad_validation: true` の場合、基準未達なら `decision_threshold=1.01` にして新規entryを止め、metadata に `validation_gate_passed`、`threshold_selection_method`、`warning_ja` を残します。
- 学習直後のモデルは `models/fx_scalping/candidates/{run_id}.json` に保存されます。test backtest、stress test、walk-forward の promotion gate を通過した候補だけ `latest_scalping_model.json` に昇格し、不合格時は既存 latest を更新しません。
- realtime paper は昇格済み latest モデルだけを読み込みます。candidate や promotion 未通過モデルは、誤って shadow 運用に混ざらないよう日本語エラーで停止します。
- fee、slippage、spread、entry latency は tick replay の損益へ反映します。`realized_pips` は net pips の別名で、gross は `realized_gross_pips` を確認してください。
- accepted / rejected signal を `signals.csv` に出力します。`reject_reason` で threshold不足、spread超過、volatility不足、cooldown、日次損失停止、連敗停止、stale tick、blackout などを確認できます。
- backtest で labels がある場合だけ、分析用に `future_long_net_pips` などをjoinします。実時間paper simulationや将来のlive系では未来結果を使いません。
- `blackout_windows_jst` でロールオーバーや手動ニュース回避時間を設定できます。日跨ぎwindowにも対応します。
- `spread_stress_multipliers` と `latency_ms_grid` で spread拡大 / latency悪化のstress結果を `stress_results.csv` / JSON に保存します。stress結果は自動合否ではなく、脆弱性の警告材料です。
- `max_daily_loss_amount` と `max_consecutive_losses` はtick replayの新規entry停止に使われ、翌日にはリセットされます。
- backtest と realtime paper は共通の signal/risk/execution policy を使います。paper 側も stale tick、blackout、spread z-score、spread-to-mean ratio、rejected signal logging、entry latency を再現します。
- backtest と paper simulation の signals / trades / outcomes は `outcome_store_dir` 配下へ run 横断で保存され、次回以降の分析・再学習データとして読み込めます。
- probability calibration report として probability decile 別の取引数、勝率、平均/合計 net pips、profit factor、Brier score、calibration curve CSV を出力します。
- デスクトップの「レポート」ページは、通常バックテストに加えて `reports/scalping/*/summary.json` のスキャルピング検証結果も一覧表示します。
- 将来のprivate broker連携向けには `ScalpingOrderPlan` で注文意図を共通化しています。ただし既定はdry-runで、private brokerへの実注文送信は未実装かつ無効です。

設定例:

```yaml
strategy:
  fx_scalping:
    label_source: tick
    validation_ratio: 0.15
    test_ratio: 0.15
    purge_seconds: null
    max_daily_loss_amount: 100000.0
    max_consecutive_losses: 5
    max_tick_gap_seconds: 5
    min_validation_net_pips: 0.0
    min_validation_profit_factor: 1.05
    min_validation_trade_count: 50
    fail_closed_on_bad_validation: true
    walk_forward_enabled: true
    min_test_profit_factor: 1.05
    min_test_trade_count: 50
    min_test_net_profit: 0.0
    max_test_drawdown_amount: 100000.0
    min_stress_profit_factor: 1.0
    min_stress_net_profit: -50000.0
    min_walk_forward_pass_ratio: 0.6
    outcome_store_enabled: true
    outcome_store_dir: runtime/scalping_outcomes
    blackout_windows_jst:
      - start: "05:55"
        end: "06:10"
        reason: rollover
      - start: "23:55"
        end: "00:10"
        reason: cross_midnight_manual
    spread_stress_multipliers: [1.0, 1.2, 1.5, 2.0]
    latency_ms_grid: [0, 250, 500, 1000]
```

特徴量列は拡張されています。古いスキャルピングモデルを新しい特徴量定義で黙って使うことはできません。読み込み時に日本語エラーを出すため、旧モデルは再学習してください。

今回の変更でも live trading は既定で無効のままです。GMO private broker、実売買安全ゲート、実注文の部分約定や通信断の再現は別途検証が必要です。

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
