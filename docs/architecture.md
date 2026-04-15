# アーキテクチャ

FXAutoTrade Lab は、データ取得、特徴量生成、戦略評価、シミュレーション、ブローカー送信、デスクトップ UI を疎結合に分離しています。

## フロー

1. `config/` が YAML と `.env` をロード
2. `data/` が Fixture / JForex CSV / GMO public API からバーを取得し Parquet キャッシュへ保存
3. `features/` がマルチタイムフレームの特徴量を生成
4. `context/` がイベント blackout / セッション / spread 文脈を追加
5. `strategies/` が説明可能なシグナルと日本語理由を生成
6. `simulation/` が共有現金・Bid/Ask 前提でバックテスト
7. `reporting/` が HTML/CSV/JSON/Markdown に出力
8. `persistence/` が SQLite に runs/orders/fills/positions/signals/events を保存
9. `desktop/` と `cli/` が同じ `application.py` を利用

## 実行モード

- `local_sim`: ブローカー送信なし、ローカル約定のみ
- `gmo_sim`: GMO public API を読みつつ、約定はローカルシミュレーション

## 公式主系

- 主戦略は `fx_breakout_pullback`
- シグナル判定は 15 分足 / 1 時間足
- 執行は 1 分足
- ML は参加許可フィルタとしてのみ使う
