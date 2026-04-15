# アーキテクチャ

FXAutoTrade Lab は、データ取得、特徴量生成、戦略評価、シミュレーション、ブローカー送信、デスクトップ UI を疎結合に分離しています。

## フロー

1. `config/` が YAML と `.env` をロード
2. `data/` が Fixture または Alpaca からバーを取得し Parquet キャッシュへ保存
3. `features/` がマルチタイムフレームの特徴量を生成
4. `context/` がベンチマーク/流動性/セッション文脈を追加
5. `strategies/` が説明可能なスコアと日本語理由を生成
6. `simulation/` が共有現金・複数ポジションでバックテスト
7. `reporting/` が HTML/CSV/JSON/Markdown に出力
8. `persistence/` が SQLite に runs/orders/fills/positions/signals/events を保存
9. `desktop/` と `cli/` が同じ `application.py` を利用

## 実行モード

- `local_sim`: ブローカー送信なし、ローカル約定のみ
- `alpaca_paper`: Alpaca の紙口座のみ
- `alpaca_live`: 実アダプタは存在するが v1 では hard-disabled
