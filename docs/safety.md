# 安全ポリシー

- 実売買は既定で無効
- 実売買ブローカーを追加しても明示的安全フラグが必須
- UI/README/ヘルプに注意文を表示
- 実時間シミュレーションと実運用の差を明記
- automation 開始時と各サイクルで口座/注文/ポジションを再同期
- GMO 実時間データ取得で障害が出た場合は再接続を試み、失敗時はポーリングへフォールバック
- 日次損失制限でキルスイッチを発動
- キルスイッチ時は未完了注文キャンセルと全ポジションクローズを試行
- macOS 通知/ログ/Webhook で注文、エラー、再接続、停止理由を可視化
- デスクトップ起動時に stale process を整理し、終了時に pid 状態を掃除

## 将来の実売買に必要な安全フラグ

- `LIVE_TRADING_ENABLED=true`
- `I_UNDERSTAND_REAL_MONEY_RISK=true`
- `CONFIRM_BROKER_MODE=<future_real_broker_mode>`
- `CONFIRM_LIVE_BROKER_CLASS=<future_real_broker_class>`
