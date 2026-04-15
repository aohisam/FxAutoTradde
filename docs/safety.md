# 安全ポリシー

- ライブ取引は既定で無効
- `alpaca_live` 選択だけでは注文できない
- 4 つの環境変数安全ゲートが必須
- UI/README/ヘルプに注意文を表示
- ペーパー取引とシミュレーションの限界を明記
- automation 開始時と各サイクルで口座/注文/ポジションを再同期
- paper/live のストリーミング断は再接続を試み、失敗時はポーリングへフォールバック
- 日次損失制限でキルスイッチを発動
- キルスイッチ時は未完了注文キャンセルと全ポジションクローズを試行
- macOS 通知/ログ/Webhook で注文、エラー、再接続、停止理由を可視化
- Alpaca の paper/live 資格情報は macOS キーチェーン保存を優先
- デスクトップ起動時に stale process を整理し、終了時に pid 状態を掃除

## 必須安全フラグ

- `LIVE_TRADING_ENABLED=true`
- `I_UNDERSTAND_REAL_MONEY_RISK=true`
- `CONFIRM_BROKER_MODE=alpaca_live`
- `CONFIRM_LIVE_BROKER_CLASS=AlpacaLiveBroker`
