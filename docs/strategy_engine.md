# 戦略エンジン

## 実装済み戦略

### FxBreakoutPullbackStrategy

- 1時間足 EMA50 > EMA200
- EMA50 傾き > 0
- ADX または ATR percentile によるトレンド許可
- 15 分足終値ベースのブレイク判定
- 1 分足の浅い押し確認後のみ再上昇でエントリー
- Bid/Ask 前提の conservative intrabar 執行
- ATR トレーリング、1 時間足トレンド崩れで部分手仕舞い
- ML は参加許可フィルタとしてのみ追加可能

### BaselineTrendPullbackStrategy

- 日足 50EMA > 200EMA
- 日足終値 > 200EMA
- 日足 slope 正
- 下位足の押し目 + RSI 回復 + 任意の出来高確認
- ATR ストップ、トレーリング、最大保有期間

### MultiTimeframePatternScoringStrategy

以下のカテゴリを重み付き合成します。

- トレンドレジーム
- 押し目/継続
- ブレイクアウト/圧縮
- ローソク/値動き
- マルチタイムフレーム整合
- マーケットコンテキスト

各判定では、日本語の説明文とカテゴリ別サブスコアを保持します。

## 制約

- パターン名を魔法の予測子として扱わない
- 将来データで訓練しない
- ノールックアヘッドを優先
- walk-forward は未来漏れを避け、期間別評価サマリーを優先します
