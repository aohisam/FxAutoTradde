# リスク管理・資金管理ルール、個人投資家の成績統計、月次黒字継続の現実的確率(USD/JPY 自動売買・国内業者前提)

調査日: 2026-10-04。対象システム前提: 口座 5,000,000 JPY、1 注文あたり固定ノーショナル 300,000 JPY、日次最大損失 100,000 JPY、ATR ベース損切り、1 分〜1 時間足、USD/JPY のみ。

調査手段の制約(重要): 本セッションでは Web 検索枠を使い切り、かつ多くのサイト(click-sec.com, ffaj.or.jp, fsa.go.jp, zai.diamond.jp, oanda.jp, fxcm.com, ssrn.com, esma.europa.eu, wikipedia 等)への直接取得がネットワーク側で遮断されたため、各統計は検索結果スニペットに基づく。原典 URL は付けたが、全文照合できていない数値には「スニペット準拠」と注記した。数式・表の多くは本調査で Python により計算した(前提を明記)。業者条件は 2026 年 10 月時点で再確認が必要。

本リポジトリとの整合: `README.md` の「GMO public API」は GMOコインの外国為替FX API(`forex-api.coin.z.com`)を指し、GMOクリック証券(FXネオ)ではない。両者は別会社・別サービスであり、本ノートでは明確に区別する。

---

## 主要質問 1: ポジションサイジング(固定フラクショナル / ボラティリティ目標 / 分数ケリー / 固定ノーショナル)と、最大ドローダウン・連敗確率の公式

### Takeaway
実務家・書籍の推奨は「1 取引あたり口座の 0.5〜2% リスク」「年率 10〜25% のボラ目標」「フルケリーの半分以下」に収斂しており、いずれも「期待値ではなく破綻確率とドローダウン分布を制御する」思想で一致する。所有者の固定 300,000 JPY ノーショナルは口座比 0.06 倍(年率ボラ約 0.5%)で、これらの推奨帯より 1〜2 桁保守的である。月次黒字確率はサイズに依存しない(シャープレシオのみに依存)ため、サイズは「絶対額の P&L と DD の大きさ」を決めるだけだが、現在の日次損失上限 100,000 JPY は 1 ポジション 300,000 JPY では 5,000 pips 相当となり、実質的に発動しない設定になっている。

### Cited Findings
- Robert Carver はリスク許容度を「ボラティリティ目標(年率換算した期待標準偏差)」という単一の数値で表し、ポジションサイズは「直近の価格ボラティリティ」で割って決める方式を推奨。ケリー基準に基づく最適サイズは「不運な連続に備えてフルケリーでは攻めすぎ」であるため半ケリーを採用すべきと述べる(書籍 "Systematic Trading"(2015)、"Leveraged Trading" のレビュー) — [CXO Advisory: A Few Notes on Systematic Trading](https://www.cxoadvisory.com/big-ideas/a-few-notes-on-systematic-trading)
- ケリー最適のボラ目標はシャープレシオにほぼ等しい(SR 0.5 → 50%、SR 1 → 100%)が、「ケリー同様に悪い結果を避けるため半分にすべき」。ファンデッド・チャレンジ向けシミュレーションでは「Rob's Recommendation: 15% ボラ」、SR=1.5・ボラ 15% で成功率約 52% と記載(第三者のシミュレータページ、スニペット準拠) — [Trading Parameters simulator (moontowermeta)](https://trading-sim.moontowermeta.com/) ; [Top Traders Unplugged SI108: The Case For and Against the Kelly Criterion ft. Robert Carver](https://www.toptradersunplugged.com/captivate-podcast/si108-the-case-for-and-against-the-kelly-criterion-ft-robert-carver)
- 「0.5×ケリーは期待成果の大部分を確保しつつ、悲惨な結果の分散を大幅に減らすため、フルケリーよりはるかに最適に近い」 — [Kelly Criterion: Optimal Position Sizing for Traders](https://ryanoconnellfinance.com/kelly-criterion/)
- シャープレシオの時間スケーリング(年率 = √12 × 月次)は IID の特殊条件下でのみ成立し、系列相関があると過大・過小評価になる(Lo 2002) — [The Statistics of Sharpe Ratios (Lo, FAJ 2002)](https://traders.studentorg.berkeley.edu/papers/The-Statistics-of-Sharpe-Ratios.pdf) ; [CFA Institute 掲載ページ](https://rpc.cfainstitute.org/research/financial-analysts-journal/2002/the-statistics-of-sharpe-ratios)
- FXCM の教育資料は、勝率 30% でも RR 3:1 なら黒字(10 取引で +$2)、RR 1:1 なら赤字(−$4)という例で「RR が勝率への依存を緩和する」と説明 — [FXCM: Best Practices of Successful Traders](https://www.fxcm.com/uk/education/traits-successful-traders/)
- 本リポジトリの `RiskManager` は「risk_per_trade(既定 1%) ÷ ATR ストップ距離」のリスクベース、口座比率、固定金額(`fixed_order_amount`)の 3 モードを実装し、`max_daily_loss_amount` 既定 100,000 JPY、`max_daily_loss_pct` 2%、`atr_stop_multiple` 2.2 を持つ(内部資料) — `src/fxautotrade_lab/execution/risk.py`, `src/fxautotrade_lab/config/models.py`

### 計算(本調査、Python。前提: 1 取引の損失を 1R、利益を bR、勝率 p、取引間独立)

所有者の数値への換算(USD/JPY=150 と仮定):
- 300,000 JPY = 2,000 USD。1 pip(0.01 円)= 20 JPY、1 円変動 = 2,000 JPY。レバレッジ 0.06 倍。
- 日次損失上限 100,000 JPY(口座の 2.0%)= 1 ポジションあたり 5,000 pips(50 円)の逆行。同時 5 ポジションでも 1,000 pips/ポジション。**単独では到達不能**。
- ATR ストップごとの 1 取引リスク(代表的 ATR: 1 分 2 pips、15 分 7 pips、1 時間 18 pips): 1 分×1.5ATR = 60 JPY(口座比 0.0012%)、15 分×1.5ATR ≈ 210 JPY(0.0042%)、1 時間×1.5ATR ≈ 540 JPY(0.0108%)。日次上限に達するには 1 日 185〜1,667 回の連続全損が必要。
- 固定 300k ノーショナルの口座年率ボラ ≈ 0.54%(USD/JPY 年率ボラ 9% 仮定)、日次 1σ P&L ≈ 1,700 JPY。
- Carver 型ボラ目標に換算すると、目標 10% → ノーショナル 5,000,000〜6,250,000 JPY(300k 注文 17〜21 本分)、目標 15% → 7,500,000〜9,375,000 JPY(25〜31 本分)、目標 25% → 12,500,000〜15,625,000 JPY(42〜52 本分)。
- 固定フラクショナル 0.5% / 1% / 2% = 25,000 / 50,000 / 100,000 JPY のリスク/取引。1 時間足 27 pips ストップなら 1% リスクは約 1,850 USD/pip… ではなく 50,000 JPY ÷ 27 pips = 1,852 JPY/pip = 約 185,000 USD(2,780 万円)のノーショナルに相当し、レバレッジ約 5.6 倍。
- 半ケリー(f* = p − (1−p)/b): p=0.55,b=1.0 → フル 10%、半 5% = 250,000 JPY/取引。p=0.45,b=1.5 → 半 4.2% = 208,000 JPY/取引。推定誤差を考えると実務推奨(0.5〜2%)はさらにその 1/3〜1/10 である。

期待最大ドローダウン(モンテカルロ、R 単位。5,000 試行):

| p | b | E[R]/取引 | 取引数 N | 平均 MDD(R) | 95% MDD(R) | 99% MDD(R) | 1R=600 JPY(所有者)なら 95% MDD | 1R=1%(50,000 JPY)なら |
|---|---|---|---|---|---|---|---|---|
| 0.45 | 1.5 | +0.125 | 100 / 500 / 1000 | 10.2 / 17.9 / 21.6 | 18.5 / 29.5 / 34.5 | 24.5 / 38.5 / 44.0 | 11,100 / 17,700 / 20,700 JPY | 92.5 万 / 147.5 万 / 172.5 万 JPY |
| 0.55 | 1.0 | +0.10 | 100 / 500 / 1000 | 8.2 / 14.6 / 17.6 | 15.0 / 25.0 / 28.0 | 19.0 / 32.0 / 36.0 | 9,000 / 15,000 / 16,800 JPY | 75 万 / 125 万 / 140 万 JPY |
| 0.60 | 0.8 | +0.08 | 100 / 500 / 1000 | 7.7 / 13.8 / 16.7 | 14.2 / 23.4 / 27.4 | 18.6 / 30.0 / 34.6 | 8,500 / 14,000 / 16,400 JPY | 71 万 / 117 万 / 137 万 JPY |
| 0.50 | 1.0 | 0(エッジ無) | 100 / 500 / 1000 | 11.5 / 27.1 / 38.7 | 21 / 49 / 70 | 26 / 63 / 90 | 12,600 / 29,400 / 42,000 JPY | 105 万 / 245 万 / 350 万 JPY |

ブラウン運動近似(年率ボラ σ、年率シャープ S、1 年間、日次 252 ステップ)の平均 / 95% 最大 DD: S=0.5,σ=10% → 9.9% / 18.6%、S=1,σ=10% → 8.5% / 15.8%、S=2,σ=10% → 6.5% / 11.4%、S=3,σ=10% → 5.3% / 8.9%。σ=15% ではそれぞれ 1.5 倍。目安「年間の期待 MDD ≈ 年率ボラ × (0.5〜1.0)」で、S が 1 → 3 に改善しても MDD は約 4 割しか減らない。

N 取引中に k 連敗以上が起きる確率(マルコフ連鎖で厳密計算):

| 勝率 p | N | k=5 | k=6 | k=8 | k=10 | k=12 | 期待最長連敗 ≈ ln N / ln(1/(1−p)) |
|---|---|---|---|---|---|---|---|
| 0.40 | 100 | 97.6% | 87.3% | 49.0% | 20.5% | 7.7% | 9.0 |
| 0.40 | 250 | 100% | 99.5% | 82.5% | 45.2% | 19.1% | 10.8 |
| 0.50 | 100 | 81.0% | 54.6% | 17.0% | 4.4% | 1.1% | 6.6 |
| 0.50 | 250 | 98.6% | 86.8% | 38.4% | 11.2% | 2.9% | 8.0 |
| 0.50 | 1000 | 100% | 100% | 86.1% | 38.5% | 11.4% | 10.0 |
| 0.60 | 100 | 45.9% | 21.2% | 3.6% | 0.6% | 0.1% | 5.0 |
| 0.60 | 250 | 79.2% | 45.9% | 9.2% | 1.5% | 0.2% | 6.0 |
| 0.60 | 1000 | 99.8% | 91.7% | 32.4% | 6.1% | 1.0% | 7.5 |

固定フラクショナルでの「−D% 到達確率」(1,000 取引、モンテカルロ): p=0.55,b=1 で f=0.5% → −10% 到達 1.9%、−20% 到達 0.0%; f=1% → 12.6% / 1.7%; f=2% → 35.5% / 12.3%。p=0.45,b=1.5 で f=1% → 18.8% / 2.8%、f=2% → 43.5% / 18.4%。

### Inferences
- 書籍ベース(本セッションで URL 未検証): Ralph Vince の optimal f(幾何成長最大化)はフルケリーと同種で DD が極端に深くなるため実務では分数化が常識、Van Tharp の「期待値 = Σ(R 倍数)/N」と「1R = 初期リスク」の枠組み、Tomasini & Jaekle "Trading Systems" のパーセント・ボラティリティ法(ATR で割る)はいずれも Carver のボラ目標と同じ数学(サイズ ∝ 1/ボラ)に帰着する。「月次 P&L が最も滑らか」なのは理論上ボラ目標法(各取引のリスク額を一定化)で、固定ノーショナルはボラ変動がそのまま P&L 分散に乗る。ただし USD/JPY 単一通貨・ATR ストップ・1R 固定であれば「固定ノーショナル × ATR ストップ」は実質「リスク額 ∝ ATR」となり、ボラが高い局面でリスクが増える(ボラ目標法と逆)点に注意。ATR 正規化したいなら数量 = 一定リスク額 ÷ (ATR×倍率) に切り替える方が月次分散は小さくなる。
- 現行設定 `max_consecutive_losses: 5` は、勝率 50% の正しいシステムでも 100 取引あたり 81% の確率で偶然発動する。停止閾値はそのシステムの p と月間 N で「偶然発生確率 < 5%」となる k(p=0.5, N=250 なら k≈12、p=0.6 なら k≈9〜10、p=0.4 なら k≈14〜15)に置くべき。
- 日次損失上限 100,000 JPY は固定 300k ノーショナルでは意味を持たない。意味を持たせるには(a) 上限を R 倍数で定義(例: 1 日の想定取引数 × 平均リスク × 3〜5、15 分足なら 1,000〜2,000 JPY 程度)、(b) ノーショナルをボラ目標(年率 5〜10% = 250 万〜625 万 JPY)に引き上げる、のどちらか。(b) を取る場合は 2% 日次上限が FTMO の 5% より厳しく、−2% 到達確率は年率ボラ 10% なら日次 σ ≈ 0.63% で約 3.2σ、年数回未満。

### Gaps
- Vince、Tharp、Tomasini & Jaekle、Kaufman の原典ページ URL は取得できず(書籍)。「どの方式が最も滑らかな月次 P&L を与えるか」を直接比較した一次研究は見つからなかった(上記は数学的帰結)。
- 連敗確率・最大 DD の公式は本調査の計算であり、取引間の独立性を仮定している。同一通貨ペアで複数戦略を並走させる場合は実効 N が減るため、実データでブートストラップし直す必要がある。

---

## 主要質問 2: 損失リミット(日次 / 週次 / 最大 DD キルスイッチ / 月次 ±X% で停止)の効果

### Takeaway
ランダムウォーク下では損失リミットも利益ロックインも期待値を下げる(Kaminski & Lo)が、リターンにモメンタム/レジーム持続があるときはストップ系ルールが期待値とボラを同時に改善する。月次の利益ロックインは「黒字月の割合」を 10pp 前後押し上げる代わりに月次期待値を 20〜45% 削るというトレードオフを持ち(本調査シミュレーション)、長期シャープは改善しない。プロップファームのデータでは日次損失上限が最も頻繁に抵触する制約(シミュレーション失敗の約 60%)である。

### Cited Findings
- Kaminski & Lo "When do stop-loss rules stop losses?": ランダムウォーク仮説の下では単純な 0/1 ストップロスは常に期待リターンを下げるが、モメンタムが存在する場合は価値を加える。1950〜2004 年の月次データで、特定のストップルールはストップ発動期間中にバイ&ホールドに対して月 50〜100bp を加え、モメンタム戦略では平均月次リターンを 1.01% → 1.73%(+71.3%)、標準偏差を 6.07% → 4.67%(−23%)に改善 — [MIT DSpace: When do stop-loss rules stop losses?](https://dspace.mit.edu/handle/1721.1/114876) ; [PDF](https://dspace.mit.edu/bitstream/handle/1721.1/114876/Lo_When%20Do%20Stop-Loss.pdf)
- プロップファーム制約を日次で再現したバックテストでは「個人口座でプロフィットファクター 2.0 の戦略が、$5,000 日次損失上限とトレーリング DD の下では 1.1 になり得る」「3 年で最大 DD 8% の戦略でも 1 日 −6% の日が存在し 5% 日次ルールに抵触する。集計指標ではこれが埋もれる」 — [Backtrex: Backtesting With Prop Firm Rules](https://backtrex.com/en/blog/backtesting-prop-firm-rules) ; [TradeZella: Prop Firm Backtesting](https://www.tradezella.com/blog/prop-firm-backtesting)
- 「シミュレーション失敗のおよそ 60% は日次損失上限の抵触であり、総 DD や期限ではない。5% の日次上限が最もタイトな構造的制約で、誰にでもある 1 回の悪い日がチャレンジを即終了させる」 — [Backtrex: Prop firm challenge success rate](https://backtrex.com/en/blog/prop-firm-challenge-success-rate)
- 「正の期待値を持つ戦略は損失上限下でもエッジを保つ(上限日には取引数が減るだけ)が、劣化状態のトレーダーは最大限の損害を与える能力を失う」 — [LuxAlgo: Loss-control Rules](https://www.luxalgo.com/library/concept/loss-control-rules.md)
- 本リポジトリは `halt_for_day_on_daily_loss: true`、`max_trades_per_day: 120`、ロールオーバー(05:55–06:10 JST)と日付跨ぎ(23:55–00:10)のブラックアウトを既に持つ(内部資料) — `README.md`, `src/fxautotrade_lab/config/models.py`

### 計算(本調査: 1 か月 21 営業日、日次リターン = 日次 SR + N(0,1)、8,000 試行)

| 年率 SR | ルール | 黒字月の確率 | 月次期待 P&L(日次 σ 単位) | 期待値の変化 |
|---|---|---|---|---|
| 1.0 | なし | 61.5% | +1.31 | — |
| 1.0 | 月間 +2σ_day で停止 | 71.7% | +0.75 | −43% |
| 1.0 | 月間 +4σ_day で停止 | 63.3% | +1.08 | −18% |
| 1.0 | 月間 −3σ_day で停止 | 55.0% | +1.09 | −17%(黒字月は減る) |
| 1.0 | +4 / −3 の両方 | 56.8% | +0.83 | −37% |
| 2.0 | なし | 71.8% | +2.64 | — |
| 2.0 | +2σ_day で停止 | 80.0% | +1.36 | −48% |
| 2.0 | −3σ_day で停止 | 65.9% | +2.30 | −13% |
| 0.5 | なし | 56.1% | +0.65 | — |
| 0.5 | +2σ_day で停止 | 67.3% | +0.40 | −38% |

### Inferences
- 月次利益ロックインは「黒字月の数」という KPI を買い、「年間リターン」を売る取引である。年間黒字額を最大化したいなら合理性は低いが、「月次黒字率」そのものを目的関数にするなら +2σ_day 程度の早期ロックインで 61% → 72%(SR 1 の場合)まで上がる。ただし長期シャープは不変〜悪化で、12 か月連続黒字の確率はなお低い(後述)。
- 月次損失側のストップ(−Y% で当月停止)は黒字月の確率をむしろ下げる(回復機会を奪う)。その価値は「テール損失の上限」と「モデル破綻の検知」にあり、黒字月率を上げる道具ではない。
- アルゴリズムには「ティルト」はないが、「レジーム変化でモデルが壊れる」という持続的な負の自己相関があり得るため、Kaminski & Lo の「モメンタム/レジーム下ではストップが有効」という条件は自動売買の日次・週次損失上限を正当化する。閾値は「正常時の日次損失分布の 3σ 超」に置くと誤発動が年数回に収まる。
- 現行構成では 100,000 JPY の日次上限より `max_consecutive_losses` と `min_validation_profit_factor` 等のゲートの方が先に働く。上限は「当日の累積 R 損失(例: −6R〜−10R)」に再定義すると、固定 300k でもボラ目標でも同じ意味になる。

### Gaps
- 「月次 +X% で停止すると黒字月の割合がどれだけ上がるか」を扱った査読研究・業界統計は見つからなかった(上表は本調査の単純シミュレーション。日次リターンの自己相関がゼロの仮定)。
- 週次損失上限の効果に関する独立した研究は見つからなかった(日次上限の議論の延長で推論)。

---

## 主要質問 3: 月次黒字の数学と実証(Φ(S/√12)、取引回数と 1 取引あたりエッジ、SG 指数、CTA、Medallion、Virtu)

### Takeaway
月次リターンが近似的に正規なら P(黒字月) = Φ(S_年率/√12)。S=1 で 61%、S=2 で 72%、S=3 で 81%、S=4 で 88%。12 か月すべて黒字の確率は S=3 でも 7.6%、S=4 で 20%、S=5 で 40% に過ぎない。実証でも、トレンドフォロー CTA は黒字月がおおむね半分強、Medallion(年率 39% ネット、1988–2018)で黒字月約 80%、Virtu のマーケットメイクでも「1,238 営業日で損失日 1 日」は日次シャープ ≈ 3(年率 ≈ 50)という方向性トレードでは到達不能な領域。**「毎月黒字」は実現可能な目標ではなく、現実的な上限は年率 SR 1.5〜2 で黒字月 65〜72%、例外的に SR 3 で 80%** である。

### Cited Findings
- Medallion は 1988〜2018 年に手数料控除後で年率 39.1% を達成し、同期間に「月次でマイナスになったのは全体の約 5 分の 1 の月のみ」。2000 年 3 月には 1 日 9,000 万ドル、翌日 8,000 万ドルの損失を出した。手数料は管理報酬 5%、成功報酬 20%(2002 年に 44% へ) — [The Daily Economy (AIER): Jim Simons and His Quants](https://thedailyeconomy.org/article/jim-simons-and-his-quants/) ; [AIER](https://www.aier.org/?p=48400) ; [Bloomberg: The Unsolved Mystery of the Medallion Fund's Success](https://brp-prod-bcc.bloomberg.com/news/articles/2019-11-12/the-unsolved-mystery-of-the-medallion-fund-s-success) ; [Marcellus](https://marcellus.in/story/why-the-medallion-fund-is-the-greatest-money-making-machine-of-all-time/)
- Virtu Financial の IPO 届出書(S-1、2014 年)は「リアルタイムのリスク管理戦略と技術の結果、対象期間の合計 1,238 営業日のうち損失日は 1 日だけ」と記載。90% 超の日で 100 万ドル超の収益。2013 年の利益は 1 億 8,400 万ドル(前年比 +108%) — [Quartz: The HFT firm that's about to go public only had one day of trading losses in four years](https://qz.com/186209/the-high-frequency-trading-firm-thats-about-to-go-public-only-had-one-day-of-trading-losses-in-four-years) ; [Finance Magnates: Virtu Financial's IPO](https://financemagnates.com/institutional-forex/technology/virtu-financials-ipo-becomes-a-bright-spark-in-the-doom-gloom-affecting-the-industry)
- SG CTA Index は 2023 年 8 月までの 8 か月で黒字 4・赤字 4、SG Trend Index も 8 か月中 4 か月黒字 — [The Full FX: Early Results Point to CTA Struggle in August](https://thefullfx.com/early-results-point-to-cta-struggle-in-august/) ; 2022 年 10 月、SG Trend Index は 21 営業日中 9 日で下落 — [The Full FX: Trend Followers Eye Record Year](https://thefullfx.com/trend-followers-eye-record-year/) ; 2019 年 4 月は SG Trend Index 構成 10 プログラムすべてが黒字 — [Hedgeweek: CTAs continue positive run led by trend following strategies](https://hedgeweek.com/news/ctas-continue-positive-run-led-trend-following-strategies) ; 「CTA は直近 9 四半期中 8 四半期で黒字」 — [Hedgeweek market data](https://www.hedgeweek.com/content_channels/market-data/page/3/)
- シャープレシオの推定値は(原リターンが非正規でも)漸近的に正規分布に従い、√q による期間変換は IID 下でのみ正しい(Lo 2002) — [The Statistics of Sharpe Ratios](https://traders.studentorg.berkeley.edu/papers/The-Statistics-of-Sharpe-Ratios.pdf) ; 月次データによるシャープ推定精度の改善 — [SSRN 2959632](https://papers.ssrn.com/abstract=2959632) ; 確率的シャープレシオ(Bailey & López de Prado) — [QuantConnect: Probabilistic Sharpe Ratio](https://www.quantconnect.com/research/17112/probabilistic-sharpe-ratio/)

### 計算(本調査: 月次リターン ~ N(μ, σ)、歪度ゼロ、系列相関ゼロ)

| 年率 SR | 月次 SR | P(黒字月) | P(黒字四半期) | P(黒字年) | P(12/12 か月黒字) | 年間の期待赤字月数 | P(12 か月中 10 か月以上黒字) |
|---|---|---|---|---|---|---|---|
| 0.3 | 0.087 | 53.5% | 56.0% | 61.8% | 0.1% | 5.6 | 3.3% |
| 0.5 | 0.144 | 55.7% | 59.9% | 69.1% | 0.1% | 5.3 | 4.7% |
| 0.75 | 0.217 | 58.6% | 64.6% | 77.3% | 0.2% | 5.0 | 6.9% |
| 1.0 | 0.289 | 61.4% | 69.1% | 84.1% | 0.3% | 4.6 | 9.9% |
| 1.5 | 0.433 | 66.7% | 77.3% | 93.3% | 0.8% | 4.0 | 18.3% |
| 2.0 | 0.577 | 71.8% | 84.1% | 97.7% | 1.9% | 3.4 | 29.9% |
| 3.0 | 0.866 | 80.7% | 93.3% | 99.9% | 7.6% | 2.3 | 58.2% |
| 4.0 | 1.155 | 87.6% | 97.7% | ~100% | 20.4% | 1.5 | 82.1% |
| 5.0 | 1.443 | 92.6% | 99.4% | ~100% | 39.5% | 0.9 | 94.5% |

取引回数と 1 取引あたりエッジ → 月次 SR(SR_月 = (E/σ)_取引 × √N_月):

| 1 取引 SR | N=20 | N=60 | N=100 | N=200 | N=400 | N=1000 |
|---|---|---|---|---|---|---|
| 0.02 | 月 0.09 / 年 0.3 / 黒字月 53.6% | 0.15 / 0.5 / 56.2% | 0.20 / 0.7 / 57.9% | 0.28 / 1.0 / 61.1% | 0.40 / 1.4 / 65.5% | 0.63 / 2.2 / 73.6% |
| 0.05 | 0.22 / 0.8 / 58.8% | 0.39 / 1.3 / 65.1% | 0.50 / 1.7 / 69.1% | 0.71 / 2.4 / 76.0% | 1.00 / 3.5 / 84.1% | 1.58 / 5.5 / 94.3% |
| 0.10 | 0.45 / 1.5 / 67.3% | 0.77 / 2.7 / 78.1% | 1.00 / 3.5 / 84.1% | 1.41 / 4.9 / 92.1% | 2.00 / 6.9 / 97.7% | 3.16 / 11 / 99.9% |
| 0.20 | 0.89 / 3.1 / 81.4% | 1.55 / 5.4 / 93.9% | 2.00 / 6.9 / 97.7% | 2.83 / 9.8 / 99.8% | — | — |

勝率 p と ペイオフ b から 1 取引 SR: p=0.45,b=1.5 → E=+0.125R, σ=1.244R, SR=0.101(黒字月 72% には月 33 取引、84% には月 99 取引が必要)。p=0.55,b=1.0 → SR=0.101(同上)。p=0.40,b=2.0 → SR=0.136(72% に月 18 取引)。p=0.60,b=1.0 → SR=0.204(72% に月 8 取引)。p=0.35,b=2.0 → SR=0.035(72% に月 276 取引)。p=0.70,b=0.5 → SR=0.073(72% に月 64 取引)。

Virtu の逆算: 損失日 1/1,238 → 日次黒字率 99.92% → 日次 SR ≈ Φ⁻¹(0.9992) ≈ 3.1 → 年率換算 ≈ 50。これはマーケットメイク(1 日数万〜数十万回の両建て約定)固有の数値であり、方向性取引の参考値にはならない。Medallion の「赤字月 ≈ 1/5」は上表で年率 SR ≈ 3 に対応する。

### Inferences
- 1 取引あたり SR 0.10 はコスト控除後としてはかなり強いエッジ(勝率 55%・RR 1:1 相当)で、それでも黒字月 72% には月 33 回、84% には月 99 回の独立した取引が要る。1 時間足で月 20〜40 回しか機会がない単一戦略では、黒字月 60〜67% が現実的上限である。
- 取引回数 N を増やす経路(1 分足スキャルプ)はコスト(質問 7)で 1 取引 SR が 0.02〜0.05 に落ちやすく、N=400〜1,000 でようやく 65〜75%。結果として「15 分〜1 時間足 × 複数独立ロジック × 月 100〜200 回」が黒字月 70% 前後を狙う現実的な設計。
- 上表は歪度ゼロ前提。トレンドフォロー型(正の歪度、勝率 35〜45%)は同じ SR でも黒字月の割合が低く、平均回帰・スキャルプ型(負の歪度、勝率 60〜80%)は高めに出るが、まれな大損で年率 SR が削られる。黒字月の「見かけの割合」に最適化すると負の歪度を抱えやすい点に注意。

### Gaps
- SG Trend Index / SG CTA Index の長期(2000 年〜)黒字月比率、Winton・AHL・Transtrend・Dunn・FX 専業マネージャーの「% positive months」は、ファクトシート(Macquarie Winton Global Alpha Fund 報告書など)へのアクセスが遮断され取得できなかった。断片的な年のデータ(2023 年 8 か月中 4 か月黒字)のみ。
- Medallion の「赤字月 1/5」は Zuckerman の著書の二次引用であり、月次系列は非公開。
- Virtu の 2015 年以降の損失日数(10-K 開示)は未取得。

---

## 主要質問 4: 個人投資家の成績統計(ESMA、CFTC、日本 FFAJ、ブラジル、台湾、FXCM)

### Takeaway
口座データに基づく統計では、個人のレバレッジ取引口座の 60〜90% が損失で、四半期ごとの黒字口座比率は 20〜40%(米国 CFTC 開示)、長期にわたり一貫して黒字なのは 1% 前後(台湾デイトレ、ブラジル先物)。日本の FFAJ 調査(自己申告、2018 年)だけが「年間黒字 60.3%」と高いが、調査方法(経験者アンケート)の違いで口座データとは比較できない。

### Cited Findings
- ESMA: 各国当局(NCA)の分析では「個人 CFD 口座の 74〜89% が損失」、顧客 1 人あたり平均損失は 1,600〜29,000 ユーロ。2018 年 3 月 23 日に ESMA 監督委員会がバイナリーオプション禁止と CFD 制限を決定 — [ESMA: ESMA agrees to prohibit binary options and restrict CFDs to protect retail investors](https://www.esma.europa.eu/node/84933) ; [Mondo Visione 転載](https://mondovisione.com/media-and-resources/news/esma-agrees-to-prohibit-binary-options-and-restrict-cfds-to-protect-retail-inves-2018327/)
- 米国 CFTC 向け四半期開示(非裁量口座): 2011 年 Q1 は GFT が 39.90% で首位、Oanda 38.10%。初期(2010 年 Q3〜Q4)の各社黒字口座比率は 20〜51% で、外れ値を除く平均 25.5%。CFTC 登録業者全体で概ね 33% が黒字 — [Finance Magnates: US Forex brokers profitability report for Q1 2011](https://www.financemagnates.com/forex/brokers/us-forex-brokers-profitability-report-for-q1-2011/) ; [BabyPips: The 2011 Q1 Profitability Report](https://www.babypips.com/news/the_2011_q1_profitability_repo) ; [BabyPips: How did your broker do in 4Q 2010](https://www.babypips.com/news/how-did-your-broker-do-in-4q-2010)
- FOREX.com の CFTC 向け開示: 2018 年 6 月末四半期、アクティブな非裁量口座 34,000 のうち 38% が黒字、62% が赤字(スニペット準拠) — [DayTrading.com: Which Countries Have the Most Profitable Retail Traders?](https://www.daytrading.com/safety/which-countries-have-the-most-profitable-retail-traders)
- 「個人投資家の 75% は実質的に『資本の寄付者』」とする業界分析 — [Finance Magnates: Analysis: 75% of Retail Investors Are Essentially 'Capital Donors'](https://www.financemagnates.com/forex/analysis-75-of-retail-investors-are-essentially-capital-donors/)
- 日本・金融先物取引業協会「外国為替証拠金取引の取引顧客における金融リテラシーに関する実態調査」(2018 年 2 月 23 日〜3 月 1 日、全国 20〜70 代の FX 取引経験者 1,000 人): 年間で利益を出している人の割合 60.3%(二次サイトのスニペット準拠) — [FFAJ プレスリリース PDF](https://www.ffaj.or.jp/wp-content/uploads/2018/09/20180927_PR.pdf) ; [FFAJ 調査結果 PDF](https://www.ffaj.or.jp/wp-content/uploads/2018/09/20180927_result.pdf) ; [FFAJ 概要 PDF](https://www.ffaj.or.jp/wp-content/uploads/2018/09/20180927_summary.pdf) ; [二次引用: crexgroup](https://crexgroup.com/ja/fx/tax-risk/fx-success-rate-myth/) ; 2022 年版の技術的要約 — [FFAJ 20220920 technical summary](https://www.ffaj.or.jp/wp-content/uploads/2022/09/20220920-technical_summary.pdf)
- FFAJ と東京外国為替市場委員会は毎年 4 月基準で「店頭 FX の実態調査」を実施 — [FFAJ: Results for the Actual Conditions Survey of OTC Retail FX Margin Trading 2017](https://www.ffaj.or.jp/wp-content/uploads/2017/08/Results_for_the_Actual_Conditions_Survey_of_OTC_Retail_FXM2017fin_jp.pdf)
- ブラジル(Chague, De-Losso, Giovannetti "Day Trading for a Living?" 2020): 2013〜2015 年にブラジル株価指数先物(世界第 3 位の出来高)でデイトレードを始めた全個人を追跡。300 日超継続した個人の 97% が損失、ブラジルの最低賃金を上回ったのは 1.1%、銀行窓口係の初任給を上回ったのは 0.5%(いずれも高リスクで)。学習効果の証拠なし — [SSRN 3423101](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3423101) ; [FGV repositório](https://repositorio.fgv.br/items/e87d04fc-4b4c-4a56-ab5a-1c84c1afde1b/full) ; [Quantpedia 解説](https://quantpedia.com/?p=4771)
- 台湾(Barber, Lee, Liu, Odean): 1992〜2006 年の全デイトレーダー。80% 超が損失、手数料控除後に一貫して予測可能な利益を出すのは 1% 未満(約 36 万人中約 1,000 人)。デイトレの集計リターンは 15 年すべてで負、手数料控除後 −23.9bp/日。デイトレは出来高の 20% 超、個人の 1% がデイトレ出来高の 50% を占める — [CXO Advisory: Do Day Traders Make Money?](https://www.cxoadvisory.com/individual-investing/do-day-traders-make-money/) ; [Berkeley 論文 PDF: Day Trading Skill](https://faculty.haas.berkeley.edu/odean/papers/day%20traders/Day%20Trading%20Skill%20110523.pdf) ; [Do Day Traders Rationally Learn About Their Ability? (PDF)](https://trevcan.duckdns.org/files/trevor/papers/emintham-Papers/Barber%2CLee%2CLiu%2COdean-%20Do%20Day%20Traders%20Rationally%20Learn%20About%20Their%20Ability.pdf)
- FXCM "Traits of Successful Traders": RR 1 は「$1 を賭けて $1 を狙う」、RR 2 は「$1 を賭けて $2」。勝率 30% の 2 人の例で、RR 3:1 は +$2、RR 1:1 は −$4。「優れた RR は勝率への圧力を和らげる」。「RR と勝率の間に明確な関係があり、両方を考慮して正の期待値に調整する」 — [FXCM: Best Practices of Successful Traders](https://www.fxcm.com/uk/education/traits-successful-traders/) ; [DailyFX 2014: Risk a Little to Make a Lot](https://www.dailyfx.com/forex/education/trading_tips/post_of_the_day/2014/02/11/risk_a_little_to_make_a_lot.html) ; [DailyFX 2011: What are the traits of successful forex traders](https://www.dailyfx.com/forex/publisher_blog/2011/12/26/what_are_the_traits_of_successful_forex_traders.html) ; [FXStreet 2014: Risk vs reward](https://www.fxstreet.com/amp/education/risk-vs-reward-ignore-what-youve-heard-201407290000)

### Inferences
- 米国の四半期黒字口座比率 30〜38% を Φ(S/2)(四半期 SR)に当てはめると、個人口座の中央値は年率 SR ≈ −0.6〜−0.3、すなわちコスト控除後の期待値が負である。FFAJ の 60.3% は自己申告・経験者母集団(生存者バイアス、未実現スワップ益の計上、2017 年の円相場が比較的穏やかだったこと)で説明でき、国内の口座ベース統計がない以上、海外の口座データ(25〜40%)を保守的な基準とすべき。
- 「一貫して黒字」(台湾 <1%、ブラジル 0.5〜1.1%)は「月次黒字の継続」よりさらに緩い基準(年単位)での数字である。したがって個人が「ほぼ毎月黒字」になる事前確率は 1% を大きく下回ると見るのが妥当。
- FXCM の研究が示す実務的含意は「勝率ではなく RR ≥ 1 と損切り遵守が黒字口座の分岐点」であり、システム設計では「平均利益 pips ≥ 平均損失 pips」を検証ゲートに入れる価値が高い。

### Gaps
- FXCM 研究の具体値(分析取引数、通貨ペア別勝率、RR≥1 を守った口座の黒字率 vs 守らなかった口座、時間帯別の成績=アジア時間のレンジ相場で個人の成績が良いという知見)は、fxcm.com / dailyfx.com への取得が遮断され、本セッションでは数値を検証できなかった。上記 URL の該当ページで確認が必要。
- CFTC 開示の 2019 年以降の四半期データ、日本の口座ベースの損益分布(金融庁「店頭 FX 業者の決済リスクへの対応に関する有識者検討会」資料 [fsa.go.jp 20180213-1/03.pdf](https://www.fsa.go.jp/news/29/singi/20180213-1/03.pdf), [20180329-1/01.pdf](https://www.fsa.go.jp/news/29/singi/20180329-1/01.pdf) に含まれる可能性)は未取得。
- 「90/90/90 ルール(90% が 90 日で 90% を失う)」は出典のない業界俗説で、本調査でも一次資料は見つからなかった — [BabyPips フォーラムでの議論](https://forums.babypips.com/t/90-90-90-principle/169605)

---

## 主要質問 5: プロップファーム(FTMO 等)のルールと合格率

### Takeaway
FTMO 型(日次最大損失 5%、最大損失 10%、目標 10%/5%)の選抜でも合格率は 5〜14%、合格者のうち実際に出金に至るのは半分(全体の約 7%)、平均出金額は口座の約 4%。失敗の最大要因は日次損失上限であり、「選抜されたトレーダーでさえ一貫性を保てない」ことを示す最良の実務データである。

### Cited Findings
- FTMO は標準チャレンジの合格率を約 9〜10% と公表、コミュニティ調査では両フェーズ通算 10〜12%。FTMO は段階別の公式合格率を公表していない — [Track360: Prop Trading Industry Statistics 2026](https://track360.io/blog/prop-trading-industry-statistics-2026) ; [Backtrex: Prop firm challenge success rate](https://backtrex.com/en/blog/prop-firm-challenge-success-rate)
- 「平均合格率は 14% だが、そのうち出金に至るのは半分、全体の 7%。出金者の平均出金額は口座価値の 4%」 — [TradeInformer: What FTMO tells us about prop firm finances](https://www.tradeinformer.com/prop-weekly/what-ftmo-tells-us-about-prop-firm-finances)
- 業界全体では購入チャレンジの 5〜14% がファンデッド口座に到達し、全購入者の約 7% のみが出金を受ける。FTMO は 2015 年以降 6.5 億ドル超を出金し、登録顧客は 450 万 — [Track360](https://track360.io/blog/prop-trading-industry-statistics-2026)
- The Funded Trader 報告: 「チャレンジを通過するのは 20 人に 1 人」 — [Finance Magnates: Only 1 in 20 Traders Pass Prop Firm Challenges](https://www.financemagnates.com/forex/only-1-in-20-traders-pass-prop-firm-challenges-reports-the-funded-trader/)
- シミュレーション失敗の約 60% が日次損失上限の抵触 — [Backtrex](https://backtrex.com/en/blog/prop-firm-challenge-success-rate) ; 合格率ガイド — [Traders Second Brain: Prop firm pass rate](https://traderssecondbrain.com/guides/prop-firm-pass-rate) ; [Hackernoon: The real reason traders blow prop firm evaluations](https://hackernoon.com/the-real-reason-traders-blow-prop-firm-evaluations) ; FTMO 100k 2-step のシミュレーションデータセット — [LuxAlgo prop-firm-sim](https://www.luxalgo.com/mcp/prop-firm-sim/ftmo-100k-2step/)
- Carver 流のボラ目標 15%・SR 1.5 でチャレンジ成功率 ≈ 52%(第三者シミュレータ、スニペット準拠) — [moontowermeta simulator](https://trading-sim.moontowermeta.com/)

### Inferences
- 合格率 10% という数字は「30 日前後で +10% を、−5%/日・−10% 累計を踏まずに達成する」確率であり、SR 1.5 程度の良いシステムでも 50% 前後にしかならない(上記シミュレータ)。つまり「毎月 +X% を損失上限内で達成し続ける」ことは、SR 1.5 でも毎月コイン投げに近い。
- FTMO 日次上限 5% は所有者の 2% より緩い。所有者が 2% を選ぶなら、年率ボラは 5〜8% 以下(日次 σ 0.3〜0.5%)に抑えないと誤発動が増える(2% = 4〜6σ なら年 1 回未満)。

### Gaps
- FTMO 公式の統計ページ(平均出金額、成功者の平均勝率・RR)は ftmo.com が取得できず未確認。上記はすべて二次分析。

---

## 主要質問 6: 日本の個人 FX のコスト・制度条件(GMOクリック証券 FXネオ、GMOコイン、外為どっとコム、SBI FXトレード、OANDA 証券)

### Takeaway
GMOクリック証券 FXネオの USD/JPY は 0.2 銭原則固定だが、適用は 9:00〜翌 3:00(27:00)JST のみで、早朝(3:00〜9:00)、指標発表時、相場急変時は例外。最低取引単位 1,000 通貨。GMOクリック証券が個人向け FX API を公開している証拠は見つからず、本リポジトリが使う「GMO API」は GMOコインの外国為替FX API(REST/WS、MARKET/LIMIT/STOP/OCO/IFD/IFDOCO、speedOrder)である。レバレッジは法令上 25 倍(証拠金率 4%)、税は申告分離 20.315%、損失 3 年繰越。OANDA 証券は REST API を提供するが口座条件(プロコース・残高 25 万円以上など)がある。

### Cited Findings
- GMOクリック証券 FXネオ: 米ドル/円スプレッド 0.2 銭、原則固定(9〜27 時・例外あり)、最低取引単位 1,000 通貨(2026 年の比較記事、スニペット準拠) — [ザイFX! 記事 492782](https://zai.diamond.jp/articles/-/492782) ; [ザイFX! 記事 496000](https://zai.diamond.jp/articles/-/496000)
- 「AM9:00〜翌 AM3:00 の時間帯にて業界最狭水準の原則固定スプレッド」「スプレッドは相場急変時、指標発表時等に拡大する場合があります」「スプレッドの拡大は市場急変時や流動性の低い時間帯に起こる自然な現象でどの FX 会社でも発生」 — [soico.jp: GMOクリック証券 FX ニュース](https://www.soico.jp/no1/news/fx/27261) ; [FXナビ](https://www.fxnav.net/?p=91341)
- スプレッド縮小の沿革: 原則 1 銭固定制(2009〜10 年頃) → 2012 年に米ドル/円 0.4 銭(2011 年 12 月に続く縮小) → 2023 年 12 月にも縮小告知 — [value-press: FXスプレッド変動幅を原則「1銭」固定制へ](https://www.value-press.com/pressrelease/42490) ; [value-press: 米ドル/円 0.4銭に](https://www.value-press.com/pressrelease/91782) ; [GMOクリック証券 2023-12-28 お知らせ PDF](https://www.click-sec.com/images_news/2023-12-28-01.pdf)
- GMOクリック証券の API: 2020 年頃の非公式ライブラリ `catsgmo` は「株価指数バイナリーオプションのデモ取引」のスクレイピングのみをサポートし、FX 取引 API ではない — [PyPI: catsgmo](https://pypi.org/project/catsgmo) ; 検索では FX 向け公開 API の存在を示す一次情報は見つからなかった。
- GMOコイン「外国為替FX」API: 非公式 SDK `gmo-fx`(v0.15.0)が参照する公式ドキュメントは https://api.coin.z.com/fxdocs/ 。SDK 実装から判明したエンドポイント: public `ticker`, `klines`(1 分足 `M1` 等、BID/ASK 指定), `symbols`(`minOpenOrderSize`, `maxOrderSize`, `sizeStep`, `tickSize` を返す), `status`; private `account/assets`, `orders`, `activeOrders`, `executions`, `latestExecutions`, `openPositions`, `positionSummary`, `speedOrder`(成行即時), `order`, `ifdOrder`, `ifoOrder`(IFDOCO), `changeOrder`, `changeOcoOrder`, `changeIfdOrder`, `changeIfoOrder`, `cancelOrders`, `cancelBulkOrder`, `closeOrder`。執行種別 `MARKET` / `LIMIT` / `STOP` / `OCO`。シンボルは USD_JPY, EUR_JPY, GBP_JPY, AUD_JPY, NZD_JPY, CAD_JPY, CHF_JPY, TRY_JPY, ZAR_JPY, MXN_JPY, EUR_USD, GBP_USD, AUD_USD, NZD_USD ほか — [PyPI: gmo-fx](https://pypi.org/project/gmo-fx/) ; [GitHub: RikitoNoto/gmo-fx-py](https://github.com/RikitoNoto/gmo-fx-py)(本調査で sdist を展開して確認)
- GMOコインは 2025 年 7 月 14 日から投資助言サービス「GPT-Trade」との API 連携を開始(「これまでの外国為替FXとの連携に加え暗号資産でも」) — [GMO インターネットグループ ニュース PDF](https://www.gmo.jp/pdf/news/gmo_news_8763.pdf)
- OANDA 証券(日本)の REST API: 「REST API を使うにはゴールド会員以上、NY サーバーの口座残高 25 万円以上、プロコースであること」が必要(スニペット準拠、2026 年時点の条件は要確認)。API トークンは「口座管理」→「API アクセスの管理」から発行。API 契約面は日本語サポート、技術面は英語メール(api@oanda.com) — [OANDA: FX API](https://www.oanda.jp/fxproduct/api) ; [OANDA Lab: API トークン発行条件](https://www.oanda.jp/lab-education/?p=27217) ; [OANDA Lab: Python で REST API](https://www.oanda.jp/lab-education/api/usage2/python/) ; [FX API でできること](https://www.oanda.jp/index.php/platform/api)
- レバレッジ規制: 個人の店頭 FX は 2010 年 8 月から証拠金 2% 以上(50 倍以下)、2011 年 8 月から 4% 以上(25 倍以下)。目的は顧客保護・業者のリスク管理・過当投機防止。2018 年初頭に金融庁が 10 倍への引き下げを検討との報道が出たが実施されず、現在も最大 25 倍 — [金融庁 有識者検討会 資料 (2018-02-13)](https://www.fsa.go.jp/news/29/singi/20180213-1/03.pdf) ; [金融庁 有識者検討会 資料 (2018-03-29)](https://www.fsa.go.jp/news/29/singi/20180329-1/01.pdf) ; [大和総研: 証券規制レポート](https://www.dir.co.jp/report/research/law-research/securities/09081901securities.html) ; [All About: FX レバレッジ規制](https://allabout.co.jp/gm/gc/325168/) ; [マネーフォワード](https://media.moneyforward.com/articles/1188) ; [幻冬舎ゴールドオンライン](https://gentosha-go.com/articles/-/15539) ; [J-CAST 2009: FX のレバレッジ規制](https://www.j-cast.com/2009/05/29042217.html)
- 税制: FX の利益は「先物取引に係る雑所得等」、申告分離課税、税率 20.315%(所得税 15% + 復興特別所得税 0.315%(2013〜2037 年)+ 住民税 5%)。FX・先物・オプション・CFD・バイナリーオプションの損益は通算可能。年間損失は翌年以降 3 年間繰越控除(毎年の確定申告が条件) — [大和証券: FX の税金](https://www.daiwa.jp/products/fx/guide/tax/) ; [外為どっとコム: FX の税金](https://www.gaitame.com/beginner/fx/tax.html) ; [PayPay 銀行 FX: 税金](https://www.paypay-bank.co.jp/fx/incometax.html) ; [松井証券 FAQ](https://support.matsui.co.jp/faq/show/1902)
- 本リポジトリの既定値: `round_trip_slippage_pips: 0.15`、`max_spread_pips: 0.6`、`slippage_bps: 1.5`、`stop_loss_pips: 1.2`(スキャルプ)、`required_stress_spread_multiplier: 1.5`、ロールオーバー 05:55〜06:10 JST ブラックアウト(内部資料) — `src/fxautotrade_lab/config/models.py`, `README.md`

### 計算(本調査: スワップの理論値)
スワップ/日 ≈ ノーショナル × 金利差 / 365(業者はこれに売買スプレッドを乗せるため、受取はこれより小さく、支払いは大きい)。1 万通貨(= 150 万円)あたり: 金利差 2% → 82 円/日、3% → 123 円/日、3.5% → 144 円/日、4% → 164 円/日、5% → 205 円/日。300,000 JPY(2,000 USD)あたり: 16 / 25 / 29 / 33 / 41 円/日 = 0.8 / 1.2 / 1.4 / 1.6 / 2.0 pips/日。水曜日の 3 日分付与(NY クローズ跨ぎ)では 3 倍。USD/JPY の売りを 1 晩持ち越すと 1〜2 pips、週末跨ぎで 3〜6 pips のコストに相当し、1 時間足スイングで数 pips の利幅を狙う場合は無視できない。

### Inferences
- 原則固定 0.2 銭の時間帯(9:00〜27:00 JST)以外、すなわち 3:00〜9:00 JST(NY 午後〜ウェリントン/シドニー早朝、ロールオーバー 6:00 または 7:00 前後を含む)は変動スプレッドであり、本システムは 1 分〜15 分足戦略ではこの時間帯を全面ブラックアウトにし、1 時間足戦略でも新規建てを避けるのが妥当。
- 「GMOクリック証券で自動売買」は API 不在のため実装経路がない(RPA/画面操作は規約リスク)。実売買への移行先候補は GMOコイン外国為替FX API または OANDA 証券 REST API(残高 25 万円以上・プロコース等)に絞られる。本リポジトリは既に GMOコイン public API(ticker/klines)を使っており、private API(speedOrder/closeOrder/IFDOCO)が将来の実売買経路として自然。
- 国内業者の「スリッページ許容幅」設定は成行で指定できる(FXネオ等)が、許容幅を狭くすると約定拒否が増える。1 分足戦略の検証ではリポジトリ既定の 0.15 pips 往復スリッページは平常時の楽観値であり、ストレス 1.5〜2.0 倍(0.3〜0.5 pips)を本番の期待値とすべき(質問 7 の表参照)。

### Gaps
- GMOクリック証券の 2024〜2026 年の USD/JPY スワップ実額(買い/売り、1 万通貨/日)は取得できなかった(click-sec.com、zai、fxnav が遮断)。比較表の所在: [FXナビ スワップ比較](https://www.fxnav.net/swap/) 、[ザイFX! スワップ記事](https://zai.diamond.jp/articles/-/496447)。上記の理論値で代替。
- GMOクリック証券の「スプレッド提示率」「約定率」「スリッページ発生率」の公表データ、早朝・月曜オープン・指標時の拡大幅(銭)の実測値は本セッションで見つからなかった。FXネオのロスカット水準(証拠金維持率)、1 注文あたり数量上限も未確認。
- GMOコイン外国為替FX API のレート制限(秒間リクエスト数)、1 注文の数量上限、API 利用資格、同社のスプレッド/スワップ条件(SDK が参照する https://coin.z.com/jp/corp/product/info/fx/?=rule )は取得できなかった。
- 外為どっとコム、SBI FXトレードの API 提供有無・スプレッド条件は未確認(検索結果に公開 API の情報なし)。

---

## 主要質問 7: 最低限必要なエッジ(コストを pips に換算、時間足別、勝率 × ペイオフの損益分岐)

### Takeaway
往復コスト c = スプレッド 0.2 + スリッページ 0.1〜0.3 = 0.3〜0.5 pips(平常時)、例外時 1〜2 pips 超。1 分足スキャルプ(利幅 2〜3 pips)はコストが総値幅の 15〜50% を占め、勝率 57〜75% が損益分岐。15 分足ブレイクアウト(12/8 pips)は 41〜50%、1 時間足(30/20 pips)は 41〜44%。1 回のスプレッド拡大(例: 2 pips)で 1 分足戦略の 5〜10 回分の期待値が消える。

### Cited Findings
- GMOクリック証券の原則固定 0.2 銭と例外(時間帯・指標・急変)— 質問 6 の出典 [ザイFX!](https://zai.diamond.jp/articles/-/492782)、[soico.jp](https://www.soico.jp/no1/news/fx/27261)
- 本リポジトリ既定のスリッページ 0.15 pips(往復)とスプレッド上限 0.6 pips、ストレス倍率 1.5〜2.0(内部資料) — `src/fxautotrade_lab/config/models.py`
- 台湾デイトレの集計リターンは手数料控除後 −23.9bp/日で 15 年すべて負(コストが個人の集計成績を支配する実証) — [CXO Advisory](https://www.cxoadvisory.com/individual-investing/do-day-traders-make-money/)
- FXCM: RR と勝率の組合せで期待値を決めよという枠組み — [FXCM: Best Practices of Successful Traders](https://www.fxcm.com/uk/education/traits-successful-traders/)

### 計算(本調査: 利幅 W、損切り L、往復コスト c。損益分岐勝率 p* = (L + c)/(W + L)、純期待値 = pW − (1−p)L − c [pips/取引])

| スタイル | W/L (pips) | c=0.3: p*, 純期待値@p=0.50/0.55/0.60 | c=0.5 | c=1.0 | c=2.0 | コスト ÷ 平均値幅 (c=0.3/0.5/1.0/2.0) |
|---|---|---|---|---|---|---|
| 1 分スキャルプ | 2/2 | 57.5%, −0.30/−0.10/+0.10 | 62.5%, −0.50/−0.30/−0.10 | 75.0%, −1.00/−0.80/−0.60 | 100%, 全て負 | 15% / 25% / 50% / 100% |
| 1 分スキャルプ | 3/2 | 46.0%, +0.20/+0.45/+0.70 | 50.0%, 0.00/+0.25/+0.50 | 60.0%, −0.50/−0.25/0.00 | 80.0%, −1.50/−1.25/−1.00 | 12% / 20% / 40% / 80% |
| 5 分 | 5/4 | 47.8%, +0.20/+0.65/+1.10 | 50.0%, 0.00/+0.45/+0.90 | 55.6%, −0.50/−0.05/+0.40 | 66.7%, −1.50/−1.05/−0.60 | 6.7% / 11% / 22% / 44% |
| 15 分ブレイクアウト | 12/8 | 41.5%, +1.70/+2.70/+3.70 | 42.5%, +1.50/+2.50/+3.50 | 45.0%, +1.00/+2.00/+3.00 | 50.0%, 0.00/+1.00/+2.00 | 3% / 5% / 10% / 20% |
| 15 分ブレイクアウト | 15/10 | 41.2%, +2.20/+3.45/+4.70 | 42.0%, +2.00/+3.25/+4.50 | 44.0%, +1.50/+2.75/+4.00 | 48.0%, +0.50/+1.75/+3.00 | 2.4% / 4% / 8% / 16% |
| 1 時間スイング | 30/20 | 40.6%, +4.70/+7.20/+9.70 | 41.0%, +4.50/+7.00/+9.50 | 42.0%, +4.00/+6.50/+9.00 | 44.0%, +3.00/+5.50/+8.00 | 1.2% / 2% / 4% / 8% |
| 1 時間スイング | 45/25 | 36.1%, +9.70/+13.2/+16.7 | 36.4%, +9.50/+13.0/+16.5 | 37.1%, +9.00/+12.5/+16.0 | 38.6%, +8.00/+11.5/+15.0 | 0.9% / 1.4% / 2.9% / 5.7% |
| 4 時間〜日足 | 90/50 | 35.9%, +19.7/+26.7/+33.7 | 36.1% | 36.4% | 37.1% | 0.4% / 0.7% / 1.4% / 2.9% |

所有者の金額換算(1 pip = 20 JPY @300k): 1 分足で純期待値 +0.2 pips × 月 400 回 = 80 pips = 1,600 JPY/月。15 分足で +2.0 pips × 月 100 回 = 200 pips = 4,000 JPY/月。1 時間足で +5 pips × 月 30 回 = 150 pips = 3,000 JPY/月。すなわち固定 300k ノーショナルでは月次 P&L の絶対額は数千円規模で、評価は pips と R 倍数で行うべき。

### Inferences
- 1 分足戦略は「勝率 ≥ 60% かつ RR ≥ 1」または「勝率 ≥ 50% かつ RR ≥ 1.5」をコスト 0.5 pips 前提で満たさないと黒字にならず、スプレッド例外時間帯の 1 回の 2 pips 拡大は正常時 5〜10 回分の期待値に相当する。1 分足を採用するなら(a) 9:00〜27:00 JST のみ、(b) 指標前後 ±5〜10 分と月曜 7:00〜9:00 を除外、(c) スプレッド > 0.3〜0.4 銭で新規停止、を必須ゲートにする。
- 15 分〜1 時間足ではコストは総値幅の 1〜10% で、勝率 42〜45% の典型的ブレイクアウト戦略でも 1 取引 +1〜+7 pips の純期待値が残る。月次黒字率を上げる主戦場はここで、問題はコストより「月間取引回数(N)の少なさ」(質問 3)。
- リポジトリの検証ゲート `min_validation_profit_factor: 1.05` は、c=0.5 pips 前提で 1 分足の PF 1.05 がストレス 2.0 倍(c=1.0)で 1.0 を割ることを意味する。本番採用の PF 閾値はスプレッド 2 倍ストレス下で ≥ 1.1〜1.2 が妥当。

### Gaps
- GMOクリック証券/GMOコインの実測スリッページ分布(時間帯別・成行/逆指値別)は未取得。逆指値(STOP)約定のスリッページは成行より大きく、ATR ストップの実効損失は L + 0.2〜1.0 pips とみなす必要があるが、実測値は自前で収集するしかない。

---

## 主要質問 8: 月次黒字率を上げるレバー(戦略/通貨/時間足の分散、取引回数、ボラ・スケーリング、イベント回避)の証拠

### Takeaway
月次黒字率はシャープレシオの単調関数なので、レバーは「SR を上げる」ことに尽きる。数学的に最も効くのは低相関ストリームの追加(相関 0.3 の 3 本で SR ×1.37、5 本で ×1.51)と、実エッジを保ったまま独立取引数を増やすこと。ボラ・スケーリングは SR を改善し(Carver のバックテスト)、歪度をやや悪化させる。イベント日の回避は「スプレッド例外」と「ファットテール」を同時に除くため、コスト面と分布面の両方で正当化できる。

### Cited Findings
- Carver: 「ボラ目標法のバックテストでは、小さな歪度の不利を大きく上回るシャープレシオの優位により、より高い最大 CAGR が得られる」(スニペット準拠) — [Two Quants Substack コメント欄](https://twoquants.substack.com/p/juicy/comments) ; [CXO Advisory: A Few Notes on Systematic Trading](https://www.cxoadvisory.com/big-ideas/a-few-notes-on-systematic-trading)
- Kaminski & Lo: ストップ系ルールはモメンタム/レジーム切替下で期待リターン上昇とボラ低下を同時にもたらす(1950〜2004、月次) — [MIT DSpace](https://dspace.mit.edu/handle/1721.1/114876)
- 2022 年 10 月に SG Trend Index は 21 営業日中 9 日で下落(日次勝率 57% の月でも月次は黒字になり得る=回数と分散の例) — [The Full FX](https://thefullfx.com/trend-followers-eye-record-year/)
- 2019 年 4 月の SG Trend Index は構成 10 プログラム全部が黒字、6 月は SG CTA Index 構成プログラムの 90% が黒字(トレンド系同士の高相関=分散効果の限界) — [Hedgeweek](https://hedgeweek.com/news/ctas-continue-positive-run-led-trend-following-strategies) ; [Hedgeweek market data](https://www.hedgeweek.com/content_channels/market-data/page/3/)
- Virtu は多数の市場・資産クラスにまたがるマーケットメイクで損失日 1/1,238 を達成(極端な回数と分散の例) — [Quartz](https://qz.com/186209/the-high-frequency-trading-firm-thats-about-to-go-public-only-had-one-day-of-trading-losses-in-four-years)
- 本リポジトリは `max_jpy_cross_positions: 1`、`max_positions: 4`、スプレッド百分位ゲート(80 パーセンタイル超で見送り)、`max_spread_to_atr_ratio: 0.15`、ロールオーバー・ブラックアウトを実装済み(内部資料) — `src/fxautotrade_lab/config/models.py`

### 計算(本調査: N 本の等 SR・等相関 ρ ストリームの合成 SR = SR × √N / √(1 + (N−1)ρ))

| N | ρ=0 | ρ=0.3 | ρ=0.5 | ρ=0.7 |
|---|---|---|---|---|
| 2 | ×1.41 | ×1.24 | ×1.15 | ×1.08 |
| 3 | ×1.73 | ×1.37 | ×1.22 | ×1.12 |
| 5 | ×2.24 | ×1.51 | ×1.29 | ×1.15 |
| 10 | ×3.16 | ×1.64 | ×1.35 | ×1.18 |

例: 単一戦略 SR 1.0(黒字月 61.4%)に、同じ USD/JPY 上で相関 0.5 の戦略を 2 本足す → SR 1.22(63.6%)。相関 0.3 の戦略を 4 本足す → SR 1.51(66.9%)。相関 0.3 で SR 2(71.8%)に達するには単体 SR 1.32 × 5 本、または SR 1.0 × 10 本超が必要。

### Inferences
- 同一通貨ペア・隣接時間足の戦略は P&L 相関が 0.5〜0.7 になりやすく、分散倍率は 1.1〜1.2 に留まる。黒字月 70% 台を目指すなら、(a) 平均回帰(アジア時間レンジ)とブレイクアウト(ロンドン/NY 時間)のように符号の異なるエッジを時間帯で分ける、(b) 将来的に EUR/USD や GBP/JPY 等の低相関ストリームを加える、(c) 時間足を 15 分と 1 時間(+日足フィルター)に分ける、の順で相関を下げる設計が必要。USD/JPY 単一・1〜60 分足のままなら、現実的な到達点は SR 1〜1.5(黒字月 61〜67%)。
- 「小さなエッジ × 多数回」は理論上 SR を √N で伸ばすが、1 分足ではコストが 1 取引 SR を 0.02〜0.05 に押し下げ(質問 7)、必要回数(月 400〜1,000 回)で執行リスク(スリッページ、約定拒否、API レート制限)に当たる。「少数の大きな取引」は歪度が正で黒字月率は低い。中間の 15 分足 × 月 100〜200 回が黒字月率の観点で最も効率的。
- イベント回避(米雇用統計、FOMC、日銀会合、CPI、月曜早朝、年末年始、NY クローズ前後)は(1) スプレッド例外時間の除外によるコスト削減、(2) 日次損失上限の誤発動を招くファットテールの除去、(3) ATR ストップの実効損失(ギャップ)縮小、の 3 経路で黒字月率を押し上げる。ESMA の損失統計とプロップファームの「失敗の 60% は日次上限」は、テール日の寄与が大きいことを間接的に支持する。
- ボラ・スケーリングは月次 P&L の分散を一定化し(高ボラ月の大損を抑える)、黒字月率を SR 改善分だけ上げる。固定ノーショナル + ATR ストップは逆に高ボラ期にリスク額が膨らむため、月次黒字率の観点では「ATR で数量を割る」方式へ変更する価値がある。

### Gaps
- 「個人は(ボラの低い)アジア時間のレンジ相場で成績が良い」とされる FXCM/DailyFX の時間帯分析は、本セッションでは原典を取得できず数値を確認できなかった(質問 4 参照)。
- USD/JPY 1 分〜1 時間足の戦略間相関、イベント日のスプレッド拡大幅・ギャップの実測分布は、公開統計がなく自前データで推定する必要がある。
- 2019 年 1 月 3 日のフラッシュクラッシュ等、国内業者のスプレッド・約定状況に関する当局資料(金融庁有識者検討会)は URL のみ把握し内容未確認。

---

## 総括(率直な結論)

- 「毎月黒字(100%)」は達成可能な目標ではない。歪度ゼロ・独立の理想条件でも、年率 SR 3(Medallion 級、黒字月 ≈ 80%)で 12 か月連続黒字の確率は 7.6%、SR 4 で 20%、SR 5 で 40%。方向性のある個人 FX システムで SR 3 以上をコスト控除後に継続した公開事例は見つからなかった。
- 達成可能な水準: USD/JPY 単一・15 分〜1 時間足・月 30〜100 回で、コスト控除後 SR 0.5〜1.5 なら黒字月 56〜67%、年間赤字月 4〜5 か月。複数の低相関ロジックと月 100〜200 回で SR 1.5〜2 に届けば 67〜72%、赤字月 3〜4 か月。黒字月 80% 以上は SR 3 相当で、個人の方向性取引では非現実的。
- 月次ロックイン(+X% で当月停止)は黒字月率を約 10pp 上げるが月次期待値を 20〜45% 削る。黒字月率を KPI にするなら選択肢だが、年間収益は減る。
- 固定 300,000 JPY ノーショナルでは、日次損失上限 100,000 JPY(5,000 pips 相当)と `max_consecutive_losses: 5` のうち、前者は発動せず後者は偶然で頻発する。上限は R 倍数(例: 日次 −6R〜−10R、連敗 k は p と N から 5% 分位で決定)で再定義すべき。ノーショナルをボラ目標(年率 5〜10% = 250 万〜625 万 JPY)に上げる場合は 2% 日次上限が意味を持つ。
- 実売買経路は GMOクリック証券ではなく GMOコイン外国為替FX API(本リポジトリの既存導線)または OANDA 証券 REST API。原則固定スプレッドの時間帯(9:00〜27:00 JST)以外と指標前後を除外し、スプレッド 2 倍ストレス下で PF ≥ 1.1〜1.2 を採用ゲートとするのが、コスト構造から導かれる最低条件である。
