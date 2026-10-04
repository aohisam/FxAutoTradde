# 学術・プロクオンツ研究に見る体系的FX戦略の実績(USD/JPY・主要通貨、2015–2026年のアウトオブサンプル重視)

> 調査日: 2026-10-04。調査環境の制約: 本セッションでは WebFetch が全ホストで遮断され、WebSearch も途中で予算上限に達したため、一次論文 PDF の表を直接読めなかった箇所がある。数値は検索結果に表示された一次資料/定評ある二次要約(CXO Advisory、Alpha Architect、Quantpedia、Top Traders Unplugged 等)から転記し、二次要約由来のものはその旨を明記した。ベンダー(運用会社・ブローカー)資料は「ベンダー資料」と表示する。論文タイトルは原語のまま。
>
> 全体を貫く結論: 学術的に再現されているFXのエッジ(トレンド/TSMOM、キャリー、バリュー、分散・ボラ調整)は、ネットの年率シャープ比でおおむね 0.3–1.0 の世界であり、正規近似 P(月次>0) ≈ Φ(SR/√12) により月次勝率は 53–61% にしかならない。「毎月黒字」(勝率 95%)には年率シャープ比 5.7 が必要で、これは分散した最良のCTA/FX専業ファンドの実績(0.3–1.0)の5–10倍である。単一通貨ペア・日中足の個人システムがこの水準に到達した査読済み証拠は見つからなかった。

---

## KQ1. トレンド/TSMOM(時系列モメンタム)— ルックバック、シャープ比、2018–2026年の実績、USD/JPY 単独の可否、月次勝率

### Takeaway
分散ポートフォリオのTSMOM(1・3・12か月ルックバック、ボラ調整)は 1880 年以降すべての10年期で正のネットリターンを示した最も頑健なエッジだが、2/20 の手数料後の平均シャープ比は約 0.4、SG Trend Index の 2000–2024 年実績シャープ比は 0.30(年率 5.9%、ボラ 13.55%)に過ぎず、2018・2023 年は年間マイナス、2024 年 8 月は円キャリー巻き戻しで単月 −4.5% を記録した。単一通貨ペア(円)の最良テクニカルルールでもシャープ比 0.75(Hsu-Taylor-Wang)であり、月次勝率は正規近似で 59% 程度。トレンド追随は構造的に「負け月が多く、たまの大相場で稼ぐ」戦略であり、「毎月黒字」とは相容れない。

### Cited Findings
- Moskowitz, Ooi & Pedersen (2012, JFE) "Time Series Momentum": 1985–2009 年、58 の流動的先物(株価指数 9・通貨 12・商品 24・債券 13)で、過去 12 か月リターンの符号でロング/ショートする戦略が全資産クラスで正のリスク調整後リターンを示した — [SSRN](https://papers.ssrn.com/abstract=2089463); [Quantpedia 要約](https://quantpedia.com/strategies/time-series-momentum-effect/); 構成内訳は [AQR データセット](https://www.aqr.com/Insights/Datasets/Time-Series-Momentum-Original-Paper-Data)
- 同論文の二次要約(ブログ、要注意): 12 か月 TSMOM のグロス・シャープ比は伝統資産で 1.17、全資産分散で 1.60 とされる(費用控除前、40% ex-ante ボラ目標でのスケーリング) — [The Menon Lab](https://themenonlab.blog/blog/time-series-momentum-hedge-fund-strategy-explained)
- Hurst, Ooi & Pedersen "A Century of Evidence on Trend-Following Investing"(AQR, 2017 改訂): 1880 年 1 月–2016 年 12 月、67 市場(商品 29・株価指数 11・債券 15・通貨ペア 12)、1・3・12 か月 TSMOM の等加重、ボラ目標 10%。**1880 年以降すべての 10 年期でネット(2%+20% 手数料後)リターンが正**、手数料後の平均シャープ比は約 0.4 — [CXO Advisory 要約](https://www.cxoadvisory.com/momentum-investing/trend-following-with-intrinsic-momentum-over-the-very-long-run); [AQR 原文](https://www.aqr.com/Insights/Research/Journal-Article/A-Century-of-Evidence-on-Trend-Following-Investing)
- 同論文について Swedroe(etf.com)は「全コストと 2/20 手数料考慮後のシャープ比 0.76」と記述しており、CXO の「0.4」と食い違う(手数料前/後、期間の違いの可能性。一次表を確認できず) — [etf.com](https://www.etf.com/sections/index-investor-corner/swedroe-why-financial-trends-persist); 矛盾: [CXO Advisory](https://www.cxoadvisory.com/momentum-investing/trend-following-with-intrinsic-momentum-over-the-very-long-run)
- Levine & Pedersen (2016, FAJ) "Which Trend Is Your Friend?": 時系列モメンタム(過去リターン符号)と移動平均クロスオーバーは、最も一般的な形では**数学的に等価な表現**であり、HP フィルタ、カルマンフィルタ等すべての線形フィルタを包含する。「トレンド・シグネチャ・プロット」でルックバック別の重みを可視化 — [CFA Institute](https://rpc.cfainstitute.org/en/research/financial-analysts-journal/2016/which-trend-is-your-friend); [CBS PDF](https://research-api.cbs.dk/ws/files/60084063/lasse_heje_pedersen_et_al_which_trend_is_your_friend_publishersversion.pdf)
- Baltas & Kosowski "Demystifying Time-Series Momentum Strategies": 1974–2012 年、71 契約、月次・週次・日次の 3 周波数。TSMOM のシャープ比は 1.20 超(グロス)、周波数間の相関は低く別個の持続現象を捉える。Yang-Zhang レンジ型ボラ推定量が最適で、売買回転を 1/3 以上削減してもパフォーマンスは統計的に劣化しない。CTA 指数のアルファはこれらを因子に含めると有意でなくなる — [SSRN](https://papers.ssrn.com/abstract=2140091); [CAIA 要約](https://caia.org/node/4984)
- Menkhoff, Sarno, Schmeling & Schrimpf (2012, JFE) "Currency momentum strategies": 1976 年 1 月–2010 年 1 月、48 通貨。クロスセクション・モメンタムの勝者−敗者スプレッドは最大年率 10%(1 か月形成/1 か月保有)。グロス 6–10% に対し**最良戦略でも取引コスト後は約 4%**。累積リターンは形成後 8–12 か月でピークを打ち、その後反転。リターンは高イディオシンクラティック・ボラ、高カントリーリスク通貨に偏り、キャリーや標準的テクニカルルールとの相関は低い。「極めて有効な裁定限界」が存在すると結論 — [BIS WP 366](https://www.bis.org/publ/work366.pdf); [City Research Online](https://openaccess.city.ac.uk/3296); [CXO Advisory 要約](https://www.cxoadvisory.com/momentum-investing/momentum-investing-for-currencies)
- Hsu, Taylor & Wang (2016, J. Int. Econ.) "Technical trading: Is it still beating the foreign exchange market?": 45 年・30 通貨、21,195 ルール(フィルタ 2,835・移動平均 12,870・サポレジ 1,890・チャネルブレイク 3,000・オシレータ 600)をステップワイズ検定でデータスヌーピング補正。先進国 9 通貨中 3 通貨が 90% 信頼で予測可能、**最良は日本円で年率シャープ比 0.75**(新興国最良はコロンビアペソ 1.28)。コスト仮定は片道 0.045%(先進国)、0.21%(新興国)。先進国で最良のルール群は移動平均 — [WUSTL](https://profiles.wustl.edu/en/publications/technical-trading-is-it-still-beating-the-foreign-exchange-market/); [CXO Advisory 要約](https://cxoadvisory.com/technical-trading/updated-comprehensive-long-term-test-of-technical-currency-trading)
- AQR "You Can't Always Trend When You Want": **SG Trend Index のシャープ比は 2010–2018 年で 0.05、2000 年以降では 0.28**。低下の主因は 67 市場における「大きな値動きの不足」であり、シグナルの有効性・分散の低下やクラウディングではないと分析 — [AQR](https://www.aqr.com/Insights/Research/Journal-Article/You-Cant-Always-Trend-When-You-Want); [The Idea Farm 要約](https://theideafarm.com/alternative-investment/you-cant-always-trend-when-you-want/)
- SG Trend Index(AUM 上位 10 社のトレンドフォロー CTA、毎年 1 月 1 日リバランス)の長期統計: 1999 年 12 月 31 日–2024 年 2 月 29 日で**年率リターン 5.90%、年率ボラ 13.55%、シャープ比 0.30** — [Invesco 資料(ベンダー)](https://invesco.com/content/dam/invesco/emea/en/pdf/RRE_2024_Q2_NavigatingMomentum.pdf); 指数定義 — [SG 方法論](https://wholesale.banking.societegenerale.com/fileadmin/indices_feeds/SG_Trend_Index_Methodology.pdf)
- SG Trend Index 年次実績: 2022 年 +27.3%(過去最高) — [SG CTA Update 2025](https://content.sgmarkets.com/CTA_UPDATE_KEEPING_UP_WITH_THE_TRENDFOLLOWERS_2025); 2023 年 −4.16% — [Top Traders Unplugged 2023 年 12 月](https://www.toptradersunplugged.com/trend-following-performance-report-december-2023/); 2024 年 +2.4%(ほぼ横ばい) — [SG CTA Update 2025](https://content.sgmarkets.com/CTA_UPDATE_KEEPING_UP_WITH_THE_TRENDFOLLOWERS_2025); 2025 年 +2.39% — [Top Traders Unplugged 2025 年 12 月](https://www.toptradersunplugged.com/trend-following-performance-report-december-2025/); 2011 年と 2018 年は構成 CTA の過半数が年間マイナス — [Man Institute](https://www.man.com/insights/is-this-time-different)
- 2024 年 8 月(円キャリー巻き戻し月): SG Trend Index は単月 **−4.51%**、年初来 +1.29% に後退 — [Top Traders Unplugged 2024 年 8 月](https://www.toptradersunplugged.com/trend-following-performance-report-august-2024/); 7 月時点で SEB Asset Selection は −3.4%、最大の損失要因は円ショート — [HedgeNordic](https://hedgenordic.com/2024/08/sebs-asset-selection-hit-hardest); 円は 1 月の 140 から 7 月の 161 まで下落した後、急反転 — [Reuters/Kitco](https://www.kitco.com/news/off-the-wire/2024-08-01/investors-scramble-get-out-way-rising-yen)
- 2025 年前半: SG Trend Index は 6 月 +1.52% だが直近 12 か月 −15.05%、最大ドローダウン 20.61%、設定来 CAGR 5.33% — [Top Traders Unplugged 2025 年 6 月](https://www.toptradersunplugged.com/trend-following-performance-report-june-2025/); 2025 年序盤の CTA 苦戦 — [The Full FX](https://thefullfx.com/ctas-tough-start-to-2025-continues/)
- 2026 年: 2 月末時点で年初来 +8.8% — [Aussie Turtles](https://www.aussieturtles.com/battle-of-the-trend-following-indexes-february-2026/); 9 月 18 日時点で 9 月 +3.23%、年初来 +14.68%、6 週連続上昇 — [Top Traders Unplugged 週報 2026-09-18](https://www.toptradersunplugged.com/trend-following-week-in-review-september-18-2026/); 10 月 3 日時点で年初来 +15.72% とする集計もある(信頼性低い二次サイト) — [Accio](https://www.accio.com/business/sg_cta_trend_index)
- ドローダウン: SG Trend Index 設定来 25 年で「20% 以上のドローダウンに遭遇する確率はほぼ 5 分の 4」 — [Man Institute](https://www.man.com/insights/is-this-time-different); 市場サイクルとマネージド・フューチャーズのドローダウン分析 — [AlphaSimplex Kaminski & Wen 2025](https://www.alphasimplex.com/assets/files/2025.06---market-cycles-and-managed-futures---kaminski-and-wen.pdf)
- BarclayHedge Currency Traders Index(通貨専業 CTA): 2017 年 −0.8%、2018 年 +5.10%、2022 年 +9.78%(別報道では +8.98%)、2023 年 +7.13%、**2024 年 +11.11%(1997 年の 11.35% 以来の好成績、2003 年以来初の 2 桁)** — [The Full FX 2024](https://thefullfx.com/currency-traders-end-year-on-strong-note/); [The Full FX 2023](https://thefullfx.com/currency-managers-highlight-cta-performance-in-2023/); [Hedgeweek 2022](https://hedgeweek.com/news/ctas-return-733-2022); Barclay CTA Index は 2018 年 −2.85% — [BarclayHedge](https://www.barclayhedge.com/barclay-cta-index-gains-0-30-in-december-down-2-85-for-the-year-a-gloomy-2018-for-most-barclay-managed-futures-indices/)
- 2022 年の CTA 指数レビュー — [Alpha Week](https://www.alpha-week.com/2022-cta-index-performance-review); トレンドフォロー入門(業界資料) — [Graham Capital 2022](https://www.grahamcapital.com/wp-content/uploads/2024/04/Trend-Following-Primer_January-2022.pdf); CTA リターン分散の実態 — [CFM](https://www.cfm.com/steady-trends-the-reality-of-cta-return-dispersion/)
- 最新のレビュー論文(未読、タイトルのみ確認): "The Science and Practice of Trend-Following Systems"(arXiv 2026) — [arXiv](https://arxiv.org/pdf/2607.19497); "Breaking the Trend: How to Avoid Cherry-Picked Signals" — [arXiv](https://arxiv.org/pdf/2504.10914); "Revisiting the Structure of Trend Premia: When Diversification Hides Redundancy" — [arXiv](https://arxiv.org/html/2510.23150v2)

### Inferences
- ルックバックについて: 学術的に再現されているのは「1–12 か月」(日次換算で約 20–250 営業日)の中長期トレンドで、1・3・12 か月の等加重が標準。EMA50/200 は Levine–Pedersen の等価性により 12 か月級 TSMOM の線形フィルタ版、Donchian-20(15 分足)は約 5 時間の値幅を見るに過ぎず、学術的に検証された時間軸(数週間–数か月)よりはるかに短い。15 分足 Donchian-20 や 1 分足押し目の有効性を支持する査読済み証拠は本調査では見つからなかった。
- 月次勝率の推定(本調査での計算、正規近似 P(月次>0)=Φ(SR/√12)): SG Trend(SR 0.30)→ 53.5%、Hurst-Ooi-Pedersen ネット(SR 0.4)→ 54.6%、Hsu-Taylor-Wang の円最良ルール(SR 0.75)→ 58.6%。実際のトレンド指数は正の歪度(負け月は小さく、勝ち月は大きい)を持つため勝率はこれよりやや低い可能性がある。
- USD/JPY 単独のトレンド追随は「可能だが低シャープ」:Hsu-Taylor-Wang で円が先進国最良(0.75)だったのは、2012–2015 年のアベノミクス、2022–2024 年の日米金利差拡大のような長期一方向相場が多かったためと解釈できるが、2024 年 8 月や 2025 年 4 月(139.88 まで急落、KQ2 参照)のような急反転で同じルールが大きく毀損する。分散のない単一ペアでは SG Trend 構成社のような 20% 級ドローダウンがより頻繁になると推測される。
- 2010 年代の低迷を「大相場の不足」で説明する AQR の分析は、裏返せば**トレンド戦略の月次損益はレジーム依存であり、設計者が制御できない**ことを意味する。毎月黒字という目標設定は戦略の統計的性質と両立しない。

### Gaps
- MOP(2012)の通貨クラス単独・通貨ペア別(JPY)のシャープ比/t 値、13–24 か月の反転の大きさ: 一次 PDF が遮断され未確認。
- Hurst-Ooi-Pedersen の通貨クラス単独シャープ比、10 年期別の表、2017–2026 年の更新値: 未確認。
- SG Trend Index の 2018–2021 年の個別年次リターン、月次系列、月次勝率(% positive months)の公式値: 検索では取得できず。
- Donchian 20/55 日(タートル型)の通貨先物でのアウトオブサンプル・シャープ比: 検索予算切れで未調査。

---

## KQ2. キャリー — 証拠、クラッシュリスク、USD/JPY キャリー 2022–2026 年、個人のスワップ・キャリーの可否

### Takeaway
通貨キャリーはグロスでシャープ比 0.5–0.9(資産クラス横断では 1.1)と高いが、負の歪度(1990–2008 年の等加重キャリーで −1.62)と景気後退期の集中ドローダウンが本質で、2024 年 8 月には USD/JPY が 1 か月で −9.9%、G10/EM キャリー・バスケットが通常の 2 倍の速度で約 10% 下落した。2026 年 8 月には日米協調介入が入るなど、USD/JPY のキャリーは「月次で安定した収益源」ではなく「数年かけて稼いだ分を数週間で失う」典型である。

### Cited Findings
- Koijen, Moskowitz, Pedersen & Vrugt "Carry": 資産クラス内のキャリー戦略はサンプル期間で平均**グロス・シャープ比約 0.7**、全資産クラス横断では 1.1。キャリーは世界的な景気後退期に全資産クラスで同時に悪化し、3 大ドローダウンは 1972 年 8 月–1975 年 9 月、1980 年 3 月–1982 年 6 月、2008 年 8 月–2009 年 2 月 — [CXO Advisory 要約](https://www.cxoadvisory.com/big-ideas/unified-carry-trade-theory/); [Koijen スライド](https://www.fmg.ac.uk/sites/default/files/2020-08/Ralph-Koijen-slides.pdf)
- Lustig, Roussanov & Verdelhan: キャリー・プレミアムは世界株式ボラティリティの変化と関連し、高金利通貨はグローバル・ボラ上昇時に減価、低金利通貨は増価する — [etf.com(Swedroe 要約)](https://www.etf.com/sections/index-investor-corner/swedroe-risks-carry-trade); 原論文の NBER 章 — [NBER](https://nber.org/system/files/chapters/c7288/c7288.pdf)
- Brunnermeier, Nagel & Pedersen "Carry Trades and Currency Crashes": キャリー通貨は負の歪度を持ち、資金制約の緩み・締まりに伴う急落(クラッシュ)リスクに晒される — [NBER WP 14473](https://www.nber.org/papers/w14473.pdf)
- Jurek(クラッシュ中立キャリー): 1990 年 1 月–2008 年 12 月の等加重キャリーの月次リターン歪度は **−1.62**。ドル・エクスポージャーを除去しクラッシュリスクをオプションでヘッジすると、キャリーの超過リターンは統計的にゼロと区別できない — [Jurek 論文 PDF](https://www.johnhcochrane.com/s/jurek_currency.pdf)
- Daniel, Hodrick & Lu "The Carry Trade: Risks and Drawdowns"(NBER WP 20433 / Critical Finance Review 2017): G10。分散された「ドル・キャリー」ポートフォリオは高い平均超過リターンと高シャープ比、歪度は最小で下方リスクなし。一方**ドル中立キャリーの異常リターンは有意でない**。株式・債券・FX・ボラ・下方株式リスクではキャリーの収益性を説明できず、下方ベータは無条件ベータと有意差なし — [NBER](https://www.nber.org/papers/w20433); [Columbia PDF](https://business.columbia.edu/sites/default/files-efs/pubfiles/6378/Daniel.Hodrick.Lu.Carry%20Trade.Critical%20Finance%20Review.2017.pdf)
- 2024 年 8 月の巻き戻し: USD/JPY は 2024 年 7 月 3 日の 161.91 から 8 月 5 日の 142.24 へ **−9.9%** — [Lambda Finance](https://www.lambdafin.com/articles/yen-carry-trade-unwind); 7 月 10 日以降で円は対ドル 9.98% 上昇、8 月 5 日は 2020 年 3 月以来最大の単日クロスアセット・ショック — [Finance Magnates](https://www.financemagnates.com/forex/analysis/massive-yen-carry-trade-unwind-sends-250-billion-shockwave-through-global-markets/); 日経 225 は 8 月 5 日に −12.4%(1987 年ブラックマンデー以来) — [FSG Journal](https://fsgjournal.nl/article/2024-09-25-the-collapse-of-the-yen-carry-trade); 7 月 31 日–8 月 5 日で日経 −20% — [AMRO Analytical Note 2024-12](https://www.amro-asia.org/wp-content/uploads/2024/12/20241219-Analytical_Note_Carry_Trade.pdf)
- JPMorgan 集計: G10・EM・グローバルのキャリー・バスケットは 2024 年 5 月以降約 10% 下落し年初来の利益を消失、下落速度は通常のキャリー・ドローダウンの 2 倍 — [CNBC 2024-08-13](https://www.cnbc.com/2024/08/13/carry-trades-why-strategists-believe-a-major-unwind-is-far-from-over.html); 巻き戻し規模の推計 2,500 億ドル — [The Full FX](https://thefullfx.com/what-does-250-billion-buy-a-carry-trade-blow-up/); BIS は統計上キャリーの正確な規模は特定できないと注記 — [BIS Quarterly Review 2024-09](https://www.bis.org/publ/qtrpdf/r_qt2409y.htm)
- 巻き戻し後の復活: 2024 年 12 月時点で円キャリーは再び人気化 — [Bloomberg 2024-12-02](https://www.bloomberg.com/news/articles/2024-12-02/yen-carry-trade-that-rattled-markets-shows-signs-of-a-comeback); 2024 年の USD/JPY キャリー復活は「2024 年最大のサプライズ」の一つ — [FOREX.com](https://www.forex.com/en/news-and-analysis/top-2024-surprises-usdjpy-js/)
- USD/JPY 2025 年: 年初来高値 158.88(1 月 10 日)、安値 139.88(4 月 22 日)、年末 156.86、年間騰落 −0.54% — [Pound Sterling Live](https://www.poundsterlinglive.com/history/USD-JPY-2025); 年間平均 149.654、レンジ 139.89–158.872 — [ExchangeRates.org.uk](https://www.exchangerates.org.uk/USD-JPY-spot-exchange-rates-history-2025.html); 4 月 21 日 140.88(関税不透明感とドル安)、夏場 144–148 で保ち合い、秋に 150 を上抜け年末に 159 近辺 — [Exchange-Rates.org](https://www.exchange-rates.org/exchange-rate-history/usd-jpy-2025)
- 2026 年の BOJ 正常化と介入: BOJ は政策金利を 25bp 引き上げ 0.75%(30 年ぶり高水準)へ — [Investing.com](https://www.investing.com/analysis/the-boj-just-pulled-the-trigger-markets-brace-for-carry-trade-chaos-200672097); 2026 年 8 月、米国の異例の支援を伴う日米協調円買い介入で USD/JPY は約 164 から 155 へ急落後、159 近辺まで戻す。日本の政策金利 1.0% に対し FF 金利 3.5–3.75%、Morgan Stanley は残存円キャリー約 5,000 億ドルと推計、「円高局面はキャリー再構築の好機と扱われている」 — [CNBC 2026-08-20](https://www.cnbc.com/2026/08/20/us-japan-yen-intervention-bank-of-japan-carry-trade.html); [OMFIF 2026-08](https://www.omfif.org/2026/08/japans-yen-intervention-and-the-us-unusual-support/); [Goldman Sachs Exchanges](https://www.goldmansachs.com/insights/goldman-sachs-exchanges/what-the-us-japan-currency-intervention-means-for-the-yen-rates-and-the-dollar); 介入の信認低下を論じる記事 — [Investing.com](https://www.investing.com/analysis/usdjpy-near-164-shows-japans-intervention-threat-is-losing-credibility-200684664)
- EM キャリー指標は 2024 年 8 月に 4% 下落したが、2026 年には調達通貨をユーロ・スイスフランへ分散しつつキャリーは継続 — [The Japan Times 2026-08-06](https://www.japantimes.co.jp/business/2026/08/06/markets/emerging-market-yen-carry-trade/)

### Inferences
- USD/JPY のロング・キャリーは 2022–2024 年半ばに「金利差 + 円安トレンド」の二重取りで極めて好調だったが、2024 年 8 月(−9.9%/1 か月)、2025 年 1–4 月(158.88→139.88、約 −12%)、2026 年 8 月(164→155、約 −5.5%)と、年 1 回程度の割合で数か月分のスワップを数日で吹き飛ばす下落が発生している。個人のスワップ狙いキャリーは「月次黒字」とは正反対の損益分布(多数の小勝ち月 + 稀な大負け月)になる。
- Daniel-Hodrick-Lu の「ドル中立キャリーは有意でない」という結果は、キャリー収益の多くがドル全体のエクスポージャーに由来することを示唆し、USD/JPY 単独キャリーはキャリー・ファクターというよりドル・ベータとして振る舞うと考えられる。
- Jurek の結果(クラッシュをヘッジすると超過リターンが消える)は、キャリーの「エッジ」がクラッシュ保険料であることを示す。したがって固定 ATR×2 の損切りでクラッシュを回避しようとすると、理論上は期待リターンも同時に失われる。

### Gaps
- USD/JPY ロング・キャリーの 2022–2026 年の実現シャープ比・最大ドローダウンを算出した一次資料(DB G10 Currency Harvest 等のインデックス年次リターン、Bloomberg キャリー指数): 検索予算切れで未取得。
- Koijen らの通貨キャリー単独の平均リターン/ボラ/シャープ比、Daniel-Hodrick-Lu の具体的なシャープ比・最大ドローダウン数値: 一次 PDF 未読。
- 日本の個人向けスワップポイント水準とブローカーのスプレッドを織り込んだ、個人キャリーのネット実績: 一次資料なし。

---

## KQ3. バリュー/PPP・実質金利差シグナルとマルチシグナル合成(トレンド+キャリー+バリュー)

### Takeaway
実質為替レート(PPP 乖離)に基づく通貨バリューは単独でシャープ比約 0.5、ファンダメンタルズ調整後で 0.8–0.9(Menkhoff et al. 2017)と、トレンド・キャリーとは異なる情報を持つ。バリューとモメンタムは負相関のため 50/50 合成でシャープ比が大きく改善し、ボラは半分未満になる(Asness-Moskowitz-Pedersen)。ただしバリューは数年単位の遅いシグナルで、日中–数日の個人システムに直接組み込める性質ではない。

### Cited Findings
- Menkhoff, Sarno, Schmeling & Schrimpf (2017, RFS 30(2), 416–441) "Currency Value": 実質為替レートは通貨超過リターンのクロスセクションを予測。**単純な実質為替レート・バリュー戦略のシャープ比は約 0.5**、生産性・輸出財の質・対外純資産・需給ギャップで調整したバリュー指標では **0.8–0.9**。予測情報はキャリー・モメンタムと異なる — [City PDF](https://openaccess.city.ac.uk/id/eprint/14851/13/FXVALUE_Rev3_cepr.pdf); [CEPR DP11324](https://cepr.org/publications/DP11324); [VoxEU](https://cepr.org/index%2Ephp/voxeu/columns/currency-valuation-and-risk-premia); [Quantpedia](https://quantpedia.com/strategies/currency-value-factor-ppp-strategy)
- Asness, Moskowitz & Pedersen (2013, JF) "Value and Momentum Everywhere": 8 市場(米英欧日の個別株、国別株価指数、国債、通貨、商品)でバリューとモメンタムは共に正の異常リターン、両者は資産クラス内・横断で負相関。**等加重合成はどの地域でも単独の半分未満のボラ**で、シャープ比と安定性が大幅に向上(米国株の 50/50 合成でシャープ比 1.13、4 株式市場分散で 1.88 — 株式の数値) — [NYU PDF](https://pages.stern.nyu.edu/~lpederse/papers/ValMomEverywhere.pdf); [CXO Advisory 要約](https://cxoadvisory.com/value-premium/combining-value-and-momentum-across-asset-classes); [etf.com](https://www.etf.com/sections/index-investor-corner/swedroe-close-look-value-momentum)
- Menkhoff et al. (2012) 通貨モメンタムはキャリーと「非常に異なる性質」を持ち相関が低い(KQ1 参照) — [BIS WP 366](https://www.bis.org/publ/work366.pdf)
- 実質為替レートに基づく FX 戦略の実務的整理(ベンダー・ブログ) — [Macrosynergy](https://macrosynergy.com/?p=3322)

### Inferences
- 時系列 vs クロスセクション: 個人が単一ペアで実装できるのは時系列シグナル(自ペアの過去リターン、自ペアの金利差、自ペアの PPP 乖離)のみで、学術的に最も強いクロスセクション・モメンタム(勝者買い/敗者売り)や通貨バリューのロング・ショート分散効果は享受できない。合成の効能(ボラ半減)は「複数の低相関リターン源の分散」から来るため、単一ペア・単一シグナルではほぼ得られない。
- USD/JPY は PPP 対比で 2022–2026 年に大幅に割安な円が続いたが、バリューは「数年単位で平均回帰する」シグナルであり、その間にトレンド/キャリーと逆方向に数十円動くことを許容する資金が必要。月次での一貫性には寄与しない。

### Gaps
- Asness-Moskowitz-Pedersen の通貨クラス単独のバリュー/モメンタム/合成シャープ比の数値: 一次 PDF 未読。
- トレンド+キャリー+バリュー 3 因子合成の通貨限定シャープ比・月次勝率(例: Deutsche Bank/Citi の FX スタイル指数、AQR Alternative Premia): 検索予算切れで未取得。

---

## KQ4. 短期リバーサル/平均回帰、日中モメンタム、日中季節性 — 0.2pip スプレッド + スリッページ後に搾取可能か

### Takeaway
FX の日中モメンタム(寄り 30 分→引け 30 分)の査読済み証拠は取引時間が明示的な RUB/USD(MICEX)に限られ、G10 の 24 時間市場で同等の効果を示す論文は本調査では見つからなかった。日中季節性(自国取引時間中に自国通貨が減価、Ranaldo 2009/Breedon-Ranaldo 2013)は統計的・経済的に有意で EUR/USD では単純な戦略が「収益的」とされるが、1 分足の負の自己相関(ビッド・アスク・バウンス)は搾取不能であり、週次では反転よりモメンタムが優勢。いずれも月次で安定した収益源とする証拠はない。

### Cited Findings
- Elaut, Frömmel & Lampaert (2018, J. Financial Markets 37, 35–51) "Intraday momentum in FX markets: Disentangling informed trading from liquidity provision": 取引時間が明示的な外為市場(MICEX の RUB/USD、2005–2014 年、約定レベルデータ)で寄り 30 分リターンが引け 30 分リターンを有意に正に予測。要因は流動性供給者のオーバーナイト保有への回避であり、効果は金融危機時により顕著、取引時間による取引集中が重要 — [IDEAS/RePEc](https://ideas.repec.org/a/eee/finmar/v37y2018icp35-51.html)
- Gao, Han, Li & Zhou (2018, JFE) "Market intraday momentum": S&P500 ETF(1993–2013)で寄り 30 分リターンが引け 30 分リターンを予測、アウトオブサンプル R² 0.018、「寄り 30 分が正なら引けまで保有、負なら現金」で 1999 年 1 月–2012 年 12 月にグロス年率 6.3% 対 −0.5%。高ボラ日・高出来高日・景気後退日・マクロ指標発表日に強い。他の主要 ETF 10 本と 2 つの国際株価指数先物でも強い(通貨は対象外) — [Alpha Architect 要約](https://alphaarchitect.com/2014/08/attention-prop-traders-the-first-half-hour-of-trading-predicts-the-last-half-hour/); [CXO Advisory 要約](https://cxoadvisory.com/calendar-effects/first-and-last-half-hours-of-trading-linked)
- 日中モメンタムのフォローアップは株式のヘッジ需要(JFE 2021)、中国商品先物(銅・鉄鋼・大豆・大豆粕で寄り 30 分が引け 30 分を正に予測)、ETF に集中し、**G10 通貨を対象とした FX フォローアップは検索で確認できなかった** — [Hedging demand and market intraday momentum](https://ideas.repec.org/a/eee/jfinec/v142y2021i1p377-403.html); [Intraday time-series momentum: Evidence from China](https://hull-repository.worktribe.com/output/3295550/intraday-time-series-momentum-evidence-from-china); [J. Futures Markets 2020](https://ideas.repec.org/a/wly/jfutmk/v40y2020i4p632-650.html); [Otago](https://ourarchive.otago.ac.nz/esploro/outputs/journalArticle/Intraday-time-series-momentum-and-investor-trading/9926495442801891)
- Breedon & Ranaldo "Intraday patterns in FX returns and order flow"(SNB WP 2011-04、J. Money, Credit and Banking 2013): 10 年分の高頻度 FX データで、通貨は**自国の取引時間中に減価**する有意な時刻効果。EUR/USD は欧州午前に下落し米国時間に上昇、EUR/USD では「単純で収益的な取引戦略」を構成できる。パターンはオーダーフローにも現れ、参加者が自国時間に外貨の買い越しになる傾向で説明 — [SNB](https://www.snb.ch/en/publications/research/working-papers/2011/working_paper_2011_04); [QMUL WP 694](https://qmul.ac.uk/sef/research/workingpapers/2012/items/694.html)
- Ranaldo "Segmentation and Time-of-Day Patterns in Foreign Exchange Markets"(SNB WP 2007-03、JBF 2009): 自国通貨は外国の営業時間に増価し、自国営業時間に減価する。統計的・経済的に高度に有意で、カレンダー効果を考慮しても長年にわたり持続。説明は自国通貨バイアス + 市場分断によるディーラー在庫の周期的不均衡 — [SNB](https://www.snb.ch/en/mmr/papers/id/working_paper_2007_03)
- 超短期の負の自己相関: FX リターンは 1 分レベルで負の自己相関の「相関ホール」(アンチパーシステンス)を示す(マイクロストラクチャ効果) — [arXiv 1011.2385](https://arxiv.org/pdf/1011.2385)
- 週次ホライズン: 63 通貨、1–4 週のルックバック/保有 16 通りで**反転ではなくクロスセクション・モメンタムが優勢**、短期モメンタムは年率 8% 超で有意、ルックバックが長いほど増加(博士論文) — [Massey University](https://mro.massey.ac.nz/handle/10179/7266)
- 週末の過剰反応と翌営業日の反転(スポット FX のカレンダー効果) — [Multinational Finance Journal 2016](https://ideas.repec.org/a/eee/mulfin/v37-38y2016ip158-167.html); フィキシング前後の G9 通貨の反転は「広範で高度に有意」 — [Bank of Canada(Whelan ら)](https://www.bankofcanada.ca/?p=223063); G10 通貨先物は 1 月に全て負のリターン(季節性) — [UMSL](https://profiles.umsl.edu/en/publications/return-seasonality-in-the-foreign-exchange-market/)
- 実務向けチュートリアル(3 か月モメンタム + 平均乖離の平均回帰を合成する FX 戦略、ベンダー資料) — [QuantConnect](https://www.quantconnect.com/tutorials/combining-mean-reversion-momentum-foreign-exchange-markets/)

### Inferences
- コスト面の判定: 日中季節性の効果量は論文では「経済的に有意」とされるが、本調査では bp 単位の数値を確認できなかった。一般に時刻効果は 1 日数 bp 規模であり、USD/JPY で 0.2pip(≈1.3bp at 150)のスプレッド + スリッページを毎日 1–2 回払うと、効果の多くが相殺される可能性が高い。EUR/USD で「収益的」とされた戦略は、インターバンク・ミッド価格に基づくもので、個人の実効コストでの検証ではない。
- 1 分足での「押し目再エントリー」は、1 分レベルの負の自己相関(ビッド・アスク・バウンス)と区別がつかないノイズ領域で動作しており、学術的に支持される持続性は週次以上のホライズンにある。1 分足ロジックは執行品質の改善(指値での約定)には使えても、エッジの源泉にはならないと考えるべき。
- FX の日中モメンタムは「取引所の明示的な取引時間」と「流動性供給者のオーバーナイト回避」が条件であり、24 時間連続取引の USD/JPY では構造的に弱いと推測される(東京・ロンドン・NY のセッション境界が擬似的な「寄り/引け」として機能する可能性はあるが、査読済み証拠なし)。

### Gaps
- Breedon-Ranaldo / Ranaldo の時刻効果の効果量(通貨別の bp/日、JPY の数値)、EUR/USD 戦略の年率リターン・シャープ比・コスト後の結果: 一次 PDF 未読。
- G10 通貨(特に USD/JPY)でセッション境界の日中モメンタムを検証した査読済み研究: 見つからず(存在しない可能性が高い)。
- 日次ホライズンの短期リバーサル(1 日–1 週)の G10 シャープ比を直接示す論文: 検索では特定できず。

---

## KQ5. ボラティリティ・ブレイクアウトとボラティリティ・ターゲティング — 逆ボラ・スケーリングは月次を平滑化するか、トレンドは高 ADX/ボラ圧縮後に強いか

### Takeaway
逆ボラ・スケーリングは株式・クレジットではシャープ比を改善するが、**通貨・債券・商品ではシャープ比への影響は無視できる**(Harvey ら 2018、60 資産・1926–2017 年)。改善が確実なのは左裾(極端な負け月)の縮小であり、「負け月を無くす」のではなく「大負け月を減らす」効果。ADX フィルタが基本トレンドモデルに価値を加えなかったとする実務検証もあり、高 ADX レジームで月次勝率が上がるという査読済み証拠は見つからなかった。

### Cited Findings
- Harvey, Hoyle, Korgaonkar, Rattray, Sargaison & Van Hemert (2018, JPM 45(1)) "The Impact of Volatility Targeting": 60 資産、1926 年からの日次データ(2017 年まで)。ボラ管理ポートフォリオのシャープ比改善は株式・クレジットなど「リスク資産」に限られ(米国株 0.40→0.48–0.51)、レバレッジ効果と関連。**債券・通貨・商品ではシャープ比への影響は無視できる**。一方、極端なリターンの発生確率は全資産クラスで低下し、左裾イベントは高ボラ時に起こるためボラ目標ポートフォリオのエクスポージャーが小さい時に当たる — [Alpha Architect 要約](https://alphaarchitect.com/volatility-targeting-improves-risk-adjusted-returns/); [Quantpedia 要約](https://quantpedia.com/the-impact-of-volatility-targeting-on-equities-bonds-commodities-and-currencies)
- Moreira & Muir (2017, JF) "Volatility-Managed Portfolios": 高ボラ時にリスクを落とす管理ポートフォリオは、株式の市場・バリュー・モメンタム・収益性・投資ファクターに加え**通貨キャリー**でも大きなアルファとシャープ比上昇を生む。ファクター・ポートフォリオでグロス・シャープ比を 50–100% 引き上げ、市場ファクターのタイミングでグロス年率アルファ 4.86%(ベータ 0.6)。機序はボラ変化が期待リターンの比例的変化で相殺されないこと — [NBER WP 22208](https://www.nber.org/papers/22208); [CXO Advisory 要約](https://www.cxoadvisory.com/?p=28346)
- Baltas & Kosowski: Yang-Zhang レンジ型ボラ推定量が効率・バイアス・事後回転率の面で最適、ボラ推定とトレンド検出の改善で回転率を 1/3 以上削減可能 — [SSRN](https://papers.ssrn.com/abstract=2140091)
- MOP(2012)/Hurst-Ooi-Pedersen は全ポジションを ex-ante ボラでスケーリング(MOP は 40%、HOP は 10% のボラ目標)しており、学術的 TSMOM の実績はすべて「ボラ調整済み」の数値である — [Quantpedia](https://quantpedia.com/strategies/time-series-momentum-effect/); [CXO Advisory](https://www.cxoadvisory.com/momentum-investing/trend-following-with-intrinsic-momentum-over-the-very-long-run)
- ADX フィルタ: 基本的なトレンドフォロー・モデルに ADX を加えても価値を加えなかったとする実装検証(実務ブログ、査読なし) — [Oxford Strat: ADX](https://oxfordstrat.com/trading-strategies/average-directional-index/)
- ボラ・レジーム依存: 伝統資産のトレンド成績はボラ・レジームに高度に依存(p<0.001)とするプレプリント(査読前) — [Preprints.org 2026](https://www.preprints.org/manuscript/202607.1835); トレンド指標とボラ感応指標の併用が乱高下局面で頑健とする論文(低位誌) — [Anser Press](https://www.anserpress.org/journal/fel/5/2/53)
- AQR の分析では 2010 年代のトレンド低迷の主因は「大きなリスク調整後の値動きの不足」であり、ボラが低い時期にトレンドが弱いことと整合 — [AQR](https://www.aqr.com/Insights/Research/Journal-Article/You-Cant-Always-Trend-When-You-Want)

### Inferences
- ATR ベースのストップ/トレーリング(ATR×2、×2.5)と ATR 連動のポジションサイズは、Harvey らの意味での「ボラ・ターゲティング」に相当し、USD/JPY のような通貨では**期待シャープ比を上げないが、2024 年 8 月型の単月大損失を緩和する**効果が期待できる。これは「月次勝率」ではなく「最悪月の深さ」に効く。
- Moreira–Muir(キャリーでも有効)と Harvey ら(通貨で無効)の相違は、対象が「キャリー・ファクター」か「通貨の買い持ち」かの違いによる。トレンド・シグナル自体にボラ・タイミングを重ねた場合の通貨単独の効果を示す証拠は確認できなかった。
- 「高 ADX(トレンド相場)でだけ取引する」という設計は、AQR の「値動きの大きさがトレンド収益を決める」という分解と方向は一致するが、ADX は遅行指標であり、トレンド検出済みの局面に入る時点で期待値の多くが消費されている可能性がある(Oxford Strat の結果と整合)。レジーム・フィルタは勝率を上げるより取引数を減らす効果が主と推測される。

### Gaps
- 高 ADX/ボラ圧縮後(スクイーズ)条件付きのトレンド収益を通貨で検証した査読済み研究: 見つからず。
- Harvey らの通貨クラス単独のシャープ比(スケール前後)の数値: 一次表未確認。

---

## KQ6. 最良の体系的 FX/CTA プログラムの月次勝率はどの程度か — シャープ比と月次勝率の数学、SG Trend・CTA・FX 専業の具体例

### Takeaway
正規近似 P(月次>0) ≈ Φ(SR_annual/√12) によれば、月次勝率 75%/85%/95% にはそれぞれ年率シャープ比 2.34/3.59/5.70 が必要である。実在の最良クラス(SG Trend 0.30、Hurst-Ooi-Pedersen ネット 0.4、P/E 投資顧問の FX 戦略ファンド 0.25、Hsu-Taylor-Wang 円最良ルール 0.75)はいずれも勝率 53–59% の領域で、FX 専業大手の FX Concepts(ピーク AUM 140 億ドル)は 2013 年に破綻、QFS は 2012・2013 年に連続 8%超の損失で 2015 年に閉鎖した。「毎月黒字」を実現した体系的 FX プログラムの公開記録は見つからなかった。

### Cited Findings
- 計算(本調査、月次リターンが i.i.d. 正規・無リスク金利ゼロの近似): P(月次>0)=Φ(SR/√12)。SR 0.3→53.5%、0.5→55.7%、0.75→58.6%、1.0→61.4%、1.5→66.7%、2.0→71.8%、2.5→76.5%、3.0→80.7%、4.0→87.6%、5.0→92.6%、6.0→95.8%。逆算: 60%→SR 0.88、70%→1.82、**75%→2.34、85%→3.59、95%→5.70**、99%→8.06。「12 か月連続黒字」の確率は SR 2.0 で 1.9%、SR 3.0 で 7.6%、SR 4.0 で 20%、SR 6.0 で 60%(出典なし、本調査の計算)
- SG Trend Index: 1999 年 12 月–2024 年 2 月でシャープ比 0.30、年率 5.90%、ボラ 13.55% — [Invesco(ベンダー)](https://invesco.com/content/dam/invesco/emea/en/pdf/RRE_2024_Q2_NavigatingMomentum.pdf); 2010–2018 年のシャープ比 0.05 — [AQR](https://www.aqr.com/Insights/Research/Journal-Article/You-Cant-Always-Trend-When-You-Want); 最大ドローダウン 20.61%、設定来 CAGR 5.33%(2025 年 6 月時点) — [Top Traders Unplugged](https://www.toptradersunplugged.com/trend-following-performance-report-june-2025/); 25 年間で 20% 超のドローダウンに遭う確率「ほぼ 5 分の 4」 — [Man Institute](https://www.man.com/insights/is-this-time-different)
- Hurst-Ooi-Pedersen の 137 年 TSMOM: 2/20 手数料後平均シャープ比約 0.4、各 10 年期で正(=10 年単位での勝率 100%、月次ではない) — [CXO Advisory](https://www.cxoadvisory.com/momentum-investing/trend-following-with-intrinsic-momentum-over-the-very-long-run)
- P/E Investments(ボストン、1995 年設立、Warren Naphtal ら、体系的 FX): FX Aggressive Strategy は 2020 年第 1 四半期に約 +34%、**2003 年以降の年率は約 13%**(アグレッシブ版)、FX Standard +22%、FX Low Volatility +5.4%(同四半期) — [BloombergQuint 2020](https://www.bloombergquint.com/business/hedge-fund-p-e-investments-surges-34-in-wild-stretch-for-fx); 2022 年は 6 月までに FX Standard +16%、通貨戦略の運用額 137 億ドル — [Hedge Fund Alpha](https://hedgefundalpha.com/news/hedge-funds-are-finally-making-money-on-fx-with-dollar-soaring/); P/E が運用助言する Ninepoint Global Macro Fund(旧 FX Strategy Fund)は設定来年率 4.8%、**シャープ比 0.25、最大ドローダウン −19.6%**(2025 年 3 月末、ベンダー資料) — [Ninepoint](https://ninepoint.com/funds/ninepoint-global-macro-fund); 月次ファクトシート(未読) — [Waystone PDF](https://www.waystone.com/wp-content/uploads/2017/01/PE-Global-Factsheet-December-2022.pdf)
- QFS Asset Management(Sanford Grossman 創業、ファンダメンタル系 FX): 2012 年 −8.6%、2013 年 −8.7%、2015 年 1 月に最後の通貨ファンドを閉鎖し約 10 億ドルを返還 — [Hedge Fund Alpha](https://hedgefundalpha.com/news/qfs-shutting-down-currency-hedge-fund/); [HedgeCo](https://www.hedgeco.net/news/?p=40063)
- FX Concepts(John Taylor、1981 年設立、FX 専業最大手): 2007/2008 年にピーク AUM 140 億ドル超、2013 年には 6.21 億ドルに減少。2011 年の欧州債務危機以降 FX のトレンドが消失し通貨運用者を直撃。最後の機関投資家 San Francisco Employees' Retirement System が 4.5 億ドル(AUM の約 66%)を解約、2013 年 10 月に清算・破産申請、債権者への債務超過 3,500 万ドル超 — [CNBC 2013-10-09](https://www.cnbc.com/2013/10/09/john-taylors-fx-concepts-confirms-wind-down.html); [ai-CIO](https://ai-cio.com/news/the-downfall-of-john-taylor); [Finance Magnates](https://financemagnates.com/institutional-forex/technology/fx-concepts-holding-company-files-for-bankruptcy-following-outflow-of-final-client/amp); [Euromoney](https://www.euromoney.com/?p=66955)
- The Cambridge Strategy(ロンドン、EM 通貨): EM Alpha Currency Programme は年率 14.2%、「マイナスの年なし」(ローンチ時 PR、ベンダー資料・時期不明) — [Hedgeweek](https://www.hedgeweek.com/cambridge-strategy-launches-emerging-markets-currency-programme/); Asian Markets Alpha +2.44%、Asian Alpha Currency Fund +3.01%(年次不明、ベンダー) — [Money Management](https://moneymanagement.com.au/news/financial-planning/asian-currency-pays); 2012 年の Risk 賞 — [Risk.net](https://www.risk.net/awards/2248507/the-cambridge-strategy-emerging-markets-alpha-programme-the-cambridge-strategy-asset-management)
- BarclayHedge Currency Traders Index の 2024 年 +11.11% は「1997 年以来の好成績」、すなわち通貨専業 CTA の年次リターンは 1997–2023 年の 27 年間にわたり概ね 1 桁台かマイナスだった — [The Full FX](https://thefullfx.com/currency-traders-end-year-on-strong-note/)

### Inferences
- 月次勝率で見ると、学術的最良(SR 0.75–1.0 グロス)でも 59–61%、実在ファンド(SR 0.25–0.4 ネット)では 53–55%。年次勝率ですら SR 0.4 で 66%、SR 1.0 で 84% にとどまる。したがって「毎月黒字」は、公開情報で確認できる限り、分散された機関投資家の体系的 FX でも達成されていない目標である。
- FX Concepts・QFS の失敗は、FX 単独(他資産に分散しない)運用が 2011–2014 年のような「トレンド・キャリーが同時に機能しない」数年を乗り切れないことを示す。USD/JPY 単独はさらに分散が小さい。
- Cambridge Strategy の「マイナス年なし」は短い実績期間の PR であり、月次勝率ではなく年次であることに注意。EM 通貨のキャリー系は 2013 年(テーパータントラム)以降に大きく劣化した事例が多い(本調査では個別数値未確認)。

### Gaps
- SG Trend Index / BTOP50 の公式「% positive months」: 取得できず(正規近似の 53–55% は推定)。
- P/E Investments の各戦略の設定来シャープ比・月次勝率・最大ドローダウン(2008 年、2016 年等): ファクトシート PDF が遮断され未確認。
- Medallion 等「極端に高シャープ比」の非 FX 事例の月次勝率: 検索予算切れで未確認。

---

## KQ7. 減衰・クラウディング — 公表後にアノマリーは弱まるか(Neely-Weller-Ulrich 2009、Hsu-Taylor-Wang 2016、通貨モメンタムの消失)

### Takeaway
FX の単純テクニカル・ルール(フィルタ、移動平均)の 1970–80 年代の利益は本物だったが 1990 年代初頭までに消失し、円でも 1990 年代以降は「概ね不採算」(Neely-Weller-Ulrich 2009、JFQA)。あまり研究されていないルール(チャネル、ARIMA、遺伝的プログラミング)は低下したが完全には消えておらず、Hsu-Taylor-Wang(2016)はデータスヌーピング補正後も円で 0.75 のシャープ比が残るとする。個人トレーダーのテクニカル分析利用は成績と負の関係(Abbey-Doukas)。適応的市場仮説どおり、公開された単純ルールほど減衰が速い。

### Cited Findings
- Neely, Weller & Ulrich (2009, JFQA) "The Adaptive Markets Hypothesis: Evidence from the Foreign Exchange Market": 過去に研究されたルールの真のアウトオブサンプル検定。1970–80 年代の超過リターンは本物でデータマイニングではないが、**フィルタ・移動平均ルールの利益機会は 1990 年代初頭までに消失**。チャネル・ARIMA・遺伝的プログラミング・マルコフ・ルールのリターンも低下したが「おそらく完全には消えていない」。EMH ではなく適応的市場仮説(Lo 2004)と整合 — [St. Louis Fed WP 2006-046](https://files.stlouisfed.org/files/htdocs/wp/2006/2006-046.pdf); [Cambridge/JFQA](https://www.cambridge.org/core/product/9D336CDCA83233819EB5CDD0F4BC0DAA); [Iowa](https://iro.uiowa.edu/esploro/outputs/journalArticle/The-Adaptive-Markets-Hypothesis-Evidence-from/9984963054302771)
- 同著者らの後続レビュー(St. Louis Fed WP 2011-021): 円/ドルに適用したフィルタ・移動平均ルールの 1 年ローリング平均年率リターン(1976–2010 年)は、**1990 年代初頭まで概ね黒字、それ以降は概ね赤字**。他の 3 ドル・ペアも同様 — [St. Louis Fed WP 2011-021](https://files.stlouisfed.org/files/htdocs/wp/2011/2011-021.pdf)
- Hsu, Taylor & Wang (2016): 21,195 ルールのステップワイズ検定後も、先進国 9 通貨中 3 通貨・新興国 21 通貨中 10 通貨で 90% 信頼の予測可能性、円で年率シャープ比 0.75(コスト片道 0.045%)。時間とともに有効性が弱まるかも検定した(結果の数値は未確認) — [CXO Advisory 要約](https://cxoadvisory.com/technical-trading/updated-comprehensive-long-term-test-of-technical-currency-trading); [WUSTL](https://profiles.wustl.edu/en/publications/technical-trading-is-it-still-beating-the-foreign-exchange-market/)
- 通貨モメンタムの減衰: 修士論文 "Momentary loss: The disappearance of cross-sectional currency momentum"(Aalto 大学)は、クロスセクション通貨モメンタムの消失を主題とする(未読、査読なし) — [Aaltodoc](https://aaltodoc.aalto.fi/items/0db57fa9-e5c6-4b7d-9dd8-1ae15a8940c6)
- Menkhoff et al. (2012): 通貨モメンタムの利益は「高イディオシンクラティック・ボラ、高カントリーリスク、取引コストの高い通貨」に集中し、裁定限界が利益の持続を説明 — [BIS WP 366](https://www.bis.org/publ/work366.pdf)
- 取引コストを無視した評価のバイアス: "No pain, no gain: You should always incorporate trading costs for a bias-free evaluation of trading rule overperformance"(Economics Letters 2022) — [IDEAS](https://ideas.repec.org:443/a/eee/ecolet/v216y2022ics0165176522001720.html)
- Abbey & Doukas "Is Technical Analysis Profitable for Individual Currency Traders?"(J. Portfolio Management 2012): 個人通貨トレーダーのデータで、**テクニカル分析の利用は成績と負の関係**。周知のテクニカル指標を使う個人トレーダーは成績が低下 — [論文 PDF](https://www.efmaefm.org/0DOUKAS/publications/pdf/IsTechnicalAnalysisProfitable_12-14-2011.pdf)
- 個人 FX/CFD トレーダーの損失率: ブローカー開示で 74–89% が損失、CFTC データでも 70–80% が損失(二次集計) — [Lambda Finance](https://www.lambdafin.com/articles/what-percent-of-forex-traders-are-profitable)
- 日本の店頭 FX 市場規模(金融先物取引業協会データ): 2024 年 12 月の店頭 FX 取引金額 1,047 兆円(11 月比 −13.3%)、11 月 1,208 兆円、1 月 1,267 兆円 — [外為どっとコム(FFAJ データ引用)](https://www.gaitame.com/media/entry/2025/01/28/142716); FFAJ 調査 PR(2018) — [FFAJ](https://www.ffaj.or.jp/wp-content/uploads/2018/09/20180927_PR.pdf)
- AQR は 2010 年代のトレンド低迷について「クラウディングや非効率性の低下ではなく値動きの小ささ」と反論しており、減衰の原因は論争中 — [AQR](https://www.aqr.com/Insights/Research/Journal-Article/You-Cant-Always-Trend-When-You-Want)

### Inferences
- 減衰の階層: 「最も単純で最も有名なルール」(日足の移動平均クロス、フィルタ)→1990 年代に消失。「中期 TSMOM/ボラ調整/分散」→2010 年代に低下(SR 0.05)したが 2022 年・2026 年に復活。「あまり研究されていないルール」→部分的に残存。EMA50/200 + ADX + Donchian という組合せは「最も有名なルール群」に属し、Neely らの枠組みでは最も減衰が進んだカテゴリーである。
- Abbey–Doukas の結果は、個人がテクニカル指標を使うこと自体が負の選択効果(過剰取引・コスト)を伴うことを示唆する。自動化は裁量ミスを減らすが、エッジの不在を補わない。
- Hsu-Taylor-Wang の円 0.75 は「2 万ルールから事後的に選んだ最良ルールのインサンプル値(ステップワイズ補正あり)」であり、事前に選んだ 1 ルールの期待値はこれを下回る。

### Gaps
- Hsu-Taylor-Wang のサブ期間(2000 年代以降)での円ルールのシャープ比低下の数値: 未確認。
- 公表後の FX アノマリー減衰を体系的に測った McLean-Pontiff 型の FX 研究: 検索で特定できず。
- 日本の個人 FX 投資家の年間損益分布(FFAJ/金融庁の公式統計): 検索では取得できず。

---

## KQ8. 個人の単一ペア日中トレンド・システムの現実的なネット・シャープ比 — 取引頻度と 1 取引あたり必要エッジ(pips)

### Takeaway
学術的・実務的証拠を総合すると、USD/JPY 単独・日中足のトレンド・システムが長期に維持できるネット・シャープ比の現実的な上限は 0.3–0.8 程度(分散 CTA 指数 0.3、円最良ルール 0.75 グロス)で、月次勝率は 53–59%。「毎月黒字」(勝率 95%、SR 5.7)を 1 取引の損益標準偏差 25pips・年 480 取引で達成するには 1 取引あたりネット 6.5pips、グロス約 7.2pips のエッジが必要で、これは学術的に確認された FX エッジ(年率数%)の桁を超える。

### Cited Findings
- コストはグロス・エッジの半分を奪う: 通貨モメンタム最良戦略はグロス 6–10% に対しネット約 4% — [CXO Advisory(Menkhoff et al. 要約)](https://www.cxoadvisory.com/momentum-investing/momentum-investing-for-currencies); Hsu-Taylor-Wang の先進国コスト仮定は片道 0.045%(USD/JPY 150 円で約 6.8 銭 ≈ 6.8pips の往復に相当し、個人の 0.2pip スプレッドよりはるかに保守的) — [CXO Advisory](https://cxoadvisory.com/technical-trading/updated-comprehensive-long-term-test-of-technical-currency-trading)
- 分散の効果: Hurst-Ooi-Pedersen の 0.4(ネット)は 67 市場の分散後、SG Trend の 0.30 は 10 社・数百市場の分散後の値。Baltas-Kosowski の 1.2 超は 71 契約・3 周波数の分散かつグロス — [CXO Advisory](https://www.cxoadvisory.com/momentum-investing/trend-following-with-intrinsic-momentum-over-the-very-long-run); [Invesco](https://invesco.com/content/dam/invesco/emea/en/pdf/RRE_2024_Q2_NavigatingMomentum.pdf); [CAIA](https://caia.org/node/4984)
- 単一通貨の最良値: 円の最良テクニカル・ルールで年率シャープ比 0.75(グロス、45 年、事後選択) — [CXO Advisory](https://cxoadvisory.com/technical-trading/updated-comprehensive-long-term-test-of-technical-currency-trading)
- 個人の実績: テクニカル分析利用は個人通貨トレーダーの成績と負の関係 — [Abbey & Doukas](https://www.efmaefm.org/0DOUKAS/publications/pdf/IsTechnicalAnalysisProfitable_12-14-2011.pdf); 個人 FX/CFD の 70–89% が損失 — [Lambda Finance](https://www.lambdafin.com/articles/what-percent-of-forex-traders-are-profitable)
- 計算(本調査、仮定: 往復コスト 0.7pips = スプレッド 0.2 + スリッページ 0.5、1 取引の損益標準偏差 s pips、年間取引数 N、年率 SR = (ネット・エッジ/s)×√N):
  - s=25pips、N=480/年: SR 1.0 にネット 1.1pips(グロス 1.8)、**SR 2.34(勝率 75%)にネット 2.7pips(グロス 3.4)**、SR 3.59(85%)にネット 4.1pips(グロス 4.8)、SR 5.70(95%)にネット 6.5pips(グロス 7.2)。年間コストドラッグは 336pips ≈ 想定元本の 2.2%(150 円換算)
  - s=25pips、N=960/年(1 分足で頻度倍): SR 5.70 に必要なグロス・エッジは 5.3pips に下がるが、年間コストドラッグは 672pips ≈ 4.5%
  - s=35pips(ATR×2 ストップ + ×2.5 トレール、ATR 15pips 級)、N=240/年: SR 2.34 にグロス 6.0pips、SR 5.70 にグロス 13.6pips
  - 参考: SR 0.75 の円最良ルールの月次勝率 58.6%、P/E の FX 戦略ファンド(SR 0.25)52.9%(出典なし、本調査の計算)

### Inferences
- 必要エッジの桁感: 年率シャープ比 5.7 を 480 取引で達成するには 1 取引あたりネット 6.5pips、つまり 1 年で約 3,100pips(≈31 円、元本比約 21%)を**標準偏差 25pips の損益で**稼ぐ必要がある。これは平均/標準偏差比 0.26/取引に相当し、Gao らの日中モメンタム(R² 0.018 ≈ 相関 0.13)や Menkhoff のネット 4%/年より 1 桁大きい。学術的証拠のどこにも、この水準の持続的エッジは存在しない。
- 現実的な設計目標: 単一ペア・日中トレンドなら「ネット SR 0.3–0.8、月次勝率 53–59%、年間勝率 65–80%、最大ドローダウンは年率ボラの 1.5–2 倍」を前提に資金管理を組むのが証拠と整合的。勝率を上げる最も確実な手段は、(a) 時間軸を学術的に支持される日足–週足(1–12 か月ルックバック)へ延ばす、(b) 他通貨ペア・他資産へ分散する、(c) トレンドにキャリー・バリュー等の低相関シグナルを加える、の 3 つで、いずれも「USD/JPY 1 本の日中」という制約の緩和を要する。
- 1 分足の押し目再エントリーと 50% 部分利確は、期待値ではなく損益分布の形(勝率↑・ペイオフ比↓)を変える操作であり、シャープ比を本質的に上げる証拠はない。部分利確はトレンド戦略の収益源(少数の大勝ち)を削る方向に働くため、月次勝率を数%上げる代償に年率リターンを下げる可能性が高い。
- 「ADX>20・EMA50/200 順張り・Donchian ブレイク」の組合せはすべて公開済みの古典ルールで、Neely らの分類では最も減衰したカテゴリー。これらでアウトオブサンプルに SR 1 超を期待する根拠は見当たらない。

### Gaps
- Robert Carver(qoppac)の「単一銘柄トレンドの期待 SR ≈ 0.2–0.3、分散で 0.8–1.0」という実務ヒューリスティックの一次ソース: 検索でブログ本文を取得できず、本ノートでは引用していない(著書 "Systematic Trading" が出典とされる)。
- 個人向けブローカーの USD/JPY 実効スリッページ(1 分足・15 分足執行)の実測研究: 見つからず。本ノートの 0.5pips は仮定。
- 15 分足 Donchian-20 / 1 時間足 EMA50/200 + ADX の通貨でのアウトオブサンプル成績を報告した査読済み研究: 見つからず(存在しない可能性が高い)。
- 2026 年版レビュー論文 "The Science and Practice of Trend-Following Systems" の単一銘柄 vs 分散のシャープ比の数値: 遮断により未読。
