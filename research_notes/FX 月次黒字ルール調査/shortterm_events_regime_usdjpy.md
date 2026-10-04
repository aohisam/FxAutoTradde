# 超短期予測可能性・イベント/ボラティリティ・レジームフィルター・USD/JPY 2022–2026 相場環境（システム設計者向け調査ノート）

*調査日: 2026-10-04（JST）。対象システム: 国内業者（スプレッド 0.2銭）経由の USD/JPY ボット。tick ベースのスキャルピング ML パイプライン（ロジスティック型モデル、最大保有時間内の TP/SL ラベル、purged walk-forward、スプレッド z スコアフィルター、JST ブラックアウト、クールダウン、日次損失停止）と 1H/15m/1m トレンド・ブレイクアウト・プルバック戦略。*

*検証状況の注記（重要）: 本セッションでは外部サイトの本文取得（WebFetch）が全ドメインでネットワーク側に遮断され、Web 検索も途中で上限に達した。以下の「Cited Findings」は、検索エンジンが返した各ページの要約・スニペットに基づく（記号 ☆）。一次資料（財務省・日銀・FRB・BIS・論文 PDF）の本文は直接確認できていないため、数値は最終レポート化の前に各 URL で再確認すること。特に 2025–2026 年の事実（介入・政策金利）は、複数の独立したページ要約が一致したものだけを採用し、一致しないものは Gaps に落とした。各項目には可能な範囲で【期間】【サンプル】【コスト込か】を付した。*

*現行設定との対応（configs/scalping_jforex.yaml, configs/backtest_fx_breakout.yaml より）: スキャル TP 1.6 / SL 1.2 pips、max_hold 90 秒、往復スリッページ 0.15 pips、max_spread 0.6 pips、max_spread_z 4.0、cooldown 5 秒、max_trades/day 120、日次損失 10 万円で停止、blackout 05:55–06:10（rollover）/21:25–21:40（manual_news_window）、purge_seconds 未設定（null）。ブレイクアウト側は ADX(14)≥20、ATR パーセンタイル≥0.20、スプレッド 80 パーセンタイル閾値、イベントフィルター無効。*

---

## 1. マイクロストラクチャ／1 秒〜1 分の予測可能性（スプレッドを払う側にとって何が残るか）

### Takeaway
学術的証拠は「超短期（秒〜分）の統計的予測可能性（負の自己相関・三角裁定）は存在したが、アルゴ取引の普及でほぼ消え、残った分はスプレッド／ティックサイズ以下で取れない」で一致している。方向的中率 51–55% は 0.2 銭スプレッド＋スリッページの下では、1 トレードで狙う値幅が総コストの 5〜10 倍以上でない限り純益にならず、小売スキャルピング（保有 5 分未満）に正の期待値があるという査読済み証拠は見つからなかった。

### Cited Findings
- ☆ Chaboud, Chiquoine, Hjalmarsson & Vega「Rise of the Machines: Algorithmic Trading in the Foreign Exchange Market」(Journal of Finance 2014)。EBS のインターディーラー高頻度データ（主要 3 通貨ペア、コンピュータ発注と人間発注を分で識別。初期版は 2006–2007 年、公刊版は 2003–2007 年とされる）。結論: アルゴ取引（AT）は価格効率の 2 指標、すなわち「三角裁定機会の頻度」と「高頻度リターンの自己相関」を改善（減少）させる。裁定機会の減少は主にコンピュータの流動性テイク、自己相関の減少はアルゴの流動性供給による。【コスト: 分析対象外（効率性指標）】 — [Bank of Canada 版 PDF](https://www.bankofcanada.ca/wp-content/uploads/2012/11/authors-rise-machines-algorithmic.pdf); [NBER 会議版](https://projects.nber.org/confer/2009/mms09/vega.pdf); [SUFE 要約](https://academicnewsletter.sufe.edu.cn/info/360092)
- ☆ 同論文の追加知見: アルゴの注文は互いに相関（戦略の多様性が低い）、長期的な為替リターン分散の大半は非アルゴ（人間）のオーダーフローが説明（人間の方が情報優位）、アルゴのオーダーフローに対して価格が一時的に過剰反応するケースがある。 — [NBER 会議版](https://projects.nber.org/confer/2009/mms09/vega.pdf); [smallake.kr 掲載 PDF](https://smallake.kr/wp-content/uploads/2014/06/vega_chaboud_chiquoine_hjalmarsson_paper_hft.pdf)
- ☆ Evans & Lyons「Order Flow and Exchange Rate Dynamics」(JPE 2002)。【期間: 1996 年 5–8 月の日次、DM/USD・JPY/USD】日次の DM/USD 対数変化の回帰で R² が 60% 超、純ドル買い 10 億ドルでドルの DM 建て価格が 0.5% 上昇。 — [NBER w7317 PDF](https://www.nber.org/papers/w7317.pdf); [JPE 版 PDF](https://www.johnhcochrane.com/s/evans_lyons_jpe.pdf); [BIS Papers 2 掲載版](https://www.bis.org/publ/bppdf/bispap02j.pdf)
- ☆ Zhou「High-Frequency Data and Volatility in Foreign-Exchange Rates」(JBES 1996)。【期間: 1990 年代前半、Olsen 社の DM/USD tick】高頻度リターンは極めて強い負の 1 次自己相関を示す（マイクロストラクチャ・ノイズ＝ビッド・アスク・バウンス）。 — [Zhou JBES 1996 PDF](https://public.econ.duke.edu/~get/browse/courses/201/spr12/DOWNLOADS/MicroStructure/Zhou-JBES-1996.pdf)
- ☆ Roll (1984) 型の説明: 取引価格ベースのリターンは bid-ask bounce により負の自己相関を持つ（「見かけの平均回帰」）。tick データでは個別株でも FX でも超高頻度で負の自己相関が観察される。 — [CFS Working Paper 569](https://gfk-cfs.de/media/CFS_WP_569.pdf); [FernUni Hagen CESA WP 14](https://www.fernuni-hagen.de/igas/docs/cesa_wp_14.pdf)
- ☆ 「DEM/USD の 40 秒未満のラグで負の自己相関が観察されるが、潜在利益が規制上のティックサイズより小さいため投機で利用不能」【出典特定は検索要約ベースで未確認（上記 CFS/FernUni/fuw.edu.pl のいずれか）】 — [fuw.edu.pl pre046119](https://fuw.edu.pl/tl_files/studia/materialy/ef/pre046119.pdf); [CFS WP 569](https://gfk-cfs.de/media/CFS_WP_569.pdf)
- ☆ 電子インターディーラー FX（1/100 秒分解能の約定データ）の再検証でも DM/USD リターンに有意な負の自己相関。 — [A reexamination of the market efficiency hypothesis: Evidence from an electronic intra-day, inter-dealer FX market（Melody Cheng）](https://northampton.academia.edu/MelodyCheng)
- ☆ USD/JPY は日次出来高の約 1/3 が東京時間に入る（EUR/USD 17%、GBP/USD 16% と対照的）。重複を除くとロンドン＞NY＞シドニー＞東京の順で出来高が多い。【OANDA 教育ページ、期間不明、コスト無関係】 — [OANDA: Best time to trade forex – volume insights](https://www.oanda.com/bvi-en/skills-and-insights/education/market-timing-and-volatility/when-to-trade/best-time-to-trade-forex-volume-insights/)
- ☆ 東京時間の USD/JPY は「1 時間あたり概ね 16 pips」のレンジ（教育サイトの目安値、期間不明）。 — [BabyPips: Tokyo session](https://www.babypips.com/it/learn/forex/can-trade-forex-tokyo-session)
- ☆ 小売 FX/CFD の損失口座比率: ESMA 系の開示で 74–89%、英 FCA 規制業者の開示で 76–84%、ポーランド KNF 調査で約 80% が損失（「ルーレット台」評）。保有時間別・スキャルピング別の内訳は見つからず。【期間: 業者開示は四半期/年次；コスト込（実損益）】 — [Finance Magnates: KNF survey](https://www.financemagnates.com/forex/brokers/knf-survey-polish-forex-market-shows-roulette-table-concept-80-lose-money/); [Finance Magnates: 75% of retail investors are 'capital donors'](https://www.financemagnates.com/forex/analysis-75-of-retail-investors-are-essentially-capital-donors/); [lambdafin まとめ](https://www.lambdafin.com/articles/what-percent-of-forex-traders-are-profitable)

### Inferences
- 損益分岐の算術（当方計算）。往復コスト c = スプレッド 0.2 + 往復スリッページ 0.15（現行設定）= 0.35 pips とすると、TP 1.6 / SL 1.2 の構造で損益分岐勝率 p* = (1.2+0.35)/(1.6+1.2) = **55.4%**（スプレッドのみなら 50.0%）。対称 1.0/1.0 pips なら p* = 67.5%（スプレッドのみで 60%）。TP/SL を 5/5 pips にして初めて p* ≈ 53.5% まで下がる。すなわち論文で見かける「51–55% の方向的中」は、1 トレードの狙い幅がコストの 5〜10 倍以上でなければ純益にならない。max_hold 90 秒でのタイムアウト決済（中途半端な損益）が混ざると分岐点はさらに上がる。
- Chaboud et al. の含意: 1 分以下の自己相関・裁定は「アルゴが流動性を供給して消す」対象であり、スプレッドを払う小売側は構造的に不利。残る可能性があるのは (a) オーダーフロー情報（小売には観測不能に近い）、(b) 時間帯・イベントに紐づくボラティリティ構造（第 5・7 節）、(c) スプレッドが広がる局面を「避ける」ことによる損失回避、の 3 つ。
- bid-ask bounce 対策: tick の mid を使ってもクォートの片側更新でノイズが残る。1 秒バー（現行 bar_rule: 1s）で負の自己相関が強く出る場合、それはシグナルではなくノイズの可能性が高い。ラベル付けを「bid で買い・ask で売り」（実効価格）で行い、特徴量側も実効価格差（≥スプレッド）を超えた動きだけを使うべき。
- 小売の損失比率 74–89% は「スキャルピングが悪い」ことの直接証明ではないが、保有時間が短いほど総コスト／総値幅比が上がるため、短期ほど分布が左に寄ると推論できる。

### Gaps
- Hasbrouck & Saar「Low-Latency Trading」(2013) は取得できず（なお同論文は NASDAQ 株式データで、FX ではない点に注意。低遅延取引がスプレッド縮小・短期ボラ低下に寄与という結論は本セッションで未確認）。
- Andersen, Bollerslev, Diebold & Vega (2003) など「発表後 5 分以内に価格が反応し尽くす」系の証拠は検索上限により未取得。
- 「保有 5 分未満の小売スキャルピングの期待値」を直接測った査読研究・業者開示は見つからなかった。CFTC 四半期の米国小売 FX 口座収益性データ（概ね 3 割前後が黒字と報じられる）も本セッションでは未確認。
- USD/JPY 固有の tick 自己相関・裁定頻度の最近（2022–2026）の計測値は見つからなかった。自社 tick_cache で 1 秒・5 秒・30 秒リターンの自己相関と「スプレッドを超える反転の頻度」を計測するのが最短。

---

## 2. FX 予測への機械学習（報告精度 vs コスト控除後の現実、リーク/過学習、purged walk-forward、特徴量ファミリー）

### Takeaway
サーベイは「深層学習が従来手法を上回る」と報告するが、ほとんどが MSE/精度ベースでコスト控除後の損益を示さず、株式でも「コスト前は日次 0.46%・シャープ 5.8」級の結果がコスト後・近年で消える例が典型。López de Prado 系の purge/embargo、DSR、PBO は「複数試行による選択バイアス」を割り引くための道具で、現行設定の purge_seconds=null は即修正対象。持続的価値の証拠があるのは（日次の）オーダーフロー、ボラティリティと時間帯の季節性で、超短期の価格パターン特徴は証拠が弱い。

### Cited Findings
- ☆ Sezer, Gudelek & Ozbayoglu「Financial time series forecasting with deep learning: A systematic literature review: 2005–2019」(Applied Soft Computing 90, 2020, 106181)。株価指数・為替・商品に分類し、CNN/DBN/LSTM 別に整理。報告ベースでは DL が従来 ML を「大幅に上回る」とする。【コスト: 多くの被引用研究はコスト控除なし】 — [arXiv 1911.13288](https://arxiv.org/abs/1911.13288); [TOBB ETU リポジトリ](https://gcris.etu.edu.tr/handle/20.500.11851/3838)
- ☆ Fischer & Krauss（FAU Discussion Paper 11/2017、後に EJOR 2018）LSTM による S&P500 構成銘柄の方向予測: 【期間: 1992–2015】取引コスト前で日次 0.46%・シャープ 5.8。コスト前後の区別が本質であることの例示（株式研究であり FX ではない）。 — [FAU IWF 11/2017 PDF](https://iwf.rw.fau.de/files/2015/12/11-2017.pdf)
- ☆ 2026 年の SSRN 論文（ID 7405518、ミラー掲載）: 取引コストのある FX で、予測モデルを変えずに「高ボラ局面に執行を限定する（volatility gating）」だけで負の期待値の戦略が正に転じ得ると主張。【一次ページ未確認】 — [SSRN 7405518（ミラー PDF）](https://pjtmgspukcuqswukozrh.supabase.co/storage/v1/object/public/mukisoft-media/research/b0e49f0e-5117-469a-a224-1ccebf4fc46b/1788670648049-ssrn-7405518.pdf)
- ☆ EUR/USD の LSTM 研究群は「適合度・予測誤差」中心で、現実的コスト後の収益性を検証したものは少数という整理。 — [E3S Web of Conferences 2023 (ICIES)](https://www.e3s-conferences.org/10.1051/e3sconf/202341201069); [AAU 修士論文](https://vbn.aau.dk/ws/files/717518836/Master_Thesis.pdf)
- ☆ López de Prado「The 10 Reasons Most Machine Learning Funds Fail」(JPM 2018) の方法論ファミリー: 情報駆動バー、トリプルバリア・ラベリング、メタラベリング、サンプル一意性、purged k-fold、combinatorial purged CV、Deflated Sharpe Ratio (DSR)、Probability of Backtest Overfitting (PBO)。 — [TradingStrategy.ai: Backtesting & Research Methodology](https://tradingstrategy.ai/docs/learn/backtesting.html)
- ☆ Purged CV の定義: テスト観測のラベル窓と重なる訓練観測を削除（purge）し、さらにテスト区間の直後の訓練観測を一定幅落とす（embargo）ことで系列相関由来のリークを吸収する。 — [LuxAlgo: Purged Cross-validation](https://www.luxalgo.com/library/concept/purged-cross-validation.md)
- ☆ Bailey & López de Prado「The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting and Non-Normality」: 複数試行の選択バイアスと非正規性によるシャープ比の過大評価を補正。 — [SSRN 2460551](https://papers.ssrn.com/abstract=2460551)
- ☆ Bailey, Borwein, López de Prado & Zhu「The Probability of Backtest Overfitting」: 試行回数に応じて結果を割り引く PBO。 — [SSRN 2326253](https://papers.ssrn.com/abstract=2326253)
- ☆ 実務側の整理: バックテストのシャープ 4 がライブで 0.5 になる典型要因（選択バイアス、ルックアヘッド、執行コスト過小評価）。 — [Why a backtest Sharpe of 4 becomes 0.5 live](https://www.techinterview.org/post/3233477314/why-backtest-sharpe-collapses-live/?format=md); [MQL5: Overfitting in Expert Advisors](https://www.mql5.com/en/blogs/post/773991)
- ☆ 日中ボラティリティの季節性（EUR/USD）: 東京→ロンドン→NY オープンにかけてボラが上昇し NY 時間を通じて低下、最高はロンドン–NY 重複、最低はシドニー–東京重複。【学術論文、期間はページ未確認】 — [Model Free Evaluation of Intraday Periodicities of Exchange Rate Return Volatility](https://dr.lib.sjp.ac.lk/items/04f21dd4-fbb1-4886-84cb-3ea1ce9ce5f5); [QuantHedge: Seasonality of Intraday Volatility in FX](https://quanthedge.substack.com/p/seasonality-of-intraday-volatility)
- ☆ オーダーフロー（第 1 節 Evans & Lyons; Chaboud et al.: 非アルゴのフローが長期分散の大半を説明）は、特徴量として最も一貫した説明力を持つが、日次・インターディーラーのデータでの証拠であり、小売の 1 秒バーで同じ力があるとは示されていない。 — [NBER w7317](https://www.nber.org/papers/w7317.pdf); [Bank of Canada 版 Chaboud et al.](https://www.bankofcanada.ca/wp-content/uploads/2012/11/authors-rise-machines-algorithmic.pdf)

### Inferences
- 現行の `purge_seconds: null` は、TP/SL ラベルが最大 90 秒先を見るため、訓練/検証/テスト境界で最低 90 秒＋特徴量ルックバックの purge と、系列相関を切るための embargo（数分〜数十分）が必要。walk-forward（20/5/5 日）でも各フォールド境界に同じ処理を入れるべき。
- `threshold_grid` 11 点 × 複数フォールド × 複数モデル候補は「多重試行」なので、validation の profit factor 1.05 のような小さなマージンは DSR/PBO で割り引くとゼロに近い可能性が高い。試行数を記録し、DSR を昇格ゲートに加えるのが筋。
- コスト前精度で評価しない: ラベルを「実効価格（買いは ask 建て・売りは bid 建て）で TP/SL 到達」に統一し、閾値選択も net pips で行う（現行 `threshold_selection_method: replay` はこの方向で妥当）。
- 特徴量の優先順位（証拠の強さ順）: (1) 実現ボラ・スプレッド・時間帯（季節性は頑健）、(2) マルチタイムフレームのトレンド状態（第 5 節の季節性と組み合わせ）、(3) 板/約定由来のフロー代理変数（小売 tick では bid/ask 更新の非対称性程度）、(4) 1 秒〜1 分の価格パターン（bid-ask bounce で汚染されやすく最も弱い）。
- 「volatility gating で負→正」という 2026 年論文の主張は、第 5 節のレジームフィルターと整合的だが一次確認できていないため、採用は自社データで再現してから。

### Gaps
- 1 分足 FX の方向予測で「精度 52–55% だがスプレッド後は赤字」を定量的に示した再現研究を、検索上限のため特定できなかった（存在は複数の二次情報が示唆）。
- López de Prado『Advances in Financial Machine Learning』の原著本文（トリプルバリア、メタラベリング、CPCV の具体式）は未取得。二次解説のみ。
- Transformer/LSTM の USD/JPY 固有のコスト後成績を報告した研究は未発見。
- arXiv 2405.08045（検索でヒット、FX の ML とコストに関する論文の可能性）は内容未確認。

---

## 3. イベントとブラックアウト（指標反応の実測、スプレッド拡大、週末ギャップ、ロールオーバー、月末）

### Takeaway
国内業者の「原則固定（例外あり）」は「取引時間の 95% 以上で提示」の定義であり、例外時間帯として月曜 7–8 時、夏時間 6 時前後／冬時間 7 時前後（NY クローズ＝ロールオーバー）、指標時が明示されている点が最も確かな「証拠」。週末ギャップは USD/JPY 144 週で平均 23.3 pips・中央値 16.4 pips・10 pips 超が 68% の週、100 pips 超が 2%。指標の初動 1 分値幅や ±15/30/60 分ブラックアウトの損益効果を定量化した信頼できる資料は見つからず、現行の固定 JST 窓は米国夏時間/冬時間でずれる構造的欠陥がある。

### Cited Findings
- ☆ スプレッド「原則固定（例外あり）」の定義: 広告スプレッドを取引可能時間の 95% 以上で提示する状態。例外として、月曜午前 7–8 時頃、夏時間の午前 6 時前後（冬時間の午前 7 時前後）など流動性低下時、指標発表時に拡大し得る。各社は「提示率」を公表。 — [soico.jp: 原則固定スプレッドの解説](https://www.soico.jp/no1/news/fx/26339); [Myforex コラム](https://myforex.com/ja/column/myf26010903.html); [外為どっとコム スプレッドに関するリスク PDF](https://www-hp.fxtrade.co.jp/doc/spread_risk.pdf); [みんなのFX お知らせ](https://min-fx.jp/press/862/)
- ☆ 週末ギャップ（USD/JPY、144 週集計、2025 年 10 月を含む直近約 2.8 年）: 平均窓幅 23.3 pips、中央値 16.4 pips、10 pips 超は 68% の週、100 pips 超は 2%、5 pips 以上は 123/144 週（85%）、最大 183.7 pips（2025-10-06）。【コスト無関係】 — [soico.jp: 週明け窓の統計](https://www.soico.jp/no1/news/fx/25531)
- ☆ 窓埋め確率「70% 超」（Badura 氏の研究として紹介、時間軸・通貨不明の二次情報） — [CREX: FX の窓の原因と戦略](https://crexgroup.com/ja/fx/analysis-strategy/fx-gap-causes-strategy/)
- ☆ 2025-10-06（月）: 自民党総裁選での高市氏勝利（10/4）を受け、ドル/円は前週末比約 3 円の円安で早朝に 149 円付近に上窓を開けて始まり、正午過ぎに 150 円台（2 カ月ぶり高値）。短期金利先物の日銀 10 月利上げ確率が 5 割台→2 割台へ急低下。 — [OANDA Lab 2025-10-07](https://www.oanda.jp/lab-education/market_news/2025_10_07_usdjpy/); [みずほ銀行 Flash 2025-10-06](https://www.mizuhobank.co.jp/forex/pdf/ma/flash251006.pdf); [SBI 証券 市況 2025-10-06](https://search.sbisec.co.jp/v2/popwin/info/fund/report/fund_shiryou_market_251006_11.pdf)
- ☆ 2024-08-05（月）: ドル/円は 145.59 で寄り付き、東京時間に 144 円台→1 月以来の安値 141.68 まで急落（寄り付き比約 390 pips）。VIX は 8/2 の 23 から 8/5 に 65 超へ。 — [三菱UFJ銀行 FX Daily 2024-08-05](https://www.bk.mufg.jp/report/dmr2024/FXDaily20240805.pdf); [Business Insider Japan](https://www.businessinsider.jp/article/291644/); [みんかぶFX ニュース](https://fx.minkabu.jp/news/378453); [BIS Bulletin 90（本文未取得）](https://www.bis.org/publ/bisbull90.pdf)
- ☆ 日銀会合日の値幅例: 2025 年 1 月会合（1/24 公表、25bp 利上げ）では結果公表後 156.40 近辺へ上昇、植田総裁会見中に 154.85 へ下落（約 155 pips の往復）。 — [三菱UFJ銀行 FX Daily 2025-01-24](https://www.bk.mufg.jp/report/dmr2025/FXDaily20250124.pdf); [OANDA Lab: 植田総裁「退屈」な会見とドル円の変動幅](https://www.oanda.jp/lab-education/market_news/20250124_boj_noevent/)
- ☆ 2025-01-15: 植田総裁の「来週の会合で利上げを判断」との発言報道と米 CPI の後に円高が進み 156 円台半ば。 — [OANDA Lab 2025-01-16](https://www.oanda.jp/lab-education/market_news/2025_01_16_usdjpy/)
- ☆ 米雇用統計発表日のドル/円「過去 1 年の平均値幅は 0.688 円」（マネックス証券レポート、集計年はページ未確認）。 — [マネックス証券 FX 記事](https://media.monex.co.jp/articles/-/13132); [マネックス FX 戦略デイリー 2015-08-10](https://info.monex.co.jp/report/fx-strategy-daily/pdf/stdaily_20150810_1.pdf)
- ☆ 雇用統計日の個別事例は三菱UFJ銀行 FX Daily に多数（例: 2022-02-04、2023-06-02、2023-09-01、2024-06-07、2024-12-06）。 — [FX Daily 2024-06-07](https://www.bk.mufg.jp/report/dmr2024/FXDaily20240607.pdf); [FX Daily 2024-12-06](https://www.bk.mufg.jp/report/dmr2024/FXDaily20241206.pdf)
- ☆ 実務家バックテスト（EUR/USD ブレイクアウト）: 利益はロンドン 08:00–17:00 に集中し、オーバーナイトで大半を吐き出す。【コスト条件はページ未確認】 — [MQL5 blog: Why the Same Strategy Wins in London and Dies in Tokyo](https://www.mql5.com/en/blogs/post/774527)

### Inferences
- 現行 blackout `05:55–06:10 JST`（rollover）は米国夏時間（NY17:00 = JST 6:00）にしか合わない。米国冬時間（11 月第 1 日曜〜3 月第 2 日曜）では NY17:00 = JST 7:00 であり、業者開示の「冬時間 7 時前後」とも整合。ブラックアウトは「NY 17:00 ET ± α」を JST に変換して自動で動かすべき。
- 同様に `21:25–21:40 JST` の手動窓は米 8:30 ET 発表（雇用統計・CPI・小売売上高）の夏時間に対応し、冬時間は 22:30 JST にずれる。FOMC 声明 14:00 ET は 03:00（夏）/04:00（冬）JST、パウエル会見はその 30 分後。日銀決定は時刻不定（概ね 11:30–13:00 JST、昼休みをまたぐ）、植田総裁会見は 15:30 JST、東京都区部 CPI は 08:30 JST。（発表時刻は一般的な公表慣行で、本調査で一次確認はしていない）
- 週末ギャップ統計から: スキャル（最大 90 秒保有）は無関係だが、ブレイクアウト戦略（15 分足で最大 30 本保有＝7.5 時間）は金曜 NY 終盤に持ち越し得る。ATR ストップが 20–30 pips 程度なら「100 pips 超のギャップが年 1 回程度（2%）」でストップを飛び越えるため、金曜のクローズ前（JST 土曜 5–6 時）にフラット化するか、ギャップ耐性のあるサイズにする根拠になる。
- 指標ブラックアウトの損益効果は未証明なので、「発表前後 ±15/30/60 分を外す」はまず自社の約定ログで A/B する。少なくとも、スプレッド z スコア（max_spread_z 4.0）とスプレッド/ATR 比のフィルターが機能していれば、イベント直後の広スプレッドは自動的に避けられるはずで、固定窓は「指標直前の薄い板で約定しても直後に逆行する」ケースへの保険として位置づける。
- 2024-08-05 と 2025-10-06 の両方が「月曜の東京時間」に起きた点は、週明け東京時間のレジームチェック（ギャップ幅・ATR 急変）を入れる根拠。

### Gaps
- 米雇用統計・米 CPI・FOMC・日銀会合・東京 CPI・米小売売上高それぞれの「発表後 1 分・5 分の USD/JPY 値幅」を体系的に集計した資料は見つからなかった（自社 tick から容易に集計可能）。
- 国内業者の指標発表時・早朝のスプレッド拡大幅（銭）の実測値は見つからなかった（「提示率」のみ開示）。これも自社 tick で計測すべき最優先項目。
- ±15/30/60 分ブラックアウトの損益効果を測った研究・バックテストは発見できなかった。
- 月末・四半期末（ロンドンフィックス 24:00/25:00 JST）の USD/JPY の統計は検索上限により未調査。
- 週末ギャップ統計の集計期間の正確な開始日・データ提供元（どの業者のレート）はページ本文未取得のため不明。

---

## 4. 介入リスク（2022・2024・2026 年の記録、時刻、値幅、2025–2026 の状況、システム側の検知・対処）

### Takeaway
円買い介入は 2022 年（9/22、10/21・24）、2024 年（4/29、5/1、7/11・12）、2026 年（4/30・5/4・5/6 で計 11 兆 7,349 億円＝月次最大、7/30 単独＋7/31 日米協調＝円買い協調は 1998 年以来）と、いずれも「160 円前後の防衛ライン」「連休・祝日や欧米時間の薄商い」「米指標直後」に集中している。単日で 4〜9 円級の急落を生み、2026 年 7 月は 163.99（7/23 高値）→155.23（8/3）。システムは価格水準（158–165 円帯）・時間帯・ATR 急変・日次損失上限の 4 重で守るべきだが、介入の有無は公表まで分からないため「事前検知」ではなく「事後の損失制限」が現実的。

### Cited Findings
- ☆ 2022-09-22: 24 年ぶりの円買い介入、金額 2 兆 8,382 億円（9 月は 22 日のみ、当時の単日最大）。ドル/円 145.90 から 140 円台前半へ急落。ロンドン時間（JST 17 時以降）に実施。 — [時事: 24年ぶり円買い介入、2.8兆円](https://sp.m.jiji.com/english/show/22951); [日本経済研究センター 齋藤コラム](https://www.jcer.or.jp/j-column/column-saito/2022121.html); [マネーフォワード解説](https://media.moneyforward.com/articles/7933)
- ☆ 2022-10-21・10-24: 152 円接近局面で「覆面介入」。21 日は 5 兆円規模（当時最大）、21・24 日合計 6 兆 3,499 億円。（日次内訳 5 兆 6,202 億円／7,296 億円は財務省四半期公表値として広く報じられたが、本セッションでは財務省ページ未取得のため未確認） — [時事: 円買い介入、5兆円規模 21日、過去最大](https://sp.m.jiji.com/english/show/22692); [JCER](https://www.jcer.or.jp/?p=98913)
- ☆ 2024-04-29: 5 兆 9,185 億円（当時の単日最大）、2024-05-01（米時間、JST 5/2 早朝）: 3 兆 8,700 億円。4/26–5/29 の月次合計 9 兆 7,885 億円。大型連休中にドル/円が急騰する場面が複数回。 — [Bernama: Japan Spent Record 5.92 Trn Yen A Day](https://bernama.com/en/news.php?id=2326431); [OANDA Lab 為替介入解説](https://www.oanda.jp/lab-education/?p=120652); [SBI 証券 市況 2024-06-03](https://search.sbisec.co.jp/v2/popwin/info/fund/report/fund_shiryou_market_240603_06.pdf)
- ☆ 2026-04-30: 午後に年初来高値 160.72 を付けた後、2024 年 7 月以来の円買い介入で一時 155 円台へ急落。政府は 160 円を事実上の防衛ラインとしていたとの見方。背景は中東情勢（原油高）によるドル高。 — [NRI 木内コラム 2026-05-07](https://www.nri.com/jp/media/column/kiuchi/20260507.html); [外為どっとコム 5月見通し 2026-05-07](https://www.gaitame.com/media/entry/2026/05/07/194754); [明治安田アセット 為替見通し 2026-05-11](https://www.myam.co.jp/market/report/2026/20260511-2020098732.html)
- ☆ 2026 年 4/28–5/27 の介入総額 11 兆 7,349 億円（月次ベース過去最大、財務省 5/29 公表）。 — [秋田魁新報（共同）2026-05-29](https://www.sakigake.jp/news/article/20260529CO0155/); [OANDA Lab 2026-05-29](https://www.oanda.jp/lab-education/market_news/kn_2026052901001858/)
- ☆ 日次内訳（財務省四半期公表、2026-08-07 報道）: 4/30 = 6 兆 2,787 億円（円買い単日最大）、5/4 = 7,802 億円、5/6 = 4 兆 6,759 億円。 — [OANDA Lab: 為替介入、過去最大の6.2兆円](https://www.oanda.jp/lab-education/market_news/kn_2026080701000327/); [TechNews（台湾）2026-08-07](https://finance.technews.tw/2026/08/07/japan-yen-intervention-april-30-6-2-trillion-jpy-single-day-record/)
- ☆ 2026 年 4–5 月介入の効果評価: 「11 兆円介入はなぜ失敗したのか」（楽天証券メディア）、「円買い・ドル売り為替介入の限界」（大和総研 2026-07-03）、三井住友DS「11.7 兆円規模となった 2026 年 4 月～5 月の為替介入の効果を考える」。ドル/円はその後 7 月に 162 円台へ再上昇。 — [楽天証券トウシル](https://media.rakuten-sec.net/articles/-/52614); [大和総研 2026-07-03](https://www.dir.co.jp/report/research/capital-mkt/securities/20260703_025877.html); [三井住友DS 市川レポート 2026-06-01](https://www.smd-am.co.jp/market/ichikawa/2026/06/irepo260601/); [同 2026-07-02 ドル円は162円台に](https://www.smd-am.co.jp/market/ichikawa/2026/07/irepo260702/); [国際通貨研究所 nl2026.29](https://www.iima.or.jp/docs/newsletter/2026/nl2026.29.pdf)
- ☆ 2026-07-01: 一時 162.84（約 39 年半ぶり）。2026-07-23: 163.99（39 年 8 カ月ぶり高値）、164 円接近。 — [外為どっとコム 2026-08-14](https://www.gaitame.com/media/entry/2026/08/14/170440); [kabucho まとめ](https://kabucho.com/articles/japan-us-coordinated-yen-intervention-2026-07-31)
- ☆ 2026-07-30 夕方（JST）: 日本単独で推定約 8.4 兆円（単日最大推定、公式値は未公表/未確認）の円買い。2026-07-31（米東部時間）: 日米協調の円買い介入（円買いでの協調は 1998 年 6 月以来 28 年ぶり、2011 年の協調円売りから数えれば 15 年ぶり）。8/3 に片山財務大臣が声明で公表。ドル/円は一時 155 円台（8/3 安値 155.23）→ 8 月中旬には 159 円台へ戻る。 — [外為どっとコム 2026-08-03](https://www.gaitame.com/media/entry/2026/08/03/130539); [同 8月見通し 2026-08-03](https://www.gaitame.com/media/entry/2026/08/03/194457); [NRI 木内コラム 2026-08-03](https://www.nri.com/jp/media/column/kiuchi/20260803.html); [松井証券 マネサテ 2026-08-03](https://www.matsui.co.jp/money-satellite/column/fx/cl-2026080301_fx.html); [国際通貨研究所 nl2026.43（2026-08-12）](https://www.iima.or.jp/docs/newsletter/2026/nl2026.43.pdf); [野村證券: 米ドル円急落、一時155円台に](https://www.nomura.co.jp/wealthstyle/article/0715/); [三井住友DS 2026-08-12: 159円台に戻ったドル円](https://www.smd-am.co.jp/market/ichikawa/2026/08/irepo260812/); [usagi-giken: 2026年 為替介入の全記録](https://usagi-giken.com/wealth-management/politics/forex-intervention-gw-2026-answer-2/)
- ☆ 2026 年 8 月時点の見方: 「再び 160 円を上抜けた際の介入リスクは相応」「介入警戒が上値を抑える」（野村）、「160 円台で為替介入の警戒ライン」（第一生命経済研）。野村は 2026 年末見通しを 152.5 円に引き上げ（中東情勢によるドル高圧力）。 — [野村 2026年末見通し 152.5円](https://www.nomura.co.jp/wealthstyle/article/0676/); [第一生命経済研 588082](https://www.dlri.co.jp/report/macro/588082.html); [同 623005: 円買い介入の可能性とその効果](https://www.dlri.co.jp/report/macro/623005.html)
- ☆ 2024-08-05 の円キャリー巻き戻し（第 3 節参照）は介入ではないが、日銀 7/31 利上げ＋米景気不安で 1 日に約 4 円の下落を生んだ介入級イベント。 — [三菱UFJ銀行 FX Daily 2024-08-05](https://www.bk.mufg.jp/report/dmr2024/FXDaily20240805.pdf)

### Inferences
- 介入の「場所と時間」のパターン（本調査で確認できた範囲）: 価格は 2022 年 146/152 円、2024 年 160 円、2026 年 160–164 円と「節目かつ過去高値更新直後」。時間は 2022-09-22 が JST 17 時台、2026-04-30 が午後（連休入り前後）、2026-07-30 が夕方。2024 年は祝日（4/29）と米 FOMC 直後の早朝（5/2 JST）、米 CPI 直後（7/11）と報じられたが時刻は本セッション未確認。共通項は「日本の実需が薄く、海外勢の円売りが積み上がった局面」。
- 検知ロジックの実装案（推論）: (1) 水準プロキシ: 直近 2 年の介入帯（154–164 円）の上端 3 円以内では、ロング側のブレイクアウトに追加確認（ADX/出来高代理）を要求し、サイズを半減。(2) 速度プロキシ: 1 分 ATR が直近 20 日中央値の 5–10 倍、または 5 分で 100 pips 超の逆行を検知したら新規停止＋既存ポジはトレーリング強化。(3) 時間プロキシ: 日本の祝日・連休中の東京時間、米主要指標直後 30 分、JST 17–18 時と 22–24 時は介入既往時間帯として「ロングのみ」サイズ縮小。(4) 日次損失停止（現行 10 万円）は介入日に機能する最後の砦。
- スキャル（90 秒保有、SL 1.2 pips）にとって介入は「スプレッド拡大＋スリッページ」の問題であり、方向リスクより執行リスク。スプレッド z スコア（4.0）とスプレッド/平均比（2.5）で即停止し、クールダウンを 5 秒ではなく「スプレッド正常化まで」に延ばす方が合理的。
- 介入後は数週間で元の水準へ戻る（2024 年 5 月→7 月、2026 年 5 月→7 月、8 月→159 円台）ため、長期バイアス（ロング優位）の前提は維持しつつ、「介入直後 1–3 日は平均回帰、その後トレンド再開」という二相を想定したパラメータ（ATR パーセンタイル閾値を介入後に一時的に上げる等）が検討に値する。

### Gaps
- 2024-07-11（3 兆 1,678 億円）・07-12（2 兆 3,670 億円）、2022-10-21/24 の日次内訳、2024-04-29 の高値 160.17 と各介入の正確な JST 時刻は、財務省ページ・一次報道を開けず未確認。
- 2026-07-30/31 の公式介入額（財務省の 7/30–8/27 月次公表、2026-08-31 頃）および日米協調介入の米国側の取引内容（一部要約に「米財務省が円買い・ユーロ売り」とあるが整合性に疑問、要確認）。
- 2025 年中の介入の有無（本調査では 2025 年の実施報道は見当たらず、「2024 年 7 月以来」の表現から 2025 年は介入なしと推定されるが未確認）。
- 2026 年 8 月中旬以降〜10 月 4 日時点のドル/円水準と口先介入の状況は未取得。

---

## 5. ボラティリティ・レジームフィルター（ATR パーセンタイル、ボリンジャー幅、ADX、インプライド・ボラ、セッション別レンジ）

### Takeaway
「高ボラ・拡大局面でトレンド/ブレイクアウトが機能し、東京時間の圧縮レンジでは機能しない」という命題は、日中ボラの季節性（東京が最低、ロンドン–NY 重複が最高）と実務家バックテスト（ブレイクアウトの利益はロンドン時間に集中）で間接的に支持されるが、ATR パーセンタイル・ボリンジャー・スクイーズ・ADX 閾値の有効性を統計的に示した資料は見つからず、これらは「広く実践」止まり。USD/JPY の 1 カ月インプライド・ボラは 2025 年に概ね 9–12.4%（2024 年末〜2025 年初は約 10% で「直近安値圏」）。

### Cited Findings
- ☆ 日中ボラの季節性（EUR/USD）: 東京→ロンドン→NY オープンで上昇、NY 時間に低下、最高はロンドン–NY 重複、最低はシドニー–東京重複。流動性が時間帯で不均一なことが原因。 — [Model Free Evaluation of Intraday Periodicities of Exchange Rate Return Volatility](https://dr.lib.sjp.ac.lk/items/04f21dd4-fbb1-4886-84cb-3ea1ce9ce5f5); [QuantHedge Substack](https://quanthedge.substack.com/p/seasonality-of-intraday-volatility); [LuxAlgo: Trading Sessions](https://www.luxalgo.com/library/concept/trading-sessions/)
- ☆ 実務家バックテスト: EUR/USD のブレイクアウト戦略はロンドン 08:00–17:00 で稼ぎ、オーバーナイトで吐き出す。東京は「レンジ市場、EUR/USD は 20–30 pips の回廊」。 — [MQL5 blog](https://www.mql5.com/en/blogs/post/774527); [LuxAlgo: Session Open/close Behaviors](https://www.luxalgo.com/library/concept/session-open-close-behaviors.md)
- ☆ USD/JPY の時間帯別ドリフト（JST、4 時間ブロック、約 5 年の終値差分、pips 表示）: 7–11 時・19–23 時・3–7 時は円安（USD/JPY 上昇）傾向、11–15 時・15–19 時・23–3 時は円高傾向。【pips の大きさ・有意性はページ未取得；コスト考慮不明】 — [Zenn: ドル円の時間帯効果](https://zenn.dev/harukii/articles/4686886549bd10); [ニッセイ基礎研究所 42141](https://www.nli-research.co.jp/files/topics/42141_ext_18_0.pdf?site=nli); [同 42046](https://www.nli-research.co.jp/files/topics/42046_ext_18_0.pdf?site=nli)
- ☆ USD/JPY 1 カ月インプライド・ボラ（2025 年）: 1 月下旬 9.3→10.85（日銀会合前後）→9.85；3 月中旬にピーク 12.4 から 11.0 へ低下；「ボラティリティ指数は 12.5% と 2024 年 11 月以来の高水準」（2025 年 3 月）；2025 年中に FX オプションの IV は記録的低水準を付けた後に反発。 — [Tickmill FX Options Insights 2025-01-21](https://www.tickmill.com/blog/fx-options-insights-210125); [同 2025-03-12](https://www.tickmill.com/blog/fx-options-insights-120325); [同 2025-02-25](https://www.tickmill.com/blog/fx-options-insights-250225); [Continuum Economics: USD/JPY consolidating recent JPY-driven volatility](https://continuumeconomics.com/a/89ad2ea2/chart-usdjpy-update-consolidating-recent-jpy-driven-volatility)
- ☆ 2024 年末: USD/JPY の 1・3 カ月 IV は約 10% で直近安値圏、3 カ月 IV は実現ボラ対比で割安（Macro Hive）。2024 年 12 月の日銀会合前にはオーバーナイト IV が急騰したが 2024 年の高値には届かず。 — [Macro Hive FX Vol Tracker](https://macrohive.com/fx-vol-tracker/fx-volatility-tracker-are-usd-jpy-implieds-cheap); [MNI: Overnight vols spike but sub-2024 highs](https://marketnews.com/overnight-vols-spike-but-sub-2024-highs-1734488216666)
- ☆ 年間レンジ（三菱UFJ系・マネックス記事の整理）: 2022 年 113–151 円（38 円）、2023 年 127–151 円（24 円）、2024 年 139–161 円（22 円；ただし 141→161→139→158 と動いた累計値幅は約 60 円）、2025 年は 5 月末時点で 139–158 円（19 円）。 — [マネックス証券 27177](https://media.monex.co.jp/articles/-/27177)
- ☆ 2024 年: 高値 161.948（2024-07-03）、安値 139.581（2024-09-16）、平均 151.4625。2025 年: 高値 158.872（2025-01-10）、安値 139.89（2025-04-22）、平均 149.654。 — [Pound Sterling Live: USD/JPY 2024](https://www.poundsterlinglive.com/history/USD-JPY-2024); [同 2025](https://www.poundsterlinglive.com/history/USD-JPY-2025)
- ☆ ATR パーセンタイル・レジームの実務ヒューリスティック（TradingView スクリプト群）: 25 パーセンタイル未満＝圧縮（ブレイクアウト確率が「しばしば上がる」）、25–50＝平穏（平均回帰向き）、Normal/Elevated 帯＝トレンド継続向き、ボラ上昇時はストップ拡大と部分建て。【証拠ではなく設計指針】 — [TradingView: ATR Regime Study [CHE]](https://my.tradingview.com/scripts/search/ATR/page-17); [TradingView: ATR Volatility Regime](https://ar.tradingview.com/scripts/search/ATR/page-17)
- ☆ ボリンジャー＋ADX(25) ブレイクアウトの定型（StockSharp 文書）: ADX が閾値未満のブレイクは全て無視、終値がバンド外＋ADX>25 でエントリー、ミドルバンドで手仕舞い。「年率約 115%、株式で最良」との記載は販促的で検証条件不明。 — [StockSharp: Bollinger Bands and ADX Breakout Strategy](https://doc.stocksharp.com/en/designer-gallery/Bollinger_ADX); [StockSharp: ADX Bollinger Strategy](https://doc.stocksharp.com/en/api-examples/0176_ADX_Bollinger)
- ☆ USD/JPY は東京時間に日次出来高の約 1/3（第 1 節、OANDA）。 — [OANDA volume insights](https://www.oanda.com/bvi-en/skills-and-insights/education/market-timing-and-volatility/when-to-trade/best-time-to-trade-forex-volume-insights/)

### Inferences
- 証拠の強さ順: (1) 時間帯によるボラの季節性＝頑健（学術＋実務）。ブレイクアウトのエントリー許可時間を JST 15:00–翌 02:00（ロンドン〜NY 前半）に絞る根拠はここにある。(2) 「ロンドン時間に稼ぎ夜に吐き出す」＝実務家の単一バックテストで、USD/JPY では未検証。(3) ATR パーセンタイル閾値（現行 0.20）・ADX≥20・スクイーズ＝設計上は妥当だが統計的裏付けなし。閾値の最適値は自社の walk-forward で決め、「東京時間を除外した上で」ATR フィルターの限界効用を測るべき（両者は相関が高く、二重掛けで取引機会だけ減る恐れ）。
- USD/JPY の時間帯別ドリフト（7–11 時に円安、11–15 時に円高など）は仲値・実需フローの季節性と整合し、「東京午前のロング・ブレイクは仲値後に失速しやすい」という設計上の警告になる。ただし pips の大きさがスプレッドの何倍かが不明で、単独で収益化できる証拠はない。
- インプライド・ボラの使い方: 1M IV が 9–10% の低位では 1 日の標準偏差 ≈ 150 円 × 9.5% / √252 ≈ 0.90 円（90 pips）、12.4% なら ≈ 1.17 円。15 分足 ATR の期待値もこれに比例するため、ATR パーセンタイルの「正常帯」を IV 水準で正規化する（IV が 2024 年 8 月や 2026 年 7–8 月のように跳ねた直後は、同じパーセンタイルでも絶対 pips が倍以上）と、介入・ショック後の過剰取引を抑えられる。
- セッション別「日中高値/安値の形成時間帯」の統計は見つからなかったが、東京が最低ボラ・ロンドン–NY 重複が最高ボラという季節性から、日中高値/安値の多くは JST 15–24 時に形成されると推論される。自社データで「日次 H/L がどの JST 時間帯に出たか」を集計すれば、ブレイクアウトの対象レンジ（東京レンジ）とエントリー窓の設計を直接裏付けられる。

### Gaps
- USD/JPY の年別の年率実現ボラ、年別の平均日中値幅（pips）、トレンド日/レンジ日の割合を定量化した資料は見つからなかった（検索上限・データ取得遮断のため当方計算も不可）。最終レポートでは「要自社計算」と明記するのが安全。
- CME JPY オプション、Deutsche Bank CVIX、JPMorgan G7 ボラ指数の 2022–2026 年の水準推移は未取得。2024 年 8 月と 2026 年 7–8 月の IV ピーク値も未取得。
- ATR パーセンタイル／ボリンジャー幅／ADX 閾値の有効性を（コスト込で）統計的に示した研究は発見できなかった。
- 東京・ロンドン・NY 各セッションの USD/JPY 平均レンジ（pips）と、日次高値/安値の形成セッション比率の統計は未発見。

---

## 6. USD/JPY レジーム 2022–2026（日銀・FRB の経路、2024-08-05、年別レンジ、スワップ、ロングバイアス vs 対称）

### Takeaway
2022–2026 年は「ゆっくり円安・突然の急落」の非対称レジーム: 日銀はマイナス金利解除（2024-03）→0.25%（2024-07）→0.5%（2025-01）→0.75%（2025-12）→1.00%（2026-06-16）→1.25%（2026-09-18）と利上げを続け、FRB は 2025 年に 3 回利下げ（3.50–3.75%）した後、2026-09-16 に 2023 年以来初の利上げ（3.75–4.00%）に転じた。にもかかわらずドル/円は 2026 年 7 月に 163.99 の 39 年 8 カ月ぶり高値を付け、介入で 155 円台へ。スワップ（買い）は年率 3.6% 台で緩やかに低下中。日中システムにスワップは無関係だが、「上昇はじわじわ・下落は介入/巻き戻しで数円」という分布の非対称性がパラメータ設計の核心。

### Cited Findings
- ☆ 日銀政策金利の経路: 2024 年 3 月マイナス金利解除（誘導目標 0.1% 程度）、2024 年 7 月 0.25%、2025 年 1 月 0.5%、2025 年 12 月 0.75%（約 30 年ぶり水準）、2026 年 1 月 23 日 据え置き（0.75%）。 — [日本経済新聞: 日銀0.75%へ利上げ決定、30年ぶり水準](https://www.nikkei.com/article/DGXZQOUB166040W5A211C2000000/); [三菱UFJ Money Canvas: 日銀の今後の利上げ動向](https://moneycanvas.bk.mufg.jp/know/column/GV5w2GST8UQjeId/); [EBC: 日本政策金利の推移と転換点](https://www.ebc.com/jp/forex/283044.html)
- ☆ 2026 年 6 月 16 日: 日銀が 0.25%pt 利上げし 1.00%。 — [FXStreet: BoJ Governor Ueda sheds light on policy outlook](https://www.fxstreet.com/news/bank-of-japan-is-expected-to-deliver-a-hawkish-hike-pressured-by-rising-inflation-202609172200); [ING: Further Bank of Japan hikes are expected, but not imminent](https://think.ing.com/snaps/bank-of-japan-further-hikes-expected-but-not-imminent/)
- ☆ 2026 年 9 月 17–18 日会合: 1.00%→1.25% へ利上げ（6 月以来 3 カ月ぶり、植田体制で最短間隔）。背景は企業物価が原油高・AI 需要で 3 カ月連続 7% 超などインフレ上振れ。1.25% でも金融環境は緩和的で名目中立金利レンジの下限付近。第一生命経済研は 2026 年 12 月に 1.5%、2027 年 1–3 月に 1.75% を予想。植田総裁は 2026-09-02 に「毎回の会合で利上げを検討」と発言。 — [第一生命経済研究所 665928](https://www.dlri.co.jp/report/macro/665928.html); [三井住友DS 2026年9月日銀政策会合レビュー](https://www.smd-am.co.jp/market/ichikawa/2026/09/irepo260924/); [大和総研 2026-09-18](https://www.dir.co.jp/report/research/economics/japan/20260918_026078.html); [大和アセット マーケットレター 2026-09-18](https://www.daiwa-am.co.jp/specialreport/market_letter/20260918_02.pdf); [Nikkei Asia: BOJ chief says rate hikes on table at every meeting](https://asia.nikkei.com/economy/bank-of-japan/boj-chief-says-rate-hikes-on-table-at-every-meeting-including-this-month-s)
- ☆ 野村の（9 月利上げ前の）メインシナリオ: 2026 年 6 月・12 月、2027 年 6 月に各 0.25%pt でターミナル 1.5%。 — [野村證券 森田京平](https://www.nomura.co.jp/wealthstyle/article/0571/)
- ☆ FRB: 2025 年 12 月 10 日に 25bp 利下げで 3.50–3.75%（2025 年 3 回目の利下げ）。 — [FRB FOMC Minutes Dec 9–10, 2025](https://www.federalreserve.gov/monetarypolicy/fomcminutes20251210.htm); [GREA: FOMC Lowers Federal Funds Rate 25bp to 3.50%-3.75%](https://grea.com/fomc-lowers-federal-funds-rate-25-basis-points/); [Al Jazeera 2025-12-10](https://www.aljazeera.com/economy/2025/12/10/us-federal-reserve-cuts-interest-rates-in-final-decision-of-the-year)
- ☆ FRB 2026 年: 1 月・3 月・4 月・6 月・7 月は 3.50–3.75% で据え置き（7 月 28–29 日は 3 名が 25bp 利上げを主張して反対票）。 — [FRB FOMC Minutes July 28–29, 2026](https://www.federalreserve.gov/monetarypolicy/fomcminutes20260729.htm); [FOMC 2026 schedule](https://macroodds.com/blog/fomc-meeting-schedule-2026)
- ☆ 2026 年 9 月 16 日: FOMC は 12–0 で 25bp 利上げ、3.75–4.00%（2023 年以来初の利上げ）。インフレが高止まり。中央値は 2026 年末 4.1%（18 人中 16 人が年内あと 1 回の利上げ）、2027 年 4.1%、2028–29 年は 3.9%/3.6% へ利下げ。次回会合は 10 月 27–28 日。 — [J.P. Morgan AM: FOMC Statement September 2026](https://am.jpmorgan.com/us/en/asset-management/adv/insights/portfolio-insights/fixed-income/fixed-income-perspectives/fomc-statement-september-2026/); [Chase: Fed Raises Rates in September](https://www.chase.com/personal/investments/learning-and-insights/article/federal-reserve-raises-rates-officials-signal-one-more-hike-in-2026); [FRB Implementation Note 2026-09-16](https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a1.htm); [Kiplinger live blog](https://www.kiplinger.com/investing/live/fed-meeting-updates-and-commentary-september-2026); [fedratecalc: Next Fed Meeting Oct 27–28](https://fedratecalc.com/fomc-meeting-schedule/)
- ☆ 2024-08-05: 寄り付き 145.59→東京時間に 141.68（1 月以来の安値）、VIX 23→65 超、日銀 7/31 利上げと FRB 利下げ観測で円キャリーの巻き戻し（第 3 節参照）。 — [三菱UFJ銀行 FX Daily 2024-08-05](https://www.bk.mufg.jp/report/dmr2024/FXDaily20240805.pdf); [アセマネOne 2024-08-06](https://www.am-one.co.jp/pdf/report/14610/240806_infogr_US.pdf)
- ☆ 2025 年の経路: 年初 150 円台後半→4 月に 140 円近辺→以後円安基調で年末に 150 円台半ば〜後半。2025 年 12 月の 0.25% 利上げ後も一時大幅な円安。 — [第一生命経済研 PDF 634581](https://www.dlri.co.jp/files/macro/634581.pdf); [国際通貨研究所 nl2025.49](https://www.iima.or.jp/docs/newsletter/2025/nl2025.49.pdf); [みずほリサーチ RT EXPRESS 2025-0165](https://www.mizuhobank.co.jp/corporate/mhri/research/report/2025-0165/index.html)
- ☆ 2026 年の経路: 4/30 高値 160.72→介入で 155 円台→7/1 162.84→7/23 163.99→7/30–31 介入で 8/3 155.23→8 月中旬 159 円台（日銀利上げ観測でも円安進行、「160 円を試す可能性」）。 — [外為どっとコム 2026-08-14](https://www.gaitame.com/media/entry/2026/08/14/170440); [NRI 2026-05-07](https://www.nri.com/jp/media/column/kiuchi/20260507.html); [kabucho](https://kabucho.com/articles/japan-us-coordinated-yen-intervention-2026-07-31)
- ☆ 年別レンジ・平均（第 5 節再掲）: 2022 年 113–151、2023 年 127–151、2024 年 139.58–161.95（平均 151.46）、2025 年 139.89–158.87（平均 149.65）。 — [マネックス 27177](https://media.monex.co.jp/articles/-/27177); [Pound Sterling Live 2024](https://www.poundsterlinglive.com/history/USD-JPY-2024); [同 2025](https://www.poundsterlinglive.com/history/USD-JPY-2025)
- ☆ スワップポイント（ドル/円買い、ザイFX! 集計）: 2025 年 6 月〜2026 年 5 月の 1 年間実績 52,956 円（年率換算 3.67%）、2025 年 7 月〜2026 年 6 月は 52,540 円（3.64%）で微減傾向。【1 万通貨あたりと推定、ページ未確認】 — [ザイFX! 495861](https://zai.diamond.jp/articles/-/495861); [ザイFX! 481751](https://zai.diamond.jp/articles/-/481751)
- ☆ BIS 2025 年 3 年次サーベイ（2025 年 4 月）: 世界の FX 取引高は 1 日 9.6 兆ドル（2022 年比 +28%）、米ドルは全取引の 89% の片側、ユーロ 28.9%、円 16.8% で第 3 位。 — [BIS: OTC foreign exchange turnover in April 2025](https://www.bis.org/statistics/rpfx25_fx.htm); [BIS プレスリリース 2025-09-30](https://www.bis.org/press/p250930.htm); [BIS media release](https://www.bis.org/media-releases/20250930-global-fx-trading-hits-96-trillion-day-april-2025-and-otc-interest-rate-derivatives-surge-79)

### Inferences
- 金利差: 2024 年 9 月時点（FF 4.75–5.00% vs 日銀 0.25%）の約 4.6%pt から、2026 年 10 月時点（FF 3.75–4.00% vs 1.25%）は約 2.6%pt へ縮小。スワップの年率 3.6% 台はこれと整合し、今後は日銀追加利上げ（12 月 1.5% 観測）と FRB 追加利上げ（年内あと 1 回）が相殺気味で、急減より緩やかな低下が基本線。
- 「金利差縮小でも円安」という 2025–2026 年の事実は、ドル/円の水準を金利差モデルだけで予測する特徴量が効きにくいことを意味する。日中システムでは金利差は特徴量に入れず、介入帯（160 円超）での非対称リスク管理に使う方が筋が良い。
- 分布の非対称性: 2022-09（約 5 円）、2022-10（約 7 円）、2024-08-05（約 4 円/日）、2026-04-30（約 5 円）、2026-07-30〜08-03（約 8.8 円）と、数円級の 1〜3 日の急落が概ね年 1–2 回。上昇側に同等の急騰は 2025-10-06（約 3 円、ギャップ）程度。対称なブレイクアウト設計で「大きな利益はショート側の数日に集中」し、ロング側は「回数は多いが 1 回あたり小さい」構造になると予想される。性能評価はロング/ショート別に分け、ショートは少数の大勝ちに依存していないか（それが介入日だけなら再現性が低い）を確認すべき。
- ロングバイアス vs 対称: オーバーナイトを持つならスワップ（約 145 円/日/万通貨 ≈ 52,956/365）がロングの追い風だが、2026 年レジームでは 160 円超で政府の「プット」が働くため、ロングの期待値は水準依存。日中完結のシステムでは対称を基本とし、160 円超はロングのサイズ縮小、介入直後 1–3 日はショートの追随を抑える（平均回帰相）という「水準×時間」の非対称ルールが実証的な妥協点。
- 年間レンジは 2022 年 38 円→2025 年 19 円と縮小しているが、累計値幅（2024 年約 60 円）は大きい。「レンジは縮んでもトレンド往復は多い」ため、トレンドフォローの機会は 2010 年代（年間レンジ 10 円前後の年が多かった）より明らかに多く、2010 年代データで最適化した ATR 絶対値・SL 幅はそのまま使えない。

### Gaps
- 2022 年 12 月・2023 年 7 月・2023 年 10 月の YCC 修正、2024 年 3 月 19 日・7 月 31 日・2025 年 1 月 24 日の正確な決定日、および 2024 年 9/11/12 月の FRB 利下げ（計 100bp）は、本セッションの検索結果に含まれず未確認（一般に知られる事実だが出典を付せない）。
- 2026 年の安値（1〜4 月の最安値）と 2026 年 8 月中旬以降〜10 月 4 日時点の水準は未取得。
- 年別の年率実現ボラ・平均日中値幅・トレンド日比率は未発見（要自社計算）。
- スワップの 2022–2024 年の水準（例: 2024 年の 1 日 200 円超/万通貨と報じられた時期）との比較データは検索上限で未取得。
- BIS サーベイの要約にあった「円デリバティブの取引高が 684% 増」は妥当性に疑問があり採用しない（要一次確認）。

---

## 7. 日本の個人・システムトレーダーが使う暦ルール（時間帯フィルター、早朝回避、金曜 NY 午後、雇用統計週、ゴトー日など）の証拠の有無

### Takeaway
証拠があるのは (a) 早朝 6–8 時（特に月曜 7–8 時、ロールオーバー）のスプレッド拡大＝業者開示で裏付け、(b) ゴトー日の 7–10 時のドル/円上昇確率 58%（2010/8–2018/7、神戸大の検証、コスト不明）、(c) 週末ギャップ統計、(d) 東京時間の低ボラ・ロンドン–NY の高ボラ、の 4 点。「15:00–02:00 のみ取引」「金曜 NY 午後回避」「雇用統計週回避」はブレイクアウト戦略について公開された統計的裏付けが見つからず「広く実践」止まり。

### Cited Findings
- ☆ ゴトー日（5・10・15・20・25・30 日）は輸入企業のドル建て決済が集中し、9:55 の仲値に向けてドル買い実需が出る。神戸大学の検証では【期間: 2010 年 8 月〜2018 年 7 月、USD/JPY】ゴトー日の 7 時〜10 時のドル/円上昇確率は 58% で統計的に有意。【pips・コストはページ未取得】 — [神戸大学 経済学部 R2_shirakisho2.pdf](https://www.econ.kobe-u.ac.jp/wp-content/uploads/2023/05/R2_shirakisho2.pdf); [幻冬舎ゴールドオンライン 77188](https://gentosha-go.com/articles/-/77188)
- ☆ ザイFX! の検証: ゴトー日アノマリーは「勝率 6 割超」で儲かるとの結果だが、ユーロ/米ドルなど他ペアに同型ルールを当てると負けトレードの方が多く総収益マイナス＝通貨ペア・時間帯依存。【期間・コストはページ未取得】 — [ザイFX! 319691](https://zai.diamond.jp/articles/-/319691); [同 page 2](https://zai.diamond.jp/articles/-/319691?page=2)
- ☆ 実務上の注意: 銀行は仲値を吊り上げるより安くドルを仕入れるのが目的で必ずしも 9:50 前後に買わない。仲値直前 5 分は「フライングの利確売りとぎりぎりのドル買いが衝突する最も危険な時間帯」。 — [幻冬舎ゴールドオンライン 77188](https://gentosha-go.com/articles/-/77188); [OANDA Lab: OANDA_Local_Time でアノマリーを利用する手法](https://www.oanda.jp/lab-education/blog_column/oanda_local_time)
- ☆ USD/JPY の時間帯別ドリフト（第 5 節再掲、JST 4 時間ブロック、約 5 年）: 7–11 時・19–23 時・3–7 時が円安方向、11–15 時・15–19 時・23–3 時が円高方向。 — [Zenn 記事](https://zenn.dev/harukii/articles/4686886549bd10); [ニッセイ基礎研 42141](https://www.nli-research.co.jp/files/topics/42141_ext_18_0.pdf?site=nli)
- ☆ 早朝スプレッド: 原則固定の例外時間帯として月曜 7–8 時、夏時間 6 時前後/冬時間 7 時前後が明記（第 3 節再掲）。 — [soico.jp 26339](https://www.soico.jp/no1/news/fx/26339); [外為どっとコム スプレッドリスク PDF](https://www-hp.fxtrade.co.jp/doc/spread_risk.pdf)
- ☆ 東京レンジ・ブレイクアウト EA（USD/JPY 15 分足、JST 9:00–15:00 のレンジを 15:00 以降に 2 pips 超えでエントリー、H1 EMA150 のトレンドフィルター、UTC 20:00 で有効期限）: 商用 EA の仕様であり成績の独立検証なし。 — [MQL5 Market: Tokyo Range Breakout Usdjpy](https://www.mql5.com/en/market/product/173783)
- ☆ ロンドン・オープン（8:00–9:00 GMT）の高安ブレイク/リテストを売買するスクリプト群: 公開スクリプトの設計思想のみで統計なし。 — [TradingView: Apex Edge – London Open Session](https://www.tradingview.com/script/aDN1CQJK-Apex-Edge-London-Open-Session/)
- ☆ ブレイクアウト戦略のバックテスト手法（スプレッド・スリッページ・時間帯を含める注意点）の解説。 — [FX Replay: ブレイクアウト戦略のバックテストは可能か?](https://fxreplay.com/ja/learn/can-you-backtest-a-breakout-strategy-heres-how-to-do-it-right)
- ☆ 週末ギャップ（第 3 節再掲）: 平均 23.3 pips、中央値 16.4 pips、10 pips 超 68% の週、100 pips 超 2%。 — [soico.jp 25531](https://www.soico.jp/no1/news/fx/25531)
- ☆ 実務家バックテスト（EUR/USD）: ブレイクアウトはロンドン 08:00–17:00 で利益、オーバーナイトで損失。 — [MQL5 blog 774527](https://www.mql5.com/en/blogs/post/774527)

### Inferences
- 「証拠あり」として採用してよいルール: (1) JST 6:00–8:00（米冬時間は 7:00–9:00 側へ 1 時間シフト）と月曜 7–8 時はスキャル停止（スプレッド開示が根拠）。(2) 金曜 NY クローズ前にブレイクアウト戦略をフラット化（ギャップ統計が根拠）。(3) ゴトー日 7:00–9:55 は USD/JPY に上向きドリフト、9:55 以降は反転しやすい（検証 1 本＋実需の説明）。ただし 58% の上昇確率は「方向」であり、スプレッド後の pips 期待値は未確認なので、単独戦略ではなく「東京午前のショート・ブレイクを抑制／ロングの早期利確」というフィルターとして使う。
- 「広く実践だが未証明」: 15:00–02:00 のみ取引（季節性からは合理的だが USD/JPY での損益証拠なし）、金曜 NY 午後の回避、雇用統計週の回避、月初・月末の回避。これらは現行 walk-forward 基盤で「フィルター有無の 2 系統」を常時並走させて自前で証拠を作るべき。
- 現行 blackout の見直し案: `05:55–06:10` を「NY 17:00 ET の前後 10 分」に変換（夏 05:50–06:10、冬 06:50–07:10 JST）し、さらに 6:00–8:00 のスプレッド条件付き停止を追加。`21:25–21:40` は「米 8:30 ET 指標の前後」に変換（夏 21:25–21:45、冬 22:25–22:45）し、指標カレンダー（現行 static_csv プロバイダ、event_blackout 前 30 分/後 15 分）を有効化する方が固定窓より合理的。
- ゴトー日・仲値の効果は 2010 年代のデータでの検証で、2022 年以降の 150 円台・介入レジームでの有効性は未確認。少なくとも 2022–2026 年の自社データで再検証してから採用。

### Gaps
- 「15:00–02:00 JST のみ」「金曜 NY 午後回避」「雇用統計週回避」「月末回避」について、USD/JPY のブレイクアウト戦略で損益改善を示した公開バックテストは見つからなかった。
- ゴトー日検証の pips 期待値・コスト条件・2019 年以降の追試、ザイFX! 検証の期間とルール詳細はページ本文未取得。
- 国内の個人 FX トレーダーの損益分布（金融先物取引業協会の調査など）は検索上限で未取得。
- 国内各社の時間帯別「提示率」実績表（例: 楽天銀行のスプレッド実績 PDF が検索にヒット: https://www.rakuten-bank.co.jp/assets/fx/info/pdf/spread_performance.pdf ）は本文未確認。早朝・指標時の実測スプレッドは自社 tick で計測するのが最も確実。
