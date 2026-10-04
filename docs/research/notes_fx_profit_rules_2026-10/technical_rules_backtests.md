# FX テクニカル売買ルールの独立検証・学術的検証（USD/JPY 優先、15分・1時間・4時間・日足）

調査日: 2026-10-04。対象: 現行 USD/JPY 自動売買（1H トレンドフィルター EMA50>EMA200・傾き3本・ADX(14)>20・ATR パーセンタイル≥20%／15m Donchian-20 ブレイク＋任意 ATR バッファ／1m 押し目再エントリー／ATR(14)×2.0 初期ストップ＋スイング余白0.1ATR／ATR×2.5 トレーリング／50% 分割決済／トレンド崩れ確認2本／スプレッドフィルター）のチューニング資料。

**調査環境の制約（重要）**: 本セッションの実行環境では学術リポジトリ（NBER, St. Louis Fed, NY Fed, RePEc, SSRN 系, 大学リポジトリ）、主要クオンツブログ（Substack 系, QuantInsti, MQL5, Babypips, OANDA Japan 等）、ブローカーの公開 API（GMO 等）への直接アクセスが全て遮断されたため、各ページの全文は取得できず、**数値は検索エンジンが返したページ要約から確認したもの**に限る。原典全文で未確認の既知の数値は「★未検証（調査者の既知情報）」と明示したので、採用前に必ず URL 先で確認すること。

**証拠品質タグ**: 【査読】査読論文・中央銀行/研究所 WP ／【公開BT】ルールとコード・指標が公開された再現可能バックテスト ／【ブログ】ルールや期間が部分的にしか示されない主張 ／【業者】ブローカー・EA 販売者の資料 ／【フォーラム】掲示板・TradingView スクリプトのコメント等。

---

## 1. 移動平均系ルール（MA クロス 5/20・20/50・50/200、EMA vs SMA、パーフェクトオーダー、価格 vs MA）は時代とコストに耐えたか

### Takeaway
日次 MA ルールの超過収益は 1970〜80 年代には本物だったが、USD/JPY では **1990 年代半ば以降に低下し 2000 年前後に消失**したという独立研究が複数一致している（Neely-Weller-Ulrich 2009、Schulmeister 2008 円ドル研究）。2015 年以降も「大規模ルール群の一部は一時的に有効」（Hsu-Taylor-Wang 2016、Zarrabi 他 2017）だが、**特定のクロス（5/20 等）や EMA/SMA の優劣を USD/JPY で裏付ける査読証拠は見当たらない**。MA は「収益源」ではなく「方向フィルター」としてのみ正当化できる。

### Cited Findings
- 【査読】Hsu, Taylor & Wang (2016) "Technical trading: Is it still beating the foreign exchange market?" Journal of International Economics 102: 188–208。45 年分の日次データ、先進・新興 30 通貨、**21,195 ルール（フィルター 2,835／MA 12,870／サポレジ 1,890／チャネルブレイク 3,000／オシレーター 600）**をデータスヌーピング対策のステップワイズ検定で評価し、先進・新興いずれでも「substantial predictability and excess profitability」を報告。ただしサブ期間・通貨グループ間で成績が大きく変動し、著者は「一時的な非合理行動と市場の未成熟」が予測可能性の源泉と解釈 — [WUSTL profile](https://profiles.wustl.edu/en/publications/technical-trading-is-it-still-beating-the-foreign-exchange-market/); [CEPR DP10018](https://cepr.org/publications/DP10018); [CXO Advisory 要約](https://cxoadvisory.com/technical-trading/updated-comprehensive-long-term-test-of-technical-currency-trading)
- 【査読】Neely, Weller & Ulrich (2009) "The Adaptive Markets Hypothesis: Evidence from the Foreign Exchange Market" JFQA 44(2): 467–488。過去研究で報告済みのルールを**真のアウトオブサンプル**で再検証。1970〜80 年代の超過収益はデータマイニングではなく本物だったが、**フィルター・MA ルールの収益機会は 1990 年代初頭までに消失**。研究の少ないルール（チャネル、ARIMA、遺伝的プログラミング、マルコフ）は低下したが完全消失はしていない可能性。適応的市場仮説と整合 — [GP bibliography 要約](https://gpbib.cs.ucl.ac.uk/gp-html/Neely_2009_JFQA.html); [St. Louis Fed WP 2006-046](https://files.stlouisfed.org/files/htdocs/wp/2006/2006-046.pdf)
- 【査読/研究所WP】Schulmeister (2008) "Profitability of Technical Currency Speculation. The Case of Yen-Dollar Trading 1976-2007" (WIFO WP 325)。**1,024 本の MA・モメンタムモデル**を円ドル日次で検証。1976–2007 では全モデルが黒字、**平均年率グロス 8.5%、最悪モデルでも 5.4%**。単発の損失回数は利益回数より多いが、**利益ポジションの保有期間が損失ポジションの 2〜6 倍**長いため利益の平均サイズが大きい。収益は「持続的トレンドの利用」のみに由来。**日次データに基づく収益性は 1990 年代半ばから低下し、2000 年以降は消失** — [WIFO 出版ページ](https://www.wifo.ac.at/publication/111817/); [IDEAS](https://ideas.repec.org/p/wfo/wpaper/y2008i325.html); [WP PDF](https://wifo.ac.at/wp-content/uploads/upload-1881/WP_2007_325.pdf)
- 【査読】Schulmeister (2009) "Profitability of technical stock trading: Has it moved from daily to intraday data?" Review of Financial Economics 18(4): 190–201（株式: S&P500）。2,580 モデルの日次成績は 1960 年以降低下し 1990 年代初頭から赤字、同じモデルを **30 分足**で走らせると 1983–2007 に平均年率グロス 7.2% — [IDEAS](https://ideas.repec.org/a/eee/revfin/v18y2009i4p190-201.html)（FX ではなく株式。短期足への「収益の移動」仮説の根拠として引用）
- 【査読】Zarrabi, Snaith & Coakley (2017) "FX technical trading rules can be profitable sometimes!" International Review of Financial Analysis 49: 113–127。**7,650 ルール**を対ドル 6 通貨・1994:3–2014:12 に適用、Barras-Scaillet-Wermers の偽発見率(FDR)で多重検定を制御。アウトオブサンプルでは「**短期間だけ**シャープレシオで優越するルールが多数」— 持続性がないことが題名の含意 — [Essex repository](https://repository.essex.ac.uk/18805/); [DOI](https://doi.org/10.1016/j.irfa.2016.12.010)
- 【査読サーベイ】Park & Irwin (2007) "What Do We Know About the Profitability of Technical Analysis?" Journal of Economic Surveys 21(4): 786–826。結論は「テクニカルは FX と商品先物では収益性あり、株式ではなし」— [IDEAS](https://ideas.repec.org/a/bla/jecsur/v21y2007i4p786-826.html)。★未検証（調査者の既知情報）: 近代的研究 95 本中 56 本が肯定、20 本が否定、19 本が混在。
- 【査読サーベイ】Menkhoff & Taylor (2007) "The Obstinate Passion of Foreign Exchange Professionals: Technical Analysis" Journal of Economic Literature 45(4): 936–972。テクニカルは FX で広く使われ、一定のルールを長期適用すれば有意な超過収益が得られ得るとしつつ、非合理性や介入だけでは説明できないと結論 — [IDEAS](https://ideas.repec.org/r/aea/jeclit/v45y2007i4p936-972.html); [Warwick WRAP](https://wrap.warwick.ac.uk/30672/)
- 【査読】LeBaron (1999) "Technical trading rule profitability and foreign exchange intervention" Journal of International Economics。日次 **150 日 MA**（価格 vs MA）ルールを使用。**FRB が介入した期間を除くと予測可能性は劇的に低下** — [NBER w5505](https://www.nber.org/papers/w5505); [要約](https://academicnewsletter.sufe.edu.cn/info/300764)
- 【査読】Okunev & White (2003) "Do Momentum-Based Strategies Still Work in Foreign Currency Markets?" JFQA。MA ベースのモメンタムで通貨をロング/ショートする戦略は 1980–2000 に **年率約 6%** の超過収益、1990 年代後半に特に強い。ルール仕様・基軸通貨に対し頑健、ブートストラップでリスクプレミアムではなく自己相関構造に由来と結論 — [Cambridge](https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/abs/do-momentumbased-strategies-still-work-in-foreign-currency-markets/F08FF94B61A04532A50C9A422B68AD12); [PDF ミラー](https://c.mql5.com/forextsd/forum/213/Do%20Momentum%20Based%20Strategies%20Still%20Work%20in%20Foreign%20Currency%20Markets.pdf)
- 【公開BT】QuantInsti EPAT プロジェクト（2025 年頃）: 主要 7 通貨ペア 2003–2025 で時系列モメンタム、MA クロス、チャネルブレイクの 3 手法を **23 回のローリング・ウォークフォワード（学習 3 年／検証 1 年）**で検証、パラメータは最高シャープではなく近傍安定性で選択。**シャープ 0.5 の「取引可能」基準を超えたのは 7 ペア中 2 ペアのみ（USDJPY 0.78、EURUSD 0.54）で、いずれも時系列モメンタム** — [QuantInsti](https://www.quantinsti.com/articles/trend-following-strategies-major-currency-markets/); [EPAT 版](https://www.quantinsti.com/articles/trend-following-strategies-major-currency-markets-epat-project/)
- 【ブログ】日本語圏の「パーフェクトオーダー」検証例: ドル円 1 時間足で勝率 56.5%、獲得 366 pips、利益約 59.4 万円との記述が検索要約に出現したが、期間・コスト・トレード数は不明（出典ページを特定しきれず。候補: [crexgroup](https://crexgroup.com/ja/fx/analysis-strategy/fx-perfect-order-strategy/), [fxnav](https://www.fxnav.net/mt4navi/keys-ma3-line/)）— 証拠としては採用不可。

### Inferences
- 1976–2007 で 1,024 モデル「全て黒字」かつ 2000 年以降「全て消失」という Schulmeister の結果は、**パラメータ選択（5/20 か 20/50 か、EMA か SMA か）よりも「その時代に持続トレンドがあったか」が成績を支配**することを意味する。クロス長の最適化に時間を割く価値は低い。
- 現行の 1H EMA50/200＋傾き 3 本は、価格 vs 長期 MA（LeBaron 型 150 日 MA の短期足版）に相当し、「方向の許可フィルター」としては学術的に筋が通るが、**フィルター自体が期待値を生む証拠はない**。
- Zarrabi らの「短期間だけ有効」と NWU の「時代とともに消失」は、**単一ルールで毎月黒字を狙う前提そのものに反する**。月次の黒字率を目標にするなら、ルールの期待値ではなく（a）トレード数と（b）月内分散を管理する設計に切り替える必要がある（§9 の試算参照）。

### Gaps
- HTW 2016 のサブ期間別シャープ、損益分岐コスト（break-even cost）、円の個別成績は原文未取得。
- Schulmeister (2009) "Technical Trading and Trends in the Dollar-Euro Exchange Rate"（日中データを含む）の数値は未取得 — [PDF](https://wifo.ac.at/wp-content/uploads/upload-2633/S_2009_TECHNICAL_TRADING_37582.pdf)。
- "Do technical trading profits remain in the foreign exchange market? Evidence from 14 currencies" (J. Int. Fin. Markets, Inst. & Money 21(2), 2011) の通貨別の衰退時期は未取得 — [IDEAS](https://ideas.repec.org/a/eee/intfin/v21y2011i2p176-206.html)。
- Brock-Lakonishok-LeBaron (1992) と Sullivan-Timmermann-White (1999) は本セッションでは未検索（株式指数が対象のため優先度を下げた）。
- 2015 年以降の USD/JPY 日次 MA クロスを**コスト込み**で検証した査読研究、および日本語圏でコスト・期間・トレード数を明記した MA クロス検証は見つからなかった。

---

## 2. ブレイクアウト系（Donchian 20/55・N 日高値・セッション/オープニングレンジ・ボリンジャーブレイク）の証拠と、15m/1H/4H/日足での持ちこたえ方

### Takeaway
日足チャネルブレイクは HTW 2016 のルール群（3,000 本）に含まれ、歴史的には FX で有効だったが、**2003–2025 のウォークフォワードでは MA クロス・チャネルブレイクともシャープ 0.5 に届かず、時系列モメンタムだけが USDJPY で生き残った**（QuantInsti）。USD/JPY の時間足 Donchian で公開されている数値は EA 販売者のもの（H1・40 期間・PF 1.50・勝率 34.5%）に限られ、15 分足の独立検証は見つからなかった。ブレイクアウトは「勝率 30% 台、少数の大勝ちが成績を運ぶ」分布であり、月次黒字という目標と構造的に相性が悪い。

### Cited Findings
- 【査読】HTW 2016 のルール群にはチャネルブレイクアウト 3,000 本が含まれ、全体として先進通貨でも予測可能性ありと報告（§1 と同一出典）— [WUSTL profile](https://profiles.wustl.edu/en/publications/technical-trading-is-it-still-beating-the-foreign-exchange-market/)
- 【公開BT】QuantInsti EPAT: チャネルブレイクアウトは 3 手法の 1 つとして 7 ペアで検証されたが、シャープ 0.5 を超えた組み合わせは時系列モメンタム×USDJPY/EURUSD のみ — [QuantInsti](https://www.quantinsti.com/articles/trend-following-strategies-major-currency-markets/)
- 【業者】MQL5 "Donchian Trend Engine" 公開バックテスト: **USDJPY H1、2017-01〜2026-06（9.4 年）、Dukascopy 実ティック・全ティックモデル、初期 1 万ドル**。既定値 **Donchian 40 期間、SL=2.0 ATR、トレーリング 8.0 ATR、リスク 0.5%/取引**で、**PF 1.50、純益 +16,522 ドル（口座 2.65 倍）、最大有効証拠金 DD 10.3%、631 取引、勝率 34.5%**。著者は「円クロスと暗号資産メジャーのような強トレンド市場では機能し、EURUSD や金では機能しなかった」と明記 — [MQL5 blog (EN)](https://www.mql5.com/en/blogs/post/773712); [同 (JA)](https://www.mql5.com/ja/blogs/post/773715)（販売者資料。最適化後の数値である可能性が高い）
- 【ブログ/公開BT】Scrapbox「InvestmentTechHack／ドンチアン・トレンド・システム」: 20 日ブレイクで仕掛け・10 日ブレイクで手仕舞い・350 日/25 日 MA をトレンドフィルター。USDJPY の結果 **損益 +15.02%、最大 DD 19.04%、取引数 89、勝率 30.23%**（期間・コストは要約に出ず）— [Scrapbox](https://scrapbox.io/InvestmentTechHack/ドンチアン・トレンド・システム); 時限退出付きの派生: [Scrapbox](https://scrapbox.io/InvestmentTechHack/時限退出付きドンチアン・トレンド)
- 【フォーラム】Babypips「Donchian Channel Trading」スレッド: 「近年の EUR/USD では **20/10 ブレイクが 55/20 を大幅に上回った**」「2008–2013 の EUR/USD で 20/10（フル・ルール）が +4,186 pips」「**USD/JPY では 55/20 が直近 5 年間負け戦略**」— [Babypips](https://forums.babypips.com/t/donchian-channel-trading/71676?page=4)（検証条件不明）
- 【業者/コード公開】MQL5 記事 "Automating Classic Market Methods in MQL5 (Part 5): The Original Turtle Trading Rules"（System 1=20 日、System 2=55 日の実装）— [MQL5](https://www.mql5.com/en/articles/23448)
- 【ブログ】TradersPost/LuxAlgo の Donchian 解説: 「非トレンド局面では **シグナルの 60〜70% が失敗**」「フィルターなしの純粋ブレイクは誤り。日足 200EMA か ADX>20 を要求せよ」「55 日 Donchian はチョッピー相場のダマシ除去用」「終値確定（close confirmation）でダマシを減らす」— [TradersPost](https://blog.traderspost.io/article/donchian-channel-trading-strategies); [LuxAlgo](https://www.luxalgo.com/blog/donchian-channels-breakout-and-trend-following-strategy/)（数値の算出根拠は示されていない）
- 【フォーラム】ロンドンブレイクアウト: TradingView のスクリプト作者が「既定設定で 3〜6 か月・100 取引超を検証したが成功とは言えず、盲目的に使うべきでない」と注記 — [TradingView](https://www.tradingview.com/scripts/breakouttrade/)。日本語解説は「英国 7 時台の最初の 1 時間はダマシが多いので 8 時開始か 15〜20 pips の保守的ターゲット」と助言 — [FX Replay](https://fxreplay.com/ja/learn/can-you-backtest-a-breakout-strategy-heres-how-to-do-it-right)
- 【業者】USDJPY 向け「Tokyo Range Breakout」EA の仕様: 東京レンジ（UTC 0:00–6:00）を自動特定、EMA150 トレンドフィルター、レンジを 2 pips 超えた瞬間にエントリー、UTC 20:00 で失効 — [MQL5 Market](https://www.mql5.com/ja/market/product/173783)（成績の独立検証なし）
- 【業者】MQL5 blog "Why the Same Strategy Wins in London and Dies in Tokyo"（セッション別の挙動差を論じる; 全文未取得）— [MQL5](https://www.mql5.com/en/blogs/post/774527)
- 【業者】OANDA Japan が TradingView 内蔵「ChannelBreakOutStrategy」と「Bollinger Band Breakout Positional Strategy – BN – 15M」を解説（15 分足ボリンジャーブレイクの成績表あり; 本文未取得）— [OANDA Lab (Channel)](https://www.oanda.jp/lab-education/tradingviewstrategy/strategy/channel_break_out_strategy); [OANDA Lab (BB 15M)](https://www.oanda.jp/lab-education/tradingviewstrategy/strategy/bollinger_band_breakout_positional_strategy_bn_15m/)

### Inferences
- 現行の **15m Donchian-20 は「直近 5 時間の高値安値」**であり、学術・公開検証が扱う 20〜55 **日**とは時間スケールが 1/50 以下。日足で確認された持続トレンド（Schulmeister）が 5 時間スケールで同程度に存在する証拠はなく、ここが本システムの最大の未検証仮定。
- 入手できた USD/JPY 時間足の唯一の数値（H1・Donchian 40・PF 1.50・勝率 34.5%・631 取引/9.4 年≒月 5.6 取引）は「業者資料」だが分布形状は学術結果（勝率 30% 台、利益の長期保有）と一致する。**この分布では月次黒字率は構造的に 6〜7 割程度に留まる**（§9 試算）。
- 「20/10 が 55/20 を上回る」「55/20 は USD/JPY で負け」というフォーラム情報は低品質だが、2020 年以降の円相場（介入・BOJ イベントによる急反転）で**長いルックバックほど反転で返す**という直感と整合。15m システムでは Donchian 期間を 20 固定にせず **15〜40 の台地**を確認すべき（§9）。
- ATR バッファや終値確認は「ダマシを減らす代わりに初動を逃す」トレードオフで、FX での定量比較は見つからなかった。採否は自前のウォークフォワードで決めるしかない。

### Gaps
- 15 分足 Donchian の独立（非販売者）検証は英語・日本語とも見つからなかった。
- オープニングレンジ/ロンドンブレイクの査読研究（FX 対象）は見つからず、Babypips「Statistical London Breakout Strategy」スレッドも数値は未取得 — [Babypips](https://forums.babypips.com/t/statistical-london-breakout-strategy/46146)。
- ブレイクアウト失敗率（例: 「60〜70%」）の算出方法・対象市場は不明。Bulkowski の 10,348 パターンのスローバック統計は株式が対象で FX への転用根拠はない（§4 参照）。
- OANDA Japan の BB 15M 戦略の成績数値、ChannelBreakOut の成績数値は未取得。

---

## 3. トレンドフィルター（ADX 20/25/30、EMA 傾き、上位足整合）は期待値を上げるのか、トレード数を減らすだけか

### Takeaway
**ADX 閾値を FX で体系的に比較した査読研究は見つからなかった。** 公開されている実務検証では「ADX フィルターは最悪のトレードを除くが、USD/JPY では成績が急落した例もある」（Babypips HLHB）。上位足フィルターについても「1 取引あたりの期待値は上がらず、最悪の取引を削るだけ」が実務家の一致した見解で、閾値 20/25/30 の差を裏付ける証拠はない。

### Cited Findings
- 【ブログ/公開BT】Babypips「HLHB Trend-Catcher System」2017-02 の検証更新: ADX(14) フィルターを追加したところ **USD/JPY では成績が「急落（plummeted）」**した — [Babypips](https://www.babypips.com/trading/forex-hlhb-system-20170219)（数値は要約に出ず）
- 【ブログ】TradingView のスクリプト解説: ADX フィルターの選択肢として「基本 ADX>25」「ADX 上昇中」「緩和 ADX>20」「強気 ADX>30」が並列され、トレーダーの間では「ADX>25 では利益シグナルを逃す」「20 や 25 到達ではトレンドキャッチャーとして遅すぎる」との声 — [TradingView](https://il.tradingview.com/script/B79szmmq-ADX-with-20-Threshold)
- 【ブログ】Liberated Stock Trader（株式）: ADX(14) の 20 上抜けを買い・下抜けを売りとすると S&P500 構成株で 28% のアウトパフォーム — [Liberated Stock Trader](https://www.liberatedstocktrader.com/adx-indicator/)（株式・単純検証・FX 転用不可）
- 【ブログ】Unger Academy "Trading with ADX: Discover the Levels that Really Work (and Why)"（閾値別の検証記事; 本文未取得）— [Unger Academy](https://ungeracademy.com/blog/trading-with-adx-discover-the-levels-that-really-work-and-why)
- 【ブログ】Alpha Algo Trading Research "Stop trading in chop, start using ADX"（ADX フィルター有無の比較; 本文未取得）— [Substack](https://algotr.substack.com/p/stop-trading-in-chop-start-using)
- 【ブログ】LuxAlgo「Higher-timeframe Trend Filter」: 「MTF は 1 取引あたりの収益性を上げない。**最悪のトレードを除去する**だけ」「逆張りエントリーをゲートすると取引数が減り、トレンド局面では最悪の損失の多くが消える」— [LuxAlgo](https://www.luxalgo.com/library/concept/higher-timeframe-trend-filter.md)
- 【ブログ】Backtrex「Multi-timeframe backtesting」: 時間軸比は **4〜6 倍**（15 分足→1 時間足、1 時間足→4 時間/日足）が一般的、HTF データの先読み（lookahead）を切らないとバックテストだけ完璧になる — [Backtrex](https://backtrex.com/en/blog/multi-timeframe-backtesting-guide)
- 【ブログ】Quantified Strategies "Multi-Timeframe Analysis And Strategy"（本文未取得）— [Substack](https://quantifiedstrategies.substack.com/p/multi-timeframe-analysis-and-strategy)
- 【公開BT】QuantInsti EPAT はパラメータを「ピークのシャープ」ではなく「近傍の安定性」で選び、23 窓のウォークフォワードで評価（§1）。トレンド定義そのものが生き残ったのは時系列モメンタムのみ — [QuantInsti](https://www.quantinsti.com/articles/trend-following-strategies-major-currency-markets/)
- 【査読】Schulmeister の円ドル結果（§1）: 収益は持続トレンド期にのみ発生し、1990 年代半ば以降に消失 — [WIFO](https://www.wifo.ac.at/publication/111817/)。これはトレンドフィルターの価値が「トレンドが存在する時代」に依存することを含意。

### Inferences
- 現行の **1H→15m（4 倍）** という時間軸比は実務慣行に合致。EMA 傾き 3 本・ADX>20・ATR パーセンタイル≥20% の 3 重フィルターは、各々が取引数を削る一方で期待値を上げる証拠がなく、**フィルター数が多いほどパラメータの自由度が増えて過学習リスクが増す**（§9 の「パラメータ 1 つにつき 30 取引」ルールに照らすと、現行システムは十数個の自由パラメータを持つため IS で 300〜500 取引が必要）。
- ADX(14) は 1H で 14 時間、15m なら 3.5 時間のウィンドウでしかなく、USD/JPY の介入・指標イベントの急変動で容易に >20 になる。**ADX の有無・閾値（なし／20／25）は USD/JPY 固有に A/B 検証する必要**があり、Babypips の例は「追加で悪化する」可能性を示す。
- ATR パーセンタイル下限（≥20%）は「低ボラ時にブレイクが伸びない」という仮定に基づくが、FX での定量的裏付けは見つからなかった。むしろ**低ボラ圧縮→拡張**（ボラティリティ・ブレイクアウト）を狙う立場とは逆の設計であり、どちらが USD/JPY 15m で優位かは自前検証が必要。

### Gaps
- FX（特に USD/JPY）で ADX 20/25/30 を比較した査読研究・再現可能な公開検証は発見できなかった。
- EMA 傾き（slope lookback）を単独で検証した資料は見つからなかった。
- Unger Academy・Alpha Algo の ADX 検証の具体値（取引数・PF・平均損益）は未取得。

---

## 4. 押し目エントリー vs ブレイクアウトエントリー（押し目の深さ、指値 vs 逆指値、「ブレイク後の最初の押し目」）

### Takeaway
入手できた唯一の定量比較（Quantamental Trader のバックテスト）は、**ブレイク即エントリー +0.09R、きれいな押し目 +0.31R、深い押し目 −0.04R だが、押し目が発生するのは 47% のみ**なので、ブレイク 1 回あたりの期待値は両者ほぼ同じ（+0.09R vs +0.06R）で「違いなし、どちらでも良い」と結論している。押し目待ちは勝率と 1 取引あたりの R を上げるが参加率を下げるトレードオフであり、総期待値を上げる証拠はない。

### Cited Findings
- 【ブログ/公開BT】Quantamental Trader "Breakout Entry vs Pullback Entry": ブレイク即エントリーの期待値 **+0.09R**、クリーンな再テスト（retest）**+0.31R**、深い再テスト **−0.04R**。再テストは **47% のケースでしか発生しない**ため、全ブレイクを取る（+0.09R）か再テストを待つ（+0.06R/ブレイク）かで期待値はほぼ同じ。ほとんどの時間軸で評決は「**NO DIFFERENCE — pick either**」— [Substack](https://quantamentaltrader.substack.com/p/breakout-entry-vs-pullback-entry)（対象市場・期間・ストップ定義は要約に出ず）
- 【ブログ】同記事および AskTraders の整理: 押し目は「明確な方向バイアスで勝率が高くタイトなストップが置けるが、初動の爆発的な値動きを逃す」、ブレイクは「大きな動きを早く掴めるがダマシが多い」— [AskTraders](https://www.asktraders.com/learn-to-trade/technical-analysis/pullback-breakout-styles-trading/)
- 【書籍統計（株式）】Bulkowski は 10,348 のチャートパターンでスローバック/プルバックを計測し、（Quantamental の引用によれば）スローバックのない上方ブレイクの方が 97% のケースで成績が良い — [Substack 経由](https://quantamentaltrader.substack.com/p/breakout-entry-vs-pullback-entry)（原典は Bulkowski の株式研究。FX への転用根拠なし。引用の正確性も未確認）
- 【ブログ】Quantified Strategies "Low-Risk Pullback Strategy"（本文未取得）— [Substack](https://quantifiedstrategies.substack.com/p/low-risk-pullback-strategy)
- 【フォーラム】Babypips「Breakout trading vs Pullback trading in trends」— 定量結果なし — [Babypips](https://forums.babypips.com/t/breakout-trading-vs-pullback-trading-in-trends/318693)

### Inferences
- 現行の **1m 押し目再エントリー（窓 5 本、最小 0.05 ATR、浅い押し目上限 0.50 ATR）**は、上記研究の「クリーンな再テスト」に近い設計で、採用すれば **勝率・R は上がるが参加率が約半分に落ちる**と予想される。月次黒字を狙うなら「参加率低下によるトレード数減少」は月内分散を悪化させる（§9）。
- 「深い押し目は期待値マイナス」という結果は、現行の shallow_pullback_max 0.50 ATR の上限設定を支持する（深い押し目＝ブレイク失敗の兆候という解釈）。
- フィボナッチ 38〜62% や EMA20 タッチといった押し目深さの定義に関する FX の定量証拠は見つからなかった。ATR 比で深さを定義する現行方式の方がボラ正規化の点で合理的。

### Gaps
- Quantamental の検証条件（銘柄、時間軸、期間、サンプル数、コスト）が未取得で、FX・USD/JPY への適用可能性は不明。
- Adam Grimes 系の統計検定（"The Art and Science of Technical Analysis"）の押し目に関する数値は見つからなかった。
- 指値 vs 逆指値エントリーの約定率・スリッページ差を FX で比較した資料は見つからなかった。

---

## 5. 平均回帰系（ボリンジャー逆張り、RSI(2)/RSI(14)、Connors 型、東京時間のレンジ回帰）は USD/JPY で 0.2 pips スプレッド後に機能するか

### Takeaway
USD/JPY 固有・コスト込みの独立検証は見つからなかった。入手できたのは EURUSD H1 の業者 EA（PF 1.2〜1.33、年 150〜200 取引ないし 22 か月で 2,800 取引超）で、**1 取引あたりの利幅が薄くスプレッドに脆弱**な構造。東京時間については「仲値（9:55 JST）前の USD/JPY 上昇（ゴトー日）」と「自国通貨は自国時間に減価する」という時間帯効果が学術的に記録されているが、単独で安定収益になるほど強くない。

### Cited Findings
- 【フォーラム】TradingView "Forex Master (EUR/USD)" の RSI-2（Connors）: 「主要 12 ペア直近 5 年、全 80 ペア 2007-11-28〜2014-06-09 で impressive results」— 数値なし — [TradingView](https://it.tradingview.com/scripts/rsi-2/page-7)
- 【ブログ】LuxAlgo「RSI-2」: Connors & Alvarez (2008) の原型は **日足**、200 日 SMA 上でのみ買い、RSI(2)<10 で入り 5 日 MA 上抜けで出る、平均保有約 2 日。失敗モードは「押し目が本物の下落トレンドに発展する時」— [LuxAlgo](https://www.luxalgo.com/library/concept/rsi-2.md)
- 【公開BT/フォーラム】ProRealCode "RSI FX KISS h1": EURUSD・1 時間足・スプレッド 2 ポイント込みで期間利益 26%、勝率 66%（期間・取引数は要約に出ず）— [ProRealCode](https://www.prorealcode.com/prorealtime-trading-strategies/rsi-fx-kiss-h1-audusd-audsgd-eurusd)
- 【公開BT/フォーラム】ProRealCode "End of Day forex trading strategy for USD/JPY"（日足終値ベースの USD/JPY 戦略; 数値未取得）— [ProRealCode](https://prorealcode.com/prorealtime-trading-strategies/end-of-day-forex-trading-strategy-usdjpy/)
- 【業者】MQL5 "BB RSI Mean Reversion EA Optimized for EURUSD": EURUSD H1 で **PF 約 1.33、最大 DD <10%、年 150〜200 取引** — [MQL5 Market](https://www.mql5.com/en/market/product/137080)
- 【業者】ボリンジャー逆張りの別例: EURUSD で 2024-01〜2025-10（22 か月）純益 23,129 ドル、PF 1.2 超、取引 2,800 回超、ライブ勝率 56.6% — [PineForge (USDJPY 版ページ)](https://getpineforge.com/strategies/usdjpy-bollinger-bands); [cTrader](https://ctrader.com/products/2304)（どちらのページの数値かは要約から特定不能）
- 【公開BT】NautilusTrader 公式チュートリアル「FX mean reversion」（コード公開、成績数値は未取得）— [NautilusTrader](https://nautilustrader.io/docs/latest/tutorials/fx_mean_reversion_ax/)
- 【査読】Ito & Yamada "Puzzles in the Forex Tokyo Fixing: Order Imbalances and Biased Pricing by Banks" (NBER w22820): 東京仲値は各行が 9:55 の銀行間レートで決定し、顧客注文量に基づき仲値ウィンドウで注文を出す。**カレンダー効果（ゴトー日等）が仲値水準と仲値前後の価格変動に影響** — [NBER](https://www.nber.org/papers/w22820); [要約](https://academicnewsletter.sufe.edu.cn/info/297738)
- 【業者】EBC「Gotobi Strategy」: 5・10 日に USD/JPY が仲値前に上昇する傾向は輸入企業のドル需要で説明されるが、「一貫性が足りず保証されたシグナルではない」— [EBC](https://www.ebc.com/forex/gotobi-strategy)
- 【査読】Breedon & Ranaldo "Intraday patterns in FX returns and order flow" (SNB WP 2011-04): 通貨は自国の取引時間帯に減価する傾向 — [SNB](https://snb.ch/en/publications/research/working-papers/2011/working_paper_2011_04)（数値未取得）。Quantpedia #269「Intraday Currency Seasonality」は欧州時間に欧州通貨が対ドルで上昇、米国時間に下落し、その差が収益になると整理 — [Quantpedia](https://quantpedia.com/Blog/page/103/?Page=33)
- 【ブログ】OANDA Japan: RSI 単純逆張りは強トレンドでの「バンドウォーク」で損失、レンジ相場でのみ有効 — [OANDA Lab](https://www.oanda.jp/lab-education/?p=16550)

### Inferences
- EURUSD H1 で PF 1.2〜1.33・高頻度という業者結果は、**1 取引あたり純益がスプレッド数本分**しかないことを意味し、USD/JPY で 0.2 pips（＋滑り・早朝の拡大）を仮定しても、15 分足以下では **期待値の大半をコストが食う**設計になりやすい。
- 東京時間（日本時間 9:00〜15:00）のレンジ回帰を USD/JPY で狙う根拠は「ゴトー日・仲値」のカレンダー効果と「自国時間減価」の 2 点だが、いずれも日次で数 pips〜十数 pips 規模の統計的傾向であり、ストップ幅を ATR×2 とする現行システムの設計とは整合しない。
- 平均回帰ルールを現行システムに混ぜるなら、トレンドフィルターが「オフ」の局面（ADX<20 かつ EMA フラット）に限定したレジーム分離が筋だが、その有効性を示す公開証拠はなかった。

### Gaps
- USD/JPY の RSI(2)・RSI(14)・BB 逆張りを **コスト込み・2015 年以降**で検証した独立資料は英語・日本語とも見つからなかった。
- Connors 型を FX に適用した書籍・ブログの数値（Quantified Strategies 等）は本文未取得。
- Breedon-Ranaldo の効果量（bps/日）、Quantpedia #269 の成績は未取得。「ドルポートフォリオの日中 W 字パターン」に言及する研究も検索要約に現れたが出典 URL を特定できなかった。

---

## 6. 決済ルール（ATR ストップ 1.5/2/3、ATR トレーリング 2.5/3.5・Chandelier、時間切れ、分割決済、建値移動、固定 R:R 1:1/1:2/1:3 vs トレーリング）

### Takeaway
決済ルールの比較で**査読水準の FX 証拠は皆無**。公開検証では「トレーリングは勝率を上げるが利益を削る（例: DD −0.18% に対し利益 −22%）」「トレンドフォローでもトレーリングと固定の差はほぼなかった」「分割決済は分布を変えるだけで期待値をタダで上げない（最大の勝ちトレードを削る）」が共通見解。Schulmeister の「利益トレードは損失トレードの 2〜6 倍長く続く」という事実は、**右裾（大勝ち）を削る 50% 分割決済に不利**に働く。

### Cited Findings
- 【公開BT/ブログ】Forexop "Trailing Stops – Can they Increase Your Profits?": 標準的なトレーリングストップは **DD をわずか 0.18% 減らす代わりに利益を 22% 減らした**。「トレンドフォロー戦略ではトレーリングが有利と予想したが、両戦略で結果はほぼ同じだった」— [Forexop](https://forexop.com/strategy/trailing-stops/)
- 【公開BT】StockCharts (Arthur Hill, 2017) "SystemTrader – Trailing Stops versus Profit Targets for a Moving Average Crossover System"（本文未取得）— [StockCharts](https://articles.stockcharts.com/article/articles-arthurhill-2017-05-systemtrader---trailing-stops-versus-profit-targets-for-a-moving-average-crossover-system/)
- 【ブログ】LuxAlgo「Scaling Out」: 分割決済は「結果の分布を作り替えるだけで、タダで改善しない。勝ちで終わる取引の割合を上げ全戻しを和らげるが、**動きが最も伸びる時にポジションが最小になるため最大の勝ちを頭打ちにする**。純期待値はどちらにも転び、第 1 目標を超えて伸びる頻度に依存」— [LuxAlgo](https://www.luxalgo.com/library/concept/scaling-out.md)
- 【ブログ】LuxAlgo「Trailing Method Taxonomy」「ATR Trailing Regime」「ATR-based Stop Distance」: Chandelier の古典設定は **ATR(22)×3**、公開されている既定倍率は概ね **2〜3.5×ATR**、小さい倍率は早く反転してノイズで振られ、大きい倍率は押しに耐えるが利益を返す。「トレンドフォローはトレーリング（固定目標は右裾を切る）、平均回帰は目標値（期待される動きが有界）」— [Taxonomy](https://www.luxalgo.com/library/concept/trailing-method-taxonomy.md); [ATR Trailing Regime](https://www.luxalgo.com/library/concept/atr-trailing-regime.md); [ATR Stop Distance](https://www.luxalgo.com/library/concept/atr-based-stop-distance.md)
- 【フォーラム】Babypips「Statistics of setting break even vs letting run」: 「統計的にはストップを動かさず走らせる方が有利」との経験則、建値移動は心理的トリックとの意見 — [Babypips](https://forums.babypips.com/t/statistics-of-setting-break-even-vs-letting-run/126971)（定量なし）
- 【業者】FXCM "Traits of Successful Traders": 1,200 万件超の取引を分析、リスクリワード 1:1 以上が成功と相関、同じ勝率 30% でも 1:1 なら −4 ドル、3:1 なら +2 ドルの例示、失敗要因は「時間帯」「過大レバレッジ」「悪い R:R」— [FXCM](https://www.fxcm.com/uk/trading-guides/traits-successful-traders/); [DailyFX 2011](https://www.dailyfx.com/forex/publisher_blog/2011/12/26/what_are_the_traits_of_successful_forex_traders.html); [DailyFX 2014](https://www.dailyfx.com/forex/education/trading_tips/post_of_the_day/2014/02/11/risk_a_little_to_make_a_lot.html)。★未検証（調査者の既知情報）: 同研究では R:R≥1:1 を守った口座の約 53% が 12 か月で黒字、守らなかった口座は約 17%。USD/JPY 等で勝率は 6 割前後なのに平均損失が平均利益を上回るため口座は赤字。
- 【査読】Schulmeister 円ドル研究（§1）: 損失回数 > 利益回数、利益ポジションの保有期間は損失ポジションの 2〜6 倍 — [WIFO](https://www.wifo.ac.at/publication/111817/)
- 【ブログ】「出口が取引結果の 70% を決める」「個人の多くは誤った出口手法を使っている」との主張 — [Traders Second Brain](https://traderssecondbrain.com/guides/take-profit-methods)（根拠データなし。採用不可）

### Inferences
- 現行の **初期 ATR×2.0／トレーリング ATR×2.5** は公開既定値レンジ（2〜3.5）の下側にあり、**トレーリング 2.5 は Chandelier 既定 3 より早く利益を返す**設計。トレンドフォローで利益が少数の長い保有に集中する以上、2.5→3.0〜3.5 の方が総利益は増え、勝率は下がる可能性が高い（要 A/B）。
- **50% 分割決済**は「月の黒字率（勝ち月の割合）」を上げる一方で総期待値を下げる可能性が高い。オーナーの「毎月黒字」目標に対しては、総利益を犠牲にして月次分散を下げる手段として「意図的に」使う選択はあり得るが、その代償を数値で把握してから決めるべき。
- 固定 R:R（1:2 等）は R:R≥1 を守る限りブローカーデータが支持するが、1:2 や 1:3 が ATR トレーリングを上回るという証拠はない。
- 建値移動と時間切れ決済は FX での定量証拠が見つからなかったため「証拠なし」に分類。

### Gaps
- ATR 倍率（1.5/2/3）や Chandelier（2.5/3.5）の FX 比較検証、時間切れ決済の検証、建値移動の検証はいずれも再現可能な資料が見つからなかった。
- StockCharts 記事の具体値、FXCM の通貨ペア別平均勝ち pips/負け pips は本文未取得。

---

## 7. ローソク足・プライスアクション・サポレジ・チャートパターン・一目均衡表・フィボナッチ

### Takeaway
**日中サポレジ（Osler 2000）と包み足（Alanazi 2020、FX 24 ペア）には査読レベルの肯定証拠**がある一方、**一目均衡表は通貨では既定・最適化いずれでも一貫した収益なし（2021 年査読）**、ローソク足反転パターンは株式で「平均リターンがゼロと区別できない」とする研究が多く、フィボナッチには信頼できる検証が見つからなかった。ピンバーの公開成績は 1 年・1 分足の EA 販売者資料のみ。

### Cited Findings
- 【査読/中銀】Osler (2000) "Support for Resistance: Technical Analysis and Intraday Exchange Rates" FRBNY Economic Policy Review 2000-07。6 社（商業銀行・投資銀行・情報ベンダー）が 1996-01〜1998-03 に日々公表したサポレジ水準を、DEM/USD・**JPY/USD**・GBP/USD の **1 分間隔**気配値（NY 9〜16 時）で検証。**公表水準が日中トレンドの中断を予測する強い証拠**、ただし予測力は通貨・会社によって異なる — [NY Fed HTML](https://www.newyorkfed.org/medialibrary/media/research/epr/00v06n2/0007osle.html); [PDF](https://resources.newyorkfed.org/medialibrary/media/research/epr/00v06n2/0007osle.pdf); [Staff Report 125](https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr125.html)。★未検証（調査者の既知情報）: 反発頻度は無作為水準より有意に高く、予測力は公表後およそ 5 営業日持続、ラウンドナンバー近傍で強い。
- 【査読】Lo, Mamaysky & Wang (2000) "Foundations of Technical Analysis" Journal of Finance 55(4): 1705–1765。カーネル回帰で米国株 1962–1996 のヘッド＆ショルダー・ダブルボトム等を自動検出し、条件付きリターン分布が無条件分布と異なる（情報を含む）ことを示した。**収益性の証明ではない** — [NBER w7613](https://www.nber.org/papers/w7613); [MIT](https://web.mit.edu/Alo/www/Papers/techanal.html)
- 【査読】Alanazi (2020) "The bullish and the bearish engulfing patterns: evidence from the forex spot market" European Journal of Finance 26(15): 1484–1505。**24 通貨ペア 2000–2018、インサンプル日足 112,792 本、アウトオブサンプル 4 時間足 148,992 本、300 万件超の気配**を走査。**特にメジャー 7 ペアで有意な収益性**を報告し、取引コストの重要性と「米ドルの優位」を記録 — [IDEAS](https://ideas.repec.org/a/taf/eurjfi/v26y2020i15p1484-1505.html); [SDL index](https://sindex.sdl.edu.sa/esploro/outputs/journalArticle/The-bullish-and-the-bearish-engulfing/9912938508331)
- 【査読】タイ株式のローソク足検証 (SAGE Open 2017): 歪度調整 t 検定と二項検定で強気・弱気反転パターンの平均リターンの多くがゼロと区別できず。Marshall-Young-Rose は米国・日本株でローソク足は無益と報告 — [SAGE](https://journals.sagepub.com/doi/10.1177/2158244017736799)
- 【査読】一目均衡表 (International Journal of Finance & Economics 26(4), 2021: 5321–5336): 株価指数 4 本（1995–2018）と主要通貨ペア 4 本（2003–2018）で検証。既定 (9,26,52) は第 1 期に複数の収益戦略を生んだが**第 2 期では一貫して価値を生めず**、パラメータ掃引でも**通貨では収益戦略が 1 つも見つからなかった** — [IDEAS](https://ideas.repec.org/a/wly/ijfiec/v26y2021i4p5321-5336.html)
- 【ブログ】Liberated Stock Trader: 一目均衡表を DJ-30 構成株 20 年（15,024 取引）で検証し勝率 10%、90% のケースでバイ＆ホールドに劣後 — [Liberated Stock Trader](https://liberatedstocktrader.com/?p=54)（株式・検証設計不明）
- 【業者/フォーラム】EarnForex "PinbarMaster" EA: GBPJPY **1 分足**、2024-02〜2025-02 で勝率 57.82%、PF 1.32、純益 15,201 ドル。2024 暦年では勝率 54.96%、PF 1.42 — [EarnForex](https://www.earnforex.com/forum/threads/a-powerful-pinbar-reversal-expert-advisor.51473/latest)（1 年・販売者・最適化の疑い）
- 【フォーラム】ピンバー戦略を 12 通貨で検証し 8 勝 1 分 3 敗との TradingView スクリプト記述 — [TradingView](https://it.tradingview.com/script/ekUY41fE-PineScript-v4-Forex-Pin-Bar-Trading-Strategy)（条件不明）
- 【ブログ】Forexop "The Engulfing Candlestick Trade – How Reliable Is It?"（本文未取得）— [Forexop](https://forexop.com/strategy/engulfing-candle-trade/)

### Inferences
- 現行システムに最も転用しやすい査読知見は **Osler の「公表サポレジ・ラウンドナンバー近傍で日中トレンドが中断されやすい」**であり、15m Donchian ブレイクの直上に 150.00 のような節目がある場合に見送る、または目標をそこに置くといったルールが論理的に導ける（効果量は要自前検証）。
- 包み足（Alanazi）は FX で唯一「コスト考慮で有意」とされた足型だが、日足・4 時間足での結果であり 15m/1m への外挿はできない。
- 一目均衡表・フィボナッチ・ピンバーは「証拠なし」または「否定的」に分類し、現行システムに追加しない。

### Gaps
- Osler の反発率・持続日数の具体値、Osler (2003) のラウンドナンバー研究は本文未取得。
- Alanazi の勝率・1 取引あたり pips・採用スプレッドは未取得。
- フィボナッチ・リトレースメントの独立検証は見つからなかった。
- 日本語圏の一目均衡表検証（通貨）は見つからなかった。

---

## 8. 時間軸効果: なぜ 1 時間足未満のルールはコスト後に失敗しやすいか、保有期間と収益性、USD/JPY の ATR と時間帯別ボラティリティ（JST）

### Takeaway
「短期足ほど 1 取引あたりの獲得 pips が小さくなる一方、スプレッド＋滑りは一定」という算術が支配的で、Schulmeister は FX の利益が「長く続く少数のポジション」に集中することを示した。**USD/JPY の年別日次 ATR の一次資料は本セッションでは取得できず**、入手できたのは年間高安・平均レート（2024: 139.58–161.95、2025: 139.89–158.87）、2024 年春の週次レンジ（6/9 週で 150 pips 超、4 週で 300 pips 超、BOJ 利上げ週は 835 pips）、過去の ATR（2015 年 158 pips、2017 年 78 pips）、時間帯別ボラ（ロンドン/NY 重複で USD/JPY が毎時約 34 pips）といった断片である。ATR は現行アプリのキャッシュから自前で算出することを推奨（末尾にスクリプト）。

### Cited Findings
- 【業者】Pepperstone「How to trade USD/JPY」: USD/JPY は米国/ロンドン/フランクフルト重複時間に **毎時約 34 pips** 動く最もボラタイルな通貨 — [Pepperstone](https://pepperstone.com/en-gb/insights/guides/forex/how-to-trade-usdjpy)（集計期間不明）
- 【ブログ/公開分析】Medium "Unveiling Forex Volatility: A Power BI Analysis of EURUSD, USDJPY, and GBPUSD": ロンドン–NY 重複（08:00–12:00 EST）が一貫して中央値ボラ最大、ピークは **3 時・8 時・10 時 (EST)**（＝JST 17 時・22 時・24 時）、USDJPY は東京時間（00:00–04:00 UTC＝JST 9〜13 時）にも顕著なボラ — [Medium](https://medium.com/@jushijun/unveiling-forex-volatility-a-power-bi-analysis-of-eurusd-usdjpy-and-gbpusd-04fc4d5d5917)
- 【ブログ】QuantHedge "Volatility Profiles of the Most Liquid Currencies" / "Seasonality of Intraday Volatility in FX"（時間帯別プロファイル; 本文未取得）— [Profiles](https://quanthedge.substack.com/p/statistical-profiles-of-the-most); [Seasonality](https://quanthedge.substack.com/p/seasonality-of-intraday-volatility)
- 【研究所】ニッセイ基礎研究所: 主要通貨ペアの 4 時間ごと終値差分を pips で時間帯別（JST 7–11 時、11–15 時、15–19 時…）に集計したレポート — [NLI Research PDF](https://www.nli-research.co.jp/files/topics/42141_ext_18_0.pdf?site=nli)（数値未取得）
- 【業者】FXStreet 2024-05-23: USD/JPY は直近 9 週のうち 6 週で 150 pips 超、4 週で 300 pips 超の週次レンジ、**BOJ 利上げ週には 835 pips** — [FXStreet](https://www.fxstreet.com/analysis/fed-interest-rate-move-contains-the-power-to-significantly-move-currency-prices-202405231251)
- 【業者】FXStreet 2017-03: USD/JPY の日次 ATR 78 pips — [FXStreet](https://www.fxstreet.com/amp/analysis/usdjpy-continuation-201703231237); FXStreet 2015-09: ATR 158 pips — [FXStreet](https://www.fxstreet.com/analysis/what-i-see-in-fx-market/2015/09/04)
- 【業者】楽天証券: 2019 年のドル円の年間値幅は約 8 円でボラティリティが小さかった — [楽天証券](https://media.rakuten-sec.net/articles/-/3604)
- 【データ】年間統計: 2024 年高値 161.948 (7/3)・安値 139.581 (9/16)・平均 151.4625; 2025 年高値 158.872 (1/10)・安値 139.89 (4/22)・平均 149.654 — [PSL 2024](https://www.poundsterlinglive.com/history/USD-JPY-2024); [PSL 2025](https://www.poundsterlinglive.com/history/USD-JPY-2025)。年平均レート 2022: 131.53、2023: 140.57、2024: 151.52 — [Traders Union](https://tradersunion.com/currencies/price-history/usd-jpy/)
- 【業者】コスト算術: 「10 pips 狙いのスキャルで往復 2 pips なら利益の 20% がコスト」「同じ pips を 50 往復で稼ぐと 2 pips×50＝100 pips のコスト」「1 時間足以上で +30 pips を狙えばスプレッドの影響は大きく下がる」— [FXStreet](https://www.fxstreet.com/education/measuring-the-trade-201611231316); [LuxAlgo break-even](https://www.luxalgo.com/blog/break-even-analysis-for-forex-traders/)
- 【査読】Schulmeister (2008, 円ドル): 利益は持続トレンドのみに由来、利益ポジションは損失ポジションの 2〜6 倍長い — [WIFO](https://www.wifo.ac.at/publication/111817/)。同 (2009, 株式): 日次で死んだモデルが 30 分足では 1983–2007 に年率 7.2% — [IDEAS](https://ideas.repec.org/a/eee/revfin/v18y2009i4p190-201.html)
- 【業者】FXCM Traits: 失敗要因の 1 つが「取引する時間帯の誤り」— [FXCM](https://www.fxcm.com/uk/trading-guides/traits-successful-traders/)

### Inferences
- 15 分足の 1 本あたりレンジは、重複時間の毎時 34 pips（Pepperstone）を機械的に按分すれば **10〜15 pips 程度、東京時間はその半分以下**と推定される（按分は粗い仮定。実測が必要）。GMO 系の USD/JPY 原則固定 0.2 pips スプレッドなら **スプレッド/15m ATR ≈ 0.02〜0.04** で、現行の上限 0.15 は通常時にほぼ常時パスし、早朝・指標時の拡大時のみ機能する設計になっている。
- したがって本システムの損益を決めるのは **スプレッドではなく「ブレイクの成否比率」と「滑り・約定遅延」**。研究モードのスプレッド・ストレス（×1.2/1.5）より **entry_delay_scenarios（0/1/2 本）と intrabar_policy** の方が結果に効くはずで、そちらの頑健性を優先して見るべき。
- 2022 年以降は年間レンジ 20 円前後（2024: 22.4 円、2025: 19.0 円）で 2019 年（8 円）の 2〜3 倍。**ATR パーセンタイル（20 日や 1 年の分位）に基づくフィルターは、2022–2025 のデータで最適化すると低ボラ回帰時に機能停止する**リスクがある。
- 時間帯別には JST 16〜17 時（ロンドン開始）、21〜24 時（NY 重複・米指標）が高ボラ、JST 早朝（4〜7 時）は低流動性でスプレッド拡大。15m ブレイクは JST 15〜24 時に集中させ、JST 2〜8 時の新規エントリーを止める時間帯ゲートは証拠（FXCM の「時間帯」要因、Medium のピーク時刻）と整合する。

### Gaps
- **USD/JPY の年別日次 ATR（2020–2026、pips）および 15m・1H の典型レンジ、JST 時間帯別ボラの一次資料は取得できなかった**（GMO 公開 API を含む全データソースが遮断）。以下のスクリプトで本アプリの parquet キャッシュから算出することを推奨:

```python
# 使い方: python atr_profile.py <cache_dir> （例: data_cache）。timezone-aware 前提。
import sys, pandas as pd
cache = sys.argv[1]
d = pd.read_parquet(f"{cache}/USD_JPY/1Day.parquet")            # 列: open/high/low/close, index=UTC
prev = d["close"].shift(1)
tr = pd.concat([d["high"]-d["low"], (d["high"]-prev).abs(), (d["low"]-prev).abs()], axis=1).max(axis=1)
atr14 = tr.rolling(14).mean()
print("年別 日次ATR(14) 平均 [pips]:\n", (atr14.groupby(d.index.year).mean()*100).round(1))
for tf in ("15Min", "1Hour"):
    b = pd.read_parquet(f"{cache}/USD_JPY/{tf}.parquet")
    rng = (b["high"]-b["low"])*100
    jst = b.index.tz_convert("Asia/Tokyo")
    print(f"\n{tf} レンジ中央値 [pips] JST時間帯別:\n", rng.groupby(jst.hour).median().round(1).to_string())
```
- ニッセイ基礎研の時間帯別 pips 表、QuantHedge の時間帯プロファイルの具体値は未取得。
- 「保有期間が長いほど収益性が高い」ことを FX で直接回帰した研究は見つからなかった（Schulmeister の保有期間非対称性が最も近い）。

---

## 9. 過剰最適化と頑健性: パラメータ台地、ウォークフォワード、信頼に必要なトレード数

### Takeaway
実務的コンセンサスは「**OOS 合計 100 取引未満のウォークフォワードは無意味**」「**自由パラメータ 1 つにつき IS で 30 取引**」「WFE（OOS/IS 効率）0.5〜0.7 以上」「近傍が平坦な台地を選ぶ」。学術側は多重検定の補正（HTW のステップワイズ、Zarrabi の FDR）を必須とし、Zarrabi の結果は「優越ルールは短期間しか続かない」こと、QuantInsti は 23 窓 WF で 7 ペア中 2 ペアしか残らないことを示す。「Donchian 15〜40 は全部似た成績」という具体的台地の公開証拠は見つからなかった。

### Cited Findings
- 【ブログ】NexusFi「Walk-Forward Analysis」: **全 OOS 窓の合計で 100 取引未満の WF は実行するな**（ノイズが信号を圧倒）、目安として **自由パラメータ 1 つにつき IS 窓で 30 取引**、**WFE 0.70 超が意味ある頑健性の基準** — [NexusFi](https://nexusfi.com/a/automation/walk-forward-analysis); ProRealCode は WFE 50% 超を許容ラインとする — [ProRealCode](https://www.prorealcode.com/blog/learning/strategy-optimisation-walk-analysis/)
- 【ブログ】LuxAlgo「Parameter Stability」: 平坦な台地＝頑健、針状ピーク＝カーブフィット。「12 窓にわたりパラメータが 18〜26 の範囲で揺れる程度なら安定した構造」、窓ごとに値が飛ぶ場合は最適化面に安定領域がない — [LuxAlgo](https://www.luxalgo.com/library/concept/parameter-stability.md)
- 【ブログ】MQL5 blog（多重検定の統計）: 無作為・非トレンドデータで 1,000 通りのパラメータを試すと「最良」は約 97% の確率で正のシャープを示す。N 試行の期待最大値は σ√(2 ln N) で膨らみ、1 万試行で約 4.3σ — [MQL5](https://www.mql5.com/en/blogs/post/772184)
- 【ブログ】LuxAlgo「Significance of Returns」: t 統計量＝平均/標準誤差。単一の事前仮説なら約 2、開発で多数の変種を試す場合は 3 以上を要求するのが通例 — [LuxAlgo](https://www.luxalgo.com/library/concept/significance-of-returns.md)
- 【フォーラム】「30 取引で有意とみなす人もいるが多くは数百を求める。勝率 65% は 20 取引では有意でないが 200 取引なら有意」— [NexusFi](https://nexusfi.com/showthread.php?p=480714)
- 【公開BT】QuantInsti EPAT: 3 年学習/1 年検証×23 窓、パラメータは近傍安定性で選択、7 ペア中 2 ペアのみ残存 — [QuantInsti](https://www.quantinsti.com/articles/trend-following-strategies-major-currency-markets/)
- 【査読】HTW 2016 はステップワイズ検定、Zarrabi 2017 は FDR で多重検定を制御し、後者は「優越ルールは短期間のみ」— [CEPR](https://cepr.org/publications/DP10018); [Essex](https://repository.essex.ac.uk/18805/)
- 【査読】Schulmeister: 1,024 モデル全てが 1976–2007 で黒字（＝トレンド期にはパラメータ面全体が台地）だが 2000 年以降全て消失（＝台地ごと沈む）— [WIFO](https://www.wifo.ac.at/publication/111817/)
- 【業者】USDJPY H1 Donchian の公開例は 9.4 年で 631 取引（月 5.6 取引）— [MQL5](https://www.mql5.com/en/blogs/post/773712)

### Inferences
- **月次黒字の確率の試算（調査者の計算、出典なし）**: 勝率 35%、平均利益 2.5R、平均損失 1R（期待値 +0.225R/取引、1 取引の標準偏差 ≈1.67R）という「健全なブレイクアウト系」を仮定すると、月 6 取引なら月次損益の平均 1.35R・標準偏差 4.1R で **黒字月の確率 ≈63%**、月 25 取引でも ≈75%、月 100 取引でようやく ≈91%（正規近似）。**単一戦略で「毎月黒字」はエッジがあっても統計的に成立しない**。達成に近づくには（a）相関の低い複数戦略・複数通貨で月内取引数を 100 以上に増やす、（b）月次損失上限で負け月の深さを制限する、のいずれかしかない。
- 現行システムの自由パラメータは 20 個前後（EMA 2 本・傾き・ADX・ATR 分位・Donchian・バッファ・押し目 3 つ・ストップ 2 つ・トレーリング・分割・確認本数・スプレッド 3 つ）。「1 パラメータにつき 30 取引」に従うと IS で **600 取引前後**が必要で、月 5〜6 取引のシステムでは 8〜10 年分に相当する。**パラメータを 6〜8 個に減らす（傾き・ATR 分位・押し目窓・スイング余白・確認本数などを固定値化）**ことが、どの個別パラメータ調整より先に効く。
- 台地確認は「最適値」ではなく「隣接 ±30% の値で PF・DD がほぼ同じか」で判定する。Donchian 15/20/25/30/40、ATR ストップ 1.5/2/2.5/3、トレーリング 2/2.5/3/3.5 の格子で、各セルの取引数が 100 を割る格子は結論を出さない。

### Gaps
- 「Donchian 15〜40 が同程度」という具体的な台地の公開検証（FX）は見つからなかった。
- Pardo のウォークフォワード原典、Aronson "Evidence-Based Technical Analysis" の数値は本セッションでは未取得。
- FX テクニカルルールのウォークフォワードを扱った査読研究は QuantInsti（非査読）以外に見つからなかった。

---

## 10. 現行 USD/JPY システムへの具体的示唆（推奨・根拠・証拠のないルールの明示）

### Takeaway
証拠に裏付けられるのは「上位足で方向を限定する」「R:R≥1 を守る」「サポレジ・節目を意識する」「パラメータを減らして OOS 取引数を確保する」までで、**15m Donchian-20、ADX 20、ATR×2.0/2.5、50% 分割、1m 押し目再エントリーの各数値を支持する独立証拠はない**（反証もない）。「毎月黒字」は単一ルールでは統計的に不成立であり、目標を「ローリング 100 取引で期待値>0 かつ月次最大損失の上限遵守」に置き換えることを推奨する。

### Cited Findings
（本節は §1〜§9 の出典に基づく整理であり、新規の引用は含まない。各行の根拠節を示す。）

| 現行ルール | 証拠の状態 | 推奨 | 根拠節 |
|---|---|---|---|
| 1H EMA50>EMA200＋傾き 3 本 | 価格 vs 長期 MA は LeBaron 型の方向フィルターとして妥当だが、収益源としての日次 MA は 2000 年頃に消失（NWU, Schulmeister） | 維持。ただし「傾き 3 本」は固定し最適化対象から外す | §1, §3 |
| ADX(14)>20 | FX での閾値比較証拠なし。USD/JPY で追加により悪化した公開例あり（Babypips HLHB） | なし／20／25 の 3 水準で WF 比較。差が WFE の誤差内なら**撤去**（パラメータ削減） | §3 |
| ATR パーセンタイル≥20% | 証拠なし。2022–25 の高ボラで最適化すると低ボラ期に停止するリスク | 撤去候補。残すなら分位の母集団を 1 年以上に | §3, §8 |
| 15m Donchian-20（±ATR バッファ） | 日足 20/55 は歴史的に有効、15m の独立検証なし。H1 の業者例は 40 期間・PF 1.50・勝率 34.5% | 15/20/30/40 の台地を確認。バッファは 0／0.1／0.25 ATR で比較し、改善が WFE 内なら 0 固定 | §2, §9 |
| 1m 押し目再エントリー | 唯一の比較で「ブレイク即」と期待値同等、参加率 47% | **オプション化**し、「見送ったブレイクの機会損失」を計測。月内取引数が不足するなら無効化 | §4 |
| ATR×2.0 初期ストップ＋0.1 ATR 余白 | 公開既定値 2〜3.5 の範囲内。比較証拠なし | 1.5/2/2.5/3 の台地確認。余白 0.1 は固定 | §6 |
| ATR×2.5 トレーリング | Chandelier 既定 3 より早く利確。トレーリングは利益を 22% 削った例あり | 2.5/3/3.5 を比較。トレンド系なら 3〜3.5 側が総利益で優位の可能性 | §6 |
| 50% 分割決済 | 右裾を削り期待値を下げ得る（LuxAlgo）。利益が長期保有に集中する FX の性質（Schulmeister）と相性が悪い | 0%／50% を同一リスクで比較し、「月次黒字率」と「総利益」のトレードオフを数値で確認してから決定 | §6 |
| トレンド崩れ確認 2 本 | 証拠なし | 固定値として最適化対象外に | §9 |
| スプレッド分位≤80%・spread/ATR≤0.15 | コスト算術上、0.2 pips の USD/JPY では通常時ほぼ常時パス。拡大時のみ機能 | 維持。加えて JST 2〜8 時の新規エントリー停止を検討（時間帯要因は FXCM・ボラ分布と整合） | §8 |
| 「毎月黒字」目標 | 単一戦略では統計的に不成立（§9 試算: 月 6 取引で黒字月≈63%） | 目標を「直近 100 取引の期待値>0」「月次損失上限」に変更。複数戦略・複数通貨で月内取引数を増やす | §9 |

**証拠が見つからなかった、または否定的なルール（追加しないこと）**: 一目均衡表（通貨で収益なし・査読）、フィボナッチ・リトレースメント（検証なし）、ピンバー（1 年・販売者資料のみ）、建値移動・時間切れ決済（検証なし）、ADX 閾値の優劣（検証なし）、ロンドン/東京レンジブレイクの既定設定（フォーラムで否定的）、RSI(2)/BB 逆張りの USD/JPY 15m（検証なし）。

**肯定証拠があり追加を検討できる要素**: 公表サポレジ・ラウンドナンバー近傍での日中反転（Osler 2000, USD/JPY 含む）→ ブレイク直上の節目でのエントリー見送り／目標設定；包み足（Alanazi 2020, 日足・4H）→ 15m 以下では未検証；時系列モメンタム（QuantInsti, USDJPY 日足シャープ 0.78 WF）→ 1H フィルターの代替定義として検討。

### Inferences
- 「正しいパラメータ値」を探す調査は、USD/JPY 15m というスケールでは文献上ほぼ空白であり、**自前のウォークフォワード以外に答えはない**。その際に文献が与える制約は「OOS 100 取引以上」「パラメータ数の削減」「近傍安定性」「多重検定の割引（t≥3）」の 4 点。
- 2020–2026 の USD/JPY は介入・BOJ 政策転換による急反転を含むため、同期間だけでの最適化は「反転で返さない短いトレーリング」を選びがちになる。2010 年代の低ボラ期（2019 年: 年間 8 円）を含めた検証でも台地が残るかを確認すべき。

### Gaps
- 全体を通じて、USD/JPY 15m/1m スケールのルール別・コスト込み・2015 年以降の独立検証は英語・日本語とも発見できなかった。
- 本セッションは全文取得が遮断されたため、§1・§6・§7 の★未検証項目（Park & Irwin の 95 本内訳、FXCM の 53%/17%、Osler の反発率・5 営業日）は URL 先で確認が必要。
