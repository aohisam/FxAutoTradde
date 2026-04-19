# 経済指標データの無料取得源 調査レポート

対象通貨ペア: **USD/JPY, EUR/JPY, AUD/JPY, GBP/JPY**
必要な情報: 政策金利の履歴 + 経済指標カレンダー (予定・予想・実績)
網羅対象国: 日本 / 米国 / ユーロ圏 / 豪州 / 英国
作成日: 2026-04-18

---

## 0. 要旨 (TL;DR)

| 役割 | 第一候補 | 補助 |
| --- | --- | --- |
| **政策金利の履歴** (FFR, ECB MRO/DFR, RBA Cash Rate, BoE Bank Rate, BoJ Policy Rate) | **FRED API** (単一 API で全 5 中銀をカバー可能) | 各中銀の公式 CSV (ECB Data Portal / RBA F1.1 / BoE IADB / BoJ 時系列) |
| **マクロ指標のヒストリカル** (CPI, 雇用, GDP, PMI, 小売 など) | **FRED API** + **OECD Data Explorer API** (SDMX) | 各国統計局 (e-Stat, Eurostat, ONS, ABS) |
| **経済指標カレンダー** (予定時刻・予想・実績・重要度) | **Trading Economics API (guest:guest)** または **Finnhub 無料枠** | **FRED releases/dates** (米データのみ) + **ForexFactory スクレイパ** (重要度付き) |
| **集約ハブ (ワンショット検証用)** | **DBnomics** (数百プロバイダを統一 API でラップ) | — |

**結論**: API キー前提で良いなら `FRED + OECD + Trading Economics (guest) + Finnhub` の 4 本立てで 4 通貨 × 5 国の主要指標は無料で揃えられる。カレンダーの重要度 (★★★) が欲しい場合だけ ForexFactory のスクレイピングを併用する。

---

## 1. 政策金利 (Central Bank Policy Rates)

すべて無料・ヒストリカル取得可能。FRED 経由だと 1 つの API で 5 中銀全部を叩けるので実装負荷が最小。

| 国 / 中銀 | 公式ソース | FRED 系列 ID (代替) | 備考 |
| --- | --- | --- | --- |
| 🇺🇸 Fed | [Fed H.15](https://www.federalreserve.gov/releases/h15/) | `FEDFUNDS` (月次), `DFF` (日次), `DFEDTARU`/`DFEDTARL` (レンジ) | 1954-07 以降。API キー必要 (無料) |
| 🇪🇺 ECB | [ECB Data Portal (旧 SDW)](https://data.ecb.europa.eu/) | `ECBMRRFR` (MRO), `ECBDFR` (DFR), `ECBMLFR` (MLF) | SDMX REST で API キー不要。CSV ダウンロード可 |
| 🇦🇺 RBA | [RBA Statistical Table F1.1](https://www.rba.gov.au/statistics/tables/) (`f1.1-data.csv`) | `IR3TIB01AUM156N` (代替) | 認証不要の静的 CSV。日次系列は F1.1、月次は A2 |
| 🇬🇧 BoE | [BoE IADB (Bank Rate)](https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp) | `BOERUKM` (月次), `BOERUKQ` (四半期) | IADB はクエリ文字列で CSV/XML 応答。認証不要 |
| 🇯🇵 BoJ | [BOJ 時系列データ検索](https://www.stat-search.boj.or.jp/index_en.html) | `INTDSRJPM193N` (割引率), `IRSTCI01JPM156N` (コールレート) | 2026-02 に公式 API が開始。JSON/CSV 両対応。認証不要 |

### 推奨実装

- 既存コードが Python なら `pip install fredapi` で FRED を一括処理、BoJ だけ最新の公式 API で補完する構成が薄い。
- 政策金利はリアルタイム性が低い (日次更新で十分) ため、夜間バッチ 1 回の取得で足りる。

---

## 2. マクロ指標のヒストリカル (CPI / 雇用 / GDP / PMI / 小売 など)

### 2-1. 万能ハブ (まずここを叩く)

| サービス | カバレッジ | 認証 | 制限 | 備考 |
| --- | --- | --- | --- | --- |
| **FRED API** | 米指標は全網羅。BoE/ECB/BoJ/RBA 政策金利と主要ドル建て系列の OECD リミックスもミラー | 無料 API キー | 120 req/min 目安 (非公開) | JSON/XML。`fredapi` が使える |
| **OECD Data Explorer API** (SDMX 2.1) | 5 カ国すべての CPI, GDP, 失業率, CLI, 小売, 賃金 などを共通コードで引ける | **キー不要** | 1 リクエスト最大 1M 観測 / URL 1000 文字 | JSON/CSV/XML。国コード `USA`, `JPN`, `DEU`+`FRA`+`ITA` or `EA20`, `AUS`, `GBR` |
| **DBnomics** | 世界数百プロバイダを統一 API でラップ (FRED, Eurostat, ECB, BoE, BoJ, ABS, ONS を含む) | 不要 | 節度を持った利用推奨 | Python 公式パッケージ `dbnomics` あり。系列コード検索が強力 |

**おすすめは OECD + FRED の二本柱**: OECD は EUR/AUD/GBP/JPY の主要マクロを共通 ID で一括取得でき、FRED は米国指標が圧倒的に細かい。両者で賄えない隙間を DBnomics で埋める。

### 2-2. 各国統計局 (primary source、遅延が最も小さい)

| 国 | ソース | API 認証 | 代表的な指標 |
| --- | --- | --- | --- |
| 🇯🇵 日本 | [e-Stat API](https://www.e-stat.go.jp/api/en) + [BOJ 時系列 API](https://www.stat-search.boj.or.jp/index_en.html) | e-Stat は appId (無料登録) / BOJ は不要 | CPI, 失業率, 鉱工業生産, 家計調査, 短観 (Tankan), 景気ウォッチャー, 国際収支 |
| 🇺🇸 米国 | [BLS Public Data API](https://www.bls.gov/developers/) + [BEA API](https://apps.bea.gov/api/signup/) | 無料登録キー | NFP, 雇用統計, CPI, PPI, PCE, GDP |
| 🇪🇺 ユーロ圏 | [Eurostat Data Browser API](https://ec.europa.eu/eurostat/web/main/data/database) + [ECB Data Portal](https://data.ecb.europa.eu/) | 不要 | HICP, 失業率, GDP フラッシュ, 景況指数 |
| 🇦🇺 豪州 | [ABS Indicator REST API](https://www.abs.gov.au/about/data-services/application-programming-interfaces-apis/indicator-api) / [ABS Data API (Beta)](https://www.abs.gov.au/about/data-services/application-programming-interfaces-apis/data-api-user-guide) | Indicator API はメールでキー申請 (無料) / Data API は不要 | CPI (2025-11 以降月次), 労働力調査, 小売売上 |
| 🇬🇧 英国 | [ONS API](https://developer.ons.gov.uk/) | 不要 | CPI/CPIH, GDP, 労働市場統計, 小売, 住宅価格 |

**実運用上の小ワザ**: 直近発表の "初速" が欲しいときだけ統計局 API を叩き、それ以外は FRED/OECD にキャッシュされた値で揃えると、レート制限と実装コストのバランスが良い。

### 2-3. 民間 API (補助)

| サービス | 強み | 無料枠 | 注意点 |
| --- | --- | --- | --- |
| **Alpha Vantage - Economic Indicators** | GDP / CPI / NFP / 失業率 / FFR / 小売 などをシンプルな JSON で返す | **25 req/日** と非常に小さい | 米指標のみ。ヒストリカル検証用の補助で十分 |
| **Finnhub `/economic` + `/economic-code`** | 系列コード検索が楽。ETL 的な使い方に強い | 60 req/分 | ヒストリカル深度は `free tier` ではやや浅い |
| **Financial Modeling Prep** | 主要指標を広く扱う | Free tier 250 req/日 | 一部 endpoint は `stable` のみ公開 |

---

## 3. 経済指標カレンダー (予定時刻 / 予想 / 実績 / 重要度)

**実時間トレードフィルタ** に使うため、発表の *直前*・*直後* に値が揃うソースが望ましい。無料で揃えられる候補は以下。

| サービス | カバレッジ | 予定時刻 | 予想 | 実績 | 重要度 ★ | 認証 | 利用制限 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Trading Economics Calendar API** | 全世界 196 カ国 (JPY/USD/EUR/AUD/GBP すべて網羅) | ✅ | ✅ | ✅ | ✅ (Low/Medium/High) | `guest:guest` (無料デモ) / 本格利用は有償 | デモキーは機能/件数制限あり。商用配布は不可 |
| **Finnhub `/calendar/economic`** | 主要通貨 (5 通貨すべて対応) | ✅ | ✅ | ✅ | ✅ | 無料 API キー | 60 req/分、ヒストリカル深度は有料で拡張 |
| **FRED `releases/dates`** + `series/{id}` | 米系のみ (発表日時) | 米指標に限定 ✅ | ❌ | 実績は発表後に取得 ✅ | ❌ | 無料キー | **米国以外のカレンダーは取れない** |
| **Financial Modeling Prep Economic Calendar** | 主要 G20 | ✅ | ✅ | ✅ | ✅ | 無料キー (250 req/日) | 過去データの連続取得は有料 |
| **FXStreet Economic Calendar API** | 全世界 | ✅ | ✅ | ✅ | ✅ | 商用は契約必須 (デモは手触り程度) | 無料で fetch できる範囲は限定的 |
| **ForexFactory (スクレイピング)** | FX トレーダー視点で高粒度、★ アイコン + Actual/Forecast/Previous が揃う | ✅ | ✅ | ✅ | ✅ (★★★) | 不要 | **ToS 上 "個人利用・学習目的" に限定**。商用配布・再配布不可 |
| **investpy / investing.com スクレイピング** | 世界網羅 | ✅ | ✅ | ✅ | ✅ | 不要 | Cloudflare でブロックされがち。投資.com の ToS 要確認 |

### 推奨構成

- **現実的な本命**: `Finnhub` を主、`Trading Economics (guest)` を副で突き合わせ。両方を 15 分ごとにポーリングし、イベント単位で片方欠損を埋める。
- **商用配布せずローカル検証だけ** なら `ForexFactory` スクレイパを並列で動かすと、★ (重要度) と前回修正値 (revised) が取れて backtest 精度が上がる。
- **ヒストリカル** (例: 過去 10 年分の "NFP の予想 vs 実績") は無料 API で揃いにくい。どうしても欲しい場合、`market-calendar-tool` (Python) で ForexFactory を遡って取得し、**ローカル保存して再配布しない** 形で運用する。

---

## 4. 4 通貨ペアに特化したミニマム取得リスト

各通貨ペアの値動きに直結しやすい指標を挙げる。`FRED + OECD` を基本 API とした場合の取得プランも添える。

### USD/JPY
- 米: FFR (`FEDFUNDS`), NFP (`PAYEMS`), CPI (`CPIAUCSL`), コア PCE (`PCEPILFE`), ISM 製造業 (`NAPM`), 失業率 (`UNRATE`), 小売 (`RSAFS`), GDP (`GDP`)
- 日: BoJ 政策金利 (BOJ API), CPI (e-Stat), 短観 (e-Stat), 貿易収支 (e-Stat)

### EUR/JPY
- ユーロ圏: ECB MRO/DFR (ECB Data Portal), HICP (Eurostat), 失業率 (Eurostat), GDP フラッシュ (Eurostat), PMI (S&P Global、Trading Economics 経由が楽)
- 日: 上に同じ

### AUD/JPY
- 豪: RBA Cash Rate (RBA F1.1), CPI (ABS, 四半期 + 2025-11 からの月次), 雇用統計 (ABS), GDP (ABS), 貿易収支 (ABS)
- 日: 上に同じ

### GBP/JPY
- 英: BoE Bank Rate (BoE IADB), CPI/CPIH (ONS), GDP (ONS), 労働市場 (ONS), BRC 小売売上 (Trading Economics 経由)
- 日: 上に同じ

---

## 5. 実装時の注意点

1. **タイムゾーン**: ほぼすべてのソースは UTC または現地時間で返す。取引エンジンが JST 前提なら、取得層で `pandas.Timestamp.tz_convert("Asia/Tokyo")` へ正規化する。
2. **リビジョン (改定値)**: CPI/GDP は後日改定されることが多い。FRED ALFRED (`realtime_start`, `realtime_end`) を使うとバックテスト時点で "当時入手できた値" を再現できる。リアルタイムフィルタ用は現在値、backtest 用は vintage と二系統で保存するのが筋。
3. **欠損**: `investpy` / Trading Economics guest は突然応答を返さなくなることがある。**1 次ソース (FRED / OECD / 各国統計局) を常時併走** させて整合性チェックを入れること。
4. **レート制限**: FRED 120/分、Alpha Vantage 25/日、Finnhub 60/分、OECD 実質無制限 (応答サイズ制限あり)。初回バルク取得は OECD/DBnomics、差分更新は FRED/Finnhub が相性が良い。
5. **ライセンス**: FRED/OECD/各国統計局 (e-Stat, Eurostat, ONS, BoE, RBA, ABS, BoJ) は**非商用/商用問わず再配布可** (明示的にオープンデータ)。一方 **ForexFactory / investing.com / FXStreet / Trading Economics の実データ** は**再配布不可**。アプリに組み込んで UI に表示するだけなら概ね可だが、CSV 公開や二次配布は NG。
6. **`.env` への追加想定**: `FRED_API_KEY`, `FINNHUB_API_KEY`, `TRADINGECON_API_KEY` (任意), `ESTAT_APP_ID`, `BEA_API_KEY`, `BLS_API_KEY`, `ABS_INDICATOR_API_KEY`。プロジェクトの `.env.example` にドラフトとして並べておくと導入が楽。

---

## 6. 推奨する次のステップ

1. **Phase 1 (ヒストリカル整備)**: `FRED + OECD + BoJ + RBA CSV` で政策金利 + 主要マクロ 5 国分を静的 CSV にダンプし、`data_cache/` に parquet で保存する層を追加。
2. **Phase 2 (カレンダー連携)**: `Finnhub` の無料キーで `/calendar/economic?from=YYYY-MM-DD&to=...` を毎 15 分ポーリング、SQLite に upsert。直前 60 分以内にイベントがあるときは FX 参加許可フィルタを減点する。
3. **Phase 3 (精度向上)**: backtest 用に ForexFactory スクレイパで過去 5 年分をワンショット取得 → vintage 差分を ALFRED で検証 → `research_run` の頑健性チェックに反映。

---

## 7. 参考リンク

- FRED: <https://fred.stlouisfed.org/docs/api/fred/>
- ECB Data Portal API: <https://data.ecb.europa.eu/help/api/overview>
- RBA Statistical Tables: <https://www.rba.gov.au/statistics/tables/>
- BoE IADB: <https://www.bankofengland.co.uk/boeapps/database/>
- BOJ Time-Series API: <https://www.stat-search.boj.or.jp/info/api_manual_en.pdf>
- OECD Data Explorer: <https://data-explorer.oecd.org/>
- DBnomics: <https://db.nomics.world/>
- Eurostat: <https://ec.europa.eu/eurostat/web/main/data/database>
- ONS: <https://www.ons.gov.uk/>
- ABS API: <https://www.abs.gov.au/about/data-services/application-programming-interfaces-apis/indicator-api>
- e-Stat API: <https://www.e-stat.go.jp/api/en>
- Alpha Vantage: <https://www.alphavantage.co/documentation/>
- Finnhub Economic Calendar: <https://finnhub.io/docs/api/economic-calendar>
- Trading Economics Calendar API: <https://docs.tradingeconomics.com/economic_calendar/snapshot/>
- ForexFactory Calendar: <https://www.forexfactory.com/calendar>
