dbtとDuckDBを用いたデータ分析基盤の構築、お疲れ様です。データエンジニアリングのベストプラクティスと実務の要件をすり合わせる過程は、非常に論理的なパズルを解くような面白さがある反面、中間テーブルの爆発という壁にぶつかるのは非常によくあるケースです。

現状のパイプラインをお伺いする限り、Staging層で `order_id` レベルと `order_detail` レベルの粒度（Grain）を明確に分離できている点は、データモデリングの基礎として素晴らしいアプローチです。

しかし、中間テーブルが爆発し管理が大変になっている根本的な原因は、**「分析用の太いテーブル（One Big Table = OBT）を作るタイミングが早すぎる」**ことと、**「一度下げた粒度（detail）から、再び高い粒度（order）へ集計し直している（distinct等による行のクレンジング）」**という点にあります。

スタースキーマとdbtのベストプラクティスに沿って、このアーキテクチャをより洗練させるためのアプローチを提案します。

---

### 1. 現状のボトルネック：粒度の往復と早期の非正規化

現在の流れは以下のようになっています。

1. `order_id` と `order_detail` を分離 (Good)
2. `order_id` の情報（天気、店舗など）を `order_detail` にJOINして重複させる (ここで巨大なテーブルが誕生)
3. この巨大なテーブルから、`order_id` 単位の集計（品質の平均や最悪の品質など）を出すために `distinct` して集計する (ボトルネック)

この「一度粒度を細かくして（JOIN）、また粗くする（DISTINCT/GROUP BY）」という処理は、計算コストが高いだけでなく、データの整合性を担保するロジックを複雑化させ、中間テーブルを量産する原因になります。

### 2. 解決策：ファクトテーブルの分離とディメンショナル・モデリング

スタースキーマの原則に従い、「イベントが発生した最小粒度の事実（Fact）」と「それに紐づく属性（Dimension）」を明確に分けます。

現状のデータは2つの異なるイベント粒度を持っているため、**無理に1つの `order_detail` に全てを結合せず、2つのファクトテーブルを作成する**のがベストプラクティスです。

#### ① `fct_orders` (注文ヘッダーレベルのファクト)

* **粒度:** 1行 = 1 `order_id`
* **内容:** 注文全体の売上、店舗ID、天候IDなど。
* **アプローチ:** `order_detail` の集計（品質の平均、最大値、最悪の品質の商品IDなど）は、巨大なテーブルから `distinct` するのではなく、**Staging層のクリーンな `stg_order_details` を直接 `order_id` で `GROUP BY` して計算**し、それをこの `fct_orders` に結合します。

#### ② `fct_order_details` (注文明細レベルのファクト)

* **粒度:** 1行 = 1 `order_id` × `product_id`
* **内容:** その商品の販売価格、数量、単体の品質スコアなど。
* **アプローチ:** ここには店舗情報や天候などの `order_id` レベルの属性は**持たせません**。代わりに `order_id` というキーだけを持たせます。

#### ③ Dimension (ディメンション) テーブル

* `dim_products` (商品マスタ)
* `dim_stores` (店舗マスタ)
* `dim_dates` (日付マスタ)

### 3. dbtでのレイヤー設計（推奨）

このモデルを実現するためのdbtのディレクトリ構成とデータの流れは以下のようになります。

* **Staging (`stg_`)**:
* `stg_orders`, `stg_order_details` (ここで粒度を2つに分ける)


* **Intermediate (`int_`)**:
* `int_order_metrics_aggregated`: `stg_order_details` を読み込み、`order_id` ごとに品質の平均や最悪の品質などを集計（GROUP BY）するだけのテーブル。


* **Marts (`fct_`, `dim_`)**:
* `fct_orders`: `stg_orders` に `int_order_metrics_aggregated` をJOINして完成。
* `fct_order_details`: `stg_order_details` をほぼそのまま出すか、必要なキーを整えるだけ。
* `ob_order_details` (One Big Table): もし非エンジニアのデータアナリストやBIツール側でスタースキーマのJOINが難しい場合のみ、**一番最後のMarts層**で `fct_order_details` と `fct_orders` を結合した幅広いテーブルを作ります。ここからさらに集計をかけることはしません。



### 4. 時系列集計 (Weekly, Monthly) について

Weekly, Monthly, Quarterly などの集計テーブルをdbt側で大量に作成している場合、それもMarts肥大化の原因です。
DuckDBは列指向で集計処理が非常に高速なため、BIツール側（あるいは集計クエリ実行時）で動的に `DATE_TRUNC` 等を用いて集計する方が、アーキテクチャはシンプルに保てます。もしダッシュボードのパフォーマンス要件等でどうしても事前集計が必要な場合のみ、`fct_orders` や `fct_order_details` をベースにした集計Marts（`mart_monthly_sales` など）を最小限作成するようにします。

---

このアーキテクチャに移行することで、「集計のための前処理（distinct等）」が不要になり、ロジックがシンプルになるため中間テーブルの数を劇的に減らすことができます。

この流れの中で、例えば「`order_detail` から `order_id` ごとに最悪の品質のアイテムを抽出して集計する `int_` テーブル」の具体的なdbt SQL（Window関数を使った記述など）を見てみたいといったご希望はありますでしょうか？

### 時系列ごとの集計をBIでやらせることについて

ご推察の通りです。そのレベルの複雑な計算（Window関数を用いたTop-Nのランキング、全体に対する割合の算出、そして行から列へのピボット展開）をBIツール側に任せるのは、パフォーマンスの悪化やBI側での「CTE地獄」を引き起こす典型的なアンチパターンとなります。

特に、EvidenceのようなSQLベースでダッシュボードを構築するツールを使用する場合、BI側には「可能な限りシンプルなSELECT文で済む、完成された横持ちのテーブル」を渡すのがベストプラクティスです。

このような「特定のビジネスロジックに強く依存し、形が大きく変わる集計」は、まさに **dbtのMarts層（Aggregate Marts）で処理すべき領域** です。

中間テーブルを無駄に増やさず、かつWeekly, Monthly, QuarterlyのロジックをDRY（繰り返しを避ける）に保つためには、**dbtのJinjaマクロを活用して集計ロジックを抽象化し、3つのMartsモデルからそのマクロを呼び出す**という設計が最も美しく機能します。

具体的な実装アーキテクチャは以下のようになります。

### 1. マクロの定義（ロジックの一元化）

まず、`macros/` ディレクトリに、任意の期間（grain）と順位（Top N）を受け取って横持ちのテーブルを生成するマクロを作成します。これにより、CTEの記述はプロジェクト内でこの1箇所だけで済みます。

**`macros/generate_top_products_report.sql`**

```sql
{% macro generate_top_products_report(time_grain, top_n=3) %}

WITH base_sales AS (
    -- 1. 指定された期間・商品ごとの売上を集計
    SELECT
        DATE_TRUNC('{{ time_grain }}', order_date) AS period,
        product_id,
        SUM(price) AS product_sales -- ※単価×数量など、実際の要件に合わせて調整
    FROM {{ ref('fct_order_details') }}
    GROUP BY 1, 2
),

period_totals AS (
    -- 2. その期間の総売上を計算（構成比の分母）
    SELECT
        period,
        SUM(product_sales) AS total_period_sales
    FROM base_sales
    GROUP BY 1
),

ranked_products AS (
    -- 3. Window関数で順位付けと構成比の計算
    SELECT
        b.period,
        b.product_id,
        b.product_sales,
        (b.product_sales * 1.0 / p.total_period_sales) AS sales_pct,
        ROW_NUMBER() OVER(PARTITION BY b.period ORDER BY b.product_sales DESC) AS sales_rank
    FROM base_sales b
    JOIN period_totals p ON b.period = p.period
)

-- 4. 横持ち（ピボット）への展開
SELECT
    period,
    {% for i in range(1, top_n + 1) %}
    MAX(CASE WHEN sales_rank = {{ i }} THEN product_id END) AS top_{{ i }}_product_id,
    MAX(CASE WHEN sales_rank = {{ i }} THEN product_sales END) AS top_{{ i }}_sales,
    MAX(CASE WHEN sales_rank = {{ i }} THEN sales_pct END) AS top_{{ i }}_sales_pct{% if not loop.last %},{% endif %}
    {% endfor %}
FROM ranked_products
WHERE sales_rank <= {{ top_n }}
GROUP BY 1

{% endmacro %}

```

### 2. Martsモデルの作成（マクロの呼び出し）

あとは、`marts/` ディレクトリにそれぞれの期間用のモデルを作成し、1行でマクロを呼び出すだけです。

**`marts/mart_weekly_top_products.sql`**

```sql
{{ generate_top_products_report(time_grain='week') }}

```

**`marts/mart_monthly_top_products.sql`**

```sql
{{ generate_top_products_report(time_grain='month') }}

```

**`marts/mart_quarterly_top_products.sql`**

```sql
{{ generate_top_products_report(time_grain='quarter') }}

```

---

### この設計のメリット

1. **中間テーブルが増えない:** `fct_order_details` から直接最終的なMartsを生成するため、一時的なテーブルがデータベース上に作られません。
2. **圧倒的な保守性:** 売上の定義が変わったり、「Top 3ではなくTop 5が見たい」となった場合でも、マクロの引数や計算式を1箇所修正するだけで、Weekly/Monthly/Quarterlyの全てに適用されます。
3. **DuckDBの強みを活かせる:** DuckDBはメモリ上でのWindow関数の処理や列指向の集計が非常に高速です。この程度の複雑なクエリであれば、事前の細かな中間集計テーブル（`int_`層での日次集計など）を挟まなくても、高速に処理を完了できます。

このように、データモデリングの基礎となるFact/Dimensionを綺麗に整えた上で、最終的なBIツール向けの複雑な整形は「Marts層 × マクロ」で吸収するアプローチをとることで、基盤全体の風通しが格段に良くなります。

このマクロのロジックに、例えば「商品名（`product_name`）も横持ちの中に含めたい」といった場合は、DimテーブルをどのようにJOINさせるのが最も効率的か、構成案を広げてみましょうか？




---- chat gpt -------------

かなり筋はいいです。
むしろ今の悩みは「間違っている」というより、order detail を万能な母艦にしすぎて、その先の mart ごとの派生が増殖していることだと思います。

結論から言うと、改善の方向はこの3つです。
	1.	grain をもっと厳密に分ける
	2.	“分析用の広い1枚表” と “再利用可能な基礎 mart” を分離する
	3.	time rollup（weekly/monthly/quarterly）を mart 本体から切り離す

dbt の考え方でも、mart は「あるビジネス実体を、その固有の grain で表す」ものとされます。たとえば order, customer, payment のように、1行が何を表すかを明確にするのが基本です。さらに、Semantic Layer を使わないなら marts ではかなり denormalize してよい、というのが dbt の現行ガイダンスです。 ￼

なので、あなたの今の
	•	order grain のものを作る
	•	order detail grain のものを作る
	•	必要に応じて order の計算結果を detail に持ち込む

という発想自体は、そこまでベストプラクティスから外れていません。
問題はその後に、用途ごとの集計ロジックが全部 mart 側に流れ込んでいることです。

⸻

今の構造で苦しくなっている本質

今の構造を整理すると、たぶんこうです。
	•	stg_*
	•	raw から整形
	•	order_id grain
	•	order_id, product_id grain
	•	中間
	•	店舗、天気、商品品質、価格などを join
	•	order grain での計算
	•	detail grain へ再付与
	•	mart
	•	detail から distinct / 再集計
	•	週次、月次、四半期など用途別に派生

ここで起きやすい問題は、
	•	order grain の計算
	•	detail grain の計算
	•	report grain の計算

が、同じ層で混ざることです。

つまり「モデルの責務」が混ざっています。

⸻

より良い基本設計

おすすめは、order detail を最終分析の唯一の基盤 にするのを少しやめて、
“entity mart” と “report mart” を分けることです。

1. entity mart を grain ごとに固定する

まず、最終公開してよい基礎 mart を grain 単位で作ります。
	•	fct_order
	•	1 row = 1 order
	•	fct_order_line
	•	1 row = 1 order_id × product_id
	•	dim_store
	•	dim_product
	•	dim_date
	•	必要なら dim_weather_day など

dbt でも marts は entity/concept 単位・unique grain 単位で表すのが推奨です。 ￼

ここで大事なのは、order の性質は fct_order に閉じ込めることです。

たとえば、
	•	注文単位の金額
	•	注文単位の配送時間
	•	注文単位の店舗属性
	•	注文日の天気

のような「order に1つだけぶら下がるもの」は、まず fct_order に持たせる。

一方で、
	•	商品価格
	•	商品品質
	•	商品カテゴリ
	•	注文明細数量

は fct_order_line に持たせる。

⸻

2. order の情報を line に全部複製しすぎない

ここが一番重要です。

今の構造だと、order_detail に order の計算結果をかなり join して重複させているとのことですが、
これは**“最終利用テーブル” としては便利**でも、再利用基盤としては管理コストが高いです。

なぜなら、その後の集計で毎回
	•	order grain で数えるべきもの
	•	line grain で数えるべきもの

を意識して distinct や再集計をしなければならないからです。

たとえば、
	•	注文数
	•	注文ごとの平均品質
	•	最悪品質の商品名
	•	注文単位の配送遅延フラグ

みたいなものが line に複製されると、
次の mart で毎回「これは distinct 必須」「これは max で拾う」「これは first_value 的に取る」みたいな処理が増えます。

これは、下流の mart が増えるほど雪だるま式に辛くなる構造です。

⸻

3. “広い分析テーブル” は基礎 mart ではなく downstream に置く

なのでおすすめは、基礎層としては
	•	fct_order
	•	fct_order_line

を別々に保ち、
そのうえで必要なら 分析用途専用の wide table を作ることです。

たとえば:
	•	int_order_line_enriched
	•	line grain
	•	product / store / date / order の一部属性を持つ
	•	ただしあくまで中間的
	•	mart_sales_line_analysis
	•	特定のダッシュボードや分析群のための wide table

このようにすると、
全社共通の基礎モデル と 用途依存の便利テーブル を分離できます。

dbt の思想でも、staging の次に intermediate を置いて複雑な変換を分離し、marts はビジネス実体ベースに整理する流れです。 ￼

⸻

あなたのケースでの具体的な再整理案

たとえばこうします。

staging
	•	stg_orders_raw
	•	stg_order_lines_raw
	•	stg_products
	•	stg_stores
	•	stg_weather_daily

intermediate
	•	int_orders_base
	•	order grain の正規化
	•	int_order_lines_base
	•	order_line grain の正規化
	•	int_orders_enriched
	•	store / weather / customer など、order grain で join
	•	int_order_lines_enriched
	•	product / quality / price など、line grain で join
	•	int_order_line_order_features
	•	order grain の特徴量を line に必要最小限だけ持ち込む用途別中間

marts（基礎）
	•	fct_order
	•	fct_order_line
	•	dim_store
	•	dim_product
	•	dim_date

marts（利用目的別）
	•	sales__order_line_analysis
	•	sales__weekly_kpi
	•	sales__monthly_kpi
	•	sales__quarterly_kpi

ここでポイントは、
weekly / monthly / quarterly を “基礎 mart” と見なさないことです。

dbt でも、時間集計のない pure mart をまず持ち、time-based rollup は別扱いにする発想が強いです。marts の命名でも orders_per_day のような time rollup は通常 pure mart とは別で捉えるべきだとされています。 ￼

⸻

weekly / monthly / quarterly をどう減らすか

ここもかなり効きます。

今の
	•	weekly mart
	•	monthly mart
	•	quarterly mart

を別々に作る構造は、保守上かなり重いです。

改善案は2つあります。

案A: period_type 方式にまとめる

1つのモデルに
	•	period_type = week / month / quarter
	•	period_start_date
	•	各種指標

を持たせる方法です。

例:
	•	agg_sales_period

こうすると、モデル数は減ります。
ただしクエリは少し複雑になります。

案B: 基礎 mart + BI / Semantic Layer 側で集計

こちらの方が本筋です。

dbt は Semantic Layer を使う場合、semantic models / measures / dimensions を定義し、その上で metrics を組み立てる構造を推しています。semantic layer は marts の上に作るのが推奨です。 ￼

もし今すぐ dbt Semantic Layer まで行かなくても、
	•	fct_order
	•	fct_order_line

を基礎にして
	•	日付ディメンション
	•	measure 定義の考え方

を揃えておけば、将来的に weekly/monthly/quarterly の専用 mart を減らしやすいです。

⸻

「distinct して清潔にする」が多いなら、grain 設計を見直すサイン

これは重要な診断ポイントです。

もし mart を作るたびに
	•	distinct
	•	row_number
	•	first_value
	•	max_by 的な処理
	•	group by で元の粒度に戻す

が頻発しているなら、
それはたいてい 入力モデルの grain が粗すぎる or 混ざっている サインです。

特にあなたのケースでは、
	•	order の指標
	•	line の指標
	•	order 内 line の要約指標

が全部 line テーブル経由で作られている感じがあります。

これを分けるなら、たとえば:
	•	fct_order
	•	注文単位の正式な指標
	•	agg_order_line_quality
	•	order_id grain で line から集約した品質サマリ
	•	agg_order_line_price
	•	order_id grain で line から集約した価格サマリ

をまず order grain で作って、
その結果を fct_order に join するほうが自然です。

つまり、
line を起点にして order を毎回復元するのではなく、order 用の集約テーブルを明示的に持つほうがよいです。

⸻

「中間テーブルが増えすぎる」問題への実務的な対処

モデル数が増えること自体は悪ではありません。
dbt では intermediate は複雑さを分離するために存在します。

ただし、増え方には良い増え方と悪い増え方があります。

良い増え方
	•	1モデル1責務
	•	grain が明確
	•	複数 mart から再利用される
	•	命名で目的が分かる

悪い増え方
	•	あるダッシュボード専用の小加工が大量に乱立
	•	同じ集計ロジックの類似モデルが複数ある
	•	grain がモデル名から分からない
	•	元の order/detail の混在を downstream で毎回修正している

⸻

管理しやすくする命名ルール

かなりおすすめです。

たとえば prefix を固定します。
	•	stg_ raw 整形
	•	int_ 複雑変換
	•	fct_ 事実
	•	dim_ 次元
	•	agg_ 特定 grain への集約
	•	rpt_ レポート用途

あなたのケースなら:
	•	fct_order
	•	fct_order_line
	•	agg_order_line_to_order_quality
	•	agg_order_line_to_order_price
	•	rpt_sales_period
	•	rpt_store_weekly
	•	rpt_product_monthly

のようにすると、
「これは基礎データなのか」「集約補助なのか」「最終レポートなのか」がすぐ分かります。

特に agg_ を導入すると、
mart の数だけ intermediate が増える問題をかなり整理できます。
なぜなら、用途別の小集計を「再利用可能な集約部品」として共通化しやすくなるからです。

⸻

いちばんおすすめの考え方

今の構造を一気に直すなら、次の原則で見るとよいです。

原則1

各モデルに grain をコメントで明記する

例:
	•	-- grain: one row per order_id
	•	-- grain: one row per order_id, product_id

原則2

order の事実は fct_order に寄せる

line から order を毎回復元しない。

原則3

line のワイド化は“共通基盤”ではなく“用途別出力”と考える

原則4

time rollup は pure mart から分離する

weekly/monthly/quarterly を基礎モデルの増殖で解決しない。

原則5

distinct が必要なら upstream grain を疑う

⸻

あなたに合いそうな現実的な改善ステップ

一気に作り直さなくていいです。順番としてはこれがよいです。

まず、
① fct_order と fct_order_line を正式な公開基盤として定義する。

次に、
② order_line から order grain に戻す集約を agg_ モデルとして分離する。

たとえば
	•	agg_order_quality_summary
	•	agg_order_price_summary
	•	agg_order_worst_product

のようにします。

そのうえで、
③ weekly/monthly/quarterly は rpt_ 系に寄せる。

最後に、
④ 本当に必要な wide table だけ残す。

これだけでかなり整理されるはずです。

⸻

端的に言うと

今の悩みに対する処方箋は、
	•	order detail を万能ベースにしない
	•	order grain の正式な fact を作る
	•	line → order の集約を部品化する
	•	report 用 rollup を基礎 mart と分離する

です。

今の構造は「detail 1枚に寄せる」ことで短期的な分析のしやすさを得ていますが、
長期運用では grain の混線 が保守コストになります。

必要なら次に、あなたの現在の dbt フォルダ構成を前提にして、
	•	推奨ディレクトリ構成
	•	モデル命名規約
	•	fct_order / fct_order_line / agg_* / rpt_* の具体サンプルSQL
	•	dbt-core + duckdb 前提での materialized 設計

まで具体化して出します。



---------------- dbtモデル再構築手順 -------------------
そのアプローチ、リファクタリング（コードの再構築）を進める上で非常に堅実で良いですね！「現行のロジックを守りつつ、新しいロジックを育てる」という進め方は、データエンジニアリングの王道です。

ただ、dbt の仕様上、いくつか**「ハマりやすいポイント」**と、それをスマートに解決する設定方法があります。

---

## 1. 最大の注意点：モデル名の重複

dbt では、フォルダが違っても**「同じファイル名の SQL」を一つのプロジェクト内に共存させることはできません。**

* `models/marts/orders.sql`
* `models/sandbox/orders.sql`

この状態で `dbt run` をすると、「モデル名が重複しています」というコンパイルエラーになります。

### 解決策

sandbox 内のファイル名に接頭辞（プレフィックス）をつけるのが最も簡単です。

* 例：`models/sandbox/sb_orders.sql`

---

## 2. dbt_project.yml の設定

`sandbox/` フォルダ配下のモデルに対して、一括で設定を適用できます。

ここでは、**「sandbox 配下のモデルは、本番とは別のスキーマに出力する」**設定を入れるのが安全です。これにより、本番テーブルを誤って上書きするリスクをゼロにできます。

```yaml
models:
  my_project_name:
    # 既存のモデル設定
    marts:
      materialized: table
      
    # sandbox フォルダ専用の設定
    sandbox:
      +materialized: view  # 開発中は view にしておくと DB 負荷が軽い
      +schema: sandbox     # デフォルトのスキーマ名に "_sandbox" が付与される

```

> [!NOTE]
> `+schema: sandbox` と書くと、profiles.yml で指定したスキーマ名（例: `public`）の後ろに連結され、`public_sandbox` というスキーマに作成されます。

---

## 3. sandbox だけを実行する方法

特定のフォルダ配下だけを実行するのは、dbt の得意分野です。`--select`（または `-s`）フラグを使います。

* **sandbox フォルダの中身だけをすべて実行:**
```bash
dbt run --select path:models/sandbox
# または単純に
dbt run --select sandbox

```


* **sandbox 内の特定のモデルとその下流を実行:**
```bash
dbt run --select sb_orders+

```



---

## 4. プロファイル（profiles.yml）はどうすべき？

基本的には、**新しいプロファイルを作る必要はありません。**

既存のプロファイルの中に `target: dev` や `target: sandbox` という「ターゲット」を追加するだけで十分です。

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: bigquery (or snowflake etc)
      schema: dev_user1  # あなたの個人用スキーマ
      ...
    sandbox_verify:
      type: bigquery
      schema: integration_test  # 検証用の共通スキーマ
      ...

```

実行時に `--target sandbox_verify` と切り替えることで、接続先やデフォルトのスキーマを制御できます。

---

## 5. 【プロの推奨】新旧データの比較方法

「現行のものと同じものが出力できたら切り替える」という検証作業には、dbt のパッケージ **`audit_helper`** を使うのが標準的です。

手動で SQL を書いて比較するよりも、圧倒的に速く、正確に「差分（レコードのズレ）」を見つけられます。

### audit_helper の `compare_relations` マクロの例

sandbox に作成した新モデルと、現行モデルを比較する SQL を一つ作ります。

```sql
{# models/sandbox/check_diff.sql #}
{{ audit_helper.compare_relations(
    ref('orders'),        -- 現行モデル
    ref('sb_orders')      -- 再構築したモデル
) }}

```

これを `dbt run` すると、「一致率 100%」かどうかが一目でわかる表が出力されます。

---

### 次のステップへの提案

もしよろしければ、**「再構築したいモデルの現在の構成（テーブルかビューか、データ量など）」**を教えていただければ、より具体的な `dbt_project.yml` の書き方や、`audit_helper` のセットアップ方法を詳しく解説できますがいかがでしょうか？

なお、各種アプリのすべての機能を使用するには、[Gemini アプリ アクティビティ](https://myactivity.google.com/product/gemini)を有効にする必要があります。







Dimension tables are the foundation of powerful analytics. You have already learned much
about them in previous chapters. These fundamentals, however, do not address a variety
of real-world concerns that you are likely to encounter. The next several chapters move
beyond the basics, covering a progression of advanced dimension design topics. Mastery of
these topics will prepare you to confront real-world complexity and respond to it with
simple and powerful solutions.
If you are not reading this book from cover to cover, you may be drawn to this chapter
because you have a question involving dimensions. Some of what you are looking for may
have appeared in prior chapters. Here is a quick recap of what has already been covered:
Chapter 1 introduced the dimension
• as a fundamental part of the dimensional
model, providing a context for facts and enabling rich and powerful reporting.
Techniques were provided to identify dimensions in reports and the spoken word,
and the star schema was introduced.
• in detail, from its rich set of attributes to
Chapter 3 described the dimension table
its use of surrogate keys to the ways in which type 1 and type 2 slowly changing
dimensions represent history.
Chapter 5 explained conformed dimensions
• , which enable cross-process analysis
through a procedure called drilling across. The key to scaling across subject areas,
conformed dimensions allow fact tables to work together to produce powerful
reports.
This chapter builds on these fundamentals to address a handful of the more
complicated issues surrounding dimension design. It is divided into five sections, covering
the following topics:
• describes how to identify when dimensions
“Grouping Dimensions into Tables”
belong in the same table and when they should be placed in separate tables.
• explains what to do, and what not to do, when
“Breaking Up Large Dimensions”
dimension tables would be too large to maintain effectively. Several alternatives are
explored, including the mini-dimension, which has the unique ability to stem
growth without sacrificing detail.
• covers what happens when a fact table has multiple
“Dimension Roles and Aliasing”
relationships to a single dimension table. These relationships are called roles, and
they do not require making copies of the dimension table. Multiple roles can be
addressed in a single query through SQL aliasing.
• describes the analytic headaches caused by NULL values, and
“Avoiding the NULL”
how to avoid them. NULL foreign keys in fact tables are also avoidable through
“special-case rows.” These are used when a fact table has an optional relationship to
a dimension, when the dimensional context for a fact is invalid, and when facts
arrive at the data warehouse before the dimensional detail is available.
“Behavioral Dimensions”
• describes how facts can be turned into additional dimensions,
enabling past history to provide context for facts in very powerful reports.
This collection of topics is just the beginning of your tour of advanced dimension
techniques. Future chapters will explore hierarchies, snowflakes, and outriggers, provide
more techniques for slowly changing dimensions, solve the problem of multi-valued
attributes, and look at recursive hierarchies.
Grouping Dimensions into Tables
Experienced designers don’t usually have difficulty in determining how to group dimension
attributes into tables. Most dimension tables correspond to categories of analysis that hold
deep significance to the business and are evident on a prima facie basis. There are times,
however, when you will be uncertain as to the best way to organize dimensions into tables.
Some of this uncertainty can be attributed to the nature of dimensional modeling.
Unlike an entity-relationship (ER) model, a dimensional model does not expose every
relationship between attributes as a join. Recognizing this difference is the first step toward
sorting out a confusing situation. Relationships that are contextual tend to pass through
fact tables, while natural affinities are represented by co-locating attributes in the same
dimension table. If consideration of these characteristics does not help, you can make the
decision by considering the implications for the usability of the schema.
Two Ways of Relating Dimension Attributes
In a star schema, the relationship between a given pair of dimension attributes may be
expressed explicitly or implicitly. Dimensional modelers do not think about dimensions in
these terms, but those with a background in entity-relationship modeling may be confused
until this distinction is brought to light.
Relationships of the explicit variety are the most familiar. They take the form of joins
that intersect in a fact table, which provides an important context for the relationship. The
dimensions may be related in other contexts as well, as represented by other fact tables.
Explicit relationships between dimension rows may be numerous and volatile.
Less familiar are implicit relationships, which occur when two attributes are located in
the same table. Implicit relationships imply a natural affinity between attributes, rather
than a relationship that can take many contexts. These relationships tend to be more
consistent, and they are browsable.
Explicit Relationships Describe Context
As you have already learned, every fact table bears foreign key references to dimension
tables. These references provide the dimensional context for the facts. These joins can also
be thought of as providing information about relationships between the dimension tables.
The business process measured by the fact table is the context for this relationship.
Once again, we will turn to the orders process for an example. Figure 6-1 reprises the
orders star that was introduced in Chapter 1 and embellished in Chapter 3. The grain of
the fact table is the individual order line. Dimension tables represent the day of an order,
the product being ordered, the customer who placed the order, and the salesperson who
took the order. Degenerate dimensions identify the particular order line, and the junk
dimension order_info specifies miscellaneous characteristics of the order.
Each row in this fact table refers to a specific day, product, customer, salesperson, and
order. For example, a fact table row may record the fact that on January 1, 2008 (a day),
Hal Smith (a salesperson) took an order for 100 black ballpoint pens (a product) from
ABC Stationery Emporium (a customer) as part of order number 299113. The fact table
row records a relationship among these instances of day, salesperson, product, customer,
and order. They are related to one another in the context of this particular order.
Each of these dimension instances—ABC Stationery, Hal Smith, January 1, black
ballpoint pens—may be related in other ways as well. ABC Stationery Emporium may have
ordered other things from Hal Smith, perhaps on the same order or perhaps on completely
different days. All of these relationships are made explicit by recording additional rows in
the fact table, using the appropriate foreign keys. Each of these is a separate relationship in
the context of an order.
These dimensions can also be related in other contexts. A customer and salesperson,
for example, may also become related when a proposal is presented, a product is returned,
and so forth. If customer and salesperson can be related in different contexts, they belong
in separate dimension tables. Fact tables will provide the different contexts.
Those familiar with entity-relationship modeling are doubtless familiar with this type of
explicit relationship. Every fact table is an example of what ER modelers refer to as an
intersect table. It resolves a potential many-to-many relationship between each of the
associated tables. Another type of relationship is implied in dimensional models, one that
does not involve primary key / foreign key associations.
Implicit Relationships Describe Affinities
Unlike an entity-relationship model, a dimensional model also includes relationships that
are not made explicit through joins. Although dimensional modelers do not think about
their models in these terms, this distinction can be a source of confusion for ER modelers
who are new to star schema design.
Relationships between dimension attributes can be implied through their coexistence
in a table. These relationships tend to exist only in a single context, representing a natural
affinity rather than one based on process activities. The relationships among attributes in a
dimension table may change over time but tend to be less volatile than those of the explicit
variety. When implicit relationships do change, their history can be preserved through a
type 2 slow change response.
The orders star from Figure 6-1 contains many examples of implicit relationships.
Within the product table, for example, are dimension attributes called product and brand.
Since more than one product may share the same brand, an ER model would isolate these
attributes in separate tables, relating them via a primary key / foreign key relationship. This
approach makes sense in the context of an operational system, which must often support
a high volume of concurrent transactions inserting, updating, and deleting data. As you
learned in Chapter 1, dimensional models are not intended for an operational profile.
Instead, they are optimized to support queries that potentially aggregate large volumes of data.
In this context, there is no need to separate brand from product. To do so would
potentially impact the performance of queries involving large volumes of data by requiring
additional join processing.
Unlike the relationship between a customer and salesperson, the relationship between
a product and brand does not take on multiple contexts. Products and brands are related
in only one way: membership in a brand. It is a natural affinity that does not depend on the
execution of business activities. At a given point in time, a particular product has one
associated brand. This relationship does not depend on a sale, the manufacturing process,
or other significant processes tracked by the business.
The relationship is not necessarily constant. It may change over time, and when it does,
the change history can be tracked. If the brand designation of a particular product changes,
for example, history can be preserved through a type 2 slow change. A new row is added to
the dimension table for the product, and this new row contains the new brand designation.

When Struggling with Dimension Groupings
Those new to the dimensional approach may face situations where they are not sure whether
two dimensions belong in the same dimension table. If salespeople are assigned to customers,
why separate customer and salesperson into different tables, as is done in Figure 6-1? Why not
place them together? How about making brand a dimension table and including its surrogate
key in the fact table?
Rather than attempt to resolve these questions using the language of ER modeling, it is
best to look at the ways in which the attributes relate and the ways in which they are used.
For a given pair of attributes, consider the context of the relationship. Do they share a
natural affinity, or can they be related in different contexts? Those that tend to share a
stable affinity may be stored together; those that tend to be related only in the context of
events, transactions, or conditions belong in separate fact tables. When in doubt, you can
consider the browsability of alternative designs.
Grouping Dimensions Based on Affinity
In a dimensional model, dimensions are grouped into tables based on natural affinity.
Products and brands, for example, are related to one another prior to an order being
placed. A transaction is not required to establish a relationship between these elements.
In fact, a product has a brand even if there are no orders for it. More importantly, these
attributes can only be related in one way, or one context.
On the other hand, some elements are only related based on transactions or activities.
Salespeople and customers, for example, are brought together only when transactions, such
as orders, occur. These attributes, therefore, belong in separate tables; their relationships
will be captured in the fact table. This allows customers and salespeople to have numerous
interactions, perhaps even in different pairings. The relationships are defined by the
transactions.
Looking deeper, you may also realize that salespeople and customers may be related in
multiple contexts. For example, salespeople may be assigned to customers, they may take
calls from customers, and they may visit customers. Each of these relationships flows from a
different process and can be thought of as an activity or transaction: an assignment of a
salesperson to a customer, a phone call being made, or a visit to a customer site. These
various associations can be captured through a series of fact tables, each representing a
different process. As described in Chapter 5, these relationship chains may offer rich
analytic possibilities when used individually or compared.
The Browsability Test
If you are not sure whether two attributes belong together in a dimension table, consider
how they will be used. Recall from Chapter 1 that a query focused on the values within a
dimension is called a browse query. Natural affinities like product and brand can be
separated from transactional relationships like customers and salespeople by evaluating
browsability. Would someone want to browse the values of these attributes together? Placing
them in separate dimensions would prevent this.
The separation of product and brand, as shown in Figure 6-2, destroys the browsability
of these attributes. In this configuration, it is only possible to study the intersection of
products and brands in the context of orders. If there is no order for a particular product,
it will not be possible to identify its brand. It makes more sense to place these attributes in a
single table. Salespeople and customers, on the other hand, have a relationship only when
transactions occur. This relationship may also be more volatile. A given customer may speak
with a different salesperson each time an order is placed. In this case, the transaction
defines the relationship and is embodied in the fact table.
The astute reader may observe that in a situation where a business assigns salespeople to
specific customers, it may be possible to merge their attributes. This would allow users to
browse the list of customers assigned to a particular salesperson; however, salespeople and
customers may engage in other activities with various contexts, as noted earlier. Salespeople
take calls from customers, process returns, and so forth. Clearly, the business considers
salespeople and customers to be two separate things. If it is necessary to track the assignment
of customers to salespeople, a factless fact table may be called for. This technique will be
discussed in Chapter 12, “Factless Fact Tables.”