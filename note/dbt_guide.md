## How we style our dbt models
私たちが dbt モデルをスタイルする方法

### Fields and model names
フィールドとモデル名

👥 Models should be pluralized, for example, customers, orders, products.
👥 モデル名は複数形にするべきである。例：customers、orders、products。

🔑 Each model should have a primary key.
🔑 各モデルは主キー（primary key）を持つべきである。

🔑 The primary key of a model should be named _id, for example, account_id. This makes it easier to know what id is being referenced in downstream joined models.
🔑 モデルの主キーは _id の形式で命名するべきである。例：account_id。これにより、下流の結合モデルで参照されている id が何であるかを把握しやすくなる。

Use underscores for naming dbt models; avoid dots.
dbt モデルの命名にはアンダースコア（underscore）を使い、ドット（dot）は避ける。

✅ models_without_dots
✅ models_without_dots

❌ models.with.dots
❌ models.with.dots

Most data platforms use dots to separate database.schema.object, so using underscores instead of dots reduces your need for quoting as well as the risk of issues in certain parts of dbt. For more background, refer to this GitHub issue.
ほとんどのデータプラットフォームは database.schema.object を区切るのにドットを使用しているため、ドットの代わりにアンダースコアを使うことでクォートの必要性が減り、dbt の一部で問題が起こるリスクも軽減される。詳細はこの GitHub issue を参照。

🔑 Keys should be string data types.
🔑 キーは文字列型（string data type）であるべきである。

🔑 Consistency is key! Use the same field names across models where possible. For example, a key to the customers table should be named customer_id rather than user_id or ‘id’.
🔑 一貫性が重要である！可能な限りモデル間で同じフィールド名を使用すること。例えば、customers テーブルへのキーは user_id や ‘id’ ではなく customer_id と命名するべきである。

❌ Do not use abbreviations or aliases. Emphasize readability over brevity. For example, do not use cust for customer or o for orders.
❌ 略語やエイリアスを使わないこと。簡潔さよりも可読性を重視すること。例えば、customer を cust、orders を o としてはならない。

❌ Avoid reserved words as column names.
❌ カラム名に予約語（reserved words）を使用しないこと。

➕ Booleans should be prefixed with is_ or has_.
➕ 真偽値（boolean）は is_ または has_ を接頭辞にすべきである。

🕰️ Timestamp columns should be named _at(for example, created_at) and should be in UTC. If a different timezone is used, this should be indicated with a suffix (created_at_pt).
🕰️ タイムスタンプ（timestamp）カラムは _at の形式（例：created_at）で命名し、UTC で保存するべきである。別のタイムゾーンを使用する場合はサフィックスで示すこと（例：created_at_pt）。

📆 Dates should be named _date. For example, created_date.
📆 日付（date）カラムは _date の形式で命名するべきである。例：created_date。

🔙 Events dates and times should be past tense — created, updated, or deleted.
🔙 イベントの日付や時刻は過去形で表すべきである — created、updated、deleted。

💱 Price/revenue fields should be in decimal currency (19.99 for $19.99; many app databases store prices as integers in cents). If a non-decimal currency is used, indicate this with a suffix (price_in_cents).
💱 価格や売上（price/revenue）のフィールドは小数の通貨（例：$19.99 は 19.99）で表すべきである。多くのアプリのデータベースは価格をセント単位の整数で保存している。非小数の通貨を使用する場合はサフィックスで示す（例：price_in_cents）。

🐍 Schema, table and column names should be in snake_case.
🐍 スキーマ（schema）、テーブル（table）、カラム（column）の名前は snake_case で表記するべきである。

🏦 Use names based on the business terminology, rather than the source terminology. For example, if the source database uses user_id but the business calls them customer_id, use customer_id in the model.
🏦 ソースの用語ではなく、ビジネス用語に基づいた名前を使用すること。例えば、ソースデータベースが user_id を使っていても、ビジネス上は customer_id と呼ばれているなら、モデルでは customer_id を使用するべきである。

🔢 Versions of models should use the suffix _v1, _v2, etc for consistency (customers_v1 and customers_v2).
🔢 モデルのバージョンは一貫性のために _v1、_v2 などのサフィックスを使用すべきである（例：customers_v1、customers_v2）。

🗄️ Use a consistent ordering of data types and consider grouping and labeling columns by type, as in the example below. This will minimize join errors and make it easier to read the model, as well as help downstream consumers of the data understand the data types and scan models for the columns they need. We prefer to use the following order: ids, strings, numerics, booleans, dates, and timestamps.
🗄️ データ型の順序を一貫して使用し、以下の例のようにカラムを型ごとにグループ化・ラベル付けすることを検討するべきである。これにより結合エラーが最小化され、モデルを読みやすくなり、さらにデータの利用者がデータ型を理解し必要なカラムを探しやすくなる。推奨する順序は次の通り：id、文字列（string）、数値（numeric）、真偽値（boolean）、日付（date）、タイムスタンプ（timestamp）。

Example model
モデル例

```sql
with

source as (

    select * from {{ source('ecom', 'raw_orders') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- strings
        status as order_status,

        ---------- numerics
        (order_total / 100.0)::float as order_total,
        (tax_paid / 100.0)::float as tax_paid,

        ---------- booleans
        is_fulfilled,

        ---------- dates
        date(order_date) as ordered_date,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed

```


## How we style our SQL
私たちが SQL をスタイルする方法

### Basics
基本ルール

☁️ Use SQLFluff to maintain these style rules automatically.
☁️ SQLFluff を使用して、これらのスタイルルールを自動的に維持する。

Customize .sqlfluff configuration files to your needs.
必要に応じて .sqlfluff の設定ファイルをカスタマイズする。

Refer to our SQLFluff config file for the rules we use in our own projects.
私たちのプロジェクトで使用しているルールについては、SQLFluff の設定ファイルを参照すること。

Exclude files and directories by using a standard .sqlfluffignore file. Learn more about the syntax in the .sqlfluffignore syntax docs.
標準的な .sqlfluffignore ファイルを使用して、ファイルやディレクトリを除外する。.sqlfluffignore の構文についてはドキュメントを参照すること。

Excluding unnecessary folders and files (such as target/, dbt_packages/, and macros/) can speed up linting, improve run times, and help you avoid irrelevant logs.
不要なフォルダやファイル（例：target/、dbt_packages/、macros/）を除外することで、リント処理が高速化され、実行時間が改善され、不要なログを回避できる。

👻 Use Jinja comments ({# #}) for comments that should not be included in the compiled SQL.
👻 コンパイル後の SQL に含めたくないコメントには Jinja コメント ({# #}) を使用する。

⏭️ Use trailing commas.
⏭️ カンマは行末（trailing comma）に置く。

4️⃣ Indents should be four spaces.
4️⃣ インデントは 4 スペースにする。

📏 Lines of SQL should be no longer than 80 characters.
📏 SQL の 1 行は 80 文字以内に収める。

⬇️ Field names, keywords, and function names should all be lowercase.
⬇️ フィールド名、キーワード、関数名はすべて小文字（lowercase）にする。

🫧 The as keyword should be used explicitly when aliasing a field or table.
🫧 フィールドやテーブルにエイリアスをつける場合は、as キーワードを明示的に使用する。

info
補足情報

☁️ dbt users can use the built-in SQLFluff Studio IDE integration to automatically lint and format their SQL. The default style sheet is based on dbt Labs style as outlined in this guide, but you can customize this to fit your needs. No need to setup any external tools, just hit Lint! Also, the more opinionated sqlfmt formatter is also available if you prefer that style.
☁️ dbt ユーザーは、SQLFluff Studio IDE の組み込み機能を使って SQL を自動的にリント・整形できる。デフォルトのスタイルシートはこのガイドに記載された dbt Labs スタイルに基づいているが、必要に応じてカスタマイズできる。外部ツールのセットアップは不要で、「Lint!」を押すだけでよい。また、より意見の強いスタイルを持つ sqlfmt フォーマッタも利用可能である。

### Fields, aggregations, and grouping
フィールド・集計・グルーピング

🔙 Fields should be stated before aggregates and window functions.
🔙 フィールドは集計関数やウィンドウ関数の前に記載する。

🤏🏻 Aggregations should be executed as early as possible (on the smallest data set possible) before joining to another table to improve performance.
🤏🏻 集計はできるだけ早い段階で（可能な限り小さいデータセットで）実行し、他のテーブルに結合する前に処理することでパフォーマンスを改善する。

🔢 Ordering and grouping by a number (eg. group by 1, 2) is preferred over listing the column names (see this classic rant for why). Note that if you are grouping by more than a few columns, it may be worth revisiting your model design.
🔢 列名を列挙するよりも、番号での並び替えやグルーピング（例：group by 1, 2）が推奨される（理由については有名な議論を参照）。ただし、複数以上の列でグルーピングする場合は、モデル設計を見直す価値がある。

### Joins
結合（Joins）

👭🏻 Prefer union all to union unless you explicitly want to remove duplicates.
👭🏻 重複を削除したい場合を除き、union よりも union all を使用する。

👭🏻 If joining two or more tables, always prefix your column names with the table name. If only selecting from one table, prefixes are not needed.
👭🏻 2 つ以上のテーブルを結合する場合は、常にカラム名にテーブル名のプレフィックスをつける。1 つのテーブルから選択するだけならプレフィックスは不要。

👭🏻 Be explicit about your join type (i.e. write inner join instead of join).
👭🏻 結合タイプを明示すること（例：join ではなく inner join と書く）。

🥸 Avoid table aliases in join conditions (especially initialisms) — it’s harder to understand what the table called “c” is as compared to “customers”.
🥸 結合条件ではテーブルのエイリアスを避けること（特に頭字語）。“c” より “customers” の方が理解しやすい。

➡️ Always move left to right to make joins easy to reason about - right joins often indicate that you should change which table you select from and which one you join to.
➡️ 結合は常に左から右に進めることで理解しやすくする。right join が必要な場合は、どのテーブルを基点にするかを見直すべきであることが多い。

### ‘Import’ CTEs
「インポート」CTE

🔝 All {{ ref(’…’) }} statements should be placed in CTEs at the top of the file.
🔝 すべての {{ ref(’…’) }} ステートメントはファイルの冒頭の CTE に置くべきである。

📦 ‘Import’ CTEs should be named after the table they are referencing.
📦 「インポート」CTE は参照しているテーブル名で命名するべきである。

🤏🏻 Limit the data scanned by CTEs as much as possible. Where possible, only select the columns you’re actually using and use where clauses to filter out unneeded data.
🤏🏻 CTE がスキャンするデータは可能な限り制限する。使用するカラムだけを選択し、不要なデータは where 句で絞り込むこと。

For example:
例：
```sql

with

orders as (

    select
        order_id,
        customer_id,
        order_total,
        order_date

    from {{ ref('orders') }}

    where order_date >= '2020-01-01'

)

```

### ‘Functional’ CTEs
「機能的」CTE

☝🏻 Where performance permits, CTEs should perform a single, logical unit of work.
☝🏻 パフォーマンスが許す範囲では、CTE は単一の論理的な処理単位を担うべきである。

📖 CTE names should be as verbose as needed to convey what they do e.g. events_joined_to_users instead of user_events (this could be a good model name, but does not describe a specific function or transformation).
📖 CTE 名は、その処理内容を伝えるために十分に詳細であるべきである。例：user_events ではなく events_joined_to_users。前者は良いモデル名にはなり得るが、具体的な処理や変換を表していない。

🌉 CTEs that are duplicated across models should be pulled out into their own intermediate models. Look out for chunks of repeated logic that should be refactored into their own model.
🌉 複数のモデルで重複している CTE は、中間モデルに切り出すべきである。繰り返されているロジックの塊は、独自のモデルにリファクタリングすることを検討する。

🔚 The last line of a model should be a select * from your final output CTE. This makes it easy to materialize and audit the output from different steps in the model as you’re developing it. You just change the CTE referenced in the select statement to see the output from that step.
🔚 モデルの最後の行は、最終的な出力 CTE からの select * にするべきである。これにより、開発中に異なるステップの出力を簡単に実体化・検証できる。select 文で参照する CTE を変更するだけで、そのステップの出力を確認できる。

### Model configuration
モデルの設定

📝 Model-specific attributes (like sort/dist keys) should be specified in the model.
📝 モデル固有の属性（ソートキーやディストリビューションキーなど）はモデル内で指定するべきである。

📂 If a particular configuration applies to all models in a directory, it should be specified in the dbt_project.yml file.
📂 ディレクトリ内のすべてのモデルに適用する設定は、dbt_project.yml ファイルに記載するべきである。

👓 In-model configurations should be specified like this for maximum readability:
👓 モデル内の設定は次のように記載して、可読性を最大化する：

```sql
{{
    config(
      materialized = 'table',
      sort = 'id',
      dist = 'id'
    )
}}


```

Example SQL
SQL の例
```sql
with

events as (

    ...

),

{# CTE comments go here #}
filtered_events as (

    ...

)

select * from filtered_events

```


Example SQL
SQL の例

```sql
with

my_data as (

    select
        field_1,
        field_2,
        field_3,
        cancellation_date,
        expiration_date,
        start_date

    from {{ ref('my_data') }}

),

some_cte as (

    select
        id,
        field_4,
        field_5

    from {{ ref('some_cte') }}

),

some_cte_agg as (

    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5

    from some_cte

    group by 1

),

joined as (

    select
        my_data.field_1,
        my_data.field_2,
        my_data.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when my_data.cancellation_date is null
                and my_data.expiration_date is not null
                then expiration_date
            when my_data.cancellation_date is null
                then my_data.start_date + 7
            else my_data.cancellation_date
        end as cancellation_date,

        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5

    from my_data

    left join some_cte_agg
        on my_data.id = some_cte_agg.id

    where my_data.field_1 = 'abc' and
        (
            my_data.field_2 = 'def' or
            my_data.field_2 = 'ghi'
        )

    having count(*) > 1

)

select * from joined


```



以下の pythonファイルを作って実行してみたのですが、コード自体は動くものの、
http://localhost:6080/vnc.html?autoconnect=1&host=localhost&port=6080&resize=remote
にアクセスしても何も表示されませんでした。
問題はどこにありますか？


playwright_sample.py

```py
import os
os.environ["DISPLAY"] = ":99"

from playwright.sync_api import sync_playwright

# Chromium画面は以下で確認可能
# http://localhost:6080/vnc.html?autoconnect=1&host=localhost&port=6080&resize=remote



print("Starting Playwright with GUI...")
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=FalseでGUI表示
    page = browser.new_page()
    print("Opening browser...")
    page.goto("https://qiita.com/")
    print("Waiting for manual login...")

    # page.pause()  # 👈 ここでNoVNC画面を使って「手動でログイン」！
    input('Press Enter after manual login...')  # 手動ログイン後にEnterを押す

    # 以降は自動処理でOK
    page.goto("https://qiita.com/s_rokuemon/items/3e66cde2a1435825f9be")
    print(page.title())
    input('Press Enter to close the browser...')
    browser.close()

```
以下の pythonファイルを作って実行してみたのですが、コード自体は動くものの、
http://localhost:6080/vnc.html?autoconnect=1&host=localhost&port=6080&resize=remote
にアクセスしても何も表示されませんでした。
問題はどこにありますか？


playwright_sample.py

```py
import os
os.environ["DISPLAY"] = ":99"

from playwright.sync_api import sync_playwright

# Chromium画面は以下で確認可能
# http://localhost:6080/vnc.html?autoconnect=1&host=localhost&port=6080&resize=remote



print("Starting Playwright with GUI...")
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=FalseでGUI表示
    page = browser.new_page()
    print("Opening browser...")
    page.goto("https://qiita.com/")
    print("Waiting for manual login...")

    # page.pause()  # 👈 ここでNoVNC画面を使って「手動でログイン」！
    input('Press Enter after manual login...')  # 手動ログイン後にEnterを押す

    # 以降は自動処理でOK
    page.goto("https://qiita.com/s_rokuemon/items/3e66cde2a1435825f9be")
    print(page.title())
    input('Press Enter to close the browser...')
    browser.close()

```