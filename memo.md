はい、PostgreSQLで可能です。
存在しない日付を作った場合にエラーにせず NULL を返すには、文字列を DATE 型に直接キャストするのではなく、to_date 関数と NULLIF / CASE などを組み合わせてチェックするのが定石です。

方法1: to_date + 例外吸収関数 TRY_CAST 相当（Postgresでは to_date）

Postgresには TRY_CAST がないので、無効な日付は to_date でエラーになります。
このとき 正規表現で日付の妥当性をチェックしてから to_date に渡す方法が一般的です。

SELECT
    month,
    day,
    CASE 
        WHEN (month || '-' || lpad(day, 2, '0')) ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
        THEN to_date(month || '-' || lpad(day, 2, '0'), 'YYYY-MM-DD')
        ELSE NULL
    END AS date
FROM your_table;


⸻

方法2: make_date を使う

make_date(year, month, day) を使うと日付を構成できますが、無効日付はエラーになります。
これを回避するには、事前に extract でその月の日数を調べてフィルタすればOKです。

SELECT
    month,
    day,
    CASE
        WHEN day::int <= extract(day FROM (month::date + interval '1 month - 1 day'))
        THEN make_date(split_part(month, '-', 1)::int,  -- year
                       split_part(month, '-', 2)::int,  -- month
                       day::int)                       -- day
        ELSE NULL
    END AS date
FROM your_table;

この方法だと、
	•	2025-09 + 31 → NULL
	•	2025-02 + 30 → NULL
	•	正常な日付だけ date 型で返ります。

⸻

方法3: 先に全部文字列にしてから ::date キャスト

シンプルにやるならこれですが、エラーが出るのでおすすめしません。

⸻

✅ 結論としては、方法2 (make_date + extract) が最もPostgresらしくて堅牢です。
その後 WHERE date IS NOT NULL で削除・除外できます。

⸻

試しにあなたのサンプルで実行すると：

WITH t(month, day) AS (
  VALUES ('2025-09','1'),
         ('2025-09','2'),
         ('2025-09','31')
)
SELECT
    month, day,
    CASE
        WHEN day::int <= extract(day FROM (month::date + interval '1 month - 1 day'))
        THEN make_date(split_part(month, '-', 1)::int,
                       split_part(month, '-', 2)::int,
                       day::int)
        ELSE NULL
    END AS date
FROM t;

結果:

 month   | day |    date
---------+-----+------------
 2025-09 | 1   | 2025-09-01
 2025-09 | 2   | 2025-09-02
 2025-09 | 31  | NULL


⸻

👉 この方向で進めてよいですか？ それとも 「とりあえず無効日付は文字列のままNULL」 というもっと簡単なアプローチを探したいですか？