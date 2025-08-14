#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
csv_to_db.py (UPSERT対応版 / Python 3.12+)
- data/<table_name>/*.csv(,*.csv.gz) を低メモリでPostgreSQLへロード
- 2モード:
  1) append（既定）: 既存を消さずに追記
  2) upsert         : 自然キー(--pk)を基に、既存は上書き・新規は追加
- rawスキーマ想定（全列TEXTで安全に着地し、型付けはdbt側で行う前提）
- 依存: psycopg2
"""

import argparse
import gzip
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql


# ----------------------------
# 引数
# ----------------------------
def parse_args():
    """
    コマンドライン引数を解析。
    - --mode: append (既定) / upsert
    - --pk:   upsert時の一意性キー列（カンマ区切り; 複合可）
    - --create: テーブル未存在時に最初のCSVのヘッダーからTEXTで作成
    - --truncate: ロード前に既存データ削除（append時のみ実用的）
    """
    p = argparse.ArgumentParser(description="Load multiple CSVs into Postgres efficiently (append / upsert).")
    p.add_argument("--data-root", default="data", help="データのルートディレクトリ（例: data）")
    p.add_argument("--table", required=True, help="テーブル名（例: tablename）")
    p.add_argument("--schema", default="raw", help="スキーマ名（既定: raw）")
    p.add_argument("--host", default=os.getenv("POSTGRES_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")))
    p.add_argument("--user", default=os.getenv("POSTGRES_USER", "postgres"))
    p.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "postgres"))
    p.add_argument("--dbname", default=os.getenv("POSTGRES_DB", "postgres"))
    p.add_argument("--encoding", default="utf-8", help="CSVファイルのエンコーディング（既定: utf-8）")
    p.add_argument("--delimiter", default=",", help="CSVの区切り文字（既定: ,）")
    p.add_argument("--quotechar", default='"', help='CSVの引用符（既定: "）')
    p.add_argument("--truncate", action="store_true", help="ロード前にテーブルを空にする（append運用時に有用）")
    p.add_argument("--create", action="store_true", help="テーブルが無ければ作成（最初のCSVヘッダーからTEXTで定義）")
    p.add_argument("--mode", choices=["append", "upsert"], default="append", help="追記 or UPSERT（既定: append）")
    p.add_argument("--pk", default="", help="upsert時の一意キー列。カンマ区切りで複数可（例: id / store_id,order_date）")
    return p.parse_args()


# ----------------------------
# 接続
# ----------------------------
def connect(args):
    """
    PostgreSQLへ接続（autocommit=False）
    """
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
    )
    conn.autocommit = False
    return conn


# ----------------------------
# ファイルユーティリティ
# ----------------------------
def list_csv_files(data_dir: Path) -> list[Path]:
    """
    対象ディレクトリ内の .csv と .csv.gz をソートして列挙。
    """
    return sorted([*data_dir.glob("*.csv"), *data_dir.glob("*.csv.gz")])


def read_header(path: Path, encoding: str) -> list[str]:
    """
    先頭行(ヘッダー)だけを読み、列名リストとして返す。
    """
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "rt"
    with opener(path, mode, encoding=encoding, newline="") as f:
        header_line = f.readline().rstrip("\n\r")
    return header_line.split(",")


def validate_headers_consistent(files: list[Path], encoding: str) -> list[str]:
    """
    全CSVでヘッダー一致を検証（列名・順序）。不一致なら中止。
    """
    if not files:
        raise ValueError("CSVファイルが見つかりません。")
    base = read_header(files[0], encoding)
    for p in files[1:]:
        h = read_header(p, encoding)
        if h != base:
            raise ValueError(
                f"ヘッダー不一致: {files[0].name} と {p.name} で列が異なります。\n{files[0].name}: {base}\n{p.name}: {h}"
            )
    return base


# ----------------------------
# スキーマ/テーブル管理
# ----------------------------
def ensure_schema(cur, schema: str):
    """
    スキーマ作成（存在すれば何もしない）。
    """
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


def table_exists(cur, schema: str, table: str) -> bool:
    """
    テーブル存在確認。
    """
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def create_table_text_columns(cur, schema: str, table: str, columns: list[str]):
    """
    全列 TEXT でテーブル作成。列名はヘッダーをそのまま採用。
    """
    identifiers = [sql.Identifier(c) for c in columns]
    text_cols = [sql.SQL("{} TEXT").format(col) for col in identifiers]
    query = sql.SQL(
        "CREATE TABLE {}.{} ({})"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(text_cols),
        )
    cur.execute(query)


def truncate_table(cur, schema: str, table: str):
    """
    データ全削除（構造は維持）。
    """
    cur.execute(sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))


def ensure_unique_constraint(cur, schema: str, table: str, pk_cols: list[str]):
    if not pk_cols:
        raise ValueError("--pk を指定してください")

    # 既に同じ列集合の一意制約/PKがあるかを確認
    cur.execute("""
        SELECT array_agg(a.attname ORDER BY a.attname) AS cols
        FROM pg_index i
        JOIN pg_class t   ON i.indrelid = t.oid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE n.nspname = %s AND t.relname = %s AND i.indisunique
        GROUP BY i.indexrelid
    """, (schema, table))
    unique_sets = {tuple(sorted(row[0])) for row in cur.fetchall()}

    if tuple(sorted(pk_cols)) in unique_sets:
        return  # 既に同じ列集合のユニークインデックス/制約がある

    # ない場合は追加
    constraint_name = f"{table}_ux"
    col_idents = [sql.Identifier(c) for c in pk_cols]
    q = sql.SQL(
        "ALTER TABLE {}.{} ADD CONSTRAINT {} UNIQUE ({})"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.Identifier(constraint_name),
        sql.SQL(", ").join(col_idents),
    )
    cur.execute(q)

# ----------------------------
# COPY ロード（append / upsert）
# ----------------------------
def copy_into_target_append(cur, schema: str, table: str, csv_path: Path, delimiter: str, quotechar: str, encoding: str):
    """
    appendモード: CSVをそのままターゲットにCOPY（HEADER付き）。
    """
    opener = gzip.open if csv_path.suffix == ".gz" else open
    with opener(csv_path, "rt", encoding=encoding, newline="") as f:
        copy_sql = sql.SQL(
            "COPY {}.{} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER %s, QUOTE %s)"
        ).format(sql.Identifier(schema), sql.Identifier(table))
        cur.copy_expert(cur.mogrify(copy_sql.as_string(cur), (delimiter, quotechar)).decode(), f)


def create_temp_table(cur, temp_name: str, columns: list[str]):
    """
    UPSERT用: セッションローカルの一時テーブルを作成（全列TEXT）。
    - 先に DROP TABLE IF EXISTS で掃除
    - Composed に .format() しないよう一発 format で安全に生成
    - トランザクション終了時に自動削除（ON COMMIT DROP）
    """
    # 既存があれば削除
    drop_q = sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_name))
    cur.execute(drop_q)

    # CREATE TEMP TABLE ... (col1 TEXT, col2 TEXT, ...) ON COMMIT DROP
    idents = [sql.Identifier(c) for c in columns]
    text_cols = [sql.SQL("{} TEXT").format(c) for c in idents]
    create_q = sql.SQL(
        "CREATE TEMP TABLE {} ({}) ON COMMIT DROP"
    ).format(
        sql.Identifier(temp_name),
        sql.SQL(", ").join(text_cols),
    )
    cur.execute(create_q)


def copy_into_temp(cur, temp_name: str, csv_path: Path, delimiter: str, quotechar: str, encoding: str):
    """
    UPSERT用: CSVを一時テーブルへCOPY（HEADER付き）。
    """
    opener = gzip.open if csv_path.suffix == ".gz" else open
    with opener(csv_path, "rt", encoding=encoding, newline="") as f:
        copy_sql = sql.SQL(
            "COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER %s, QUOTE %s)"
        ).format(sql.Identifier(temp_name))
        cur.copy_expert(cur.mogrify(copy_sql.as_string(cur), (delimiter, quotechar)).decode(), f)


def upsert_from_temp(cur, schema: str, table: str, temp_name: str, columns: list[str], pk_cols: list[str]):
    """
    一時テーブルからターゲットへUPSERT。
    - ON CONFLICT (pk_cols...) DO UPDATE SET 非PK列=EXCLUDED.非PK列
    """
    col_idents = [sql.Identifier(c) for c in columns]
    pk_idents  = [sql.Identifier(c) for c in pk_cols]

    non_pk_cols = [c for c in columns if c not in pk_cols]
    set_pairs = [
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in non_pk_cols
    ]

    col_list    = sql.SQL(", ").join(col_idents)
    select_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    pk_list     = sql.SQL(", ").join(pk_idents)

    if set_pairs:
        set_list = sql.SQL(", ").join(set_pairs)
        q = sql.SQL(
            "INSERT INTO {}.{} ({}) "
            "SELECT {} FROM {} "
            "ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            col_list,
            select_list,
            sql.Identifier(temp_name),
            pk_list,
            set_list,
        )
    else:
        q = sql.SQL(
            "INSERT INTO {}.{} ({}) "
            "SELECT {} FROM {} "
            "ON CONFLICT ({}) DO NOTHING"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            col_list,
            select_list,
            sql.Identifier(temp_name),
            pk_list,
        )
    cur.execute(q)


# ----------------------------
# メイン
# ----------------------------
def main():
    """
    フロー
    1) 引数解析・対象CSV列挙・ヘッダー一致検証
    2) DB接続・スキーマ確保・テーブル作成（必要時）
    3) append:   直接COPY
       upsert:   一時テーブルにCOPY → ON CONFLICTでUPSERT
    """
    args = parse_args()
    data_dir = Path(args.data_root) / args.table
    files = list_csv_files(data_dir)
    if not files:
        print(f"[INFO] ファイルなし: {data_dir} に CSV が見つかりません。")
        return

    headers = validate_headers_consistent(files, args.encoding)
    pk_cols = [c.strip() for c in args.pk.split(",") if c.strip()] if args.mode == "upsert" else []

    conn = connect(args)
    try:
        with conn.cursor() as cur:
            # スキーマ＆テーブル
            ensure_schema(cur, args.schema)
            exists = table_exists(cur, args.schema, args.table)

            if not exists and args.create:
                print(f"[INFO] テーブル未存在のため作成: {args.schema}.{args.table}")
                create_table_text_columns(cur, args.schema, args.table, headers)
                exists = True
            elif not exists and not args.create:
                raise RuntimeError(
                    f"テーブル {args.schema}.{args.table} が存在しません。--create を指定して自動作成するか、先に作成してください。"
                )

            # appendモードでtruncate指定 → フルリロードに便利
            if args.mode == "append" and args.truncate and exists:
                print(f"[INFO] TRUNCATE: {args.schema}.{args.table}")
                truncate_table(cur, args.schema, args.table)

            # upsertモードでは一意制約が必要
            if args.mode == "upsert":
                ensure_unique_constraint(cur, args.schema, args.table, pk_cols)

            # ファイルごとに処理（低メモリ）
            for i, path in enumerate(files, 1):
                print(f"[INFO] ({i}/{len(files)}) {path.name} 処理中 …")

                if args.mode == "append":
                    # そのままターゲットにCOPY
                    copy_into_target_append(
                        cur, args.schema, args.table, path, args.delimiter, args.quotechar, args.encoding
                    )
                else:
                    # UPSERT: 一時テーブルに入れてからマージ
                    temp_name = f"tmp_{args.table}"
                    create_temp_table(cur, temp_name, headers)
                    copy_into_temp(cur, temp_name, path, args.delimiter, args.quotechar, args.encoding)
                    upsert_from_temp(cur, args.schema, args.table, temp_name, headers, pk_cols)
                    # 一時テーブルはトランザクション終了時に自動削除

        conn.commit()
        print(f"[DONE] {len(files)} 個のファイルを {args.mode.upper()} で {args.schema}.{args.table} へ反映完了。")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()