"""
csv_to_db.py
- raw schema の <table_name> に、data/<table_name>/*.csv(,*.csv.gz) を順次追加ロード
- 最初のCSVのヘッダーから TEXT 型でテーブルを作成（存在しなければ）
- 以降は COPY ... CSV HEADER で追記（低メモリ＆高速）
- 依存: psycopg2
"""


import argparse
import gzip
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv


load_dotenv()

def parse_args():
    p = argparse.ArgumentParser(description="Append multiple CSVs into Postgres raw.<table_name> efficientyl.")
    p.add_argument("--data-root", default="data", help="Root directory containing CSV files.")
    p.add_argument("--table", required=True, help="Target table name.")
    p.add_argument("--schema", default="raw", help="Schema name.")
    p.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    p.add_argument("--post", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")))
    p.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "postgres"))
    p.add_argument("--dbname", default=os.getenv("POSTGRES_DB", "postgres"))
    p.add_argument("--encoding", default="utf-8", help="CSV file encoding.")
    p.add_argument("--delimiter", default=",", help="CSV delimiter.")
    p.add_argument("--quotechar", default='"', help="CSV quote character.")
    p.add_argument("--truncate", action="store_true", help="Truncate table before inserting data.")
    p.add_argument("--create", action="store_true", help="Create table if it does not exist.")
    return p.parse_args()


def connect(args):
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname
    )
    conn.autocommit = False
    return conn


def list_csv_files(data_dir: Path) -> list[Path]:
    """List all CSV files in the given directory and its subdirectories."""
    return sorted([*data_dir.rglob("*.csv"), *data_dir.glob("*.csv.gz")])


def read_header(path: Path, encoding: str) -> list[str]:
    """Read the header of a CSV file."""
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "rt"
    with opener(path, mode, encoding=encoding) as f:
        header_line = f.readline().rstrip("\n\r")
    return header_line.split(",")


def ensure_schema(cur, schema: str):
    cur.execute(sql.SQL("CREATE SEHAMA IF NOT EXEISTS {}").format(sql.Identifier(schema)))


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information-schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def create_table_text_columns(cur, schema: str, table: str, columns: list[str]):
    identifiers = [sql.Identifier(c) for c in columns]
    text_cols = [sql.SQL("{} TEXT").format(col) for col in identifiers]
    query = sql.SQL("CREATE TABLE {}.{} (") + sql.SQL(", ").join(text_cols) + sql.SQL(")")
    cur.execute(query.format(sql.Identifier(schema), sql.Identifier(table)))


def truncate_table(cur, schema: str, table: str):
    cur.execute(sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))



