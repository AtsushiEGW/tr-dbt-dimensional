import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path


load_dotenv()

def get_env(name: str, default: str | os.PathLike[str] | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Environment variable {name} is required.")
    return str(v)


def get_engine() -> Engine:
    """_summary_
    create_engine()の pool_pre_ping=True 引数について
    - 接続を使う直前に「ping（SELECT 1 などの軽いクエリ）」を投げて、有効かどうかを確認する。
    - 死んでいたら、その接続を捨てて新しい接続を作り直す。
    - その結果、アプリケーション側から見たら「突然の接続切れ」がほぼ起こらなくなる。

    コスト
    - 毎回の接続利用前に1クエリ走るので、ほんの少しオーバーヘッドはある。ただし一般的な業務アプリ・ETLなら無視できるレベル。

    Returns:
        Engine: create_engine(url) を実行
    """
    user = get_env("postgres_user")
    pwd = get_env("postgres_password")
    host = get_env("postgres_host")
    port = get_env("postgres_port", "5432")
    db = get_env("postgres_db")
    url = f"postgres+psycopg://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ensure_schema(engine: Engine, schema: str):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    sql = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        return conn.execute(text(sql), {"schema": schema, "table": table}).scalar() is not None


def get_table_columns(engine: Engine, schema: str, table: str) -> list[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    ORDER BY ordinal_position
    """
    with engine.connect() as conn:
        return list(conn.execute(text(sql), {"schema": schema, "table": table}).scalars().all())


def create_text_table(engine: Engine, schema: str, table: str, columns: list[str], pk_cols: list[str]):
    """すべて TEXT の列で作成。存在しない場合のみ。
    """
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    pk_sql = f", PRIMARY KEY ({', '.join([f'\"{c}\"' for c in pk_cols])})" if pk_cols else ""
    ddl = f'CREATE TABLE "{schema}"."{table}" ({cols_sql}{pk_sql});'
    with engine.begin() as conn:
        conn.execute(text(ddl))


def add_missing_text_columns(engine: Engine, schema: str, table: str, missing_cols: list[str]):
    if not missing_cols:
        return 
    with engine.begin() as conn:
        for c in missing_cols:
            conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{c}" TEXT'))


def get_paths() -> dict[str, Path]:
    data_dir = Path(get_env("DATA_DIR", "./data"))
    return {
        "CSV_ROOT": Path(get_env("CSV_ROOT", str(data_dir / "csvs_demo"))),
        "PARQUET_ROOT": Path(get_env("PARQUET_ROOT", str(data_dir / "parquet"))),
        "ARCHIVE_ROOT": Path(get_env("ARCHIVE_ROOT", str(data_dir / "archive")))
    }


def get_retention_days() -> int:
    return int(get_env("csv_retention_days", "90"))

def today_stamp() -> str:
    return datetime.now().strftime("%Y%m%d")