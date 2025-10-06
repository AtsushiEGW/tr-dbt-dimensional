from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ---------------------------
# ENV & PATHS
# ---------------------------

def get_env(name: str, default: str | os.PathLike[str] | None = None) -> str:
    """環境変数取得。default が Path の場合は str 化。未設定かつ default なしはエラー。"""
    v = os.getenv(name, default if default is None else str(default))
    if v is None:
        raise RuntimeError(f"Environment variable {name} is required.")
    return v


def get_paths() -> dict[str, Path]:
    """主要ディレクトリ（CSV, PARQUET, ARCHIVE, LANDING）を Path で返す。"""
    data_dir = Path(get_env("DATA_DIR", "./data"))
    return {
        "CSV_ROOT": Path(get_env("CSV_ROOT", data_dir / "db_ingestion")),
        "PARQUET_ROOT": Path(get_env("PARQUET_ROOT", data_dir / "parquet")),
        "ARCHIVE_ROOT": Path(get_env("ARCHIVE_ROOT", data_dir / "archive")),
        "LANDING_ROOT": Path(get_env("LANDING_ROOT", data_dir / "landing")),
    }


def today_stamp() -> str:
    return datetime.now().strftime("%Y%m%d")


def new_batch_id() -> str:
    """辞書順=時系列になるバッチID（UTC時刻 + 8hex）。"""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rand = secrets.token_hex(4)
    return f"{ts}_{rand}"


def landing_batch_dir(root: Path, namespace: str, table: str, run_date: str, batch_id: str) -> Path:
    """landing の 4階層パスを生成。"""
    return root / f"namespace={namespace}" / f"table={table}" / f"run_date={run_date}" / f"batch_id={batch_id}"


# ---------------------------
# DB ENGINE & DDL
# ---------------------------

def get_engine() -> Engine:
    """
    SQLAlchemy Engine。pool_pre_ping=True で接続切れを自動検出。
    """
    host = get_env("POSTGRES_HOST", "localhost")
    port = get_env("POSTGRES_PORT", "5432")
    user = get_env("POSTGRES_USER", "postgres")
    password = get_env("POSTGRES_PASSWORD", "")
    db = get_env("POSTGRES_DB", "postgres")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True, future=True)


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
    """すべて TEXT 列 + 主キーで作成（存在しない場合）。"""
    cols_sql = ", ".join([f'"{c}" TEXT' for c in columns])
    pk_sql = f", PRIMARY KEY ({', '.join([f'\"{c}\"' for c in pk_cols])})" if pk_cols else ""
    ddl = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({cols_sql}{pk_sql});'
    with engine.begin() as conn:
        conn.execute(text(ddl))


def add_missing_text_columns(engine: Engine, schema: str, table: str, missing_cols: list[str]):
    if not missing_cols:
        return
    with engine.begin() as conn:
        for c in missing_cols:
            conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{c}" TEXT'))