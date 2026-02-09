from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
import duckdb  # 追加

# ---------------------------
# ENV & PATHS
# ---------------------------
# ... (get_env, get_paths, today_stamp, new_batch_id, landing_batch_dir は変更なし) ...

def get_env(name: str, default: str | os.PathLike[str] | None = None) -> str:
    v = os.getenv(name, default if default is None else str(default))
    if v is None:
        raise RuntimeError(f"Environment variable {name} is required.")
    return v

def get_paths() -> dict[str, Path]:
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
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rand = secrets.token_hex(4)
    return f"{ts}_{rand}"

def landing_batch_dir(root: Path, namespace: str, table: str, run_date: str, batch_id: str) -> Path:
    return root / f"namespace={namespace}" / f"table={table}" / f"run_date={run_date}" / f"batch_id={batch_id}"

# ---------------------------
# DB CONNECTION (DuckDB)
# ---------------------------

def get_duckdb_path() -> str:
    """DuckDBファイルのパスを返す"""
    data_dir = Path(get_env("DATA_DIR", "./data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "datalake.duckdb"
    return str(db_path)

def get_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """DuckDBへの接続を返す"""
    path = get_duckdb_path()
    # 永続化ファイルへ接続
    conn = duckdb.connect(path, read_only=False)
    return conn

def ensure_schema(conn: duckdb.DuckDBPyConnection, schema: str):
    conn.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")