import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String,
    insert, select, text
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv

# ========================
# .env を読み込み
# ========================
load_dotenv()

# 環境変数を取得（必須項目が欠けていたら即エラー）
def require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"環境変数 {key} が設定されていません")
    return value

DB_USER = require_env("POSTGRES_USER")
DB_PASSWORD = require_env("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "db")  # デフォルトで "db"
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = require_env("POSTGRES_DB")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ========================
# Engine 作成
# ========================
engine = create_engine(DATABASE_URL, echo=True, future=True)

# ========================
# メタデータ準備
# ========================
SCHEMA = "hello_world"
metadata = MetaData(schema=SCHEMA)

# ========================
# スキーマ作成（権限がなければエラー表示）
# ========================
with engine.begin() as conn:
    try:
        # AUTHORIZATION を指定して所有者を明示
        conn.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} AUTHORIZATION {DB_USER}")
        )
        print(f"✅ ensured schema {SCHEMA}")
    except ProgrammingError as e:
        print(f"⚠️ CREATE SCHEMA 失敗: {e}")
        exists = conn.execute(
            text("SELECT 1 FROM pg_namespace WHERE nspname=:n"),
            {"n": SCHEMA},
        ).fetchone()
        if not exists:
            raise RuntimeError(
                f"スキーマ {SCHEMA} を作成できませんでした。"
                f" DB所有者や CREATE 権限を確認してください。"
            )

# ========================
# テーブル定義
# ========================
hello_world_table = Table(
    "hello_world",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("message", String(255), nullable=False),
)

# テーブル作成
metadata.create_all(engine)
print("✅ hello_world.hello_world テーブル作成確認")

# ========================
# サンプルデータ挿入
# ========================
sample_data = [
    {"message": "Hello, World!"},
    {"message": "こんにちは、世界！"},
    {"message": "Bonjour le monde!"},
    {"message": "Hola, Mundo!"},
]

with engine.begin() as conn:
    # 既存データが空ならだけ挿入
    if conn.execute(select(hello_world_table.c.id).limit(1)).fetchone() is None:
        conn.execute(insert(hello_world_table), sample_data)
        print("✅ サンプルデータを挿入しました")
    else:
        print("ℹ️ サンプルデータは既に存在しています")

# ========================
# データ確認
# ========================
with engine.connect() as conn:
    result = conn.execute(select(hello_world_table)).fetchall()
    for row in result:
        print(row)

print("🎉 完了: hello_world.hello_world テーブル作成 & サンプルデータ確認 OK")