import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def run_sql_file(conn, sql_path: Path) -> None:
    with conn, conn.cursor() as cur:
        with sql_path.open("r", encoding="utf-8") as f:
            sql = f.read()
        cur.execute(sql)

def main():
    load_dotenv(override=False)

    host = os.getenv("POSTGRES_HOST", "db")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    dbname = os.getenv("POSTGRES_DB", "mypostgres")

    sql_path = Path(__file__).parent / "hello_world.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL File Not Found: {sql_path}")

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    )

    try:
        run_sql_file(conn, sql_path)
        print("✅️ Ingestion done: hello_world.greetings inserted.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()