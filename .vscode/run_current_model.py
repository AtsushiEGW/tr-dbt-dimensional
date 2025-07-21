# .vscode/run_current_model.py
import subprocess
import sys
from pathlib import Path

def main():
    # dbt プロジェクトのルート（adventureworks 配下）
    project_dir = Path(__file__).parent.parent / "adventureworks"
    db_path = project_dir / "target" / "adventureworks.duckdb"
    file_path = Path(sys.argv[1])
    duckdb_cli_path = "/Users/lychee/.duckdb/cli/latest/duckdb"

    if file_path.suffix != ".sql":
        print("Not a SQL file.")
        return

    model_name = file_path.stem
    schema_name = file_path.parent.name
    table_name = f'"{schema_name}"."{model_name}"'

    cmd = (
        f"dbt run --select {model_name} --profiles-dir . && "
        f"{duckdb_cli_path} {db_path} -c 'SELECT * FROM {table_name} LIMIT 5;'"
    )

    subprocess.run(cmd, shell=True, check=True, cwd=project_dir)

if __name__ == "__main__":
    main()