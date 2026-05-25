import argparse
import os
from pathlib import Path

import duckdb

BASE_DIR = Path("/opt/kest/duckdb")


def load_sql(path: Path) -> str:
    sql = path.read_text(encoding="utf-8")
    for key, value in os.environ.items():
        sql = sql.replace(f"${{{key}}}", value)
    return sql


def run_sql(conn: duckdb.DuckDBPyConnection, path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing SQL file: {path}")
    conn.execute(load_sql(path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage",
        required=True,
        choices=["serving", "bronze_views"],
    )
    args = parser.parse_args()

    db_path = BASE_DIR / "kest.duckdb"
    conn = duckdb.connect(str(db_path))

    if args.stage == "bronze_views":
        run_sql(conn, BASE_DIR / "config" / "bronze.sql")
        run_sql(conn, BASE_DIR / "serving" / "bronze_views.sql")
    elif args.stage == "serving":
        run_sql(conn, BASE_DIR / "config" / "etl.sql")
        run_sql(conn, BASE_DIR / "serving" / "gold_views.sql")

    conn.close()


if __name__ == "__main__":
    main()
