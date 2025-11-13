import os
import sqlite3
import csv
from typing import List

THIS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(THIS_DIR, os.pardir))
DB_PATH = os.path.join(ROOT_DIR, "ecommerce.db")
SQL_FILE = os.path.join(ROOT_DIR, "sql", "queries.sql")


def split_sql_statements(sql_text: str) -> List[str]:
    # Simple splitter: split on semicolons that end a statement
    # Not robust for semicolons inside strings, but fine for our file.
    parts = [s.strip() for s in sql_text.split(';')]
    return [p for p in parts if p]


def print_query_result(cur: sqlite3.Cursor, query_idx: int) -> None:
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []

    print(f"\n-- Query {query_idx} results --")
    if not rows:
        print("(no rows)")
        return

    # Print CSV header + rows
    writer = csv.writer(os.sys.stdout)
    writer.writerow(cols)
    for r in rows:
        writer.writerow(r)


def main() -> None:
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"Database not found: {DB_PATH}. Run scripts/ingest_sqlite.py first.")
    if not os.path.exists(SQL_FILE):
        raise SystemExit(f"SQL file not found: {SQL_FILE}.")

    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        sql_text = f.read()

    statements = split_sql_statements(sql_text)
    if not statements:
        raise SystemExit("No SQL statements to execute.")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        qn = 1
        for stmt in statements:
            stmt_stripped = stmt.strip()
            if not stmt_stripped:
                continue
            try:
                cur.execute(stmt_stripped)
                print_query_result(cur, qn)
            except Exception as e:
                print(f"[ERROR] Query {qn} failed: {e}\nSQL:\n{stmt_stripped}\n")
            qn += 1


if __name__ == "__main__":
    main()
