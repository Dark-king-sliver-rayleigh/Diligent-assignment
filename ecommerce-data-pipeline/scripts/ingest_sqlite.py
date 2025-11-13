import os
import sqlite3
from typing import List, Optional

import pandas as pd

# Resolve project structure
THIS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(THIS_DIR, os.pardir))
DATA_DIR = os.path.join(ROOT_DIR, "data")
DB_PATH = os.path.join(ROOT_DIR, "ecommerce.db")

TABLES: List[str] = [
    "products",
    "customers",
    "orders",
    "order_items",
    "reviews",
]

# Optional: simple date parsing map by table -> columns
DATE_COLS = {
    "products": ["created_at"],
    "customers": ["join_date"],
    "orders": ["order_date"],
    "reviews": ["review_date"],
}


def read_csv_if_exists(table: str) -> Optional[pd.DataFrame]:
    """Read a CSV from DATA_DIR if present; return None if missing."""
    csv_path = os.path.join(DATA_DIR, f"{table}.csv")
    if not os.path.exists(csv_path):
        print(f"[WARN] Missing CSV, skipping table: {table} -> {csv_path}")
        return None

    parse_dates = DATE_COLS.get(table, None)
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig", parse_dates=parse_dates)
        # Normalize column names to lowercase snake_case (optional, but consistent)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"[ERROR] Failed reading {csv_path}: {e}")
        return None


def create_indexes(conn: sqlite3.Connection) -> None:
    """Create useful indexes to speed up typical joins and lookups."""
    sql_statements = [
        # Example join accelerators
        "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);",
        "CREATE INDEX IF NOT EXISTS idx_item_order ON order_items(order_id);",
        "CREATE INDEX IF NOT EXISTS idx_item_product ON order_items(product_id);",
        "CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id);",
        # Extras that often help
        "CREATE INDEX IF NOT EXISTS idx_reviews_customer ON reviews(customer_id);",
        "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);",
    ]

    cur = conn.cursor()
    for stmt in sql_statements:
        try:
            cur.execute(stmt)
        except Exception as e:
            print(f"[WARN] Index creation failed for: {stmt} -> {e}")
    conn.commit()


def main() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print(f"[INFO] Ingesting CSVs from: {DATA_DIR}")
    print(f"[INFO] Target SQLite DB: {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        loaded: List[str] = []
        skipped: List[str] = []

        for table in TABLES:
            df = read_csv_if_exists(table)
            if df is None:
                skipped.append(table)
                continue

            try:
                df.to_sql(table, conn, if_exists="replace", index=False)
                loaded.append(table)
                print(f"[OK] Loaded {table}: {len(df):,} rows")
            except Exception as e:
                print(f"[ERROR] Failed writing table {table}: {e}")
                skipped.append(table)

        # Create indexes after tables are present
        create_indexes(conn)

    print("\n[SUMMARY]")
    print(f"  Loaded tables  : {', '.join(loaded) if loaded else 'none'}")
    print(f"  Skipped tables : {', '.join(skipped) if skipped else 'none'}")
    print("Ingestion complete.")


if __name__ == "__main__":
    main()
