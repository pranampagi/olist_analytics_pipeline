"""
scripts/upload_to_snowflake.py
──────────────────────────────
One-time (or re-runnable) script that PUTs Olist CSV files into the
Snowflake internal stage and COPYs them into the RAW schema tables.

Dataset source:
  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
  Download and unzip all CSVs into a single local directory.

Usage:
  # Initial load
  python scripts/upload_to_snowflake.py --data-dir /path/to/olist/csvs

  # Re-load (truncates tables first)
  python scripts/upload_to_snowflake.py --data-dir /path/to/olist/csvs --truncate

  # Dry run — validate env and files without touching Snowflake
  python scripts/upload_to_snowflake.py --data-dir /path/to/olist/csvs --dry-run

Environment variables (or .env file):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CSV filename → RAW table mapping ─────────────────────────────────────────
FILE_TABLE_MAP: Dict[str, str] = {
    "olist_orders_dataset.csv":               "RAW_ORDERS",
    "olist_customers_dataset.csv":            "RAW_CUSTOMERS",
    "olist_order_items_dataset.csv":          "RAW_ORDER_ITEMS",
    "olist_products_dataset.csv":             "RAW_PRODUCTS",
    "olist_sellers_dataset.csv":              "RAW_SELLERS",
    "olist_order_reviews_dataset.csv":        "RAW_ORDER_REVIEWS",
    "olist_order_payments_dataset.csv":       "RAW_ORDER_PAYMENTS",
    "product_category_name_translation.csv":  "RAW_PRODUCT_CATEGORY_TRANSLATION",
}

# Minimum rows expected after load — used for sanity check
MIN_ROW_COUNTS: Dict[str, int] = {
    "RAW_ORDERS":                      90_000,
    "RAW_CUSTOMERS":                   90_000,
    "RAW_ORDER_ITEMS":                100_000,
    "RAW_PRODUCTS":                    30_000,
    "RAW_SELLERS":                      3_000,
    "RAW_ORDER_REVIEWS":               90_000,
    "RAW_ORDER_PAYMENTS":             100_000,
    "RAW_PRODUCT_CATEGORY_TRANSLATION":    70,
}


# ── Connection ────────────────────────────────────────────────────────────────
def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        logger.error(f"Missing required environment variable: {key}")
        sys.exit(1)
    return value


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=_require_env("SNOWFLAKE_ACCOUNT"),
        user=_require_env("SNOWFLAKE_USER"),
        password=_require_env("SNOWFLAKE_PASSWORD"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "OLIST_DB"),
        schema="RAW",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "OLIST_ROLE"),
        session_parameters={"QUERY_TAG": "olist_raw_loader"},
    )


# ── Helpers ───────────────────────────────────────────────────────────────────
def truncate_all_tables(cursor) -> None:
    logger.info("Truncating all RAW tables…")
    for table in FILE_TABLE_MAP.values():
        cursor.execute(f"TRUNCATE TABLE IF EXISTS OLIST_DB.RAW.{table}")
        logger.info(f"  ✓ Truncated {table}")


def put_file_to_stage(cursor, file_path: Path) -> None:
    """Upload a single CSV to the internal stage with auto-compression."""
    put_sql = (
        f"PUT file://{file_path.resolve()} "
        f"@OLIST_DB.RAW.OLIST_STAGE "
        f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    cursor.execute(put_sql)
    rows = cursor.fetchall()
    for row in rows:
        # columns: source, target, source_size, target_size, source_compression,
        #          target_compression, status, message
        status = row[6] if len(row) > 6 else "unknown"
        logger.info(f"    Stage → {row[1]} | {status}")


def copy_into_table(cursor, table: str, staged_filename: str) -> int:
    """COPY from stage into raw table. Returns number of rows loaded."""
    copy_sql = f"""
        COPY INTO OLIST_DB.RAW.{table}
        FROM @OLIST_DB.RAW.OLIST_STAGE/{staged_filename}
        FILE_FORMAT = (FORMAT_NAME = 'OLIST_DB.RAW.CSV_FORMAT')
        ON_ERROR    = 'ABORT_STATEMENT'
        FORCE       = TRUE
    """
    cursor.execute(copy_sql)
    rows = cursor.fetchall()
    total_loaded = 0
    for row in rows:
        # COPY result columns: file, status, rows_parsed, rows_loaded, ...
        rows_loaded = row[3] if len(row) > 3 else 0
        total_loaded += int(rows_loaded or 0)
        logger.info(f"    {row[0]} → {rows_loaded} rows loaded")
    return total_loaded


def validate_row_counts(cursor) -> bool:
    """Check that each table meets the minimum row count threshold."""
    logger.info("\nValidating row counts…")
    all_ok = True
    for table, min_count in MIN_ROW_COUNTS.items():
        cursor.execute(f"SELECT COUNT(*) FROM OLIST_DB.RAW.{table}")
        actual = cursor.fetchone()[0]
        ok = actual >= min_count
        icon = "✅" if ok else "❌"
        logger.info(f"  {icon} {table}: {actual:>10,} rows  (min: {min_count:,})")
        if not ok:
            all_ok = False
    return all_ok


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Olist CSV files from local disk to Snowflake RAW schema"
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory containing the Olist CSV files",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate RAW tables before loading (idempotent re-load)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate files and env without touching Snowflake",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        logger.error(f"Data directory not found: {data_dir}")
        sys.exit(1)

    # Pre-flight: check which files exist
    found, missing = [], []
    for filename in FILE_TABLE_MAP:
        (found if (data_dir / filename).exists() else missing).append(filename)

    if missing:
        logger.warning(f"Missing files (will be skipped): {missing}")
    if not found:
        logger.error("No Olist CSV files found in the specified directory.")
        sys.exit(1)

    logger.info(f"Found {len(found)}/{len(FILE_TABLE_MAP)} CSV files in {data_dir}")

    if args.dry_run:
        logger.info("Dry-run mode — no changes made to Snowflake.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        if args.truncate:
            truncate_all_tables(cursor)

        total_rows = 0
        for filename, table in FILE_TABLE_MAP.items():
            file_path = data_dir / filename
            if not file_path.exists():
                logger.warning(f"Skipping {filename} — file not found")
                continue

            logger.info(f"\nProcessing {filename} → {table}")
            put_file_to_stage(cursor, file_path)
            staged_name = f"{filename}.gz"
            rows_loaded = copy_into_table(cursor, table, staged_name)
            total_rows += rows_loaded

        logger.info(f"\nTotal rows loaded across all tables: {total_rows:,}")

        all_ok = validate_row_counts(cursor)
        if not all_ok:
            logger.error("Row count validation failed — check logs above.")
            sys.exit(1)

        logger.info("\n✅ Raw data load complete and validated.")

    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Snowflake error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
