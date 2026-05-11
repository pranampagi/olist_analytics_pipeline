"""
airflow/dags/olist_ingest_dag.py
─────────────────────────────────
Olist Ingest DAG
Schedule: daily at 01:00 UTC

Loads raw Olist data from the Snowflake internal stage into RAW schema tables,
validates row counts, then triggers the downstream dbt transform DAG.

Task flow:
  check_stage_files
      │
      ├── load_RAW_ORDERS (truncate → copy_into)
      ├── load_RAW_CUSTOMERS (truncate → copy_into)
      ├── load_RAW_ORDER_ITEMS (truncate → copy_into)
      ├── load_RAW_PRODUCTS (truncate → copy_into)
      ├── load_RAW_SELLERS (truncate → copy_into)
      ├── load_RAW_ORDER_REVIEWS (truncate → copy_into)
      ├── load_RAW_ORDER_PAYMENTS (truncate → copy_into)
      └── load_RAW_PRODUCT_CATEGORY_TRANSLATION (truncate → copy_into)
              │
          validate_row_counts
              │
          trigger_dbt_transform

Airflow connections required:
  - snowflake_olist  (Snowflake connection type)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

# ── Constants ─────────────────────────────────────────────────────────────────
SNOWFLAKE_CONN_ID = "snowflake_olist"
DATABASE          = "OLIST_DB"
RAW_SCHEMA        = "RAW"
WAREHOUSE         = "OLIST_WH"
STAGE             = f"{DATABASE}.{RAW_SCHEMA}.OLIST_STAGE"
FILE_FORMAT       = f"{DATABASE}.{RAW_SCHEMA}.CSV_FORMAT"

# CSV filename (after compression in stage) → target table
STAGE_FILE_TABLE_MAP = {
    "olist_orders_dataset.csv.gz":               "RAW_ORDERS",
    "olist_customers_dataset.csv.gz":            "RAW_CUSTOMERS",
    "olist_order_items_dataset.csv.gz":          "RAW_ORDER_ITEMS",
    "olist_products_dataset.csv.gz":             "RAW_PRODUCTS",
    "olist_sellers_dataset.csv.gz":              "RAW_SELLERS",
    "olist_order_reviews_dataset.csv.gz":        "RAW_ORDER_REVIEWS",
    "olist_order_payments_dataset.csv.gz":       "RAW_ORDER_PAYMENTS",
    "product_category_name_translation.csv.gz":  "RAW_PRODUCT_CATEGORY_TRANSLATION",
}

MIN_ROW_COUNTS = {
    "RAW_ORDERS":                      90_000,
    "RAW_CUSTOMERS":                   90_000,
    "RAW_ORDER_ITEMS":                100_000,
    "RAW_PRODUCTS":                    30_000,
    "RAW_SELLERS":                      3_000,
    "RAW_ORDER_REVIEWS":               90_000,
    "RAW_ORDER_PAYMENTS":             100_000,
    "RAW_PRODUCT_CATEGORY_TRANSLATION":    70,
}

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "pranam",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry":   False,
    "email":            ["your_email@example.com"],  # update before deploying
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="olist_ingest",
    description="Load Olist CSVs from Snowflake internal stage into RAW schema tables",
    schedule_interval="0 1 * * *",   # 01:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "ingestion", "raw", "bronze"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    # ── 1. Verify that stage files are present ────────────────────────────────
    check_stage_files = SnowflakeOperator(
        task_id="check_stage_files",
        sql=f"LIST @{STAGE};",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse=WAREHOUSE,
        doc_md="Lists all files in the internal stage. Fails if stage is empty.",
    )

    # ── 2 + 3. Per-table load groups: truncate → COPY INTO ────────────────────
    load_task_groups = []

    for staged_file, table in STAGE_FILE_TABLE_MAP.items():
        with TaskGroup(group_id=f"load_{table.lower()}") as tg:

            truncate = SnowflakeOperator(
                task_id="truncate",
                sql=f"TRUNCATE TABLE IF EXISTS {DATABASE}.{RAW_SCHEMA}.{table};",
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse=WAREHOUSE,
            )

            copy_into = SnowflakeOperator(
                task_id="copy_into",
                sql=f"""
                    COPY INTO {DATABASE}.{RAW_SCHEMA}.{table}
                    FROM @{STAGE}/{staged_file}
                    FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
                    ON_ERROR    = 'ABORT_STATEMENT'
                    FORCE       = TRUE;
                """,
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse=WAREHOUSE,
            )

            truncate >> copy_into

        load_task_groups.append(tg)

    # ── 4. Row count validation ────────────────────────────────────────────────
    def _validate_row_counts(**context) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        failures = []

        for table, min_count in MIN_ROW_COUNTS.items():
            result = hook.get_first(
                f"SELECT COUNT(*) FROM {DATABASE}.{RAW_SCHEMA}.{table}"
            )
            actual = result[0] if result else 0
            ok = actual >= min_count
            icon = "✅" if ok else "❌"
            print(f"  {icon} {table}: {actual:>10,} rows  (min: {min_count:,})")
            if not ok:
                failures.append(
                    f"{table}: got {actual:,} rows, expected ≥ {min_count:,}"
                )

        if failures:
            raise ValueError(
                "Row count validation FAILED:\n" + "\n".join(failures)
            )

        print("\nAll row count checks passed ✅")

    validate_row_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=_validate_row_counts,
        doc_md="Asserts minimum row counts on every RAW table to catch partial loads.",
    )

    # ── 5. Trigger downstream dbt DAG ─────────────────────────────────────────
    trigger_dbt_transform = TriggerDagRunOperator(
        task_id="trigger_dbt_transform",
        trigger_dag_id="olist_transform",
        wait_for_completion=False,
        reset_dag_run=True,
        doc_md="Triggers the olist_transform DAG on successful ingest + validation.",
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    check_stage_files >> load_task_groups >> validate_row_counts >> trigger_dbt_transform
