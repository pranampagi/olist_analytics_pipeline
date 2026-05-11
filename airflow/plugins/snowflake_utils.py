"""
airflow/plugins/snowflake_utils.py
───────────────────────────────────
Shared Snowflake helper functions used across Olist DAGs.
Import in DAGs with:  from snowflake_utils import get_table_row_count, ...
"""

import logging
from typing import Any, Dict, List, Optional

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)


def get_table_row_count(
    conn_id: str, database: str, schema: str, table: str
) -> int:
    """Return the current row count for a given fully-qualified table."""
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    result = hook.get_first(
        f"SELECT COUNT(*) FROM {database}.{schema}.{table}"
    )
    return int(result[0]) if result else 0


def log_table_stats(
    conn_id: str,
    database: str,
    schema: str,
    tables: List[str],
) -> Dict[str, int]:
    """Log and return row counts for a list of tables."""
    stats: Dict[str, int] = {}
    for table in tables:
        count = get_table_row_count(conn_id, database, schema, table)
        stats[table] = count
        logger.info(f"  {table}: {count:>10,} rows")
    return stats


def run_sql(conn_id: str, sql: str) -> List[Any]:
    """Execute arbitrary SQL and return all result rows."""
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    return hook.get_records(sql)


def assert_min_row_count(
    conn_id: str,
    database: str,
    schema: str,
    table: str,
    min_count: int,
) -> None:
    """Raise ValueError if a table has fewer rows than expected."""
    actual = get_table_row_count(conn_id, database, schema, table)
    if actual < min_count:
        raise ValueError(
            f"{database}.{schema}.{table} has {actual:,} rows "
            f"— expected at least {min_count:,}."
        )
    logger.info(f"  ✅ {table}: {actual:,} rows (min {min_count:,})")


def get_latest_load_timestamp(
    conn_id: str, database: str, schema: str, table: str
) -> Optional[str]:
    """Return the MAX(_LOADED_AT) timestamp from a RAW table as a string."""
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    result = hook.get_first(
        f"SELECT MAX(_LOADED_AT) FROM {database}.{schema}.{table}"
    )
    return str(result[0]) if result and result[0] else None
