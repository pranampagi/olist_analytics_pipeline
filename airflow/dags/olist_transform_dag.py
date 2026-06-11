"""
airflow/dags/olist_transform_dag.py
────────────────────────────────────
Olist Transform DAG
Schedule: triggered by olist_ingest (or run manually)

Orchestrates the full dbt pipeline in layer order:
  deps → source_freshness → staging → intermediate → marts → docs_generate

Task flow:
  dbt_deps
      │
  dbt_source_freshness
      │
  ┌── staging ──────────────────┐
  │  dbt_run_staging            │
  │      │                      │
  │  dbt_test_staging           │
  └─────────────────────────────┘
      │
  ┌── intermediate ─────────────┐
  │  dbt_run_intermediate       │
  │      │                      │
  │  dbt_test_intermediate      │
  └─────────────────────────────┘
      │
  ┌── marts ────────────────────┐
  │  dbt_run_marts              │
  │      │                      │
  │  dbt_test_marts             │
  └─────────────────────────────┘
      │
  dbt_docs_generate

Environment variables needed on the Airflow worker:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
  DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_TARGET
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# ── dbt CLI path config ───────────────────────────────────────────────────────
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_TARGET       = os.environ.get("DBT_TARGET",       "prod")

DBT_BASE = (
    f"dbt --no-use-colors "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--target {DBT_TARGET}"
)

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "pranam",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry":   False,
    "email":            ["your_email@example.com"],  # update before deploying
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="olist_transform",
    description="Run dbt staging → intermediate → marts pipeline for Olist analytics",
    schedule_interval=None,    # externally triggered by olist_ingest
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "dbt", "transform", "silver", "gold"],
    doc_md=__doc__,
    max_active_runs=1,
) as dag:

    # ── 1. Install dbt packages ───────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_BASE} deps",
        doc_md="Installs dbt-utils and dbt_expectations packages.",
    )

    # ── 2. Source freshness check ─────────────────────────────────────────────
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_BASE} source freshness",
        doc_md=(
            "Checks that _LOADED_AT on RAW tables is recent enough. "
            "Warns after 25h, errors after 49h (configured in sources.yml)."
        ),
    )

    # ── 3 + 4. Staging layer ──────────────────────────────────────────────────
    with TaskGroup("staging") as staging_group:
        run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"{DBT_BASE} run --select staging",
            doc_md="Builds all stg_* views in the STAGING schema.",
        )
        test_staging = BashOperator(
            task_id="dbt_test_staging",
            bash_command=f"{DBT_BASE} test --select staging",
            doc_md="Runs schema + singular tests on all staging models.",
        )
        run_staging >> test_staging

    # ── 5 + 6. Intermediate layer ─────────────────────────────────────────────
    with TaskGroup("intermediate") as intermediate_group:
        run_intermediate = BashOperator(
            task_id="dbt_run_intermediate",
            bash_command=f"{DBT_BASE} run --select intermediate",
            doc_md="Builds int_* ephemeral models (joined / enriched views).",
        )
        test_intermediate = BashOperator(
            task_id="dbt_test_intermediate",
            bash_command=f"{DBT_BASE} test --select intermediate",
            doc_md="Runs schema tests on intermediate models.",
        )
        run_intermediate >> test_intermediate

    # ── 7 + 8. Marts layer ────────────────────────────────────────────────────
    with TaskGroup("marts") as marts_group:
        run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"{DBT_BASE} run --select marts",
            doc_md="Materialises all mart_* tables in the MARTS schema.",
        )
        test_marts = BashOperator(
            task_id="dbt_test_marts",
            bash_command=f"{DBT_BASE} test --select marts",
            doc_md="Runs schema + singular tests on all mart models.",
        )
        run_marts >> test_marts

    # ── 9. Generate and publish dbt docs ──────────────────────────────────────
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT_BASE} compile --write-catalog",
        doc_md=(
            "Generates the dbt documentation site with full data lineage graph. "
            "Serve locally with: dbt docs serve --port 8081"
        ),
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    (
        dbt_deps
        >> dbt_source_freshness
        >> staging_group
        >> intermediate_group
        >> marts_group
        >> dbt_docs_generate
    )
