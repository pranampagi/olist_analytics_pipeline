# Olist E-Commerce Analytics Pipeline
### dbt · Apache Airflow · Snowflake · GitHub Actions

A production-grade ELT analytics engineering pipeline built on the
[Brazilian E-Commerce (Olist) dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Demonstrates the **modern data stack**: orchestrated ingestion, layered dbt transformations,
schema-tested gold-layer marts, and CI/CD — from raw CSV to business-ready analytics tables.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                │
│   Olist CSVs (Kaggle) → Snowflake Internal Stage                   │
└─────────────────────────────┬──────────────────────────────────────┘
                              │  Airflow: olist_ingest DAG (daily 01:00 UTC)
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  ·  OLIST_DB.RAW                                     │
│  RAW_ORDERS  RAW_CUSTOMERS  RAW_ORDER_ITEMS  RAW_PRODUCTS          │
│  RAW_SELLERS  RAW_ORDER_REVIEWS  RAW_ORDER_PAYMENTS                │
│  RAW_PRODUCT_CATEGORY_TRANSLATION                                  │
└─────────────────────────────┬──────────────────────────────────────┘
                              │  Airflow: olist_transform DAG (triggered)
                              │  dbt run --select staging
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  ·  OLIST_DB.STAGING  (dbt views)                    │
│  stg_orders · stg_customers · stg_order_items · stg_products       │
│  stg_sellers · stg_order_reviews · stg_order_payments              │
└─────────────────────────────┬──────────────────────────────────────┘
                              │  dbt run --select intermediate
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│  SILVER+  ·  ephemeral (compiled inline, no physical table)        │
│  int_order_items_enriched · int_customer_orders                    │
└─────────────────────────────┬──────────────────────────────────────┘
                              │  dbt run --select marts
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  ·  OLIST_DB.MARTS  (dbt tables)                       │
│  mart_revenue_by_category   mart_seller_performance                │
│  mart_customer_lifetime_value   mart_monthly_revenue_trend         │
└─────────────────────────────┬──────────────────────────────────────┘
                              │  dbt docs generate
                              ▼
                    dbt Lineage Graph + Docs Site
```

---

## Project Structure

```
olist_analytics_pipeline/
├── snowflake/
│   └── setup.sql                      # One-time Snowflake bootstrap (DDL)
│
├── scripts/
│   └── upload_to_snowflake.py         # CSV → stage → RAW schema loader
│
├── airflow/
│   ├── dags/
│   │   ├── olist_ingest_dag.py        # Bronze ingest DAG (daily schedule)
│   │   └── olist_transform_dag.py     # dbt transform DAG (triggered)
│   └── plugins/
│       └── snowflake_utils.py         # Shared Snowflake helper functions
│
├── dbt/
│   ├── dbt_project.yml                # Project config (schemas, materializations)
│   ├── profiles.yml                   # Snowflake connection (prod + dev targets)
│   ├── packages.yml                   # dbt-utils, dbt_expectations
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml            # Source declarations + freshness checks
│   │   │   ├── schema.yml             # Staging model tests
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_sellers.sql
│   │   │   ├── stg_order_reviews.sql
│   │   │   └── stg_order_payments.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── schema.yml
│   │   │   ├── int_order_items_enriched.sql
│   │   │   └── int_customer_orders.sql
│   │   │
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── mart_revenue_by_category.sql
│   │       ├── mart_seller_performance.sql
│   │       ├── mart_customer_lifetime_value.sql
│   │       └── mart_monthly_revenue_trend.sql
│   │
│   ├── tests/                         # Singular (custom) dbt tests
│   │   ├── assert_no_negative_revenue.sql
│   │   ├── assert_delivery_delay_reasonable.sql
│   │   └── assert_all_customers_have_clv_segment.sql
│   │
│   ├── macros/
│   │   ├── generate_schema_name.sql   # DEV_ prefix for dev target
│   │   └── cents_to_currency.sql
│   │
│   └── analyses/                      # Ad-hoc business queries
│       ├── top_10_revenue_categories.sql
│       ├── delivery_delay_by_seller_state.sql
│       ├── customer_repeat_purchase_rate.sql
│       └── monthly_gmv_trend.sql
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml                 # PR → compile + run + test + docs
│
├── docker-compose.yml                 # Local Airflow (webserver + scheduler + postgres)
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Setup Guide

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Python | ≥ 3.11 | python.org |
| Docker Desktop | latest | docker.com |
| Snowflake account | free trial OK | app.snowflake.com |
| Kaggle account | free | kaggle.com |

---

### Step 1 — Download the Dataset

1. Go to [Kaggle: Brazilian E-Commerce (Olist)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Click **Download** → unzip to a local folder, e.g. `~/data/olist/`
3. You should see 8 CSV files including `olist_orders_dataset.csv`

---

### Step 2 — Bootstrap Snowflake

1. Log into your Snowflake account as `ACCOUNTADMIN`
2. Open a worksheet, paste the contents of `snowflake/setup.sql`
3. Update the password on line ~55 (`ChangeMe123!` → your own password)
4. Run the entire script
5. Verify with the final SELECT at the bottom — you should see 8 RAW tables

---

### Step 3 — Configure Credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in your Snowflake account identifier, user, and password.

> **Finding your account identifier:**
> Snowflake UI → Admin → Accounts → copy the `<orgname>-<accountname>` format

---

### Step 4 — Load Raw Data to Snowflake

```bash
pip install -r requirements.txt

# Initial load
python scripts/upload_to_snowflake.py --data-dir ~/data/olist/

# To reload from scratch (idempotent)
python scripts/upload_to_snowflake.py --data-dir ~/data/olist/ --truncate

# Dry run (validate only — no Snowflake calls)
python scripts/upload_to_snowflake.py --data-dir ~/data/olist/ --dry-run
```

Expected output:
```
2024-01-15 10:23:01 | INFO     | Found 8/8 CSV files in /Users/.../data/olist/
2024-01-15 10:23:04 | INFO     | Processing olist_orders_dataset.csv → RAW_ORDERS
2024-01-15 10:24:01 | INFO     | ✅ RAW_ORDERS:   99,441 rows  (min: 90,000)
...
2024-01-15 10:27:30 | INFO     | ✅ Raw data load complete and validated.
```

---

### Step 5 — Run dbt Manually (Quickstart)

```bash
cd dbt

# Install dbt packages (dbt-utils, dbt_expectations)
dbt deps --profiles-dir . --project-dir .

# Check source freshness
dbt source freshness --profiles-dir . --project-dir .

# Run all models: staging → intermediate → marts
dbt run --profiles-dir . --project-dir .

# Run all tests
dbt test --profiles-dir . --project-dir .

# Generate and serve docs (opens browser at http://localhost:8080)
dbt docs generate --profiles-dir . --project-dir .
dbt docs serve --profiles-dir . --project-dir . --port 8081
```

---

### Step 6 — Run with Airflow (Full Orchestration)

```bash
# First-time: initialise Airflow DB + create admin user
docker compose --profile init up airflow-init

# Start Airflow (webserver + scheduler + postgres)
docker compose up -d

# Open Airflow UI
open http://localhost:8080
# Login: admin / admin
```

In the Airflow UI:

1. Go to **Admin → Connections** → create a new connection:
   - Conn ID: `snowflake_olist`
   - Conn Type: `Snowflake`
   - Host: `<your-account>.snowflakecomputing.com`
   - Schema: `RAW`
   - Login / Password: from your `.env`
   - Extra: `{"warehouse": "OLIST_WH", "role": "OLIST_ROLE", "database": "OLIST_DB"}`

2. Enable the **`olist_ingest`** DAG → click ▶ **Trigger DAG**
3. Watch it complete → `olist_transform` is automatically triggered
4. Check the Graph view to see all tasks pass ✅

---

### Step 7 — CI/CD via GitHub Actions

1. Push this repo to GitHub
2. Go to **Settings → Secrets and variables → Actions**
3. Add each variable from `.env` as a repository secret
4. Open a Pull Request — the `dbt CI` workflow runs automatically:
   - `dbt deps` → `dbt compile` → `dbt run (dev)` → `dbt test` → `dbt docs generate`
5. The dbt docs are uploaded as a downloadable artifact on each run

---

## dbt Model Reference

### Staging layer (`OLIST_DB.STAGING`) — views

| Model | Source table | Key transformations |
|---|---|---|
| `stg_orders` | RAW_ORDERS | Timestamps renamed; `delivery_delay_days`, `is_late_delivery`, `approval_time_hours` derived |
| `stg_customers` | RAW_CUSTOMERS | City lowercased, state uppercased |
| `stg_order_items` | RAW_ORDER_ITEMS | Null-safe `price`/`freight_value`; `total_item_value` derived |
| `stg_products` | RAW_PRODUCTS + translation | English category applied; `volume_cm3` derived |
| `stg_sellers` | RAW_SELLERS | City/state normalised |
| `stg_order_reviews` | RAW_ORDER_REVIEWS | Deduplicated by `order_id`; `sentiment` bucket; `response_time_hours` |
| `stg_order_payments` | RAW_ORDER_PAYMENTS | Aggregated to order grain; `total_payment_value`; payment type flags |

### Intermediate layer (ephemeral — no physical table)

| Model | Description |
|---|---|
| `int_order_items_enriched` | Wide order-item record: items + orders + products + sellers |
| `int_customer_orders` | Customer-level aggregation: spend, satisfaction, delivery, payment behaviour |

### Marts layer (`OLIST_DB.MARTS`) — tables

| Mart | Business question answered |
|---|---|
| `mart_revenue_by_category` | Which categories drive the most revenue? (with Pareto analysis) |
| `mart_seller_performance` | How is each seller performing? (revenue + reviews + delivery tier) |
| `mart_customer_lifetime_value` | What is each customer's CLV and segment? |
| `mart_monthly_revenue_trend` | How is monthly GMV trending? (MoM growth + 3-month rolling average) |

---

## Data Quality Framework

| Layer | Mechanism | What it checks |
|---|---|---|
| Source | `sources.yml` freshness | `_LOADED_AT` within 25h (warn) / 49h (error) |
| Staging | `schema.yml` generic tests | `not_null`, `unique`, `accepted_values` on all key columns |
| Staging | `dbt_utils.expression_is_true` | `price >= 0`, `freight_value >= 0`, `total_item_value >= 0` |
| Marts | `schema.yml` generic tests | Primary key uniqueness + range checks on all mart tables |
| Marts | Singular tests | `assert_no_negative_revenue`, `assert_delivery_delay_reasonable`, `assert_all_customers_have_clv_segment` |

Run all tests: `dbt test --profiles-dir dbt/ --project-dir dbt/`

---

## Sample Query Results

After a successful pipeline run, open a Snowflake worksheet and try:

```sql
-- Top 5 revenue categories
SELECT revenue_rank, category_name_english, total_revenue, revenue_share_pct
FROM OLIST_DB.MARTS.MART_REVENUE_BY_CATEGORY
ORDER BY revenue_rank
LIMIT 5;

-- Monthly GMV trend (last 6 months of data)
SELECT month_label, monthly_gmv, mom_gmv_growth_pct, rolling_3m_avg_gmv
FROM OLIST_DB.MARTS.MART_MONTHLY_REVENUE_TREND
ORDER BY order_month DESC
LIMIT 6;

-- Seller performance breakdown by tier
SELECT performance_tier, COUNT(*) AS seller_count,
       ROUND(AVG(avg_review_score), 2) AS avg_score,
       ROUND(AVG(late_delivery_rate) * 100, 1) AS avg_late_pct
FROM OLIST_DB.MARTS.MART_SELLER_PERFORMANCE
GROUP BY performance_tier
ORDER BY performance_tier;

-- CLV segment distribution
SELECT clv_segment, COUNT(*) AS customers,
       ROUND(AVG(total_spend), 2) AS avg_spend,
       ROUND(AVG(avg_review_score), 2) AS avg_satisfaction
FROM OLIST_DB.MARTS.MART_CUSTOMER_LIFETIME_VALUE
GROUP BY clv_segment
ORDER BY avg_spend DESC;
```

---

## Tech Stack

| Tool | Role | Version |
|---|---|---|
| Apache Airflow | Pipeline orchestration and scheduling | 2.9.2 |
| dbt Core | SQL transformation, testing, documentation | 1.8.0 |
| dbt-snowflake | dbt adapter for Snowflake | 1.8.0 |
| dbt-utils | Cross-database macros + extra generic tests | ≥1.0.0 |
| dbt_expectations | Great Expectations-style dbt tests | ≥0.10.0 |
| Snowflake | Cloud data warehouse | Free trial |
| Docker / Docker Compose | Local Airflow runtime | latest |
| GitHub Actions | CI/CD on pull requests | — |
| Python | Loader script + Airflow DAGs | 3.11 |

---

## Dataset Credit

Olist Brazilian E-Commerce Dataset.
Available at: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
Licensed under CC BY-NC-SA 4.0.
