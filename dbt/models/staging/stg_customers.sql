-- models/staging/stg_customers.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Cleaned customer records.
--
-- Key note: CUSTOMER_ID is unique per *order*, not per person.
-- CUSTOMER_UNIQUE_ID identifies the actual human across multiple orders.
-- For person-level aggregations always group by customer_unique_id.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist_raw', 'RAW_CUSTOMERS') }}
),

cleaned as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix                    as zip_code_prefix,
        lower(trim(customer_city))                  as city,
        upper(trim(customer_state))                 as state
    from source
    where customer_id is not null
)

select * from cleaned
