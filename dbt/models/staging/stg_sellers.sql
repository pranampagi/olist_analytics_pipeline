-- models/staging/stg_sellers.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Cleaned seller registry with normalised city and state formatting.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist_raw', 'RAW_SELLERS') }}
),

cleaned as (
    select
        seller_id,
        seller_zip_code_prefix          as zip_code_prefix,
        lower(trim(seller_city))        as city,
        upper(trim(seller_state))       as state
    from source
    where seller_id is not null
)

select * from cleaned
