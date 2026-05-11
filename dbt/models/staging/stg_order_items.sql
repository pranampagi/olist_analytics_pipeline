-- models/staging/stg_order_items.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Order line items with null-safe price/freight and total_item_value.
--
-- Grain: one row per (order_id, order_item_id) — i.e. one row per product
-- within an order. An order with 3 distinct products has 3 rows.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist_raw', 'RAW_ORDER_ITEMS') }}
),

cleaned as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,

        -- Null-safe financial columns
        coalesce(price,         0)                  as price,
        coalesce(freight_value, 0)                  as freight_value,

        -- Derived: total value of this line item (price + shipping)
        coalesce(price, 0) + coalesce(freight_value, 0) as total_item_value

    from source
    where order_id      is not null
      and order_item_id is not null
)

select * from cleaned
