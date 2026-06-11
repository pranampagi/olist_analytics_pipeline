-- models/intermediate/int_order_items_enriched.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Wide denormalised order-item record.
-- Grain: one row per (order_id, order_item_id).
--
-- Joins:
--   order_items  ←→  orders    (order context + delivery timing)
--   order_items  ←→  products  (category + physical attributes)
--   order_items  ←→  sellers   (seller location)
--
-- Downstream users: mart_seller_performance, mart_revenue_by_category,
--                   mart_monthly_revenue_trend
-- ─────────────────────────────────────────────────────────────────────────────

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_status,
        purchased_at,
        approved_at,
        delivered_at,
        estimated_delivery_at,
        is_late_delivery,
        delivery_delay_days,
        order_month,
        order_week,
        order_year,
        order_month_num
    from {{ ref('stg_orders') }}
),

products as (
    select
        product_id,
        category_name_english,
        category_name_portuguese,
        product_weight_g,
        volume_cm3
    from {{ ref('stg_products') }}
),

sellers as (
    select
        seller_id,
        city    as seller_city,
        state   as seller_state
    from {{ ref('stg_sellers') }}
),

enriched as (
    select
        -- ── Keys ────────────────────────────────────────────────────────────
        oi.order_id,
        oi.order_item_id,
        o.customer_id,

        -- ── Order context ────────────────────────────────────────────────────
        o.order_status,
        o.purchased_at,
        o.delivered_at,
        o.is_late_delivery,
        o.delivery_delay_days,
        o.order_month,
        o.order_week,
        o.order_year,
        o.order_month_num,

        -- ── Product context ──────────────────────────────────────────────────
        oi.product_id,
        p.category_name_english,
        p.category_name_portuguese,
        p.product_weight_g,
        p.volume_cm3,

        -- ── Seller context ───────────────────────────────────────────────────
        oi.seller_id,
        s.seller_city,
        s.seller_state,

        -- ── Financials ───────────────────────────────────────────────────────
        oi.price,
        oi.freight_value,
        oi.total_item_value

    from order_items oi
    left join orders   o  on oi.order_id   = o.order_id
    left join products p  on oi.product_id = p.product_id
    left join sellers  s  on oi.seller_id  = s.seller_id
)

select * from enriched
