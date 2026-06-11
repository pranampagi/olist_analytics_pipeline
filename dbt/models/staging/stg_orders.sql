-- models/staging/stg_orders.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Cleaned and typed orders with derived delivery metrics.
--
-- Key transformations:
--   • order_status lowercased for consistency
--   • All timestamps renamed for clarity
--   • delivery_delay_days: positive = late, negative = early
--   • is_late_delivery: boolean
--   • approval_time_hours: purchase → approved
--   • Convenience date truncations: order_month, order_week, order_year
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist_raw', 'RAW_ORDERS') }}
),

cleaned as (
    select
        -- ── Primary keys ────────────────────────────────────────────────────
        order_id,
        customer_id,

        -- ── Status ──────────────────────────────────────────────────────────
        lower(trim(order_status))                               as order_status,

        -- ── Timestamps (renamed for clarity) ────────────────────────────────
        order_purchase_timestamp                                as purchased_at,
        order_approved_at                                       as approved_at,
        order_delivered_carrier_date                            as shipped_at,
        order_delivered_customer_date                           as delivered_at,
        order_estimated_delivery_date                           as estimated_delivery_at,

        -- ── Delivery delay (positive = late) ────────────────────────────────
        case
            when order_delivered_customer_date  is not null
             and order_estimated_delivery_date  is not null
            then datediff(
                'day',
                order_estimated_delivery_date,
                order_delivered_customer_date
            )
        end                                                     as delivery_delay_days,

        -- ── Late delivery flag ───────────────────────────────────────────────
        case
            when order_delivered_customer_date  is not null
             and order_estimated_delivery_date  is not null
            then (order_delivered_customer_date > order_estimated_delivery_date)
        end                                                     as is_late_delivery,

        -- ── Approval turnaround ──────────────────────────────────────────────
        case
            when order_approved_at is not null
            then datediff('hour', order_purchase_timestamp, order_approved_at)
        end                                                     as approval_time_hours,

        -- ── Date grain helpers ───────────────────────────────────────────────
        date_trunc('month', order_purchase_timestamp)           as order_month,
        date_trunc('week',  order_purchase_timestamp)           as order_week,
        year(order_purchase_timestamp)                          as order_year,
        month(order_purchase_timestamp)                         as order_month_num

    from source
    where order_id is not null
)

select * from cleaned
