-- models/marts/mart_customer_lifetime_value.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Question: What is each customer's lifetime value, and how
-- should we segment them for retention and re-engagement campaigns?
--
-- Grain: one row per customer_id (Olist order-scoped ID).
--
-- CLV Segment thresholds (based on Olist dataset distribution):
--   high_value  — total_spend in top 20%   (>= 80th percentile)
--   mid_value   — total_spend in 20–60%
--   low_value   — total_spend bottom 40%
--
-- Note on Olist's customer model:
--   Each order generates a new CUSTOMER_ID. CUSTOMER_UNIQUE_ID is the true
--   person-level identifier. Since most customers only ever placed 1 order,
--   the customer_unique_id logic is included as a passthrough column for
--   any deeper repeat-purchase analysis.
-- ─────────────────────────────────────────────────────────────────────────────

with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

-- Calculate dataset-wide percentile thresholds for CLV segmentation
percentiles as (
    select
        percentile_cont(0.40) within group (order by total_spend) as p40_spend,
        percentile_cont(0.80) within group (order by total_spend) as p80_spend
    from customer_orders
),

clv_calc as (
    select
        co.*,
        p.p40_spend,
        p.p80_spend,

        -- CLV segment label
        case
            when co.total_spend >= p.p80_spend then 'high_value'
            when co.total_spend >= p.p40_spend then 'mid_value'
            else 'low_value'
        end                                                             as clv_segment,

        -- Repeat purchase flag (Olist: most customers order once, so 1 = rare loyalty)
        case when co.total_orders > 1 then true else false end          as is_repeat_customer,

        -- Days since last order (churn risk proxy)
        -- dataset_end_date is the last known date in the Olist dataset
        datediff(
            'day',
            co.last_order_at,
            cast('{{ var("dataset_end_date") }}' as timestamp_ntz)
        )                                                               as days_since_last_order,

        -- Cancellation rate
        round(
            co.canceled_orders / nullif(co.total_orders, 0)::float,
        4)                                                              as cancellation_rate,

        -- Positive review rate
        round(
            co.positive_reviews
            / nullif(co.positive_reviews + co.negative_reviews + 1, 0)::float,
        4)                                                              as positive_review_rate

    from customer_orders co
    cross join percentiles p
)

select
    -- Identity
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,

    -- CLV classification
    clv_segment,
    is_repeat_customer,

    -- Order history
    total_orders,
    delivered_orders,
    canceled_orders,
    cancellation_rate,

    -- Spend
    round(total_spend,      2)  as total_spend,
    round(avg_order_value,  2)  as avg_order_value,
    round(max_order_value,  2)  as max_order_value,

    -- Satisfaction
    round(avg_review_score, 2)  as avg_review_score,
    positive_reviews,
    negative_reviews,
    positive_review_rate,

    -- Delivery experience
    round(avg_delivery_delay_days, 1) as avg_delivery_delay_days,
    late_deliveries,

    -- Payment behaviour
    preferred_payment_type,
    ever_used_credit_card,
    max_installments_used,

    -- Tenure
    first_order_at,
    last_order_at,
    customer_tenure_days,
    days_since_last_order,

    -- Segment thresholds (useful for documentation / auditing)
    round(p40_spend, 2)         as p40_spend_threshold,
    round(p80_spend, 2)         as p80_spend_threshold

from clv_calc
order by total_spend desc
