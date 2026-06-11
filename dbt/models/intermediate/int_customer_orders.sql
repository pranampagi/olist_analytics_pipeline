-- models/intermediate/int_customer_orders.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Customer-level aggregation — one row per customer_id (order-scoped).
-- Grain: one row per customer_id (note: each order creates a new customer_id
-- in Olist's schema; group by customer_unique_id for true person-level CLV).
--
-- Joins:
--   orders ←→ payments  (spend per order)
--   orders ←→ reviews   (satisfaction per order)
--
-- Downstream users: mart_customer_lifetime_value
-- ─────────────────────────────────────────────────────────────────────────────

with orders as (
    select
        order_id,
        customer_id,
        order_status,
        purchased_at,
        delivered_at,
        is_late_delivery,
        delivery_delay_days
    from {{ ref('stg_orders') }}
),

customers as (
    select
        customer_id,
        customer_unique_id,
        city,
        state
    from {{ ref('stg_customers') }}
),

payments as (
    select
        order_id,
        total_payment_value,
        primary_payment_type,
        used_credit_card,
        max_installments
    from {{ ref('stg_order_payments') }}
),

reviews as (
    select
        order_id,
        review_score,
        sentiment
    from {{ ref('stg_order_reviews') }}
),

order_enriched as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.city                          as customer_city,
        c.state                         as customer_state,
        o.order_status,
        o.purchased_at,
        o.delivered_at,
        o.is_late_delivery,
        o.delivery_delay_days,
        coalesce(p.total_payment_value, 0)  as order_value,
        p.primary_payment_type,
        p.used_credit_card,
        p.max_installments,
        r.review_score,
        r.sentiment
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join payments  p on o.order_id    = p.order_id
    left join reviews   r on o.order_id    = r.order_id
),

-- Aggregate to customer level (customer_id grain — one order per customer in Olist)
customer_summary as (
    select
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state,

        -- Order counts
        count(order_id)                                             as total_orders,
        count_if(order_status = 'delivered')                        as delivered_orders,
        count_if(order_status = 'canceled')                         as canceled_orders,

        -- Spend
        sum(order_value)                                            as total_spend,
        avg(order_value)                                            as avg_order_value,
        max(order_value)                                            as max_order_value,

        -- Satisfaction
        avg(review_score)                                           as avg_review_score,
        count_if(sentiment = 'positive')                            as positive_reviews,
        count_if(sentiment = 'negative')                            as negative_reviews,

        -- Delivery performance
        avg(delivery_delay_days)                                    as avg_delivery_delay_days,
        count_if(is_late_delivery = true)                           as late_deliveries,

        -- Payment behaviour
        mode(primary_payment_type)                                  as preferred_payment_type,
        max(used_credit_card::int)                                  as ever_used_credit_card,
        max(max_installments)                                       as max_installments_used,

        -- Tenure
        min(purchased_at)                                           as first_order_at,
        max(purchased_at)                                           as last_order_at,
        datediff(
            'day',
            min(purchased_at),
            max(purchased_at)
        )                                                           as customer_tenure_days

    from order_enriched
    group by
        customer_id,
        customer_unique_id,
        customer_city,
        customer_state
)

select * from customer_summary
