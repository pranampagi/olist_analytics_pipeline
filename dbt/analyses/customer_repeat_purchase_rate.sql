-- analyses/customer_repeat_purchase_rate.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Ad-hoc analysis: What fraction of Olist's customers are repeat buyers?
-- Because Olist issues a new CUSTOMER_ID per order, we must group by
-- CUSTOMER_UNIQUE_ID to measure true repeat purchase behaviour.
-- ─────────────────────────────────────────────────────────────────────────────

with customer_base as (
    select
        customer_unique_id,
        count(customer_id)              as order_count,
        sum(total_spend)                as lifetime_spend,
        min(first_order_at)             as first_order_at,
        max(last_order_at)              as last_order_at
    from {{ ref('mart_customer_lifetime_value') }}
    group by customer_unique_id
),

summary as (
    select
        count(*)                                        as total_unique_customers,
        countif(order_count > 1)                       as repeat_customers,
        countif(order_count = 1)                       as one_time_customers,

        round(
            countif(order_count > 1)
            / nullif(count(*), 0)::float * 100,
        2)                                              as repeat_purchase_rate_pct,

        round(avg(lifetime_spend), 2)                   as avg_lifetime_spend,
        round(avg(
            case when order_count > 1 then lifetime_spend end
        ), 2)                                           as avg_ltv_repeat_customers,
        round(avg(
            case when order_count = 1 then lifetime_spend end
        ), 2)                                           as avg_ltv_one_time_customers
    from customer_base
)

select * from summary
