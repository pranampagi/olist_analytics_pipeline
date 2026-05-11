-- models/marts/mart_seller_performance.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Question: How is each seller performing across revenue, delivery,
-- and customer satisfaction — and which sellers are at risk?
--
-- Grain: one row per seller_id.
-- Materialized as TABLE for fast filtering and ranking.
--
-- Performance tier logic:
--   top     — avg_review_score ≥ 4.0 AND late_delivery_rate < 0.10
--   good    — avg_review_score ≥ 3.5 AND late_delivery_rate < 0.20
--   average — avg_review_score ≥ 3.0
--   poor    — avg_review_score < 3.0 OR late_delivery_rate ≥ 0.30
-- ─────────────────────────────────────────────────────────────────────────────

with enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

reviews as (
    select order_id, review_score, sentiment
    from {{ ref('stg_order_reviews') }}
),

-- Join enriched items with reviews at order level
items_with_reviews as (
    select
        e.*,
        r.review_score,
        r.sentiment
    from enriched e
    left join reviews r on e.order_id = r.order_id
    where e.order_status = 'delivered'
),

seller_metrics as (
    select
        seller_id,
        seller_city,
        seller_state,

        -- Volume
        count(distinct order_id)                                as total_orders,
        count(*)                                                as total_items_sold,
        count(distinct category_name_english)                   as distinct_categories,

        -- Revenue
        round(sum(price),             2)                        as total_revenue,
        round(avg(price),             2)                        as avg_item_price,
        round(sum(total_item_value),  2)                        as total_gmv,
        round(avg(total_item_value),  2)                        as avg_order_value,

        -- Customer satisfaction
        round(avg(review_score), 2)                             as avg_review_score,
        countif(sentiment = 'positive')                        as positive_reviews,
        countif(sentiment = 'negative')                        as negative_reviews,
        countif(sentiment = 'neutral')                         as neutral_reviews,

        -- Delivery performance
        round(avg(delivery_delay_days), 1)                      as avg_delivery_delay_days,
        round(
            countif(is_late_delivery = true)
            / nullif(count(*), 0)::float,
        4)                                                      as late_delivery_rate,

        -- First and last sale dates
        min(purchased_at)                                       as first_sale_at,
        max(purchased_at)                                       as last_sale_at,
        datediff('day', min(purchased_at), max(purchased_at))   as active_days

    from items_with_reviews
    group by seller_id, seller_city, seller_state
),

-- Assign performance tier
tiered as (
    select
        *,
        case
            when avg_review_score >= 4.0 and late_delivery_rate < 0.10 then 'top'
            when avg_review_score >= 3.5 and late_delivery_rate < 0.20 then 'good'
            when avg_review_score >= 3.0                                then 'average'
            else 'poor'
        end                                                     as performance_tier,

        -- Revenue rank globally
        rank() over (order by total_revenue desc)               as revenue_rank,

        -- Revenue rank within state
        rank() over (
            partition by seller_state
            order by total_revenue desc
        )                                                       as revenue_rank_in_state

    from seller_metrics
)

select
    seller_id,
    seller_city,
    seller_state,
    performance_tier,
    revenue_rank,
    revenue_rank_in_state,
    total_orders,
    total_items_sold,
    distinct_categories,
    total_revenue,
    avg_item_price,
    total_gmv,
    avg_order_value,
    avg_review_score,
    positive_reviews,
    negative_reviews,
    neutral_reviews,
    avg_delivery_delay_days,
    late_delivery_rate,
    first_sale_at,
    last_sale_at,
    active_days
from tiered
order by revenue_rank
