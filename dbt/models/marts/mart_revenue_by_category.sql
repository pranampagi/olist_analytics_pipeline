-- models/marts/mart_revenue_by_category.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Question: Which product categories generate the most revenue,
-- and what share of total GMV does each category represent?
--
-- Grain: one row per product category.
-- Materialized as TABLE for fast dashboard querying.
--
-- Key columns:
--   total_revenue        — sum of price (BRL) for delivered orders
--   total_freight        — sum of freight_value for delivered orders
--   total_gmv            — revenue + freight (full spend)
--   order_count          — number of delivered orders in this category
--   revenue_rank         — 1 = highest revenue category
--   revenue_share_pct    — this category's % share of total revenue
--   cumulative_revenue_pct — Pareto: top N categories cover X% of revenue
-- ─────────────────────────────────────────────────────────────────────────────

with enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

delivered_only as (
    select *
    from enriched
    where order_status = 'delivered'
),

category_metrics as (
    select
        category_name_english,

        -- Volume
        count(distinct order_id)                as order_count,
        count(*)                                as item_count,
        count(distinct seller_id)               as seller_count,

        -- Revenue breakdown
        round(sum(price),         2)            as total_revenue,
        round(sum(freight_value), 2)            as total_freight,
        round(sum(total_item_value), 2)         as total_gmv,

        -- Per-order averages
        round(avg(price),         2)            as avg_item_price,
        round(avg(freight_value), 2)            as avg_freight_value,
        round(avg(total_item_value), 2)         as avg_order_value,

        -- Delivery performance for this category
        round(avg(delivery_delay_days), 1)      as avg_delivery_delay_days,
        round(
            countif(is_late_delivery = true)
            / nullif(count(*), 0)::float,
        4)                                      as late_delivery_rate

    from delivered_only
    group by category_name_english
),

-- Grand total for share calculation
totals as (
    select sum(total_revenue) as grand_total_revenue
    from category_metrics
),

ranked as (
    select
        cm.*,
        t.grand_total_revenue,

        -- Rank by revenue (1 = top)
        rank() over (order by cm.total_revenue desc)    as revenue_rank,

        -- Revenue share %
        round(
            cm.total_revenue / nullif(t.grand_total_revenue, 0) * 100,
        2)                                              as revenue_share_pct,

        -- Cumulative Pareto revenue %
        round(
            sum(cm.total_revenue) over (
                order by cm.total_revenue desc
                rows between unbounded preceding and current row
            ) / nullif(t.grand_total_revenue, 0) * 100,
        2)                                              as cumulative_revenue_pct

    from category_metrics cm
    cross join totals t
)

select
    revenue_rank,
    category_name_english,
    order_count,
    item_count,
    seller_count,
    total_revenue,
    total_freight,
    total_gmv,
    avg_item_price,
    avg_freight_value,
    avg_order_value,
    avg_delivery_delay_days,
    late_delivery_rate,
    revenue_share_pct,
    cumulative_revenue_pct,
    grand_total_revenue
from ranked
order by revenue_rank
