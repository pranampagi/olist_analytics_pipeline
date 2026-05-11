-- models/marts/mart_monthly_revenue_trend.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Question: How is Olist's monthly GMV trending, and what is the
-- month-over-month growth rate?
--
-- Grain: one row per calendar month (for delivered orders only).
-- Materialized as TABLE — ordered ascending for time-series charting.
--
-- Key columns:
--   monthly_gmv           — total revenue + freight for delivered orders
--   order_count           — number of distinct delivered orders
--   mom_gmv_change        — absolute change in GMV vs prior month
--   mom_gmv_growth_pct    — % change vs prior month
--   rolling_3m_avg_gmv    — 3-month rolling average (smoothed trend)
--   cumulative_gmv        — running total GMV since first month
-- ─────────────────────────────────────────────────────────────────────────────

with enriched as (
    select * from {{ ref('int_order_items_enriched') }}
),

monthly as (
    select
        order_month,
        order_year,
        order_month_num,

        -- Volume
        count(distinct order_id)                as order_count,
        count(distinct customer_id)             as unique_customers,
        count(distinct seller_id)               as active_sellers,
        count(*)                                as total_items_sold,

        -- Revenue
        round(sum(price),           2)          as monthly_revenue,
        round(sum(freight_value),   2)          as monthly_freight,
        round(sum(total_item_value), 2)         as monthly_gmv,

        -- Unit economics
        round(avg(total_item_value), 2)         as avg_order_value,
        round(avg(price),            2)         as avg_item_price,

        -- Delivery quality
        countif(is_late_delivery = true)        as late_deliveries,
        round(
            countif(is_late_delivery = true)
            / nullif(count(*), 0)::float,
        4)                                      as late_delivery_rate

    from enriched
    where order_status = 'delivered'
      and order_month is not null
    group by order_month, order_year, order_month_num
),

with_window_functions as (
    select
        *,

        -- Prior month GMV (LAG)
        lag(monthly_gmv) over (order by order_month) as prev_month_gmv,

        -- MoM change (absolute)
        monthly_gmv - lag(monthly_gmv) over (order by order_month)
                                                    as mom_gmv_change,

        -- MoM growth %
        round(
            (monthly_gmv - lag(monthly_gmv) over (order by order_month))
            / nullif(lag(monthly_gmv) over (order by order_month), 0) * 100,
        2)                                          as mom_gmv_growth_pct,

        -- 3-month rolling average GMV
        round(
            avg(monthly_gmv) over (
                order by order_month
                rows between 2 preceding and current row
            ),
        2)                                          as rolling_3m_avg_gmv,

        -- Cumulative GMV (running total)
        round(
            sum(monthly_gmv) over (
                order by order_month
                rows between unbounded preceding and current row
            ),
        2)                                          as cumulative_gmv

    from monthly
)

select
    order_month,
    order_year,
    order_month_num,
    order_count,
    unique_customers,
    active_sellers,
    total_items_sold,
    monthly_revenue,
    monthly_freight,
    monthly_gmv,
    avg_order_value,
    avg_item_price,
    late_deliveries,
    late_delivery_rate,
    prev_month_gmv,
    mom_gmv_change,
    mom_gmv_growth_pct,
    rolling_3m_avg_gmv,
    cumulative_gmv
from with_window_functions
order by order_month
