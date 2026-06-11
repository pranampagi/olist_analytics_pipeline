-- analyses/delivery_delay_by_seller_state.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Ad-hoc analysis: Which Brazilian states have the worst delivery delays,
-- and how does it correlate with customer review scores?
-- ─────────────────────────────────────────────────────────────────────────────

with seller_stats as (
    select
        seller_state,
        count(distinct seller_id)           as seller_count,
        sum(total_orders)                   as total_orders,
        round(avg(avg_delivery_delay_days), 2) as avg_delay_days,
        round(avg(late_delivery_rate), 4)   as avg_late_rate,
        round(avg(avg_review_score), 2)     as avg_review_score,
        sum(total_revenue)                  as total_revenue,
        count_if(performance_tier = 'top')   as top_sellers,
        count_if(performance_tier = 'poor')  as poor_sellers
    from {{ ref('mart_seller_performance') }}
    group by seller_state
),

ranked as (
    select
        *,
        rank() over (order by avg_delay_days desc)  as delay_rank,
        rank() over (order by avg_review_score asc) as satisfaction_rank
    from seller_stats
)

select
    delay_rank,
    seller_state,
    seller_count,
    total_orders,
    avg_delay_days,
    avg_late_rate,
    avg_review_score,
    total_revenue,
    top_sellers,
    poor_sellers,
    satisfaction_rank
from ranked
order by delay_rank
