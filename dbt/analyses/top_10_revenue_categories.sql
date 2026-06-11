-- analyses/top_10_revenue_categories.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Ad-hoc analysis: Top 10 revenue-generating product categories with
-- Pareto (80/20) annotation.
-- Run: dbt compile --select top_10_revenue_categories → check target/compiled
-- ─────────────────────────────────────────────────────────────────────────────

select
    revenue_rank,
    category_name_english,
    order_count,
    total_revenue,
    revenue_share_pct,
    cumulative_revenue_pct,

    -- Flag whether this category is in the Pareto 80% of revenue
    case
        when cumulative_revenue_pct <= 80 then '✅ Pareto top 80%'
        else '— Long tail'
    end as pareto_flag

from {{ ref('mart_revenue_by_category') }}
where revenue_rank <= 10
order by revenue_rank
