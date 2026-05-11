-- analyses/monthly_gmv_trend.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Ad-hoc analysis: Monthly GMV trend with MoM growth and 3-month
-- rolling average. Designed to be pasted into a BI tool or Snowsight
-- worksheet for a time-series chart.
-- ─────────────────────────────────────────────────────────────────────────────

select
    to_char(order_month, 'YYYY-MM')     as month_label,
    order_count,
    unique_customers,
    active_sellers,
    monthly_gmv,
    mom_gmv_growth_pct,
    rolling_3m_avg_gmv,
    cumulative_gmv,
    late_delivery_rate,

    -- Visual growth indicator
    case
        when mom_gmv_growth_pct > 20  then '🚀 +' || mom_gmv_growth_pct || '%'
        when mom_gmv_growth_pct > 0   then '📈 +' || mom_gmv_growth_pct || '%'
        when mom_gmv_growth_pct = 0   then '➡️  0%'
        when mom_gmv_growth_pct < 0   then '📉 '  || mom_gmv_growth_pct || '%'
        else '—'
    end                                 as growth_indicator

from {{ ref('mart_monthly_revenue_trend') }}
order by order_month
