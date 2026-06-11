-- tests/assert_all_customers_have_clv_segment.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Singular test: every customer in mart_customer_lifetime_value must have
-- a non-null CLV segment. Returns failing rows. Passing = zero rows.
-- ─────────────────────────────────────────────────────────────────────────────

select
    customer_id,
    total_spend,
    clv_segment
from {{ ref('mart_customer_lifetime_value') }}
where clv_segment is null
   or clv_segment not in ('high_value', 'mid_value', 'low_value')
