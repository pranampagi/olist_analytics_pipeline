-- tests/assert_no_negative_revenue.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Singular test: verifies that no row in mart_revenue_by_category has
-- negative total_revenue. Returns rows that FAIL the assertion.
-- A passing test returns zero rows.
-- ─────────────────────────────────────────────────────────────────────────────

select
    category_name_english,
    total_revenue
from {{ ref('mart_revenue_by_category') }}
where total_revenue < 0
