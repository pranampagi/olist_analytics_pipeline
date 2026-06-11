-- tests/assert_delivery_delay_reasonable.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Singular test: no delivered order should have a delivery delay
-- of more than 365 days or less than -365 days (data quality guard).
-- Returns failing rows. A passing test returns zero rows.
-- ─────────────────────────────────────────────────────────────────────────────

select
    order_id,
    delivery_delay_days
from {{ ref('stg_orders') }}
where delivery_delay_days is not null
  and (
      delivery_delay_days >  365
   or delivery_delay_days < -365
  )
