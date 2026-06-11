-- models/staging/stg_order_payments.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Payment data aggregated from row-per-payment to ONE ROW PER ORDER.
--
-- Raw: one order paid by credit_card (R$80) + voucher (R$20) → 2 rows.
-- Staged: one row with total_payment_value = 100, primary_payment_type = credit_card.
--
-- This aggregation makes joining to orders much easier for downstream models.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist_raw', 'RAW_ORDER_PAYMENTS') }}
),

order_level as (
    select
        order_id,

        -- Total amount paid for the order across all payment methods
        sum(payment_value)                                          as total_payment_value,

        -- Count of distinct payment instalments
        count(distinct payment_sequential)                          as payment_rows_count,

        -- Maximum installment count across all payment methods (credit card focused)
        max(payment_installments)                                   as max_installments,

        -- Most common payment type for this order (MODE is Snowflake-native)
        mode(lower(payment_type))                                   as primary_payment_type,

        -- Boolean flags for common payment types
        max(case when lower(payment_type) = 'credit_card' then 1 else 0 end) = 1
                                                                    as used_credit_card,
        max(case when lower(payment_type) = 'boleto'      then 1 else 0 end) = 1
                                                                    as used_boleto,
        max(case when lower(payment_type) = 'voucher'     then 1 else 0 end) = 1
                                                                    as used_voucher,
        max(case when lower(payment_type) = 'debit_card'  then 1 else 0 end) = 1
                                                                    as used_debit_card

    from source
    where order_id is not null
    group by order_id
)

select * from order_level
