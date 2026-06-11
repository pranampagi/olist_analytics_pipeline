-- models/staging/stg_order_reviews.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Cleaned customer reviews with sentiment bucket and response time.
--
-- Deduplication strategy: some orders have multiple review rows in RAW.
-- We keep the latest review per order (by review_creation_date).
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist_raw', 'RAW_ORDER_REVIEWS') }}
),

-- Deduplicate: keep only the latest review per order
deduped as (
    select *,
        row_number() over (
            partition by order_id
            order by review_creation_date desc nulls last
        ) as rn
    from source
    where review_id    is not null
      and order_id     is not null
      and review_score between 1 and 5
),

cleaned as (
    select
        review_id,
        order_id,
        review_score,

        -- Sentiment bucket derived from score
        case
            when review_score >= 4 then 'positive'
            when review_score  = 3 then 'neutral'
            else 'negative'
        end                                             as sentiment,

        -- Strip empty strings to null for cleaner downstream analytics
        nullif(trim(review_comment_title),   '')        as review_title,
        nullif(trim(review_comment_message), '')        as review_message,

        review_creation_date                            as review_created_at,
        review_answer_timestamp                         as review_answered_at,

        -- Seller response time in hours
        case
            when review_answer_timestamp is not null
             and review_creation_date    is not null
            then datediff('hour', review_creation_date, review_answer_timestamp)
        end                                             as response_time_hours

    from deduped
    where rn = 1
)

select * from cleaned
