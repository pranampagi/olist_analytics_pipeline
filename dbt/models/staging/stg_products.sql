-- models/staging/stg_products.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Product catalogue enriched with English category translation.
-- NULL category names are replaced with 'unknown'.
-- Adds volume_cm3 as a proxy for product size / shipping complexity.
-- ─────────────────────────────────────────────────────────────────────────────

with products as (
    select * from {{ source('olist_raw', 'RAW_PRODUCTS') }}
),

category_translation as (
    select * from {{ source('olist_raw', 'RAW_PRODUCT_CATEGORY_TRANSLATION') }}
),

joined as (
    select
        p.product_id,

        -- Prefer English name; fall back to Portuguese; fall back to 'unknown'
        coalesce(
            t.product_category_name_english,
            p.product_category_name,
            'unknown'
        )                                               as category_name_english,

        p.product_category_name                         as category_name_portuguese,

        -- Physical attributes
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,

        -- Derived: volumetric proxy (cm³)
        coalesce(p.product_length_cm, 0)
            * coalesce(p.product_height_cm, 0)
            * coalesce(p.product_width_cm,  0)         as volume_cm3

    from products p
    left join category_translation t
        on lower(trim(p.product_category_name))
         = lower(trim(t.product_category_name))

    where p.product_id is not null
)

select * from joined
