{{ config(materialized = 'ephemeral') }}

with source_products as (
    select * from {{ ref('int_olist__products_deduped') }}
),
source_category_translation as (
    select * from {{ ref('stg_olist__category_translation') }}
),

renamed as (
    select
        product_id,
        COALESCE(C.category_name_english, sp.product_category_name, 'Unknown') as product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        sp._processed_at,
        CURRENT_TIMESTAMP as dbt_updated_at
    from source_products sp 
    left JOIN source_category_translation C
        ON sp.product_category_name = C.category_name_portuguese
)

select * from renamed