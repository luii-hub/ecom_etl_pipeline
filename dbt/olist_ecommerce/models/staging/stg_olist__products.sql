-- staging model for olist_products
with source as (
    select * from {{ source('olist', 'olist_products') }}
),

renamed as (
    select
        product_id::TEXT as product_id,
        product_category_name::TEXT as product_category_name,
        product_name_length::INT as product_name_length,
        product_description_length::INT as product_description_length,
        product_photos_qty::INT as product_photos_qty,
        product_weight_g::FLOAT as product_weight_g,
        product_length_cm::FLOAT as product_length_cm,
        product_height_cm::FLOAT as product_height_cm,
        product_width_cm::FLOAT as product_width_cm,
        CURRENT_TIMESTAMP as _processed_at
    from source
    where product_id is not null
)

select * from renamed