-- staging model for olist_orders
with source as (
    select * from {{ source('olist', 'olist_order_items') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id', 'product_id']) }} as order_item_key,
        order_id::TEXT as order_id,
        order_item_id::TEXT as order_item_id,
        product_id::TEXT as product_id,
        seller_id::TEXT as seller_id,
        shipping_limit_date::TIMESTAMP as shipping_limit_date,
        price::FLOAT as price,
        freight_value::FLOAT as freight_value,
        CURRENT_TIMESTAMP as _processed_at
    from source
    where order_id is not null
)

select * from renamed