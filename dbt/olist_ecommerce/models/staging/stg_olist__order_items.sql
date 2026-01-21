-- staging model for olist_orders
select

    order_item_id::TEXT as order_item_id,
    order_id::TEXT as order_id,
    product_id::TEXT as product_id,
    seller_id::TEXT as seller_id,
    shipping_limit_date::TIMESTAMP as shipping_limit_date,
    price::FLOAT as price,
    freight_value::FLOAT as freight_value,
    CURRENT_TIMESTAMP as _processed_at

from {{ source('olist', 'olist_order_items') }}