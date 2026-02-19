with order_items as (
    select * from {{ ref('int_olist__order_items') }}
)

select  
    order_item_id,
    order_id,
    product_id,
    seller_id,
    price,
    freight_value

from order_items