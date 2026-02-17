with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__order_items'), 'order_item_key', '_processed_at') }}
)

select 

    order_item_key as order_item_id,
    order_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    _processed_at,
    CURRENT_TIMESTAMP as dbt_updated_at

from ranked_data
where row_num = 1