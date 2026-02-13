with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__orders'), 'order_id', '_processed_at') }}
)

select 
    order_id,
    customer_order_id,
    order_status,
    purchase_timestamp,
    approved_at,
    delivered_customer_date,
    estimated_delivery_date,
    _processed_at
from ranked_data
where row_num = 1