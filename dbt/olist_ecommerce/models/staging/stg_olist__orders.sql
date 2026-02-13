-- staging model for olist_orders
with source as (
    select * from {{ source('olist', 'olist_orders') }}
),

renamed as (
    select
        order_id::TEXT as order_id,
        customer_id::TEXT as customer_order_id,
        order_status::TEXT as order_status,
        order_purchase_timestamp::TIMESTAMP as purchase_timestamp,
        order_approved_at::TIMESTAMP as approved_at,
        order_delivered_carrier_date::TIMESTAMP as delivered_carrier_date,
        order_delivered_customer_date::TIMESTAMP as delivered_customer_date,
        order_estimated_delivery_date::TIMESTAMP as estimated_delivery_date,
        CURRENT_TIMESTAMP as _processed_at
    from source
    where order_id is not null
)

select * from renamed