-- staging model for order_payments
with source as (
    select * from {{ source('olist', 'olist_order_payments') }}
),

renamed as (
    select
        order_id::TEXT as order_id,
        payment_sequential::INT as payment_sequential,
        payment_type::TEXT as payment_type,
        payment_installments::INT as payment_installments,
        payment_value::FLOAT as payment_value,
        CURRENT_TIMESTAMP as _processed_at
    from source
    where order_id is not null
)

select * from renamed