-- staging model for order_payments
select 
    order_id::TEXT as order_id,
    payment_sequential::INT as payment_sequential,
    payment_type::TEXT as payment_type,
    payment_installments::INT as payment_installments,
    payment_value::FLOAT as payment_value,
    CURRENT_TIMESTAMP as _processed_at

from {{ source('olist', 'olist_order_payments') }}