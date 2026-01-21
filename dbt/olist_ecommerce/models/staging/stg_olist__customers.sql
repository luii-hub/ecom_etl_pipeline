-- staging model for olist_customers
select 

    customer_unique_id::TEXT as customer_id,
    customer_id::TEXT as customer_order_id,
    customer_zip_code_prefix::TEXT as zip_code_prefix,
    INITCAP(customer_city)::TEXT as city,
    UPPER(customer_state)::TEXT as state,
    CURRENT_TIMESTAMP as _processed_at

from {{ source('olist', 'olist_customers') }}


