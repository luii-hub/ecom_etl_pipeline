-- staging models for olist_sellers
select

    seller_id::TEXT as seller_id,
    seller_zip_code_prefix::INT as seller_zip_code_prefix,
    seller_city::TEXT as seller_city,
    seller_state::TEXT as seller_state,
    CURRENT_TIMESTAMP as _processed_at

from {{ source('olist', 'olist_sellers') }}

