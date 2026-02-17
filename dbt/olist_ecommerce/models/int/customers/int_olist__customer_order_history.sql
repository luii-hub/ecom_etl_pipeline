with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__customers'), 'customer_order_id', '_processed_at') }}
)

select 

    customer_id,
    customer_order_id,
    zip_code_prefix,
    city,
    state,
    _processed_at,
    CURRENT_TIMESTAMP as dbt_updated_at

from ranked_data
where row_num = 1