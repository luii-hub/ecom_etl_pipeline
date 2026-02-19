with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__sellers'), 'seller_id', '_processed_at') }}
)
select 

    seller_id,
    seller_zip_code_prefix,
    INITCAP(seller_city) as seller_city,
    seller_state,
    _processed_at,
    CURRENT_TIMESTAMP as dbt_updated_at

from ranked_data
where row_num = 1