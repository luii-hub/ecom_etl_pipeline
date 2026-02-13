with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__geolocation'), 'zip_code_prefix', '_processed_at') }}
)

select 
    zip_code_prefix,
    city,
    state,
    _processed_at,
    CURRENT_TIMESTAMP as dbt_updated_at
from ranked_data
where row_num = 1