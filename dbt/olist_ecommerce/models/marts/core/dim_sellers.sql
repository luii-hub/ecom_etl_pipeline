with sellers as (
    select * from {{ ref('int_olist__sellers') }}
)

select 

    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
    
from sellers