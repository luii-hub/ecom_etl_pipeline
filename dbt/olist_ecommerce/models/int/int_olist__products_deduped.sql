with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__products'), 'product_id', '_processed_at') }}
)

select 
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    _processed_at
from ranked_data
where row_num = 1