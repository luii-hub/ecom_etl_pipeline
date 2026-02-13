with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__order_reviews'), 'review_id', '_processed_at') }}
)

select 
    review_id,
    order_id,
    review_score,
    review_title,
    review_message,
    review_creation_date,
    review_answer_timestamp,
    _processed_at
from ranked_data
where row_num = 1