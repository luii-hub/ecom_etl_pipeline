-- staging model for olist_order_reviews
select

    review_id::TEXT as review_id,
    order_id::TEXT as order_id,
    review_score::INT as review_score,
    review_comment_title::TEXT as review_title,
    review_comment_message::TEXT as review_message,
    review_creation_date::TIMESTAMP as review_creation_date,
    review_answer_timestamp::TIMESTAMP as review_answer_timestamp,
    CURRENT_TIMESTAMP as _processed_at

from {{ source('olist', 'olist_order_reviews') }}