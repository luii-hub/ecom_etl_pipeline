with order_reviews as (
    select * from {{ ref('int_olist__order_reviews_deduped') }}
),
order_map as (
    select 
        customer_id,
        order_id
    from {{ ref('fct_orders') }}
)

select 
    
    r.order_review_item_pk,
    r.review_id,
    m.customer_id,
    r.order_id,
    r.review_score,
    r.review_title,
    r.review_message,
    r.review_creation_date,
    r.review_answer_timestamp

from order_reviews r
left join order_map m on r.order_id = m.order_id