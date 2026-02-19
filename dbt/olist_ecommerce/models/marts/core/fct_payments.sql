with payments as (
    select * from {{ ref('int_olist__order_payments_deduped') }}
),
order_context as (
    select 
        order_id, 
        customer_id 
    from {{ ref('fct_orders') }}
)
select
    p.payment_id,
    p.order_id,
    c.customer_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value
from payments p
left join order_context c on p.order_id = c.order_id