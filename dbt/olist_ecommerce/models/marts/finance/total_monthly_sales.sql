with orders as (
    select * from {{ ref('fct_orders') }}
),
order_payments_summary as (
    
    select
        order_id,
        sum(payment_value) as total_payment_value
    from {{ ref('fct_payments') }}
    group by 1

)

select 
    COALESCE(date_trunc('month', approved_at), date_trunc('month', purchase_timestamp)) as month,
    sum(total_payment_value) as total_monthly_sales
from orders o
inner join order_payments_summary ops on o.order_id = ops.order_id
group by 1
order by 1

