with customers as (
    select * from {{ ref('int_olist__customer_order_history') }}
),

orders_items_joined as (
    -- Combine orders and items once to get all the data we need
    select 
        o.customer_id,
        o.order_id,
        o.order_status,
        oi.price
    from {{ ref('fct_orders') }} o
    left join {{ ref('fct_order_items') }} oi on o.order_id = oi.order_id
),

customer_metrics as (
    -- Calculate all metrics in ONE group by
    select 
        customer_id,
        count(distinct order_id) as total_orders,
        -- Use Conditional Aggregation instead of separate CTEs
        count(distinct case when order_status = 'delivered' then order_id end) as successful_orders,
        sum(price) as lifetime_spent
    from orders_items_joined
    group by 1
)

select 
    c.customer_id,
    c.zip_code_prefix,
    c.city,
    c.state,
    coalesce(m.total_orders, 0) as total_orders,
    coalesce(m.successful_orders, 0) as successful_orders,
    coalesce(m.lifetime_spent, 0) as lifetime_spent
from customers c
left join customer_metrics m on c.customer_id = m.customer_id