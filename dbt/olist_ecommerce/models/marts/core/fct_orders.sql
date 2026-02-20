with orders as (
    select * from {{ ref('int_olist__orders_deduped') }}
),
order_items as (
    select * from {{ ref('int_olist__order_items') }}
),
customers as (
    select * from {{ ref('int_olist__customer_order_history') }}
),
order_item_count as (
    select 
        order_id,
        count(*) as items_count
    from order_items
    group by 1
)
select 
    o.order_id,
    c.customer_id,
    o.order_status,
    o.purchase_timestamp,
    o.approved_at,
    oic.items_count,
    o.delivered_customer_date,
    o.estimated_delivery_date
from orders o
inner join customers c 
    on o.customer_order_id = c.customer_order_id
left join order_item_count oic 
    on o.order_id = oic.order_id