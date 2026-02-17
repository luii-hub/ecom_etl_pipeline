with ranked_data as (
    {{ dedupe_by_column(ref('stg_olist__order_payments'), 'order_payment_item_pk', '_processed_at') }}
)

select 

    order_payment_item_pk,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    _processed_at,
    CURRENT_TIMESTAMP as dbt_updated_at

from ranked_data
where row_num = 1