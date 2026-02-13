-- deduplicate by id, making sure we hav unique instance per row.
{% macro dedupe_by_column(model_ref, partition_col, order_col) %}
with source as (
    select * from {{ model_ref }}
),
ranked as (
    select 
        *,
        ROW_NUMBER() OVER(PARTITION BY {{ partition_col }} ORDER BY {{ order_col }} DESC) as row_num
    from source
)

select
    *
from ranked

{% endmacro %}