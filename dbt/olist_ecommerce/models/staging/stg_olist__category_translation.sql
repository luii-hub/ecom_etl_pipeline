-- staging model for olist_category_name-translation
select

    category_name::TEXT as category_name_portuguese,
    category_name_english::TEXT as category_name_english,
    CURRENT_TIMESTAMP as _processed_at

from {{ source('olist', 'olist_category_name_translation') }}