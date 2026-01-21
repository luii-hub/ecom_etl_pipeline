-- staging model for olist_geolocation
select

    geolocation_zip_code_prefix::TEXT as zip_code_prefix,
    geolocation_lat::TEXT as latitude,
    geolocation_lng::TEXT as longitude,
    INITCAP(geolocation_city)::TEXT as city,
    UPPER(geolocation_state)::TEXT as state,
    CURRENT_TIMESTAMP as _processed_at

FROM {{ source('olist', 'olist_geolocation') }}