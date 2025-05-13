-- Materializations
-- src
-- models/src/src_listings.sql
--query will be destination for this view
WITH raw_listings AS (
    SELECT *
    FROM {{ source('airbnb','listings') }} -- use jinja template instead of direct table AIRBNB.RAW.RAW_LISTINGS
)
SELECT id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
FROM raw_listings
