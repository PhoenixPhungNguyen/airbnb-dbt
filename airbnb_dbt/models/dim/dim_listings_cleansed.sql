WITH src_listings AS (
    SELECT *
    FROM {{ ref('src_listings') }}
)
SELECT
    listing_id,
    listing_name,
    room_type,
    CASE 
        WHEN minimum_nights = 0 THEN 1 --night = 0 change into 1 because no 0 night
        ELSE minimum_nights
    END AS minimum_nights,
    host_id,
    REPLACE(price_str,'$') :: NUMBER(10,2) AS price, -- convert price into NUMBER and delete $, and change colum name into price
    created_at,
    updated_at
FROM
    src_listings