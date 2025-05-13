--models/src/src_reviews.sql :
WITH raw_reviews AS (
    SELECT *
    FROM {{ source('airbnb','reviews') }} -- use jinja template instead of direct table  AIRBNB.RAW.RAW_REVIEWS
)
SELECT listing_id,
    date AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment
FROM raw_reviews