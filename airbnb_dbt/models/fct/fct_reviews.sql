--define incremental
{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}
WITH src_reviews AS (
    SELECT *
    FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null
--Check data is incremental or not
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}

-- Check incremental model in snowflake for testing
--SELECT * FROM "AIRBNB"."DEV"."FCT_REVIEWS" WHERE listing_id=3176;

--INSERT INTO "AIRBNB"."RAW"."RAW_REVIEWS"
--VALUES (3176, CURRENT_TIMESTAMP(), 'Zoltan', 'excellent stay!', 'positive');

--SELECT * FROM "AIRBNB"."RAW"."RAW_REVIEWS" WHERE listing_id=3176;

--run dbt
--check again SELECT * FROM "AIRBNB"."DEV"."FCT_REVIEWS" WHERE listing_id=3176;