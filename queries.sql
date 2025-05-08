-- Materializations
-- src
-- models/src/src_listings.sql
WITH raw_listings AS (
    SELECT *
    FROM AIRBNB.RAW.RAW_LISTINGS
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

--models/src/src_reviews.sql :
WITH raw_reviews AS (
    SELECT *
    FROM AIRBNB.RAW.RAW_REVIEWS
)
SELECT listing_id,
    date AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment
FROM raw_reviews

-- models/src/src_hosts.sql
WITH raw_hosts AS (
    SELECT *
    FROM AIRBNB.RAW.RAW_HOSTS
)
SELECT id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM raw_hosts


-- dim
-- models/dim/dim_listings_cleansed.sql :
WITH src_listings AS (
    SELECT *
    FROM {{ ref('src_listings') }}
)
SELECT
    listing_id,
    listing_name,
    room_type,
    CASE 
        WHEN minimum_nights = 0 THEN 1
        ELSE minimum_nights
    END AS minimum_nights,
    host_id,
    REPLACE(price_str,'$') :: NUMBER(10,2) AS price,
    created_at,
    updated_at
FROM
    src_listings

-- models/dim/dim_hosts_cleansed.sql
WITH src_hosts AS (
    SELECT *
    FROM {{ ref('src_hosts') }}
)
SELECT
    host_id,
    NVL(host_name,'Anonymous') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts


-- fact
-- models/fct/fct_reviews.sql
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
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}

-- Check incremental model
SELECT * FROM "AIRBNB"."DEV"."FCT_REVIEWS" WHERE listing_id=3176;

INSERT INTO "AIRBNB"."RAW"."RAW_REVIEWS"
VALUES (3176, CURRENT_TIMESTAMP(), 'Zoltan', 'excellent stay!', 'positive');




-- Sources and seeds
-- models/mart/mart_fullmoon_reviews.sql
{{ 
    config(
        materialized = 'table',
    )
}}
WITH fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),
full_moon_dates AS (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
)
SELECT
    r.*,
    CASE
    WHEN fm.full_moon_date IS NULL THEN 'not full moon'
    ELSE 'full moon'
    END AS is_full_moon
FROM fct_reviews r
LEFT JOIN full_moon_dates fm
ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, fm.full_moon_date))




-- Snapshots
-- snapshots/scd_raw_listings.sql
{% snapshot scd_raw_listings %}
{{
    config(
        target_schema='DEV',
        unique_key='id',
        strategy='check',
        check_cols=['timestamp']
        invalidate_hard_deletes=True
    )
}}
SELECT *
FROM {{ source('airbnb', 'listings') }}
{% endsnapshot %}

-- update table
UPDATE AIRBNB.RAW.RAW_LISTINGS
SET
    MINIMUM_NIGHTS=30,
    updated_at=CURRENT_TIMESTAMP() WHERE ID=3176;

-- select 3176 again
SELECT *
FROM AIRBNB.DEV.SCD_RAW_LISTINGS WHERE ID=3176;




-- Tests
-- tests/dim_listings_minumum_nights.sql
SELECT *
FROM {{ ref('dim_listings_cleansed') }}
WHERE minimum_nights < 1
LIMIT 10

-- tests/consistent_created_at.sql
SELECT *
FROM {{ ref('dim_listings_cleansed') }} l
INNER JOIN {{ ref('fct_reviews') }} r
USING (listing_id)
WHERE l.created_at >= r.review_date




-- Macros
-- macros/no_nulls_in_columns.sql
{% macro no_nulls_in_columns(model) %}
    SELECT * FROM {{ model }} WHERE
    {% for col in adapter.get_columns_in_relation(model) -%}
    {{ col.column }} IS NULL OR
    {% endfor %}
    FALSE
{% endmacro %}

-- tests/no_nulls_in_dim_listings.sql
{{ no_nulls_in_columns(ref('dim_listings_cleansed')) }}

-- macros/positive_value.sql
{% test positive_value(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name}} < 1
{% endtest %}

-- models/fct_reviews.sql
{{
    config(
        materialized = 'incremental',
        on_schema_change='fail'
    )
}}
WITH src_reviews AS (
    SELECT *
    FROM {{ ref('src_reviews') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_id,
    *
FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
    AND review_date > (select max(review_date) from {{ this }})
{% endif %}




-- Analyses
-- CREATE THE REPORTER ROLE AND PRESET USER IN SNOWFLAKE
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS REPORTER;
CREATE USER IF NOT EXISTS PBI
    PASSWORD='pbiPassword123'
    LOGIN_NAME='pbi'
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE='COMPUTE_WH'
    DEFAULT_ROLE='REPORTER'
    DEFAULT_NAMESPACE='AIRBNB.DEV'
    COMMENT='PowerBI user for creating reports';
GRANT ROLE REPORTER TO USER PBI;
GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
GRANT USAGE ON DATABASE AIRBNB TO ROLE REPORTER;
GRANT USAGE ON SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- We don't want to grant select rights here; we'll do this through
-- GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
-- GRANT SELECT ON FUTURE VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;

-- analyses/full_moon_no_sleep.sql :
WITH fullmoon_reviews AS (
    SELECT * FROM {{ ref('fullmoon_reviews') }}
)
SELECT
    is_full_moon,
    review_sentiment,
    COUNT(*) as reviews
FROM
    fullmoon_reviews
GROUP BY
    is_full_moon,
    review_sentiment
ORDER BY
    is_full_moon,
    review_sentiment