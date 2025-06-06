{{
  config(
    materialized = 'incremental',
    on_schema_change = 'fail'
    )
}}
WITH src_reviews AS (
    SELECT 
            *,
            {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} AS review_PK
    FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null
{% if is_incremental() %}
    AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}