WITH source_reviews AS (
    SELECT *
    FROM {{ ref('src_reviews') }}
)
SELECT *
FROM source_reviews