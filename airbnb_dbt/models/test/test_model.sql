--reference models from dbt
WITH source_reviews AS (
    SELECT * 
    FROM {{('src_reviews')}}
)
SELECT * FROM source_reviews