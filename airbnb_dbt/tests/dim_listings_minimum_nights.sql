--run dbt test --select dim_listings_cleansed
SELECT *
FROM {{ ref('dim_listings_cleansed') }}
WHERE minimum_nights < 1
LIMIT 10