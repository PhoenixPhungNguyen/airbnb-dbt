--use dbt compile to check reference 
-- models/src/src_hosts.sql
WITH raw_hosts AS (
    SELECT *
    FROM {{ source('airbnb','hosts') }} -- use jinja template instead of direct table  AIRBNB.RAW.RAW_HOSTS
)                       
SELECT id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM raw_hosts
