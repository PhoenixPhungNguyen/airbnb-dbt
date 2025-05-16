--run by 'dbt snapshot' command
--save snapshot schema of all tables
--dbt depends on id, updated_at columns to track history change
--invalidate_hard_deletes colum: defaults as False (dbt does not save), True: dbt saves and remarks this record not valid
{% snapshot scd_raw_listings %}
{{
    config(
        target_schema='DEV',
        unique_key='id',  
        strategy='timestamp',
        updated_at = 'updated_at',
        invalidate_hard_deletes=True 
    )
}}
SELECT *
FROM {{ source('airbnb', 'listings') }}
{% endsnapshot %}

--TEST ON SNOWFLAKE
-- update table
--UPDATE AIRBNB.RAW.RAW_LISTINGS
--SET
--   MINIMUM_NIGHTS=30,
--    updated_at=CURRENT_TIMESTAMP() WHERE ID=3176;

--run dbt snapshot

-- select 3176 again
--SELECT *
--FROM AIRBNB.DEV.SCD_RAW_LISTINGS WHERE ID=3176;