--test by running dbt run command
{{ no_nulls_in_columns(ref('dim_listings_cleansed')) }}