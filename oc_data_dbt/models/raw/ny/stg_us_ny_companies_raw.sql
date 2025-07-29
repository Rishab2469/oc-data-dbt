{{
    config(
        materialized='table',
        catalog_name='bigquery_iceberg_catalog'
    )
}}

SELECT 
    1 as col1