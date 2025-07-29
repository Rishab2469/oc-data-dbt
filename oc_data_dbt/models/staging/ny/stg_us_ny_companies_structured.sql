{{ config(
    materialized = 'table',
    catalog_name  = 'bigquery_iceberg_catalog'
) }}


select 
    000 as _meta_load_id,
    '{{ run_started_at }}' as _meta_load_timestamp,
    'ny_companies' as _meta_load_name,
    '1.0.0' as _meta_load_version,
    'METADATA$FILENAME' as _meta_stg_file_name,
    'METADATA$FILE_LAST_MODIFIED 'as _meta_stg_file_last_modified,
    'METADATA$FILE_ROW_NUMBER' as _meta_stg_file_row_number,
    'METADATA$FILE_CONTENT_KEY' AS _meta_stg_file_hash,
    'us_ny' as _meta_source_system,
    '' as _meta_source_entity,
    'US' as _meta_country,
    'us_ny' as _meta_jurisdiction,
    '' as _meta_registration_authority_code,
    *
from {{ source('eix_staging', 'stg_eix_ny_ext') }}
limit 100