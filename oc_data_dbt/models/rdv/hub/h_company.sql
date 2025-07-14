{{ config(
        materialized='incremental', 
        unique_key='company_hk'
    ) 
}}

with source as (
    select
        entity_id,
        _meta_load_timestamp,
        _meta_source_system,
        _meta_jurisdiction,
        _meta_registration_authority_code,
        _meta_stg_file_name,
        _meta_stg_file_last_modified
    from {{ ref('stg_us_ny_companies_structured') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['entity_id']) }} as company_hk,
    entity_id,
    _meta_load_timestamp,
    _meta_source_system,
    _meta_jurisdiction,
    _meta_registration_authority_code,
    _meta_stg_file_name,
    _meta_stg_file_last_modified
from source 