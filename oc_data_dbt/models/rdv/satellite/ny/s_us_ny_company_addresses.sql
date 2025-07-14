{{ config(
    materialized='incremental', 
    unique_key='company_hk'
) }}

with source as (
    select
        entity_id,
        dos_process_name,
        dos_process_address_1,
        dos_process_address_2,
        dos_process_city,
        dos_process_state,
        dos_process_zip,
        _meta_load_timestamp,
        _meta_source_system
    from {{ ref('stg_us_ny_companies_structured') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['entity_id']) }} as company_hk,
    *,
    {{ dbt_utils.generate_surrogate_key([
        'entity_id',
        'dos_process_name',
        'dos_process_address_1',
        'dos_process_address_2',
        'dos_process_city',
        'dos_process_state',
        'dos_process_zip'
    ]) }} as sat_hashdiff
from source 