{{ config(
        materialized='incremental', 
        unique_key='company_hk'
) 
}}

with source as (
    select
        entity_id,
        current_entity_name,
        initial_dos_filing_date,
        county,
        jurisdiction,
        entity_type,
        dos_process_name,
        dos_process_address_1,
        dos_process_address_2,
        dos_process_city,
        dos_process_state,
        dos_process_zip,
        ceo_name,
        ceo_address_1,
        ceo_address_2,
        ceo_city,
        ceo_state,
        ceo_zip,
        registered_agent_name,
        registered_agent_address_1,
        registered_agent_address_2,
        registered_agent_city,
        registered_agent_state,
        registered_agent_zip,
        location_name,
        location_address_1,
        location_address_2,
        location_city,
        location_state,
        location_zip,
        _meta_load_timestamp,
        _meta_source_system
    from {{ ref('stg_us_ny_companies_structured') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['entity_id']) }} as company_hk,
    *,
    {{ dbt_utils.generate_surrogate_key([
        'entity_id',
        'current_entity_name',
        'initial_dos_filing_date',
        'county',
        'jurisdiction',
        'entity_type',
        'dos_process_name',
        'dos_process_address_1',
        'dos_process_address_2',
        'dos_process_city',
        'dos_process_state',
        'dos_process_zip',
        'ceo_name',
        'ceo_address_1',
        'ceo_address_2',
        'ceo_city',
        'ceo_state',
        'ceo_zip',
        'registered_agent_name',
        'registered_agent_address_1',
        'registered_agent_address_2',
        'registered_agent_city',
        'registered_agent_state',
        'registered_agent_zip',
        'location_name',
        'location_address_1',
        'location_address_2',
        'location_city',
        'location_state',
        'location_zip'
    ]) }} as sat_hashdiff
from source 