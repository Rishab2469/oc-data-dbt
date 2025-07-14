{{ config(
    materialized='incremental', 
    unique_key='company_hk'
) }}

with source as (
    select
        entity_id,
        registered_agent_name,
        registered_agent_address_1,
        registered_agent_address_2,
        registered_agent_city,
        registered_agent_state,
        registered_agent_zip,
        _meta_load_timestamp,
        _meta_source_system
    from {{ ref('stg_us_ny_companies_structured') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['entity_id']) }} as company_hk,
    *,
    {{ dbt_utils.generate_surrogate_key([
        'entity_id',
        'registered_agent_name',
        'registered_agent_address_1',
        'registered_agent_address_2',
        'registered_agent_city',
        'registered_agent_state',
        'registered_agent_zip'
    ]) }} as sat_hashdiff
from source 