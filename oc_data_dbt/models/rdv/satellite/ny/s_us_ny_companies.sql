{{ config(
    materialized='incremental', 
    unique_key='company_hk'
) }}

with source as (
    select
        entity_id,
        current_entity_name,
        initial_dos_filing_date,
        county,
        jurisdiction,
        entity_type,
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
        'entity_type'
    ]) }} as sat_hashdiff
from source 