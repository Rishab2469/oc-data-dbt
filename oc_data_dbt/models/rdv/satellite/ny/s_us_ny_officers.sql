{{ config(
    materialized='incremental', 
    unique_key='company_hk'
) }}

with source as (
    select
        entity_id,
        ceo_name,
        ceo_address_1,
        ceo_address_2,
        ceo_city,
        ceo_state,
        ceo_zip,
        _meta_load_timestamp,
        _meta_source_system
    from {{ ref('stg_us_ny_companies_structured') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['entity_id']) }} as company_hk,
    *,
    {{ dbt_utils.generate_surrogate_key([
        'entity_id',
        'ceo_name',
        'ceo_address_1',
        'ceo_address_2',
        'ceo_city',
        'ceo_state',
        'ceo_zip'
    ]) }} as sat_hashdiff
from source 