{{ config(
    materialized='incremental',
    unique_key='dos_id'
) }}

with src as (
    select * from {{ ref('stg_us_ny_companies_structured') }}
),
with_hash as (
    select
        *,
        md5(
            coalesce(CAST(_meta_registration_authority_code AS varchar), '-1') || '' ||
            coalesce(CAST(dos_id AS varchar), '-1') || '' ||
            coalesce(CAST(current_entity_name AS varchar), '-1') || '' ||
            coalesce(CAST(initial_dos_filing_date AS varchar), '-1') || '' ||
            coalesce(CAST(county AS varchar), '-1') || '' ||
            coalesce(CAST(jurisdiction AS varchar), '-1') || '' ||
            coalesce(CAST(entity_type AS varchar), '-1') || '' ||
            coalesce(CAST(dos_process_name AS varchar), '-1') || '' ||
            coalesce(CAST(dos_process_address_1 AS varchar), '-1') || '' ||
            coalesce(CAST(dos_process_address_2 AS varchar), '-1') || '' ||
            coalesce(CAST(dos_process_city AS varchar), '-1') || '' ||
            coalesce(CAST(dos_process_state AS varchar), '-1') || '' ||
            coalesce(CAST(dos_process_zip AS varchar), '-1') || '' ||
            coalesce(CAST(ceo_name AS varchar), '-1') || '' ||
            coalesce(CAST(ceo_address_1 AS varchar), '-1') || '' ||
            coalesce(CAST(ceo_address_2 AS varchar), '-1') || '' ||
            coalesce(CAST(ceo_city AS varchar), '-1') || '' ||
            coalesce(CAST(ceo_state AS varchar), '-1') || '' ||
            coalesce(CAST(ceo_zip AS varchar), '-1') || '' ||
            coalesce(CAST(registered_agent_name AS varchar), '-1') || '' ||
            coalesce(CAST(registered_agent_address_1 AS varchar), '-1') || '' ||
            coalesce(CAST(registered_agent_address_2 AS varchar), '-1') || '' ||
            coalesce(CAST(registered_agent_city AS varchar), '-1') || '' ||
            coalesce(CAST(registered_agent_state AS varchar), '-1') || '' ||
            coalesce(CAST(registered_agent_zip AS varchar), '-1') || '' ||
            coalesce(CAST(location_name AS varchar), '-1') || '' ||
            coalesce(CAST(location_address_1 AS varchar), '-1') || '' ||
            coalesce(CAST(location_address_2 AS varchar), '-1') || '' ||
            coalesce(CAST(location_city AS varchar), '-1') || '' ||
            coalesce(CAST(location_state AS varchar), '-1') || '' ||
            coalesce(CAST(location_zip AS varchar), '-1')
        ) as _meta_dv_datahash,
        md5(upper(coalesce(CAST(_meta_registration_authority_code AS varchar), '-1') || '~' || coalesce(CAST(dos_id AS varchar), '-1'))) as _meta_dv_id,
        current_timestamp() as _meta_dv_load_time,
        '{{ var("timemillis", "0") }}' as _meta_dv_run_id,
        _meta_source_system as _meta_dv_source_system,
        _meta_source_entity as _meta_dv_source_entity,
        'load_s_us_ny_companies_from_stg_us_ny_companies_structured_01' as _meta_dv_load_name,
        1000512 as _meta_dv_package_version
    from src
)

select
    _meta_dv_id,
    _meta_dv_load_time,
    _meta_dv_run_id,
    _meta_dv_source_system,
    _meta_dv_source_entity,
    _meta_dv_load_name,
    _meta_dv_package_version,
    _meta_dv_datahash,
    _meta_stg_file_name,
    _meta_stg_file_last_modified,
    _meta_stg_file_row_number,
    _meta_stg_file_hash,
    _meta_country,
    _meta_jurisdiction,
    _meta_registration_authority_code,
    dos_id as entity_id,
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
    location_zip
from with_hash src
{% if is_incremental() %}
where not exists (
    select 1
    from (
        select
            t1._meta_dv_datahash
        from (
            select
                s._meta_dv_datahash,
                row_number() over (
                    partition by s._meta_dv_id
                    order by s._meta_dv_load_time desc
                ) as dv_load_time_last
            from {{ this }} s
        ) t1
        where t1.dv_load_time_last = 1
    ) trg
    where trg._meta_dv_datahash = src._meta_dv_datahash
)
{% endif %} 