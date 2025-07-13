{{ config(
    materialized = 'table'
) }}

WITH final_data AS (
    SELECT 
        -- Metadata columns
        000 as _meta_load_id,
        '{{ run_started_at }}' as _meta_load_timestamp,
        '<loadname>' as _meta_load_name,
        '<packageversion>' as _meta_load_version,
        'METADATA$FILENAME' as _meta_stg_file_name,
        'METADATA$FILE_LAST_MODIFIED 'as _meta_stg_file_last_modified,
        'METADATA$FILE_ROW_NUMBER' as _meta_stg_file_row_number,
        'METADATA$FILE_CONTENT_KEY' AS _meta_stg_file_hash,
        '<source_system_name>' as _meta_source_system,
        '<source_entity_name>' as _meta_source_entity,
        'US' as _meta_country,
        'us_ny' as _meta_jurisdiction,
        'RA000603' as _meta_registration_authority_code,
        --Business columns
        dos_id,
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
    FROM {{ ref('stg_us_ny_companies_raw') }}
)

SELECT 
    * 
from final_data
