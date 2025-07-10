{{ config(
    materialized = 'table',
    pre_hook = "{{ create_parse_csv_line() }}"
) }}

WITH raw_data AS (
    SELECT
        _meta_load_id,
        _meta_load_timestamp,
        _meta_load_name,
        _meta_load_version,
        _meta_stg_file_name,
        _meta_stg_file_last_modified,
        _meta_stg_file_row_number,
        _meta_stg_file_hash,
        _meta_source_system,
        _meta_source_entity,
        _meta_country,
        _meta_jurisdiction,
        _meta_registration_authority_code,
        full_row_csv,
        PARSE_CSV_LINE(full_row_csv) AS parsed_array
    FROM {{ ref('stg_us_ny_companies_raw') }}
),

final_data AS (
    SELECT 
        _meta_load_id,
        _meta_load_timestamp,
        _meta_load_name,
        _meta_load_version,
        _meta_stg_file_name,
        _meta_stg_file_last_modified,
        _meta_stg_file_row_number,
        _meta_stg_file_hash,
        _meta_source_system,
        _meta_source_entity,
        _meta_country,
        _meta_jurisdiction,
        _meta_registration_authority_code,
        parsed_array[0]  ::STRING AS dos_id,
        parsed_array[1]  ::STRING AS current_entity_name,
        parsed_array[2]  ::DATE   AS initial_dos_filing_date,
        parsed_array[3]  ::STRING AS county,
        parsed_array[4]  ::STRING AS jurisdiction,
        parsed_array[5]  ::STRING AS entity_type,
        parsed_array[6]  ::STRING AS dos_process_name,
        parsed_array[7]  ::STRING AS dos_process_address_1,
        parsed_array[8]  ::STRING AS dos_process_address_2,
        parsed_array[9]  ::STRING AS dos_process_city,
        parsed_array[10] ::STRING AS dos_process_state,
        parsed_array[11] ::STRING AS dos_process_zip,
        parsed_array[12] ::STRING AS ceo_name,
        parsed_array[13] ::STRING AS ceo_address_1,
        parsed_array[14] ::STRING AS ceo_address_2,
        parsed_array[15] ::STRING AS ceo_city,
        parsed_array[16] ::STRING AS ceo_state,
        parsed_array[17] ::STRING AS ceo_zip,
        parsed_array[18] ::STRING AS registered_agent_name,
        parsed_array[19] ::STRING AS registered_agent_address_1,
        parsed_array[20] ::STRING AS registered_agent_address_2,
        parsed_array[21] ::STRING AS registered_agent_city,
        parsed_array[22] ::STRING AS registered_agent_state,
        parsed_array[23] ::STRING AS registered_agent_zip,
        parsed_array[24] ::STRING AS location_name,
        parsed_array[25] ::STRING AS location_address_1,
        parsed_array[26] ::STRING AS location_address_2,
        parsed_array[27] ::STRING AS location_city,
        parsed_array[28] ::STRING AS location_state,
        parsed_array[29] ::STRING AS location_zip
    FROM raw_data
)

SELECT 
    * 
from final_data
