{{ config(
    materialized = 'table',
    pre_hook = "{{ create_parse_csv_line() }}"
) }}

WITH raw_data AS (
    SELECT
        full_row_csv,
        PARSE_CSV_LINE(full_row_csv) AS parsed_array
    FROM {{ ref('raw_ny_model') }}  -- replace with actual raw model name
),

final_data AS (
    SELECT
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
where
    current_entity_name IS NOT NULL
    AND dos_id IS NOT NULL
    AND initial_dos_filing_date IS NOT NULL
    AND entity_type NOT IN (
        'DOMESTIC BUSINESS CORPORATION RESERVATION',
        'DOMESTIC PROFESSIONAL CORPORATION RESERVATION',
        'DOMESTIC PROFESSIONAL SERVICE LIMITED LIABILITY COMPANY RESERVATION',
        'DOMESTIC NOT-FOR-PROFIT CORPORATION RESERVATION',
        'DOMESTIC LIMITED LIABILITY COMPANY RESERVATION',
        'FOREIGN LIMITED LIABILITY COMPANY RESERVATION',
        'FOREIGN BUSINESS CORPORATION RESERVATION',
        'FOREIGN NOT-FOR-PROFIT CORPORATION RESERVATION',
        'FOREIGN LIMITED PARTNERSHIP RESERVATION',
        'FOREIGN PROFESSIONAL CORPORATION RESERVATION',
        'DOMESTIC LIMITED PARTNERSHIP RESERVATION',
        'FOREIGN PROFESSIONAL SERVICE LIMITED LIABILITY COMPANY RESERVATION'
    )