{{ 
    config(
        materialized = 'table',
        post_hook = "
            COPY INTO {{this}}
            FROM @my_s3_stage
            FILE_FORMAT = (
                TYPE = 'CSV'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
            )
        "
    ) 
}}



-- This creates an empty table (but COPY will fill it)
SELECT
    -- Business columns
    CAST(NULL AS VARCHAR(100)) AS dos_id,
    CAST(NULL AS VARCHAR(100)) AS current_entity_name,
    CAST(NULL AS VARCHAR(100)) AS initial_dos_filing_date,
    CAST(NULL AS VARCHAR(100)) AS county,
    CAST(NULL AS VARCHAR(100)) AS jurisdiction,
    CAST(NULL AS VARCHAR(100)) AS entity_type,
    CAST(NULL AS VARCHAR(100)) AS dos_process_name,
    CAST(NULL AS VARCHAR(100)) AS dos_process_address_1,
    CAST(NULL AS VARCHAR(100)) AS dos_process_address_2,
    CAST(NULL AS VARCHAR(100)) AS dos_process_city,
    CAST(NULL AS VARCHAR(100)) AS dos_process_state,
    CAST(NULL AS VARCHAR(100)) AS dos_process_zip,
    CAST(NULL AS VARCHAR(100)) AS ceo_name,
    CAST(NULL AS VARCHAR(100)) AS ceo_address_1,
    CAST(NULL AS VARCHAR(100)) AS ceo_address_2,
    CAST(NULL AS VARCHAR(100)) AS ceo_city,
    CAST(NULL AS VARCHAR(100)) AS ceo_state,
    CAST(NULL AS VARCHAR(100)) AS ceo_zip,
    CAST(NULL AS VARCHAR(100)) AS registered_agent_name,
    CAST(NULL AS VARCHAR(100)) AS registered_agent_address_1,
    CAST(NULL AS VARCHAR(100)) AS registered_agent_address_2,
    CAST(NULL AS VARCHAR(100)) AS registered_agent_city,
    CAST(NULL AS VARCHAR(100)) AS registered_agent_state,
    CAST(NULL AS VARCHAR(100)) AS registered_agent_zip,
    CAST(NULL AS VARCHAR(100)) AS location_name,
    CAST(NULL AS VARCHAR(100)) AS location_address_1,
    CAST(NULL AS VARCHAR(100)) AS location_address_2,
    CAST(NULL AS VARCHAR(100)) AS location_city,
    CAST(NULL AS VARCHAR(100)) AS location_state,
    CAST(NULL AS VARCHAR(100)) AS location_zip
LIMIT 0
