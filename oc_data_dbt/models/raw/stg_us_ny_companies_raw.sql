{{ 
    config(
    materialized='view', 
    post_hook='{{ create_external_tables() }}'
    ) 
}}
-- This is a raw layer model for NY jurisdiction, data will be loaded from post hook.
SELECT
    NULL AS _meta_load_id,
    NULL AS _meta_load_timestamp,
    NULL AS _meta_load_name,
    NULL AS _meta_load_version,
    NULL AS _meta_stg_file_name,
    NULL AS _meta_stg_file_last_modified,
    NULL AS _meta_stg_file_row_number,
    NULL AS _meta_stg_file_hash,
    NULL AS _meta_source_system,
    NULL AS _meta_source_entity,
    NULL AS _meta_country,
    NULL AS _meta_jurisdiction,
    NULL AS _meta_registration_authority_code,
    NULL AS FULL_ROW_CSV
WHERE FALSE
