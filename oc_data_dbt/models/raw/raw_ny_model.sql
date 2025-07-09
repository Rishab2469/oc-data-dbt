{{ 
    config(
    materialized='table', 
    tags=['raw']
    ) 
}}
-- This is a raw layer model for NY jurisdiction
SELECT id, name
FROM {{ source('my_source', 'ext_table_ny') }}
