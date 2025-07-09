{{ 
    config(
    materialized='table', 
    tags=['raw']
    ) 
}}
-- This is a raw layer model for demonstration
SELECT id, name
FROM {{ source('my_source', 'my_source_table') }}
