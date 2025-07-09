-- Example staging layer model for NY jurisdiction
SELECT id, name
FROM {{ ref('raw_ny_model') }}
