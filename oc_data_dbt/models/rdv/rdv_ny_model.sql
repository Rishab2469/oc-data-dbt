-- Example rdv layer model for NY jurisdiction
SELECT id, name
FROM {{ ref('staging_ny_model') }}
