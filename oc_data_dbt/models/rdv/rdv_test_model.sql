-- Example rdv layer model
SELECT id, name
FROM {{ ref('staging_test_model') }}
