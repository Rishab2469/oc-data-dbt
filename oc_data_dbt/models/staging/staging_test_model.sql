-- Example staging layer model
SELECT id, name
FROM {{ ref('raw_test_model') }}
