{{ config(
    materialized='incremental',
    unique_key='entity_id'
) }}

WITH officers AS (
    SELECT
        entity_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'name', officer_name,
                'position', officer_position,
                'address', OBJECT_CONSTRUCT(
                    'street_address', officer_street_address,
                    'locality', officer_locality,
                    'region', officer_region,
                    'postal_code', officer_postal_code
                )
            )
        ) AS officers
    FROM {{ ref('stg_us_ny_companies') }}
    WHERE officer_name IS NOT NULL
    GROUP BY entity_id
),
agents AS (
    SELECT
        entity_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'name', agent_name,
                'position', agent_position,
                'address', OBJECT_CONSTRUCT(
                    'street_address', agent_street_address,
                    'locality', agent_locality,
                    'region', agent_region,
                    'postal_code', agent_postal_code
                )
            )
        ) AS agents
    FROM {{ ref('stg_us_ny_companies') }}
    WHERE agent_name IS NOT NULL
    GROUP BY entity_id
)
SELECT
    s.entity_id,
    s.entity_name AS name,
    s.entity_type AS company_type,
    s.incorporation_date,
    s.jurisdiction_code,
    s.county,
    OBJECT_CONSTRUCT(
        'care_of', s.registered_address_care_of,
        'street_address', s.registered_address_street_address,
        'locality', s.registered_address_locality,
        'region', s.registered_address_region,
        'postal_code', s.registered_address_postal_code
    ) AS registered_address,
    officers.officers,
    agents.agents,
    OBJECT_CONSTRUCT(
        'location_address_1', s.location_address_1,
        'location_address_2', s.location_address_2,
        'location_city', s.location_city,
        'location_name', s.location_name,
        'location_state', s.location_state,
        'location_zip', s.location_zip
    ) AS all_attributes,
    s._meta_stg_file_name,
    s._meta_stg_file_last_modified
FROM {{ ref('stg_us_ny_companies') }} s
LEFT JOIN officers ON s.entity_id = officers.entity_id
LEFT JOIN agents ON s.entity_id = agents.entity_id

{% if is_incremental() %}
  WHERE s._meta_stg_file_last_modified > (SELECT COALESCE(MAX(_meta_stg_file_last_modified), '1900-01-01') FROM {{ this }})
{% endif %}