{{ config(
    materialized='incremental',
    unique_key='entity_id'
) }}

WITH officers AS (
    SELECT
        entity_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'name', ceo_name,
                'position', 'chief executive officer',
                'address', OBJECT_CONSTRUCT(
                    'street_address', 
                        CASE 
                            WHEN ceo_address_1 IS NOT NULL AND ceo_address_2 IS NOT NULL AND ceo_address_2 != ''
                                THEN ceo_address_1 || ', ' || ceo_address_2
                            ELSE COALESCE(ceo_address_1, ceo_address_2)
                        END,
                    'locality', ceo_city,
                    'region', ceo_state,
                    'postal_code', ceo_zip
                )
            )
        ) AS officers
    FROM {{ ref('s_us_ny_companies') }}
    WHERE ceo_name IS NOT NULL 
        AND ceo_name NOT IN ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')
    GROUP BY entity_id
),
agents AS (
    SELECT
        entity_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'name', registered_agent_name,
                'position', 'registered agent',
                'address', OBJECT_CONSTRUCT(
                    'street_address', 
                        CASE 
                            WHEN registered_agent_address_1 IS NOT NULL AND registered_agent_address_2 IS NOT NULL AND registered_agent_address_2 != ''
                                THEN registered_agent_address_1 || ', ' || registered_agent_address_2
                            ELSE COALESCE(registered_agent_address_1, registered_agent_address_2)
                        END,
                    'locality', registered_agent_city,
                    'region', registered_agent_state,
                    'postal_code', registered_agent_zip
                )
            )
        ) AS agents
    FROM {{ ref('s_us_ny_companies') }}
    WHERE registered_agent_name IS NOT NULL 
        AND registered_agent_name NOT IN ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')
    GROUP BY entity_id
)
SELECT
    s.entity_id,
    s.current_entity_name AS name,
    s.entity_type AS company_type,
    s.initial_dos_filing_date as incorporation_date,
    s.jurisdiction AS jurisdiction_code,
    s.county,
    CASE 
    WHEN s.dos_process_name IS NOT NULL THEN
        OBJECT_CONSTRUCT(
            'care_of', s.dos_process_name,
            'street_address', 
                CASE 
                    WHEN s.dos_process_address_1 IS NOT NULL AND s.dos_process_address_2 IS NOT NULL AND s.dos_process_address_2 != ''
                        THEN s.dos_process_address_1 || ', ' || s.dos_process_address_2
                    ELSE COALESCE(s.dos_process_address_1, s.dos_process_address_2)
                END,
            'locality', s.dos_process_city,
            'region', s.dos_process_state,
            'postal_code', s.dos_process_zip
        )
    ELSE NULL
    END AS registered_address,
    officers.officers,
    agents.agents,
    OBJECT_CONSTRUCT(
        'county', s.county,
        'location_address_1', s.location_address_1,
        'location_address_2', s.location_address_2,
        'location_city', s.location_city,
        'location_name', s.location_name,
        'location_state', s.location_state,
        'location_zip', s.location_zip
    ) AS all_attributes,
    _meta_stg_file_name as _meta_stg_file_name,
    _meta_stg_file_last_modified as _meta_stg_file_last_modified
FROM {{ ref('s_us_ny_companies') }} s
LEFT JOIN officers ON s.entity_id = officers.entity_id
LEFT JOIN agents ON s.entity_id = agents.entity_id
WHERE
    s.current_entity_name IS NOT NULL
    AND s.current_entity_name NOT IN ('', 'none', 'n/a', '.', ',', '-', 'no information available')
    AND s.entity_id IS NOT NULL
    AND s.entity_id NOT IN ('', 'none', 'n/a', '.', ',', '-', 'no information available')
    AND s.initial_dos_filing_date IS NOT NULL
    AND s.entity_type NOT IN (
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
{% if is_incremental() %}
  AND s.initial_dos_filing_date > (SELECT COALESCE(MAX(incorporation_date), '1900-01-01') FROM {{ this }})
{% endif %} 