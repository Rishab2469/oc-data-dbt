{{ config(
    materialized='incremental',
    unique_key='company_hk'
) }}

-- Hub: Contains core business keys and metadata
with hub as (
    select * from {{ ref('h_company') }}
),
-- Company satellite: Company-level descriptive attributes
company as (
    select * from {{ ref('s_us_ny_companies') }}
    where
        entity_id is not null
        and entity_id not in ('', 'none', 'n/a', '.', ',', '-', 'no information available')
        and current_entity_name is not null
        and current_entity_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available')
        and initial_dos_filing_date is not null
        and entity_type not in (
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
),
-- Officers aggregation: Array of officer objects per company, only valid names
officer_expanded as (
    SELECT
        company_hk,
        ceo_name AS name,
        ceo_address_1 AS address1,
        ceo_address_2 AS address2,
        ceo_city AS city,
        ceo_state AS state,
        ceo_zip AS postal_code,
        NULL AS country,
        'chief executive officer' AS position
    FROM  {{ ref('s_us_ny_officers') }}
    where ceo_name is not null
    and ceo_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')

    UNION ALL

    SELECT
        company_hk,
        dos_process_name AS name,
        dos_process_address_1 AS address1,
        dos_process_address_2 AS address2,
        dos_process_city AS city,
        dos_process_state AS state,
        dos_process_zip AS postal_code,
        NULL AS country,
        'DOS Process Agent' AS position
    FROM {{ ref('s_us_ny_company_addresses') }}

),

officer_combined AS (
  SELECT
    company_hk,
    ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'other_attributes', OBJECT_CONSTRUCT(
          'address', OBJECT_CONSTRUCT(
            'street_address',
              CASE
                WHEN address2 IS NOT NULL AND address2 <> ''
                  THEN TRIM(address1 || ', ' || address2)
                ELSE TRIM(address1)
              END,
            'locality', city,
            'region', state,
            'postal_code', postal_code
          )
        ),
        'name', name,
        'position', position
      )
    ) AS officers
  FROM officer_expanded
  GROUP BY company_hk 
),
-- Registered agents aggregation: Array of agent objects per company, only valid names
valid_agents as (
    select * from {{ ref('s_us_ny_registered_agents') }}
    where registered_agent_name is not null
      and registered_agent_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')
),
registered_agents_agg as (
    select
        company_hk,
        array_agg(
            object_construct(
                'name', registered_agent_name,
                'position', 'registered agent',
                'address', object_construct(
                    'street_address', 
                        case 
                            when registered_agent_address_1 is not null and registered_agent_address_2 is not null and registered_agent_address_2 != ''
                                then registered_agent_address_1 || ', ' || registered_agent_address_2
                            else coalesce(registered_agent_address_1, registered_agent_address_2)
                        end,
                    'locality', registered_agent_city,
                    'region', registered_agent_state,
                    'postal_code', registered_agent_zip
                )
            )
        ) as agents
    from valid_agents
    group by company_hk
),
-- Company addresses satellite for headquarters address
company_addresses as (
    select * from {{ ref('s_us_ny_company_addresses') }}
),
-- Registered address: Join to company_addresses to extract dos_process fields for each company
registered_address as (
    select
        c.company_hk,
        case when ca.dos_process_name is not null then
            object_construct(
                -- 'care_of', ca.dos_process_name,
                'street_address', 
                    case 
                        when ca.dos_process_address_1 is not null and ca.dos_process_address_2 is not null and ca.dos_process_address_2 != ''
                            then ca.dos_process_address_1 || ', ' || ca.dos_process_address_2
                        else coalesce(ca.dos_process_address_1, ca.dos_process_address_2)
                    end,
                'locality', ca.dos_process_city,
                'region', ca.dos_process_state,
                'postal_code', ca.dos_process_zip
            )
        else null end as registered_address
    from company c
    left join company_addresses ca
        on c.company_hk = ca.company_hk
),
-- Headquarters address: Join to company_addresses and construct headquarters_address
headquarters as (
    select
        company_hk,
        case when location_name IS NOT NULL OR location_address_1 IS NOT NULL OR location_address_2 IS NOT NULL THEN
        object_construct(
            'street_address',
                        CASE
                        WHEN location_name IS NOT NULL OR location_address_1 IS NOT NULL OR location_address_2 IS NOT NULL 
                        THEN
                            REGEXP_REPLACE(TRIM(CONCAT_WS(', ', 
                                        TRIM(COALESCE(location_name, '')),
                                        TRIM(COALESCE(location_address_1, '')),
                                        TRIM(COALESCE(location_address_2, ''))
                                    )), '\\s+', ' ')
                        ELSE NULL
                        END,
            'locality', location_city,
            'region', location_state,
            'postal_code', location_zip
        ) ELSE NULL END as headquarters_address
    from {{ref('s_us_ny_registered_addresses')}}
),
-- Final output: Join all CTEs and select canonical company structure
final as (
    select
        company.company_hk,
        company.entity_id,
        company.current_entity_name as name,
        company.entity_type as company_type,
        -- company.initial_dos_filing_date as incorporation_date,
        TO_CHAR(TO_DATE(company.initial_dos_filing_date), 'YYYY-MM-DD') as incorporation_date,
        hub._meta_jurisdiction as jurisdiction_code,
        company.county,
        registered_address.registered_address,
        headquarters.headquarters_address,
        officer_combined.officers,
        registered_agents_agg.agents,
        object_construct(
            'County', company.county,
            'Jurisdiction', company.jurisdiction
        ) as all_attributes,
        company._meta_load_timestamp,
        CASE
            WHEN company.entity_type ILIKE '%foreign%' THEN 'F'
            ELSE null
        END as branch,
        hub._meta_source_system,
        hub._meta_jurisdiction,
        hub._meta_registration_authority_code,
        hub._meta_stg_file_name,
        hub._meta_stg_file_last_modified
    from company
    left join hub on company.company_hk = hub.company_hk
    left join officer_combined on company.company_hk = officer_combined.company_hk
    left join registered_agents_agg on company.company_hk = registered_agents_agg.company_hk
    left join registered_address on company.company_hk = registered_address.company_hk
    left join headquarters on company.company_hk = headquarters.company_hk
)
select * from final
{% if is_incremental() %}
  where incorporation_date > (select coalesce(max(incorporation_date), '1900-01-01') from {{ this }})
{% endif %} 