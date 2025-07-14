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
valid_officers as (
    select * from {{ ref('s_us_ny_officers') }}
    where ceo_name is not null
      and ceo_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')
),
officers_agg as (
    select
        company_hk,
        array_agg(
            object_construct(
                'name', ceo_name,
                'position', 'chief executive officer',
                'address', object_construct(
                    'street_address', 
                        case 
                            when ceo_address_1 is not null and ceo_address_2 is not null and ceo_address_2 != ''
                                then ceo_address_1 || ', ' || ceo_address_2
                            else coalesce(ceo_address_1, ceo_address_2)
                        end,
                    'locality', ceo_city,
                    'region', ceo_state,
                    'postal_code', ceo_zip
                )
            )
        ) as officers
    from valid_officers
    group by company_hk
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
-- Registered address: Join to staging to extract dos_process fields for each company
registered_address as (
    select
        c.company_hk,
        case when s.dos_process_name is not null then
            object_construct(
                'care_of', s.dos_process_name,
                'street_address', 
                    case 
                        when s.dos_process_address_1 is not null and s.dos_process_address_2 is not null and s.dos_process_address_2 != ''
                            then s.dos_process_address_1 || ', ' || s.dos_process_address_2
                        else coalesce(s.dos_process_address_1, s.dos_process_address_2)
                    end,
                'locality', s.dos_process_city,
                'region', s.dos_process_state,
                'postal_code', s.dos_process_zip
            )
        else null end as registered_address
    from company c
    left join {{ ref('stg_us_ny_companies_structured') }} s
        on c.entity_id = s.entity_id
),
-- Registered addresses: Location and address details
addresses as (
    select * from {{ ref('s_us_ny_registered_addresses') }}
),
-- Final output: Join all CTEs and select canonical company structure
final as (
    select
        company.company_hk,
        company.entity_id,
        company.current_entity_name as name,
        company.entity_type as company_type,
        company.initial_dos_filing_date as incorporation_date,
        company.jurisdiction as jurisdiction_code,
        company.county,
        registered_address.registered_address,
        officers_agg.officers,
        registered_agents_agg.agents,
        object_construct(
            'county', company.county,
            'location_address_1', addresses.location_address_1,
            'location_address_2', addresses.location_address_2,
            'location_city', addresses.location_city,
            'location_name', addresses.location_name,
            'location_state', addresses.location_state,
            'location_zip', addresses.location_zip
        ) as all_attributes,
        company._meta_load_timestamp,
        hub._meta_source_system,
        hub._meta_jurisdiction,
        hub._meta_registration_authority_code
    from company
    left join hub on company.company_hk = hub.company_hk
    left join officers_agg on company.company_hk = officers_agg.company_hk
    left join registered_agents_agg on company.company_hk = registered_agents_agg.company_hk
    left join registered_address on company.company_hk = registered_address.company_hk
    left join addresses on company.company_hk = addresses.company_hk
)
select * from final
{% if is_incremental() %}
  where incorporation_date > (select coalesce(max(incorporation_date), '1900-01-01') from {{ this }})
{% endif %} 