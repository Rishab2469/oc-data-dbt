{{ config(
    materialized='incremental',
    unique_key='company_hk'
) }}

with hub as (
    select * from {{ ref('h_companies_ny') }}
),
sat as (
    select * from {{ ref('s_companies_ny') }}
),
officers as (
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
    from sat
    where ceo_name is not null 
        and ceo_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')
    group by company_hk
),
agents as (
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
    from sat
    where registered_agent_name is not null 
        and registered_agent_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available', 'REGISTERED AGENT REVOKED', 'REGISTERED AGENT RESIGNED')
    group by company_hk
)
select
    sat.company_hk,
    sat.entity_id,
    sat.current_entity_name as name,
    sat.entity_type as company_type,
    sat.initial_dos_filing_date as incorporation_date,
    sat.jurisdiction as jurisdiction_code,
    sat.county,
    case 
    when sat.dos_process_name is not null then
        object_construct(
            'care_of', sat.dos_process_name,
            'street_address', 
                case 
                    when sat.dos_process_address_1 is not null and sat.dos_process_address_2 is not null and sat.dos_process_address_2 != ''
                        then sat.dos_process_address_1 || ', ' || sat.dos_process_address_2
                    else coalesce(sat.dos_process_address_1, sat.dos_process_address_2)
                end,
            'locality', sat.dos_process_city,
            'region', sat.dos_process_state,
            'postal_code', sat.dos_process_zip
        )
    else null
    end as registered_address,
    officers.officers,
    agents.agents,
    object_construct(
        'county', sat.county,
        'location_address_1', sat.location_address_1,
        'location_address_2', sat.location_address_2,
        'location_city', sat.location_city,
        'location_name', sat.location_name,
        'location_state', sat.location_state,
        'location_zip', sat.location_zip
    ) as all_attributes,
    sat._meta_load_timestamp,
    hub._meta_source_system,
    hub._meta_jurisdiction,
    hub._meta_registration_authority_code
from sat
left join hub on sat.company_hk = hub.company_hk
left join officers on sat.company_hk = officers.company_hk
left join agents on sat.company_hk = agents.company_hk
where
    sat.current_entity_name is not null
    and sat.current_entity_name not in ('', 'none', 'n/a', '.', ',', '-', 'no information available')
    and sat.entity_id is not null
    and sat.entity_id not in ('', 'none', 'n/a', '.', ',', '-', 'no information available')
    and sat.initial_dos_filing_date is not null
    and sat.entity_type not in (
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
  and sat.initial_dos_filing_date > (select coalesce(max(incorporation_date), '1900-01-01') from {{ this }})
{% endif %} 