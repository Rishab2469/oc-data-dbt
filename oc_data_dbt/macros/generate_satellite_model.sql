{% macro generate_satellite_model(state_code) %}
    {{ config(
        materialized='incremental',
        unique_key='company_hk'
    ) }}
    
    {{ automate_dv.sat(
        source_model=ref('stg_us_' ~ state_code.lower() ~ '_companies_structured'),
        src_pk='entity_id',
        src_hashdiff='_meta_dv_datahash',
        src_payload=[
            'current_entity_name',
            'initial_dos_filing_date',
            'county',
            'jurisdiction',
            'entity_type',
            'dos_process_name',
            'dos_process_address_1',
            'dos_process_address_2',
            'dos_process_city',
            'dos_process_state',
            'dos_process_zip',
            'ceo_name',
            'ceo_address_1',
            'ceo_address_2',
            'ceo_city',
            'ceo_state',
            'ceo_zip',
            'registered_agent_name',
            'registered_agent_address_1',
            'registered_agent_address_2',
            'registered_agent_city',
            'registered_agent_state',
            'registered_agent_zip',
            'location_name',
            'location_address_1',
            'location_address_2',
            'location_city',
            'location_state',
            'location_zip'
        ],
        src_ldts='_meta_load_timestamp',
        src_source='_meta_source_system'
    ) }}
{% endmacro %} 