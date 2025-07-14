{% macro generate_hub_model(state_code) %}
    {{ config(
        materialized='incremental',
        unique_key='company_hk'
    ) }}
    
    {{ automate_dv.hub(
        source_model=ref('stg_us_' ~ state_code.lower() ~ '_companies_structured'),
        src_pk='entity_id',
        src_nk='entity_id',
        src_ldts='_meta_load_timestamp',
        src_source='_meta_source_system'
    ) }}
{% endmacro %} 