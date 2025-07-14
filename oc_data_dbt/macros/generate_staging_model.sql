{% macro generate_staging_model(state_code) %}
    {% set state_config = load_state_config(state_code) %}
    {% set field_mappings = state_config.field_mappings %}
    {% set metadata = get_state_metadata(state_code) %}
    
    {{ config(materialized = 'table') }}
    
    WITH final_data AS (
        SELECT 
            -- Metadata columns
            000 as _meta_load_id,
            '{{ run_started_at }}' as _meta_load_timestamp,
            'load_us_{{ state_code.lower() }}_companies' as _meta_load_name,
            '1.0.0' as _meta_load_version,
            'METADATA$FILENAME' as _meta_stg_file_name,
            'METADATA$FILE_LAST_MODIFIED 'as _meta_stg_file_last_modified,
            'METADATA$FILE_ROW_NUMBER' as _meta_stg_file_row_number,
            'METADATA$FILE_CONTENT_KEY' AS _meta_stg_file_hash,
            '{{ metadata.source_system }}' as _meta_source_system,
            '{{ metadata.source_entity }}' as _meta_source_entity,
            'US' as _meta_country,
            '{{ metadata.jurisdiction_code }}' as _meta_jurisdiction,
            '{{ metadata.registration_authority_code }}' as _meta_registration_authority_code,
            
            -- Dynamic business columns based on {{ state_code.upper() }} state configuration
            {% for mapping in field_mappings %}
            {{ mapping.source_field }} as {{ mapping.target_field }}{% if not loop.last %},{% endif %}
            {% endfor %}
            
        FROM {{ ref('stg_us_' ~ state_code.lower() ~ '_companies_raw') }}
    )
    
    SELECT * from final_data
{% endmacro %} 