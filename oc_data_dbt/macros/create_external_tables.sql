{% macro run_copy_jobs() %}
    {% set config = load_yaml('data/copy_jobs.yml') %}
    {% for job in config['copy_jobs'] %}
        {{ log('Running COPY INTO for: ' ~ job.target_table, info=True) }}
        COPY INTO {{ job.target_schema }}.{{ job.target_table }}
        FROM (
            SELECT
                {% if job.columns is defined %}
                    {{ job.columns }}
                {% else %}
                    000 as _meta_load_id,
                    {{ run_started_at }} as _meta_load_timestamp,
                    '<loadname>' as _meta_load_name,
                    '<packageversion>' as _meta_load_version,
                    'METADATA$FILENAME' as _meta_stg_file_name,
                    'METADATA$FILE_LAST_MODIFIED 'as _meta_stg_file_last_modified,
                    'METADATA$FILE_ROW_NUMBER' as _meta_stg_file_row_number,
                    'METADATA$FILE_CONTENT_KEY' AS _meta_stg_file_hash,
                    '<source_system_name>' as _meta_source_system,
                    '<source_entity_name>' as _meta_source_entity,
                    'US' as _meta_country,
                    'us_fl' as _meta_jurisdiction,
                    'RA000603' as _meta_registration_authority_code,
                    $1 AS FULL_ROW_CSV
                {% endif %}
            FROM {{ job.source_stage }}
        )
        FILE_FORMAT = (FORMAT_NAME = '{{ job.file_format }}')
        PATTERN = '{{ job.pattern }}'
        FORCE = {{ job.force | upper }};
    {% endfor %}
{% endmacro %}