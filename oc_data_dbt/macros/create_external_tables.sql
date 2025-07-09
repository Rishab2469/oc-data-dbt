{% macro create_external_tables() %}
    {% set config = load_yaml('data/external_tables.yml') %}
    {% for tbl in config['external_tables'] %}
        {{ log('Creating external table: ' ~ tbl.table_name, info=True) }}
        CREATE OR REPLACE EXTERNAL TABLE {{ tbl.schema or target.schema }}.{{ tbl.table_name }} (
            {{ tbl.columns }}
        )
        LOCATION = '{{ tbl.location }}'
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    {% endfor %}
{% endmacro %}
