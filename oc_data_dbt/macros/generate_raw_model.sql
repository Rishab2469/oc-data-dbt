{% macro generate_raw_model(state_code) %}
    {% set state_config = load_state_config(state_code) %}
    {% set state_fields = state_config.state_fields %}
    
    {{ 
        config(
            materialized = 'table',
            post_hook = "
                COPY INTO {{this}}
                FROM @my_s3_stage
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                )
                ON_ERROR = 'CONTINUE'
            "
        ) 
    }}
    
    -- Generated raw model for {{ state_code.upper() }} companies
    SELECT
        -- Dynamic fields based on {{ state_code.upper() }} state configuration
        {% for field in state_fields %}
        CAST(NULL AS VARCHAR) AS {{ field }}{% if not loop.last %},{% endif %}
        {% endfor %}

    LIMIT 0
{% endmacro %} 