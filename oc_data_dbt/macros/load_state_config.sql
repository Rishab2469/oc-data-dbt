{% macro load_state_config(state_code) %}
    {% if state_code.lower() == 'ny' %}
        {% set config = load_ny_config() %}
    {% else %}
        {% set config = {} %}
    {% endif %}
    {{ return(config) }}
{% endmacro %}

{% macro get_state_fields(state_code) %}
    {% set config = load_state_config(state_code) %}
    {{ return(config.state_fields) }}
{% endmacro %}

{% macro get_field_mappings(state_code) %}
    {% set config = load_state_config(state_code) %}
    {{ return(config.field_mappings) }}
{% endmacro %}

{% macro get_state_metadata(state_code) %}
    {% set config = load_state_config(state_code) %}
    {{ return({
        'state_code': config.state_code,
        'jurisdiction_code': config.jurisdiction_code,
        'registration_authority_code': config.registration_authority_code,
        'source_system': config.source_system,
        'source_entity': config.source_entity
    }) }}
{% endmacro %} 