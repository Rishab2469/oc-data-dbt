{% macro load_ny_config() %}
    {% set config = {
        "state_code": "NY",
        "jurisdiction_code": "us_ny",
        "registration_authority_code": "RA000603",
        "source_system": "ny_department_of_state",
        "source_entity": "companies",
        "state_fields": [
            "dos_id",
            "current_entity_name",
            "initial_dos_filing_date",
            "county",
            "jurisdiction",
            "entity_type",
            "dos_process_name",
            "dos_process_address_1",
            "dos_process_address_2",
            "dos_process_city",
            "dos_process_state",
            "dos_process_zip",
            "ceo_name",
            "ceo_address_1",
            "ceo_address_2",
            "ceo_city",
            "ceo_state",
            "ceo_zip",
            "registered_agent_name",
            "registered_agent_address_1",
            "registered_agent_address_2",
            "registered_agent_city",
            "registered_agent_state",
            "registered_agent_zip",
            "location_name",
            "location_address_1",
            "location_address_2",
            "location_city",
            "location_state",
            "location_zip"
        ],
        "field_mappings": [
            {"source_field": "dos_id", "target_field": "entity_id"},
            {"source_field": "current_entity_name", "target_field": "current_entity_name"},
            {"source_field": "initial_dos_filing_date", "target_field": "initial_dos_filing_date"},
            {"source_field": "county", "target_field": "county"},
            {"source_field": "jurisdiction", "target_field": "jurisdiction"},
            {"source_field": "entity_type", "target_field": "entity_type"},
            {"source_field": "dos_process_name", "target_field": "dos_process_name"},
            {"source_field": "dos_process_address_1", "target_field": "dos_process_address_1"},
            {"source_field": "dos_process_address_2", "target_field": "dos_process_address_2"},
            {"source_field": "dos_process_city", "target_field": "dos_process_city"},
            {"source_field": "dos_process_state", "target_field": "dos_process_state"},
            {"source_field": "dos_process_zip", "target_field": "dos_process_zip"},
            {"source_field": "ceo_name", "target_field": "ceo_name"},
            {"source_field": "ceo_address_1", "target_field": "ceo_address_1"},
            {"source_field": "ceo_address_2", "target_field": "ceo_address_2"},
            {"source_field": "ceo_city", "target_field": "ceo_city"},
            {"source_field": "ceo_state", "target_field": "ceo_state"},
            {"source_field": "ceo_zip", "target_field": "ceo_zip"},
            {"source_field": "registered_agent_name", "target_field": "registered_agent_name"},
            {"source_field": "registered_agent_address_1", "target_field": "registered_agent_address_1"},
            {"source_field": "registered_agent_address_2", "target_field": "registered_agent_address_2"},
            {"source_field": "registered_agent_city", "target_field": "registered_agent_city"},
            {"source_field": "registered_agent_state", "target_field": "registered_agent_state"},
            {"source_field": "registered_agent_zip", "target_field": "registered_agent_zip"},
            {"source_field": "location_name", "target_field": "location_name"},
            {"source_field": "location_address_1", "target_field": "location_address_1"},
            {"source_field": "location_address_2", "target_field": "location_address_2"},
            {"source_field": "location_city", "target_field": "location_city"},
            {"source_field": "location_state", "target_field": "location_state"},
            {"source_field": "location_zip", "target_field": "location_zip"}
        ]
    } %}
    {{ return(config) }}
{% endmacro %} 