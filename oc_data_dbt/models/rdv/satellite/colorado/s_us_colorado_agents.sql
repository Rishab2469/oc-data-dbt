{{ config(
    materialized='incremental',
    catalog_name='bigquery_iceberg_catalog',
    unique_key='DOS_ID',
    incremental_strategy='merge'
) }}

{%- set source_model = "stg_us_colorado_companies_raw" -%}
{%- set src_pk = ["entity_id"] -%}
{%- set src_hashdiff = [
    'agent_first_name',
    'agent_middle_name',
    'agent_last_name',
    'agent_suffix',
    'agent_organization_name'
] -%}
{%- set src_ldts = "_meta_load_timestamp" -%}
{%- set src_source = "_meta_source_system" -%}

{{ automate_dv.sat(
    source_model=source_model,
    src_pk=src_pk,
    src_hashdiff=src_hashdiff,
    src_ldts=src_ldts,
    src_source=src_source
) }} 