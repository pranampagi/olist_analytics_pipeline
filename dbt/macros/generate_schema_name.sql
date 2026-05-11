-- macros/generate_schema_name.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Overrides dbt's default schema name generator so that:
--   prod target → schema names exactly as specified in dbt_project.yml
--   dev  target → schema names prefixed with DEV_
--
-- Example:
--   prod: STAGING, INTERMEDIATE, MARTS
--   dev:  DEV_STAGING, DEV_INTERMEDIATE, DEV_MARTS
--
-- This prevents dev runs from overwriting production tables.
-- ─────────────────────────────────────────────────────────────────────────────

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' -%}
        {# In production: use the custom schema name from dbt_project.yml exactly #}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim | upper }}
        {%- else -%}
            {{ default_schema | trim | upper }}
        {%- endif -%}

    {%- else -%}
        {# In dev / CI: prefix all schemas with DEV_ to isolate from production #}
        {%- if custom_schema_name is not none -%}
            DEV_{{ custom_schema_name | trim | upper }}
        {%- else -%}
            DEV_{{ default_schema | trim | upper }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
