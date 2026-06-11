-- macros/cents_to_currency.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Utility macro: rounds a numeric column to 2 decimal places.
-- Usage: {{ cents_to_currency('price') }}
-- ─────────────────────────────────────────────────────────────────────────────

{% macro cents_to_currency(column_name, precision=2) %}
    round(cast({{ column_name }} as float), {{ precision }})
{% endmacro %}


-- macros/generate_schema_name.sql is defined below.
-- ─────────────────────────────────────────────────────────────────────────────
-- Custom schema name generator:
-- In PROD: use the schema configured in dbt_project.yml as-is.
-- In DEV:  prefix schema with 'DEV_' (e.g. STAGING → DEV_STAGING).
-- This ensures dev and prod models never collide in Snowflake.
-- ─────────────────────────────────────────────────────────────────────────────
