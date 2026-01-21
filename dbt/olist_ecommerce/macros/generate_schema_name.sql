-- This macro generates the schema name for dbt models.
-- If a custom schema name is provided, it uses that directly.
-- Otherwise, it defaults to the target schema defined in the profile.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
       If a custom schema (like 'staging') is defined in dbt_project.yml, 
       use it EXACTLY. Do not append it to the profile schema.
    #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}