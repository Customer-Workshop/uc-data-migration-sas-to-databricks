/*
  validate_required_var.sql
  Migrated from: Macro/parmv.sas — %parmv parameter validation utility

  SAS %parmv validates macro parameters at runtime, sets parmerr=1 and
  logs an ERROR when a required parameter is missing or has an invalid
  value. Every SAS program in the estate calls %parmv as its entry guard.

  dbt equivalent: compile-time assertion using Jinja. Raises a
  compilation error when a required dbt variable is missing or empty.

  Usage:
    {{ validate_required_var('report_month', var('report_month', '')) }}

  With allowed values:
    {{ validate_required_var('environment', var('environment', ''), ['dev', 'staging', 'prod']) }}
*/

{% macro validate_required_var(var_name, var_value, allowed_values=none) %}
    {%- if var_value is none or var_value | string | trim | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "Required variable '" ~ var_name ~ "' is missing or empty. "
            ~ "Set it via dbt_project.yml vars or --vars flag."
        ) }}
    {%- endif -%}

    {%- if allowed_values is not none -%}
        {%- if var_value not in allowed_values -%}
            {{ exceptions.raise_compiler_error(
                "Variable '" ~ var_name ~ "' has invalid value '" ~ var_value ~ "'. "
                ~ "Allowed values: " ~ allowed_values | join(', ')
            ) }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
