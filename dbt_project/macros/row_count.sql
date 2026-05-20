/*
  row_count.sql
  Migrated from: Macro/nobs.sas — %nobs observation count utility

  SAS %nobs returns the number of observations in a dataset, either
  from the descriptor (fast) or via PROC SQL (when a WHERE clause is
  applied). Every SAS program calls %nobs as a data quality gate
  (e.g., abort if zero rows, warn if exceeding threshold).

  dbt equivalent: a Jinja macro returning a SQL subquery that yields
  the row count. Can be used in model SQL for conditional logic or in
  post-hooks for logging.

  Usage in a model (inline count for a threshold check):
    select * from {{ ref('stg_cust_accounts') }}
    where {{ row_count(ref('stg_cust_accounts')) }} > 0

  Usage in a run-operation or post-hook (log the count):
    {{ log_row_count(ref('stg_cust_accounts'), 'stg_cust_accounts') }}
*/

{% macro row_count(relation) %}
    (select count(*) from {{ relation }})
{% endmacro %}

{% macro log_row_count(relation, model_name) %}
    {%- set count_query -%}
        select count(*) as row_count from {{ relation }}
    {%- endset -%}

    {%- if execute -%}
        {%- set results = run_query(count_query) -%}
        {%- set row_count_val = results.columns[0].values()[0] -%}
        {{ log(model_name ~ ": " ~ row_count_val ~ " rows", info=true) }}

        {%- if row_count_val == 0 -%}
            {{ log("WARNING: " ~ model_name ~ " has 0 rows — check upstream data.", info=true) }}
        {%- elif row_count_val > var('max_obs_warn', 10000000) -%}
            {{ log("WARNING: " ~ model_name ~ " has " ~ row_count_val ~ " rows — exceeds threshold.", info=true) }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
