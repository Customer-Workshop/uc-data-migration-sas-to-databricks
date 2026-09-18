{#
  The SAS programs run as of a batch business date (&run_date in
  Config/autoexec_local.sas), not "today". RUN_DATE pins the models to that
  date so their output can be compared with SAS golden outputs; unset, the
  models behave as before and run as of current_date().
#}
{% macro sas_run_date() %}
    {%- set run_date = env_var('RUN_DATE', '') -%}
    {%- if run_date -%}
        date '{{ run_date }}'
    {%- else -%}
        current_date()
    {%- endif -%}
{% endmacro %}
