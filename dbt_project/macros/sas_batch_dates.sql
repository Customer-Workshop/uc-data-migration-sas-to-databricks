/*
  sas_batch_dates.sql — the autoexec.sas macro variables as dbt helpers.

  SAS (Config/autoexec.sas):
    %let CURR_DT = %sysfunc(today(), date9.);            -> var('curr_dt')  (YYYY-MM-DD)
    %let PREV_YM = %sysfunc(intnx(month, today(), -1), yymmn6.); -> sas_report_month() default
                                                   (month before curr_dt, YYYYMM)
  Programs take these as parameters (run_date=, txn_date=, score_date=,
  report_month=), so a batch can be re-run for a fixed business date with
      dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401"}'

  monthly_regulatory_reporting.sas period setup:
    %let month_start = %sysfunc(inputn(&report_month.01, yymmdd8.), date9.);
    %let month_end   = %sysfunc(intnx(month, "&month_start"d, 0, E), date9.);
    %let rpt_label   = %substr(&report_month,1,4)-%substr(&report_month,5,2);
*/

{# "&run_date"d — the batch business date as a SQL date literal #}
{% macro sas_run_date() -%}
to_date('{{ var("curr_dt") }}')
{%- endmacro %}

{# &report_month — explicit var wins, otherwise &PREV_YM: the month before the
   batch business date (SAS derives both from today()). A blank value counts as
   unset so a job parameter can be left empty. #}
{% macro sas_report_month() -%}
{%- set rm = var('report_month', '') | string | trim -%}
{%- if rm -%}
{{ rm }}
{%- else -%}
{%- set d = modules.datetime.date.fromisoformat(var('curr_dt') | string) -%}
{{ (d.replace(day=1) - modules.datetime.timedelta(days=1)).strftime('%Y%m') }}
{%- endif -%}
{%- endmacro %}

{# Truthiness of a var that may arrive as a string from a job parameter ("true"/"false") #}
{% macro sas_var_is_true(name, default=false) -%}
{{ return((var(name, default) | string | lower) in ['true', '1', 'yes']) }}
{%- endmacro %}

{# "&month_start"d #}
{% macro sas_month_start() -%}
to_date('{{ sas_report_month() }}01', 'yyyyMMdd')
{%- endmacro %}

{# "&month_end"d — intnx(month, month_start, 0, E) #}
{% macro sas_month_end() -%}
last_day({{ sas_month_start() }})
{%- endmacro %}

{# &rpt_label — YYYY-MM #}
{% macro sas_rpt_label() -%}
'{{ sas_report_month()[0:4] }}-{{ sas_report_month()[4:6] }}'
{%- endmacro %}

{#
  intck('month', from_date, to_date) — SAS counts month *boundaries* crossed,
  not elapsed months, so 31JAN -> 01FEB is 1. Spark's months_between() is a
  fractional elapsed measure and does not match.
#}
{% macro sas_intck_month(from_date, to_date) -%}
(year({{ to_date }}) - year({{ from_date }})) * 12 + (month({{ to_date }}) - month({{ from_date }}))
{%- endmacro %}
