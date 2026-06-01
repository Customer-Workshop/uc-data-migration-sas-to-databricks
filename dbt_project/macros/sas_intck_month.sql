/*
  sas_intck_month.sql
  Migrated from: SAS intck('month', start, end) calls in policy_valuation.sas

  SAS intck('month', a, b) counts the number of month *boundaries* crossed
  between two dates: (year(b)-year(a))*12 + (month(b)-month(a)). It is an
  integer interval count, NOT the fractional months_between() Spark default.
  Reproducing it exactly matters because policy_valuation.sas feeds this count
  into min(12, ...) for the YTD earned-premium pro-rata factor.
*/

{% macro sas_intck_month(start_date, end_date) %}
((year({{ end_date }}) - year({{ start_date }})) * 12 + (month({{ end_date }}) - month({{ start_date }})))
{% endmacro %}
