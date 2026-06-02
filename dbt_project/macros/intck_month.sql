/*
  intck_month.sql

  Faithful replacement for the SAS function intck('month', from, to).

  SAS intck('month', a, b) counts the number of month boundaries crossed
  going from a to b:
      intck('month', a, b) = (year(b) - year(a)) * 12 + (month(b) - month(a))

  Databricks' months_between() returns a *fractional* difference based on the
  day-of-month, so cast(months_between(...) as int) does NOT reproduce SAS
  intck at month edges (e.g. Jan-31 -> Feb-01 is 1 boundary in SAS but ~0.03
  via months_between). This macro reproduces the SAS boundary-count semantics
  exactly using year/month arithmetic.

  Usage: {{ intck_month('start_date_expr', 'end_date_expr') }}
*/

{% macro intck_month(from_date, to_date) %}
(year({{ to_date }}) - year({{ from_date }})) * 12
    + (month({{ to_date }}) - month({{ from_date }}))
{% endmacro %}
