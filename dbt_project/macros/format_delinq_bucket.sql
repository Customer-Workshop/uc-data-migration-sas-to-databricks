/*
  Migrated from: Formats/banking_formats.sas — value DELQBKT (numeric range format
  on DAYS_PAST_DUE). Ranges are inclusive, like SAS; a missing value falls through
  to the SAS default (the raw value printed), which we render as null.
*/

{% macro format_delinq_bucket(column) %}
case
    when {{ column }} = 0 then 'Current'
    when {{ column }} between 1 and 29 then '1-29 Days'
    when {{ column }} between 30 and 59 then '30-59 Days'
    when {{ column }} between 60 and 89 then '60-89 Days'
    when {{ column }} between 90 and 119 then '90-119 Days'
    when {{ column }} between 120 and 179 then '120-179 Days'
    when {{ column }} >= 180 then '180+ Days'
    else null
end
{% endmacro %}
