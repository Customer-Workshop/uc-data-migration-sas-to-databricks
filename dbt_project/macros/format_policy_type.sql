/*
  format_policy_type.sql
  Migrated from: Formats/insurance_formats.sas — value $POLTYPE

  SAS PROC FORMAT creates a format catalog entry.
  dbt equivalent: a Jinja macro returning a CASE expression.
  Usage: {{ format_policy_type('policy_type') }}

  NOTE: The SAS format includes policy types (WL, TL, UL, VL, DNTL, VIS,
  DISAB, LTCI, RENT, UMBR) that do not appear in the Databricks seed data
  (AUTO, HOME, LIFE, HEALTH, COMMERCIAL). All branches are preserved
  source-faithfully; the catch-all maps unmapped codes to 'Unknown'.
*/

{% macro format_policy_type(column) %}
case {{ column }}
    when 'WL'    then 'Whole Life'
    when 'TL'    then 'Term Life'
    when 'UL'    then 'Universal Life'
    when 'VL'    then 'Variable Life'
    when 'AUTO'  then 'Auto Insurance'
    when 'HOME'  then 'Homeowners'
    when 'RENT'  then 'Renters'
    when 'UMBR'  then 'Umbrella'
    when 'HLTH'  then 'Health'
    when 'DNTL'  then 'Dental'
    when 'VIS'   then 'Vision'
    when 'DISAB' then 'Disability'
    when 'LTCI'  then 'Long-Term Care'
    when 'LIFE'  then 'Life'
    when 'HEALTH' then 'Health Insurance'
    when 'COMMERCIAL' then 'Commercial'
    else 'Unknown'
end
{% endmacro %}
