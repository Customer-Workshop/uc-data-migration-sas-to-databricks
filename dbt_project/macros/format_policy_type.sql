/*
  format_policy_type.sql
  Migrated from: ts-sas-legacy-analytics/Formats/insurance_formats.sas
                 — value $POLTYPE

  SAS PROC FORMAT creates a format catalog entry ($POLTYPE) that is applied
  to POLICY_TYPE in policy_valuation.sas (Step 4 `format POLICY_TYPE $POLTYPE.`).
  dbt equivalent: a Jinja macro returning a CASE expression.

  The original $POLTYPE catalog plus the codes present in the migrated source
  data (LIFE / HEALTH / COMMERCIAL) are mapped here. The catch-all `else`
  reproduces the SAS `OTHER = 'Unknown'` branch value-for-value.

  Usage: {{ format_policy_type('policy_type') }}
*/

{% macro format_policy_type(column) %}
case {{ column }}
    when 'WL'         then 'Whole Life'
    when 'TL'         then 'Term Life'
    when 'UL'         then 'Universal Life'
    when 'VL'         then 'Variable Life'
    when 'AUTO'       then 'Auto Insurance'
    when 'HOME'       then 'Homeowners'
    when 'RENT'       then 'Renters'
    when 'UMBR'       then 'Umbrella'
    when 'HLTH'       then 'Health'
    when 'DNTL'       then 'Dental'
    when 'VIS'        then 'Vision'
    when 'DISAB'      then 'Disability'
    when 'LTCI'       then 'Long-Term Care'
    when 'LIFE'       then 'Life'
    when 'HEALTH'     then 'Health'
    when 'COMMERCIAL' then 'Commercial'
    else 'Unknown'
end
{% endmacro %}
