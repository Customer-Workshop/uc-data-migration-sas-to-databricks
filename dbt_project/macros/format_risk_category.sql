/*
  format_risk_category — Replacement for SAS PROC FORMAT $RISKCAT.
  Source: ts-sas-legacy-analytics/Formats/insurance_formats.sas

  Maps risk category codes to human-readable descriptions.
*/

{% macro format_risk_category(column) %}
case {{ column }}
    when 'STD'  then 'Standard'
    when 'PREF' then 'Preferred'
    when 'SPRM' then 'Super Preferred'
    when 'SUB'  then 'Substandard'
    when 'DEC'  then 'Declined'
    else 'Unrated'
end
{% endmacro %}
