/*
  Migrated from: Formats/insurance_formats.sas — value $RISKCAT
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
