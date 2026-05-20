/*
  Migrated from: Formats/banking_formats.sas — value RISKRATE
  Numeric risk rating (1-7) to human-readable label.
*/

{% macro format_risk_rating(column) %}
case {{ column }}
    when 1 then 'Minimal Risk'
    when 2 then 'Low Risk'
    when 3 then 'Moderate Risk'
    when 4 then 'Elevated Risk'
    when 5 then 'High Risk'
    when 6 then 'Very High Risk'
    when 7 then 'Loss Expected'
    else 'Not Rated'
end
{% endmacro %}
