/*
  Migrated from: Programs/Insurance/claims_processing.sas (Step 2)
  SAS literals are intentionally retained despite the seed's 0..1 scores.
*/

{% macro format_fraud_risk(column) %}
case
    when {{ column }} >= 80 then 'HIGH'
    when {{ column }} >= 50 then 'MEDIUM'
    else 'LOW'
end
{% endmacro %}
