/*
  Migrated from: Formats/banking_formats.sas — value $LNPURP
*/

{% macro format_loan_purpose(column) %}
case {{ column }}
    when 'PURCH'  then 'Purchase'
    when 'REFI'   then 'Refinance'
    when 'CASHOUT' then 'Cash-Out Refinance'
    when 'CONST'  then 'Construction'
    when 'RENO'   then 'Renovation'
    when 'CONSOL' then 'Debt Consolidation'
    when 'EDUC'   then 'Education'
    when 'MEDIC'  then 'Medical'
    else 'Other'
end
{% endmacro %}
