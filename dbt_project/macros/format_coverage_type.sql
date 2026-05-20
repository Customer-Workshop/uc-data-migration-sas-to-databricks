/*
  Migrated from: Formats/insurance_formats.sas — value $COVTYPE
*/

{% macro format_coverage_type(column) %}
case {{ column }}
    when 'COMP' then 'Comprehensive'
    when 'COLL' then 'Collision'
    when 'LIAB' then 'Liability'
    when 'PIP'  then 'Personal Injury Protection'
    when 'UMBI' then 'Uninsured Motorist BI'
    when 'UMPD' then 'Uninsured Motorist PD'
    when 'MED'  then 'Medical Payments'
    when 'TOW'  then 'Towing'
    when 'RENT' then 'Rental Reimbursement'
    else 'Other'
end
{% endmacro %}
