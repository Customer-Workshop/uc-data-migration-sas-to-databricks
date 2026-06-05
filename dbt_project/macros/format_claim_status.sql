/*
  Migrated from: Formats/insurance_formats.sas — value $CLMSTAT
*/

{% macro format_claim_status(column) %}
case {{ column }}
    when 'NEW'  then 'New'
    when 'OPEN' then 'Open'
    when 'INV'  then 'Under Investigation'
    when 'ADJ'  then 'Adjusting'
    when 'PEND' then 'Pending Approval'
    when 'APPR' then 'Approved'
    when 'DENY' then 'Denied'
    when 'PAID' then 'Paid'
    when 'CLOS' then 'Closed'
    when 'REOP' then 'Reopened'
    when 'SUSP' then 'Suspended'
    when 'LITI' then 'In Litigation'
    else 'Unknown'
end
{% endmacro %}
