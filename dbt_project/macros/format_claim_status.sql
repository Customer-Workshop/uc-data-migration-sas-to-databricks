/*
  format_claim_status.sql
  Migrated from: Formats/insurance_formats.sas — value $CLMSTAT

  SAS PROC FORMAT creates a format catalog entry. The dbt equivalent is a
  Jinja macro returning a CASE expression. Reproduced value-for-value from
  the SAS $CLMSTAT catalog (including the OTHER -> 'Unknown' catch-all).
  Usage: {{ format_claim_status('claim_status') }}
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
