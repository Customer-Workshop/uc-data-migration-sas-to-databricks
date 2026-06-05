/*
  Migrated from: Formats/insurance_formats.sas — value $POLTYPE
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
    else 'Unknown'
end
{% endmacro %}
