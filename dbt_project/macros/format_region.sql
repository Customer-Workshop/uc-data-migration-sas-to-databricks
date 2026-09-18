/*
  Migrated from: Formats/banking_formats.sas — value $REGION
*/

{% macro format_region(column) %}
case {{ column }}
    when 'NE' then 'Northeast'
    when 'SE' then 'Southeast'
    when 'MW' then 'Midwest'
    when 'SW' then 'Southwest'
    when 'W'  then 'West'
    when 'NW' then 'Northwest'
    when 'HQ' then 'Headquarters'
    else 'Unknown'
end
{% endmacro %}
