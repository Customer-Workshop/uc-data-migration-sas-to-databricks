/*
  Migrated from: Formats/insurance_formats.sas — value LOSSRANGE
  Numeric loss amount to reporting range bucket.
*/

{% macro format_loss_range(column) %}
case
    when {{ column }} < 0 then 'Recovery'
    when {{ column }} = 0 then 'No Loss'
    when {{ column }} < 5000 then '$0-$4,999'
    when {{ column }} < 25000 then '$5K-$24,999'
    when {{ column }} < 100000 then '$25K-$99,999'
    when {{ column }} < 500000 then '$100K-$499,999'
    when {{ column }} >= 500000 then '$500K+'
    else 'Unknown'
end
{% endmacro %}
