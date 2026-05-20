/*
  Migrated from: Formats/banking_formats.sas — value BALRANGE
  Numeric balance to reporting range bucket.
*/

{% macro format_balance_range(column) %}
case
    when {{ column }} < 0 then 'Negative'
    when {{ column }} = 0 then 'Zero'
    when {{ column }} < 1000 then '$0-$999'
    when {{ column }} < 5000 then '$1K-$4,999'
    when {{ column }} < 25000 then '$5K-$24,999'
    when {{ column }} < 100000 then '$25K-$99,999'
    when {{ column }} < 500000 then '$100K-$499,999'
    when {{ column }} >= 500000 then '$500K+'
    else 'Unknown'
end
{% endmacro %}
