/*
  Tolerant numeric equality for parity tests. SAS exports numerics with the
  BEST12. format (about 11 significant digits), so exact float equality would
  report false mismatches. Two nulls compare equal; null vs non-null does not.
*/

{% macro approx_equal(a, b, tolerance='0.005') -%}
(({{ a }} is null and {{ b }} is null) or abs(({{ a }}) - ({{ b }})) <= {{ tolerance }})
{%- endmacro %}

{# relative tolerance for ratios / probabilities #}
{% macro approx_equal_rel(a, b, rel_tolerance='0.000001') -%}
(
    ({{ a }} is null and {{ b }} is null)
    or abs(({{ a }}) - ({{ b }})) <= {{ rel_tolerance }} * greatest(abs({{ a }}), abs({{ b }}), 1)
)
{%- endmacro %}
