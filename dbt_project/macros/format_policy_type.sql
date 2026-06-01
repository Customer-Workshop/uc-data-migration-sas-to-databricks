/*
  format_policy_type.sql
  Migrated from: Formats/insurance_formats.sas — value $POLTYPE

  SAS PROC FORMAT creates a format catalog entry; the dbt equivalent is a Jinja
  macro returning a CASE expression. Usage: {{ format_policy_type('policy_type') }}

  Reproduced value-for-value from $POLTYPE, INCLUDING the OTHER catch-all
  ('Unknown'). This is source-faithful, not an endorsement: the synthetic
  RAW_INS.POLICIES uses the codes LIFE / HEALTH / COMMERCIAL, none of which exist
  in the legacy $POLTYPE catalog (note the catalog spells health 'HLTH', not
  'HEALTH'), so they decode to 'Unknown'. That divergence lives in the source
  data, so it is flagged here rather than silently "corrected".
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
