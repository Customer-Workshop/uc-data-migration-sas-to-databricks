/*
  classify_interest_bucket.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 1)

  SAS Original:
    Two account-type IN-lists inside PROC SQL CASE expressions decide whether an
    account contributes to LENDING_INCOME or DEPOSIT_COST:
      LENDING : ('MTG','AUTO','PERS','CC','LOC','HELC')
      DEPOSIT : ('CHK','SAV','MMA','CD','IRA')

  dbt Equivalent:
    A single Jinja macro that is the ONE source of the model's product-type
    classification. The intermediate income model and the parity reconciliation
    control both call this macro, so the per-type mapping is asserted against the
    SAS source value-for-value (see tests/reconcile_customer_pnl_parity.sql).

  Usage: {{ classify_interest_bucket('account_type') }}  -> 'LENDING'|'DEPOSIT'|'NEITHER'
*/

{% macro classify_interest_bucket(column) %}
case
    when {{ column }} in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC') then 'LENDING'
    when {{ column }} in ('CHK', 'SAV', 'MMA', 'CD', 'IRA') then 'DEPOSIT'
    else 'NEITHER'
end
{% endmacro %}
