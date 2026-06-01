/*
  Reconciliation control: delinquency aging control totals tie out to source.

  Two SUMs must reconcile to the in-scope (credit-product) population of
  int_account_metrics:
    1. total_balance  == sum(current_balance)
    2. total_past_due == sum(past_due_amount)

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected as (
    select
        sum(current_balance) as total_balance,
        sum(coalesce(past_due_amount, 0)) as total_past_due
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

mart as (
    select
        coalesce(sum(total_balance), 0) as total_balance,
        coalesce(sum(total_past_due), 0) as total_past_due
    from {{ ref('mart_delinquency_aging') }}
)

select
    e.total_balance as expected_balance,
    m.total_balance as mart_balance,
    e.total_past_due as expected_past_due,
    m.total_past_due as mart_past_due
from expected e
cross join mart m
where abs(m.total_balance - e.total_balance) > 0.01
   or abs(m.total_past_due - e.total_past_due) > 0.01
