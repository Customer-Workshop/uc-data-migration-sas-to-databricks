/*
  Reconciliation test: delinquency total balance ties to the in-scope
  int_account_metrics population. total_past_due is intentionally excluded:
  PAST_DUE_AMOUNT is unavailable in the raw estate and is emitted as zero.
*/
with expected as (
    select sum(current_balance) as total_balance
    from {{ ref('int_account_metrics') }}
    where account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
),

actual as (
    select sum(total_balance) as total_balance
    from {{ ref('mart_delinquency_aging') }}
)

select
    e.total_balance as expected_total_balance,
    a.total_balance as model_total_balance
from expected e
cross join actual a
where abs(coalesce(e.total_balance, 0) - coalesce(a.total_balance, 0)) > 0.01
