/*
  Reconciliation test (COMPLETENESS): customer_profitability.sas

  SAS Step 1 groups STG_BANK.CUST_ACCOUNTS_DAILY by CUSTOMER_ID, and the Step 4
  MERGE keeps "if a" — i.e. exactly one P&L row per customer that holds at least
  one in-scope account. The converted anchor is int_account_metrics (the same
  in-scope account population reconciled by reconcile_account_completeness).

  This control proves the customer grain is exact: the mart has one row for every
  customer with an account and no others — no silent row loss and no join
  fan-out (which would also break the `unique` test on customer_id). It FAILS if
  the distinct customer counts diverge.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_customers as (
    select count(distinct customer_id) as n
    from {{ ref('int_account_metrics') }}
),

model_customers as (
    select count(*) as n
    from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_customers,
    m.n as model_customers,
    m.n - e.n as difference
from expected_customers e
cross join model_customers m
where e.n <> m.n
