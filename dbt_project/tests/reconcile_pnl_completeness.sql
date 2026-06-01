/*
  Reconciliation test: customer P&L completeness.

  The SAS MERGE with "if a" keeps every customer from INTEREST_INCOME
  (i.e. every customer with at least one active account in
  STG_BANK.CUST_ACCOUNTS_DAILY). The dbt model must produce exactly one
  row per customer — no silent row loss, no fan-out from the LEFT JOINs.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select count(distinct customer_id) as n
    from {{ ref('int_account_metrics') }}
),

actual as (
    select count(*) as n from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_customers,
    a.n as model_customers,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
