/*
  Reconciliation test: customer_pnl completeness.

  The SAS P&L (customer_profitability.sas Step 4) keeps only customers present in
  INTEREST_INCOME (the `if a;` on the MERGE). INTEREST_INCOME is built from
  STG_BANK.CUST_ACCOUNTS_DAILY at month end — in dbt this maps to
  stg_cust_accounts (all in-scope active accounts).

  This control verifies the mart has exactly one row per distinct customer in the
  account-holding population — no silent row loss and no fan-out.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_customers as (
    select count(distinct customer_id) as n
    from {{ ref('stg_cust_accounts') }}
),

model_customers as (
    select count(*) as n from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_customer_count,
    m.n as model_customer_count,
    m.n - e.n as difference
from expected_customers e
cross join model_customers m
where e.n <> m.n
