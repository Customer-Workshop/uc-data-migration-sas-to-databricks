/*
  Reconciliation test: mart_customer_pnl completeness.

  The SAS program (customer_profitability.sas, Step 1) groups from
  STG_BANK.CUST_ACCOUNTS_DAILY by CUSTOMER_ID (snapshot_date = month_end).
  The dbt model uses stg_cust_accounts (which already filters active accounts).

  This control verifies: the mart has exactly one row per distinct customer in
  the staging accounts — no silent row loss and no fan-out.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_customers as (
    select count(distinct customer_id) as n
    from {{ ref('stg_cust_accounts') }}
),

model_customers as (
    select count(*) as n from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_customers,
    m.n as model_customers,
    m.n - e.n as difference
from expected_customers e
cross join model_customers m
where e.n <> m.n
