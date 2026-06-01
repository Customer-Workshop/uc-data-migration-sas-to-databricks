/*
  Reconciliation test: customer P&L completeness.

  The SAS program (customer_profitability.sas, Step 4) uses:
      MERGE ... BY CUSTOMER_ID; IF A;
  which retains exactly one row per customer that appears in
  INTEREST_INCOME (i.e. has at least one in-scope account).
  The dbt model uses interest_income as the driving CTE with LEFT JOINs,
  producing the same 1:1 customer population.

  This control verifies:
    1. No silent row loss (model count >= expected)
    2. No fan-out from joins (model count <= expected)

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_customers as (
    select count(distinct customer_id) as n
    from {{ ref('int_account_metrics') }}
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
