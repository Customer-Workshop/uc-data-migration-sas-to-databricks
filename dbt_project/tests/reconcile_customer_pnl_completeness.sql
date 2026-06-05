/*
  Reconciliation test: mart_customer_pnl covers all customers with accounts.

  The SAS program (customer_profitability.sas, Step 1) aggregates by
  CUSTOMER_ID from account data. The dbt model (mart_customer_pnl) must
  have one row per customer with active accounts — no silent row loss
  or fan-out.

  Fails if this query returns any rows.
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
