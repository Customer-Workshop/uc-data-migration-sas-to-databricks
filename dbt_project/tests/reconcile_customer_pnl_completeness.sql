/*
  Reconciliation control (completeness): customer_profitability.sas

  SAS Step 4 builds REPORTS.CUSTOMER_PNL via a MERGE with "if a;", i.e. it keeps
  exactly the customers present in WORK.INTEREST_INCOME — one row per customer in
  the in-scope account population. The dbt model reproduces that contract by
  building interest_income from stg_cust_accounts (the documented in-scope
  account master) and LEFT JOINing fees/ECL onto it.

  This control proves the mart has exactly one row per in-scope customer:
    - no silent row loss (mart rows == distinct in-scope customers), and
    - no fan-out from the joins (rows == distinct customer_id in the mart).

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_customers as (
    select count(distinct customer_id) as n
    from {{ ref('stg_cust_accounts') }}
),

mart as (
    select
        count(*) as n_rows,
        count(distinct customer_id) as n_customers
    from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_customers,
    m.n_rows as mart_rows,
    m.n_customers as mart_distinct_customers
from expected_customers e
cross join mart m
where e.n <> m.n_rows
   or m.n_rows <> m.n_customers
