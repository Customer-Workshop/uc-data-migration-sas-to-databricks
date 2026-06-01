/*
  Reconciliation: customer P&L completeness.

  The SAS program (customer_profitability.sas, Step 4) uses MERGE BY CUSTOMER_ID
  with IF A, keeping exactly those customers present in the interest_income work
  table, which itself is one row per distinct CUSTOMER_ID from the accounts
  snapshot.  The dbt mart must contain the same population — no silent row loss
  (e.g. from a fanned-out join) and no extra rows.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected as (
    select count(distinct customer_id) as n
    from {{ ref('stg_cust_accounts') }}
),

actual as (
    select count(*) as n
    from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_customers,
    a.n as model_customers,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
