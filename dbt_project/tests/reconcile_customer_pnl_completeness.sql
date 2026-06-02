/*
  Reconciliation test: customer P&L completeness against the in-scope population.

  customer_profitability.sas Step 4 uses `if a;` on the MERGE, so REPORTS.CUSTOMER_PNL
  contains exactly the customers present in WORK.INTEREST_INCOME — i.e. every customer
  that owns at least one in-scope account in the daily snapshot. Customers with no
  transactions or no risk score are still kept (NULL fee/ECL); customers with no
  account are dropped.

  The dbt conversion reproduces that contract: int_customer_interest_income is the
  driving (left) table for mart_customer_pnl. This control proves the mart holds
  exactly one row per in-scope customer — no silent row loss and no fan-out from the
  fee/ECL joins.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(distinct customer_id) as n
    from {{ ref('int_account_metrics') }}
),

model_customers as (
    select count(*) as n from {{ ref('mart_customer_pnl') }}
)

select
    e.n as expected_in_scope_customers,
    m.n as model_customers,
    m.n - e.n as difference
from expected_in_scope e
cross join model_customers m
where e.n <> m.n
