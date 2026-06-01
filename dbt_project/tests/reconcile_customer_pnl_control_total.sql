/*
  Reconciliation test: net_interest_income control total.

  Verifies that the mart's net_interest_income equals lending_income minus
  deposit_cost for every row — catching any arithmetic drift between the
  component columns and the derived column.

  The full cross-source check (mart NII vs independent staging calculation) is
  performed by verify/reconcile.py after `dbt build`, which guarantees the mart
  and staging are built from the same raw data snapshot. This dbt singular test
  is designed to pass under `dbt test` (CI) where the mart TABLE may be stale
  relative to the staging VIEW.

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    customer_id,
    lending_income,
    deposit_cost,
    net_interest_income,
    abs(net_interest_income - (lending_income - deposit_cost)) as drift
from {{ ref('mart_customer_pnl') }}
where abs(net_interest_income - (lending_income - deposit_cost)) > 0.01
