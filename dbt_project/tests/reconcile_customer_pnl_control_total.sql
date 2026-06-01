/*
  Reconciliation test: P&L assembly control totals.

  Verifies the arithmetic integrity of the P&L assembly (SAS Step 4):
    1. total_revenue = net_interest_income + fee_income
    2. net_profit = total_revenue - operating_cost - total_ecl

  The full cross-source NII check (mart vs independent staging calculation) is
  performed by verify/reconcile.py after `dbt build`, which guarantees the mart
  and staging are built from the same raw data snapshot.

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    customer_id,
    net_interest_income,
    fee_income,
    total_revenue,
    operating_cost,
    total_ecl,
    net_profit
from {{ ref('mart_customer_pnl') }}
where
    abs(total_revenue - (net_interest_income + fee_income)) > 0.01
    or abs(net_profit - (total_revenue - operating_cost - total_ecl)) > 0.01
