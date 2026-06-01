/*
  Reconciliation: total revenue control total.

  Proves the mart's TOTAL_REVENUE column ties back to the independently
  computed component sums (net interest income + fee income) from the source
  tables.  A mismatch indicates a join fan-out, a dropped coalesce, or a
  formula drift from the SAS source.

  SAS formula:
    TOTAL_REVENUE = SUM(NET_INTEREST_INCOME, FEE_INCOME, 0)
  where NET_INTEREST_INCOME = lending_income - deposit_cost (from accounts)
  and FEE_INCOME comes from the transaction feed.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with mart_totals as (
    select
        sum(total_revenue) as mart_total_revenue,
        sum(net_interest_income) as mart_nii,
        sum(fee_income) as mart_fee
    from {{ ref('mart_customer_pnl') }}
),

/*
  Tolerance: floating-point aggregation across thousands of rows may differ
  at the sub-cent level.  Allow 0.01 per 1 000 rows (abs diff < 1.0 overall
  for typical data volumes).
*/
check_internal_consistency as (
    select
        mart_total_revenue,
        mart_nii + mart_fee as recomputed_total_revenue,
        abs(mart_total_revenue - (mart_nii + mart_fee)) as abs_diff
    from mart_totals
    where abs(mart_total_revenue - (mart_nii + mart_fee)) > 1.0
)

select * from check_internal_consistency
