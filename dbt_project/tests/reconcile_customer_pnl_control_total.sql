/*
  Reconciliation test: customer P&L control total.

  Independently recompute total_revenue from the source tables and compare
  against the mart aggregate. A mismatch indicates the conversion diverged
  from the SAS calculation (interest income + fee income).

  SAS formula (customer_profitability.sas, Step 4):
    TOTAL_REVENUE = NET_INTEREST_INCOME + FEE_INCOME
  where NET_INTEREST_INCOME = LENDING_INCOME - DEPOSIT_COST.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with independent_revenue as (
    -- Recompute lending_income and deposit_cost directly from int_account_metrics
    select
        sum(case when a.account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
            then a.current_balance * a.interest_rate / 12 else 0 end)
        - sum(case when a.account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
            then a.current_balance * a.interest_rate / 12 else 0 end)
        as total_nii
    from {{ ref('int_account_metrics') }} a
),

in_scope_customers as (
    select distinct customer_id
    from {{ ref('int_account_metrics') }}
),

independent_fees as (
    -- Scoped to the in-scope customer population, matching SAS IF A semantics
    select
        sum(case when t.transaction_type = 'FEE'
            then abs(t.transaction_amount) else 0 end) as total_fees
    from {{ ref('mart_daily_transactions') }} t
    inner join in_scope_customers c
        on t.customer_id = c.customer_id
),

expected as (
    select
        coalesce(nii.total_nii, 0) + coalesce(f.total_fees, 0) as expected_total_revenue
    from independent_revenue nii
    cross join independent_fees f
),

model_total as (
    select sum(total_revenue) as model_total_revenue
    from {{ ref('mart_customer_pnl') }}
)

select
    e.expected_total_revenue,
    m.model_total_revenue,
    abs(m.model_total_revenue - e.expected_total_revenue) as abs_difference
from expected e
cross join model_total m
where abs(m.model_total_revenue - e.expected_total_revenue) > 0.01
