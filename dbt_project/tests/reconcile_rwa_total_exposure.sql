/*
  Reconciliation test: RWA total exposure ties to source account balances.

  The SAS regulatory report (monthly_regulatory_reporting.sas, Step 1) sums
  CURRENT_BALANCE from accounts joined to loan_details. The dbt mart
  (mart_regulatory_rwa) must produce the same total exposure — proving the
  risk-weight grouping did not lose or duplicate rows.

  Fails if this query returns any rows.
*/
with source_total as (
    select sum(a.current_balance) as total_exposure
    from {{ ref('int_account_metrics') }} a
),

model_total as (
    select sum(total_exposure) as total_exposure
    from {{ ref('mart_regulatory_rwa') }}
)

select
    s.total_exposure as source_exposure,
    m.total_exposure as model_exposure,
    m.total_exposure - s.total_exposure as difference
from source_total s
cross join model_total m
where abs(coalesce(m.total_exposure, 0) - coalesce(s.total_exposure, 0)) > 0.01
