/*
  Reconciliation test: Step 5 PROC MEANS NWAY emits exactly one row per
  account_type/risk_rating class. This singular test enforces that summary
  grain without relying on dbt_utils.
*/
select
    score_date,
    account_type,
    risk_rating,
    count(*) as duplicate_rows
from {{ ref('mart_risk_summary') }}
group by score_date, account_type, risk_rating
having count(*) <> 1
