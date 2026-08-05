/*
  mart_risk_summary.sql
  Migrated from: Programs/Banking/credit_risk_scoring.sas (Step 5)

  PROC MEANS noprint nway emits one row per full
  ACCOUNT_TYPE × NEW_RISK_RATING class cross-classification. GROUP BY
  reproduces that grain and intentionally emits no marginal or subtotal rows.
*/
select
    score_date,
    account_type,
    risk_rating,
    count(*) as n_accounts,
    avg(pd) as avg_pd,
    avg(lgd) as avg_lgd,
    sum(ead) as total_ead,
    sum(expected_loss) as total_el
from {{ ref('mart_risk_scores') }}
where score_date = current_date()
group by score_date, account_type, risk_rating
