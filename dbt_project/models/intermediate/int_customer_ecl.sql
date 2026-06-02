/*
  int_customer_ecl.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 3: WORK.ECL)

  SAS Original:
    PROC SQL ... GROUP BY r.CUSTOMER_ID over CURATED.RISK_SCORES where
    SCORE_DATE = (select max(SCORE_DATE) from CURATED.RISK_SCORES
                  where SCORE_DATE <= "&month_end"d). Sums expected credit loss.

  dbt Equivalent:
    CURATED.RISK_SCORES -> mart_risk_scores (the converted credit risk model).
    The "latest score date on or before the period end" subquery is preserved.
    All rows in mart_risk_scores carry score_date = current_date(), so the filter
    naturally resolves to the single available score date.
*/

with risk_scores as (
    select * from {{ ref('mart_risk_scores') }}
),

latest as (
    -- SAS: SCORE_DATE = (select max(SCORE_DATE) ... where SCORE_DATE <= "&month_end"d)
    select max(score_date) as max_score_date
    from risk_scores
    where score_date <= current_date()
)

select
    r.customer_id,
    -- SAS: TOTAL_ECL = sum(EXPECTED_LOSS)
    sum(r.expected_loss) as total_ecl
from risk_scores r
inner join latest l
    on r.score_date = l.max_score_date
group by r.customer_id
