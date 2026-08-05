/*
  Reconciliation test: SAS credit_risk_scoring.sas Step 5 PROC MEANS NWAY
  summary totals tie to mart_risk_scores and the summary has exactly the
  distinct account_type/risk_rating class pairs present in the scores mart.
*/
with summary_totals as (
    select
        sum(n_accounts) as n_accounts,
        sum(total_ead) as total_ead,
        sum(total_el) as total_el,
        count(*) as group_count
    from {{ ref('mart_risk_summary') }}
),

score_totals as (
    select
        count(*) as n_accounts,
        sum(ead) as total_ead,
        sum(expected_loss) as total_el,
        count(distinct account_type || '|' || cast(risk_rating as string)) as group_count
    from {{ ref('mart_risk_scores') }}
)

select
    s.n_accounts as expected_n_accounts,
    m.n_accounts as actual_n_accounts,
    s.total_ead as expected_total_ead,
    m.total_ead as actual_total_ead,
    s.total_el as expected_total_el,
    m.total_el as actual_total_el,
    s.group_count as expected_group_count,
    m.group_count as actual_group_count
from score_totals s
cross join summary_totals m
where s.n_accounts <> m.n_accounts
   or abs(s.total_ead - m.total_ead) > 0.01
   or abs(s.total_el - m.total_el) > 0.01
   or s.group_count <> m.group_count
