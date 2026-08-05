/*
  Reconciliation test: capital adequacy totals, ratios, and statuses match
  the SAS Step 5 calculation from mart_regulatory_rwa.

  The SAS status CASE explicitly returns PASS when total RWA is zero; this
  check reproduces that branch.
*/
with expected as (
    select
        report_month,
        sum(rwa) as total_rwa
    from {{ ref('mart_regulatory_rwa') }}
    group by report_month
),

calculated as (
    select
        report_month,
        total_rwa,
        case when total_rwa > 0 then 50000000 / total_rwa * 100 else null end as cet1_ratio,
        case when total_rwa > 0 then 65000000 / total_rwa * 100 else null end as tier1_ratio,
        case when total_rwa > 0 then 80000000 / total_rwa * 100 else null end as total_capital_ratio,
        case
            when total_rwa = 0 then 'PASS'
            when 50000000 / total_rwa * 100 >= 4.5 then 'PASS'
            else 'FAIL'
        end as cet1_status,
        case
            when total_rwa = 0 then 'PASS'
            when 65000000 / total_rwa * 100 >= 6.0 then 'PASS'
            else 'FAIL'
        end as tier1_status,
        case
            when total_rwa = 0 then 'PASS'
            when 80000000 / total_rwa * 100 >= 8.0 then 'PASS'
            else 'FAIL'
        end as total_capital_status
    from expected
),

actual as (
    select *
    from {{ ref('mart_capital_adequacy') }}
)

select
    a.report_month,
    a.total_rwa as model_total_rwa,
    e.total_rwa as expected_total_rwa,
    a.cet1_ratio as model_cet1_ratio,
    e.cet1_ratio as expected_cet1_ratio,
    a.tier1_ratio as model_tier1_ratio,
    e.tier1_ratio as expected_tier1_ratio,
    a.total_capital_ratio as model_total_capital_ratio,
    e.total_capital_ratio as expected_total_capital_ratio
from actual a
full outer join calculated e
    on a.report_month = e.report_month
where a.report_month is null
   or e.report_month is null
   or abs(coalesce(a.total_rwa, 0) - coalesce(e.total_rwa, 0)) > 0.01
   or not (a.cet1_ratio <=> e.cet1_ratio)
   or not (a.tier1_ratio <=> e.tier1_ratio)
   or not (a.total_capital_ratio <=> e.total_capital_ratio)
   or a.cet1_status <> e.cet1_status
   or a.tier1_status <> e.tier1_status
   or a.total_capital_status <> e.total_capital_status
