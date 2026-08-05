/*
  mart_capital_adequacy.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 5)

  The SAS source uses hard-coded GL placeholders for capital because the
  production GL feed is not included:
    CET1_CAPITAL = 50000000, TIER1_CAPITAL = 65000000,
    TOTAL_CAPITAL = 80000000. These source quirks are reproduced verbatim
  rather than parameterised.

  SAS-to-dbt migration gaps:
    - REPORTS.LLP_COVERAGE (SAS Step 3) is not converted faithfully because
      ALLOWANCE_AMT and DAYS_PAST_DUE are unavailable in the raw estate.
    - PROC EXPORT to XLSX (SAS Step 4) is out of scope; see
      docs/SAS_TO_DBT_MIGRATION_MAP.md.
*/

with rwa_total as (
    select
        report_month,
        sum(rwa) as total_rwa
    from {{ ref('mart_regulatory_rwa') }}
    group by report_month
),

capital as (
    select
        report_month,
        total_rwa,
        50000000 as cet1_capital,
        65000000 as tier1_capital,
        80000000 as total_capital
    from rwa_total
)

select
    report_month,
    total_rwa,
    cet1_capital,
    tier1_capital,
    total_capital,
    case when total_rwa > 0 then cet1_capital / total_rwa * 100 else null end as cet1_ratio,
    case when total_rwa > 0 then tier1_capital / total_rwa * 100 else null end as tier1_ratio,
    case when total_rwa > 0 then total_capital / total_rwa * 100 else null end as total_capital_ratio,
    case
        when total_rwa = 0 then 'PASS'
        when cet1_capital / total_rwa * 100 >= 4.5 then 'PASS'
        else 'FAIL'
    end as cet1_status,
    case
        when total_rwa = 0 then 'PASS'
        when tier1_capital / total_rwa * 100 >= 6.0 then 'PASS'
        else 'FAIL'
    end as tier1_status,
    case
        when total_rwa = 0 then 'PASS'
        when total_capital / total_rwa * 100 >= 8.0 then 'PASS'
        else 'FAIL'
    end as total_capital_status
from capital
