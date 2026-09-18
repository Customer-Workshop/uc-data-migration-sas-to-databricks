/*
  mart_capital_adequacy.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 5)
  Output contract: REPORTS.CAPITAL_ADEQUACY (one row per report month)

  SAS Original:
    TOTAL_RWA = sum(RWA) over REPORTS.MONTHLY_RWA
    CET1_CAPITAL = 50,000,000; TIER1_CAPITAL = 65,000,000; TOTAL_CAPITAL = 80,000,000
      (placeholders — "would come from GL in production")
    *_RATIO = capital / TOTAL_RWA * 100 when TOTAL_RWA > 0 else missing
    *_STATUS: TOTAL_RWA = 0 -> PASS; ratio >= minimum -> PASS; else FAIL
      minimums: CET1 4.5%, Tier 1 6.0%, Total 8.0%

  dbt Equivalent:
    Aggregate over mart_regulatory_rwa. Note sum(RWA) over an empty table is
    missing in SAS (`sum(RWA) = 0` false, `>=` false -> FAIL) and null in
    Spark (same outcome).
*/

with rwa as (
    select sum(rwa) as total_rwa
    from {{ ref('mart_regulatory_rwa') }}
)

select
    '{{ sas_report_month() }}' as report_month,
    total_rwa,
    50000000 as cet1_capital,
    65000000 as tier1_capital,
    80000000 as total_capital,
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
from rwa
