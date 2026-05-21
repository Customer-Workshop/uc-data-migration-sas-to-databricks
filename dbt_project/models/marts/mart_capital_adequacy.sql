/*
  mart_capital_adequacy.sql
  Migrated from: Programs/Banking/monthly_regulatory_reporting.sas (Step 5, lines 169-190)

  SAS Original:
    PROC SQL selecting from REPORTS.MONTHLY_RWA to compute total RWA,
    then applying placeholder capital amounts (CET1=50M, Tier1=65M, Total=80M)
    and computing ratio/status columns against Basel III minimums

  dbt Equivalent:
    ref('mart_regulatory_rwa') replaces REPORTS.MONTHLY_RWA,
    var('report_month') replaces &report_month,
    capital amounts configurable via dbt vars with SAS placeholder defaults
*/

{{
    config(
        materialized='table',
        tags=['marts']
    )
}}

-- SAS: PROC SQL CREATE TABLE REPORTS.CAPITAL_ADEQUACY → dbt: SELECT from ref()
with rwa_totals as (
    select
        sum(rwa) as total_rwa
    from {{ ref('mart_regulatory_rwa') }}
)

select
    {{ var('report_month') }}                                       as report_month,
    r.total_rwa,

    -- SAS: Placeholder capital amounts (would come from GL in production)
    {{ var('cet1_capital', 50000000) }}                             as cet1_capital,
    {{ var('tier1_capital', 65000000) }}                            as tier1_capital,
    {{ var('total_capital', 80000000) }}                            as total_capital,

    -- SAS: CET1_RATIO = CET1_CAPITAL / TOTAL_RWA * 100
    case
        when r.total_rwa > 0
            then {{ var('cet1_capital', 50000000) }} / r.total_rwa * 100
        else null
    end                                                             as cet1_ratio,

    -- SAS: TIER1_RATIO = TIER1_CAPITAL / TOTAL_RWA * 100
    case
        when r.total_rwa > 0
            then {{ var('tier1_capital', 65000000) }} / r.total_rwa * 100
        else null
    end                                                             as tier1_ratio,

    -- SAS: TOTAL_CAPITAL_RATIO = TOTAL_CAPITAL / TOTAL_RWA * 100
    case
        when r.total_rwa > 0
            then {{ var('total_capital', 80000000) }} / r.total_rwa * 100
        else null
    end                                                             as total_capital_ratio,

    -- SAS: Basel III minimum thresholds — CET1 >= 4.5%
    case
        when r.total_rwa = 0 then 'PASS'
        when {{ var('cet1_capital', 50000000) }} / r.total_rwa * 100 >= 4.5 then 'PASS'
        else 'FAIL'
    end                                                             as cet1_status,

    -- SAS: Basel III minimum thresholds — Tier1 >= 6.0%
    case
        when r.total_rwa = 0 then 'PASS'
        when {{ var('tier1_capital', 65000000) }} / r.total_rwa * 100 >= 6.0 then 'PASS'
        else 'FAIL'
    end                                                             as tier1_status,

    -- SAS: Basel III minimum thresholds — Total Capital >= 8.0%
    case
        when r.total_rwa = 0 then 'PASS'
        when {{ var('total_capital', 80000000) }} / r.total_rwa * 100 >= 8.0 then 'PASS'
        else 'FAIL'
    end                                                             as total_capital_status

from rwa_totals r
