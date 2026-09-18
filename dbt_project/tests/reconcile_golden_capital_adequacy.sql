/*
  Golden parity: REPORTS.CAPITAL_ADEQUACY  <->  mart_capital_adequacy

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_capital_adequacy, business date 31JAN2024). Keys: report_month.
  Exact columns: cet1_status, tier1_status, total_capital_status, cet1_capital, tier1_capital, total_capital.
  Tolerance columns: total_rwa, cet1_ratio, tier1_ratio, total_capital_ratio (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_capital_adequacy') }}
),

g as (
    select * from {{ ref('sas_golden_capital_adequacy') }}
),

compared as (
    select
        coalesce(cast(m.report_month as string), cast(g.report_month as string)) as report_month,
        case
            when m.report_month is null then 'missing_in_dbt'
            when g.report_month is null then 'missing_in_sas'
            when not (m.cet1_status <=> g.cet1_status) then 'cet1_status'
            when not (m.tier1_status <=> g.tier1_status) then 'tier1_status'
            when not (m.total_capital_status <=> g.total_capital_status) then 'total_capital_status'
            when not (m.cet1_capital <=> g.cet1_capital) then 'cet1_capital'
            when not (m.tier1_capital <=> g.tier1_capital) then 'tier1_capital'
            when not (m.total_capital <=> g.total_capital) then 'total_capital'
            when not {{ approx_equal('cast(m.total_rwa as double)', 'cast(g.total_rwa as double)') }} then 'total_rwa'
            when not {{ approx_equal_rel('cast(m.cet1_ratio as double)', 'cast(g.cet1_ratio as double)') }} then 'cet1_ratio'
            when not {{ approx_equal_rel('cast(m.tier1_ratio as double)', 'cast(g.tier1_ratio as double)') }} then 'tier1_ratio'
            when not {{ approx_equal_rel('cast(m.total_capital_ratio as double)', 'cast(g.total_capital_ratio as double)') }} then 'total_capital_ratio'
            else null
        end as mismatch,
        m.cet1_status as dbt_cet1_status,
        g.cet1_status as sas_cet1_status,
        m.tier1_status as dbt_tier1_status,
        g.tier1_status as sas_tier1_status,
        m.total_capital_status as dbt_total_capital_status,
        g.total_capital_status as sas_total_capital_status,
        m.cet1_capital as dbt_cet1_capital,
        g.cet1_capital as sas_cet1_capital,
        m.tier1_capital as dbt_tier1_capital,
        g.tier1_capital as sas_tier1_capital,
        m.total_capital as dbt_total_capital,
        g.total_capital as sas_total_capital,
        m.total_rwa as dbt_total_rwa,
        g.total_rwa as sas_total_rwa,
        m.cet1_ratio as dbt_cet1_ratio,
        g.cet1_ratio as sas_cet1_ratio,
        m.tier1_ratio as dbt_tier1_ratio,
        g.tier1_ratio as sas_tier1_ratio,
        m.total_capital_ratio as dbt_total_capital_ratio,
        g.total_capital_ratio as sas_total_capital_ratio
    from m
    full outer join g
        on m.report_month <=> g.report_month
)

select * from compared
where mismatch is not null
