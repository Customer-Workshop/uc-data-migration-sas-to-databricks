/*
  Golden parity: REPORTS.MONTHLY_RWA  <->  mart_regulatory_rwa

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_monthly_rwa, business date 31JAN2024). Keys: report_month, account_type, customer_segment, risk_weight.
  Exact columns: n_accounts.
  Tolerance columns: total_exposure, rwa (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_regulatory_rwa') }}
),

g as (
    select * from {{ ref('sas_golden_monthly_rwa') }}
),

compared as (
    select
        coalesce(cast(m.report_month as string), cast(g.report_month as string)) as report_month,
        coalesce(cast(m.account_type as string), cast(g.account_type as string)) as account_type,
        coalesce(cast(m.customer_segment as string), cast(g.customer_segment as string)) as customer_segment,
        coalesce(cast(m.risk_weight as string), cast(g.risk_weight as string)) as risk_weight,
        case
            when m.report_month is null then 'missing_in_dbt'
            when g.report_month is null then 'missing_in_sas'
            when not (m.n_accounts <=> g.n_accounts) then 'n_accounts'
            when not {{ approx_equal('cast(m.total_exposure as double)', 'cast(g.total_exposure as double)') }} then 'total_exposure'
            when not {{ approx_equal('cast(m.rwa as double)', 'cast(g.rwa as double)') }} then 'rwa'
            else null
        end as mismatch,
        m.n_accounts as dbt_n_accounts,
        g.n_accounts as sas_n_accounts,
        m.total_exposure as dbt_total_exposure,
        g.total_exposure as sas_total_exposure,
        m.rwa as dbt_rwa,
        g.rwa as sas_rwa
    from m
    full outer join g
        on m.report_month <=> g.report_month
        and m.account_type <=> g.account_type
        and m.customer_segment <=> g.customer_segment
        and m.risk_weight <=> g.risk_weight
)

select * from compared
where mismatch is not null
