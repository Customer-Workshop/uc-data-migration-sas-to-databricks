/*
  Golden parity: REPORTS.DELINQUENCY_AGING  <->  mart_delinquency_aging

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_delinquency_aging, business date 31JAN2024). Keys: report_month, account_type, region_code, delinq_bucket.
  Exact columns: n_accounts.
  Tolerance columns: total_balance, total_past_due (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_delinquency_aging') }}
),

g as (
    select * from {{ ref('sas_golden_delinquency_aging') }}
),

compared as (
    select
        coalesce(cast(m.report_month as string), cast(g.report_month as string)) as report_month,
        coalesce(cast(m.account_type as string), cast(g.account_type as string)) as account_type,
        coalesce(cast(m.region_code as string), cast(g.region_code as string)) as region_code,
        coalesce(cast(m.delinq_bucket as string), cast(g.delinq_bucket as string)) as delinq_bucket,
        case
            when m.report_month is null then 'missing_in_dbt'
            when g.report_month is null then 'missing_in_sas'
            when not (m.n_accounts <=> g.n_accounts) then 'n_accounts'
            when not {{ approx_equal('cast(m.total_balance as double)', 'cast(g.total_balance as double)') }} then 'total_balance'
            when not {{ approx_equal('cast(m.total_past_due as double)', 'cast(g.total_past_due as double)') }} then 'total_past_due'
            else null
        end as mismatch,
        m.n_accounts as dbt_n_accounts,
        g.n_accounts as sas_n_accounts,
        m.total_balance as dbt_total_balance,
        g.total_balance as sas_total_balance,
        m.total_past_due as dbt_total_past_due,
        g.total_past_due as sas_total_past_due
    from m
    full outer join g
        on m.report_month <=> g.report_month
        and m.account_type <=> g.account_type
        and m.region_code <=> g.region_code
        and m.delinq_bucket <=> g.delinq_bucket
)

select * from compared
where mismatch is not null
