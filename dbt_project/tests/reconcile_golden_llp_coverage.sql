/*
  Golden parity: REPORTS.LLP_COVERAGE  <->  mart_llp_coverage

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_llp_coverage, business date 31JAN2024). Keys: report_month, account_type.
  Exact columns: n_loans.
  Tolerance columns: gross_loans, total_allowance, coverage_pct, npl_balance, npl_coverage_pct (macros/approx_equal.sql).

  Known runtime deviation (verify/golden/known_deviations.json, KD-001): the
  OpenSAS container resolves `case when calculated NPL_BALANCE > 0 ...` over
  the aggregate alias as 0, so the golden NPL_COVERAGE_PCT is 0 wherever
  NPL_BALANCE > 0. SAS 9.4 semantics give TOTAL_ALLOWANCE / NPL_BALANCE * 100,
  which is what the model implements; on those rows the model value is
  accepted only if it equals that formula from the golden's own figures.

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_llp_coverage') }}
),

g as (
    select * from {{ ref('sas_golden_llp_coverage') }}
),

compared as (
    select
        coalesce(cast(m.report_month as string), cast(g.report_month as string)) as report_month,
        coalesce(cast(m.account_type as string), cast(g.account_type as string)) as account_type,
        case
            when m.report_month is null then 'missing_in_dbt'
            when g.report_month is null then 'missing_in_sas'
            when not (m.n_loans <=> g.n_loans) then 'n_loans'
            when not {{ approx_equal('cast(m.gross_loans as double)', 'cast(g.gross_loans as double)') }} then 'gross_loans'
            when not {{ approx_equal('cast(m.total_allowance as double)', 'cast(g.total_allowance as double)') }} then 'total_allowance'
            when not {{ approx_equal_rel('cast(m.coverage_pct as double)', 'cast(g.coverage_pct as double)') }} then 'coverage_pct'
            when not {{ approx_equal('cast(m.npl_balance as double)', 'cast(g.npl_balance as double)') }} then 'npl_balance'
            when not (
                {{ approx_equal_rel('cast(m.npl_coverage_pct as double)', 'cast(g.npl_coverage_pct as double)') }}
                or (
                    -- KD-001
                    g.npl_coverage_pct = 0 and g.npl_balance > 0
                    and {{ approx_equal_rel('cast(m.npl_coverage_pct as double)', 'cast(g.total_allowance as double) / cast(g.npl_balance as double) * 100') }}
                )
            ) then 'npl_coverage_pct'
            else null
        end as mismatch,
        m.n_loans as dbt_n_loans,
        g.n_loans as sas_n_loans,
        m.gross_loans as dbt_gross_loans,
        g.gross_loans as sas_gross_loans,
        m.total_allowance as dbt_total_allowance,
        g.total_allowance as sas_total_allowance,
        m.coverage_pct as dbt_coverage_pct,
        g.coverage_pct as sas_coverage_pct,
        m.npl_balance as dbt_npl_balance,
        g.npl_balance as sas_npl_balance,
        m.npl_coverage_pct as dbt_npl_coverage_pct,
        g.npl_coverage_pct as sas_npl_coverage_pct
    from m
    full outer join g
        on m.report_month <=> g.report_month
        and m.account_type <=> g.account_type
)

select * from compared
where mismatch is not null
