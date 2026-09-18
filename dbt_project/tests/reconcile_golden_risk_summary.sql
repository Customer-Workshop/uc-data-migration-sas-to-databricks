/*
  Golden parity: REPORTS.RISK_SUMMARY  <->  mart_risk_summary

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_risk_summary, business date 31JAN2024). Keys: account_type, new_risk_rating.
  Exact columns: n_accounts, lgd, ead, expected_loss.
  Tolerance columns: avg_pd, avg_lgd, total_ead, total_el (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_risk_summary') }}
),

g as (
    select * from {{ ref('sas_golden_risk_summary') }}
),

compared as (
    select
        coalesce(cast(m.account_type as string), cast(g.account_type as string)) as account_type,
        coalesce(cast(m.new_risk_rating as string), cast(g.new_risk_rating as string)) as new_risk_rating,
        case
            when m.account_type is null then 'missing_in_dbt'
            when g.account_type is null then 'missing_in_sas'
            when not (m.n_accounts <=> g.n_accounts) then 'n_accounts'
            when not (m.lgd <=> g.lgd) then 'lgd'
            when not (m.ead <=> g.ead) then 'ead'
            when not (m.expected_loss <=> g.expected_loss) then 'expected_loss'
            when not {{ approx_equal_rel('cast(m.avg_pd as double)', 'cast(g.avg_pd as double)') }} then 'avg_pd'
            when not {{ approx_equal_rel('cast(m.avg_lgd as double)', 'cast(g.avg_lgd as double)') }} then 'avg_lgd'
            when not {{ approx_equal('cast(m.total_ead as double)', 'cast(g.total_ead as double)') }} then 'total_ead'
            when not {{ approx_equal_rel('cast(m.total_el as double)', 'cast(g.total_el as double)') }} then 'total_el'
            else null
        end as mismatch,
        m.n_accounts as dbt_n_accounts,
        g.n_accounts as sas_n_accounts,
        m.lgd as dbt_lgd,
        g.lgd as sas_lgd,
        m.ead as dbt_ead,
        g.ead as sas_ead,
        m.expected_loss as dbt_expected_loss,
        g.expected_loss as sas_expected_loss,
        m.avg_pd as dbt_avg_pd,
        g.avg_pd as sas_avg_pd,
        m.avg_lgd as dbt_avg_lgd,
        g.avg_lgd as sas_avg_lgd,
        m.total_ead as dbt_total_ead,
        g.total_ead as sas_total_ead,
        m.total_el as dbt_total_el,
        g.total_el as sas_total_el
    from m
    full outer join g
        on m.account_type <=> g.account_type
        and m.new_risk_rating <=> g.new_risk_rating
)

select * from compared
where mismatch is not null
