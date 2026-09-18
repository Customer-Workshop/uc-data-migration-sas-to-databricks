/*
  Golden parity: CURATED.RISK_SCORES  <->  mart_risk_scores

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_risk_scores, business date 31JAN2024). Keys: account_id.
  Exact columns: account_type, new_risk_rating, fico_score, pmt_late_90_12mo, acct_age_months.
  Tolerance columns: ltv, pd, lgd, ead, expected_loss (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_risk_scores') }}
),

g as (
    select * from {{ ref('sas_golden_risk_scores') }}
),

compared as (
    select
        coalesce(cast(m.account_id as string), cast(g.account_id as string)) as account_id,
        case
            when m.account_id is null then 'missing_in_dbt'
            when g.account_id is null then 'missing_in_sas'
            when not (m.account_type <=> g.account_type) then 'account_type'
            when not (m.new_risk_rating <=> g.new_risk_rating) then 'new_risk_rating'
            when not (m.fico_score <=> g.fico_score) then 'fico_score'
            when not (m.pmt_late_90_12mo <=> g.pmt_late_90_12mo) then 'pmt_late_90_12mo'
            when not (m.acct_age_months <=> g.acct_age_months) then 'acct_age_months'
            when not {{ approx_equal_rel('cast(m.ltv as double)', 'cast(g.ltv as double)') }} then 'ltv'
            when not {{ approx_equal_rel('cast(m.pd as double)', 'cast(g.pd as double)') }} then 'pd'
            when not {{ approx_equal_rel('cast(m.lgd as double)', 'cast(g.lgd as double)') }} then 'lgd'
            when not {{ approx_equal('cast(m.ead as double)', 'cast(g.ead as double)') }} then 'ead'
            when not {{ approx_equal_rel('cast(m.expected_loss as double)', 'cast(g.expected_loss as double)') }} then 'expected_loss'
            else null
        end as mismatch,
        m.account_type as dbt_account_type,
        g.account_type as sas_account_type,
        m.new_risk_rating as dbt_new_risk_rating,
        g.new_risk_rating as sas_new_risk_rating,
        m.fico_score as dbt_fico_score,
        g.fico_score as sas_fico_score,
        m.pmt_late_90_12mo as dbt_pmt_late_90_12mo,
        g.pmt_late_90_12mo as sas_pmt_late_90_12mo,
        m.acct_age_months as dbt_acct_age_months,
        g.acct_age_months as sas_acct_age_months,
        m.ltv as dbt_ltv,
        g.ltv as sas_ltv,
        m.pd as dbt_pd,
        g.pd as sas_pd,
        m.lgd as dbt_lgd,
        g.lgd as sas_lgd,
        m.ead as dbt_ead,
        g.ead as sas_ead,
        m.expected_loss as dbt_expected_loss,
        g.expected_loss as sas_expected_loss
    from m
    full outer join g
        on m.account_id <=> g.account_id
)

select * from compared
where mismatch is not null
