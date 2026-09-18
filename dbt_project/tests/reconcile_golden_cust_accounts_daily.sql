/*
  Golden parity: STG_BANK.CUST_ACCOUNTS_DAILY  <->  int_account_metrics

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_cust_accounts_daily, business date 31JAN2024). Keys: account_id.
  Exact columns: account_type, account_status, customer_id, dormancy_flag, high_balance_flag, acct_age_months, days_inactive, risk_rating, snapshot_date.
  Tolerance columns: current_balance, credit_limit, utilization_pct (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('int_account_metrics') }}
),

g as (
    select * from {{ ref('sas_golden_cust_accounts_daily') }}
),

compared as (
    select
        coalesce(cast(m.account_id as string), cast(g.account_id as string)) as account_id,
        case
            when m.account_id is null then 'missing_in_dbt'
            when g.account_id is null then 'missing_in_sas'
            when not (m.account_type <=> g.account_type) then 'account_type'
            when not (m.account_status <=> g.account_status) then 'account_status'
            when not (m.customer_id <=> g.customer_id) then 'customer_id'
            when not (m.dormancy_flag <=> g.dormancy_flag) then 'dormancy_flag'
            when not (m.high_balance_flag <=> g.high_balance_flag) then 'high_balance_flag'
            when not (m.acct_age_months <=> g.acct_age_months) then 'acct_age_months'
            when not (m.days_inactive <=> g.days_inactive) then 'days_inactive'
            when not (m.risk_rating <=> g.risk_rating) then 'risk_rating'
            when not (m.snapshot_date <=> g.snapshot_date) then 'snapshot_date'
            when not {{ approx_equal('cast(m.current_balance as double)', 'cast(g.current_balance as double)') }} then 'current_balance'
            when not {{ approx_equal('cast(m.credit_limit as double)', 'cast(g.credit_limit as double)') }} then 'credit_limit'
            when not {{ approx_equal('cast(m.utilization_pct as double)', 'cast(g.utilization_pct as double)') }} then 'utilization_pct'
            else null
        end as mismatch,
        m.account_type as dbt_account_type,
        g.account_type as sas_account_type,
        m.account_status as dbt_account_status,
        g.account_status as sas_account_status,
        m.customer_id as dbt_customer_id,
        g.customer_id as sas_customer_id,
        m.dormancy_flag as dbt_dormancy_flag,
        g.dormancy_flag as sas_dormancy_flag,
        m.high_balance_flag as dbt_high_balance_flag,
        g.high_balance_flag as sas_high_balance_flag,
        m.acct_age_months as dbt_acct_age_months,
        g.acct_age_months as sas_acct_age_months,
        m.days_inactive as dbt_days_inactive,
        g.days_inactive as sas_days_inactive,
        m.risk_rating as dbt_risk_rating,
        g.risk_rating as sas_risk_rating,
        m.snapshot_date as dbt_snapshot_date,
        g.snapshot_date as sas_snapshot_date,
        m.current_balance as dbt_current_balance,
        g.current_balance as sas_current_balance,
        m.credit_limit as dbt_credit_limit,
        g.credit_limit as sas_credit_limit,
        m.utilization_pct as dbt_utilization_pct,
        g.utilization_pct as sas_utilization_pct
    from m
    full outer join g
        on m.account_id <=> g.account_id
)

select * from compared
where mismatch is not null
