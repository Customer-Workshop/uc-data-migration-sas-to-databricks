/*
  Golden parity: CURATED.TXN_ANOMALIES  <->  mart_transaction_anomalies

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_txn_anomalies, business date 31JAN2024). Keys: transaction_id.
  Exact columns: account_id, anomaly_type.
  Tolerance columns: running_balance, pre_txn_balance, z_score (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_transaction_anomalies') }}
),

g as (
    select * from {{ ref('sas_golden_txn_anomalies') }}
),

compared as (
    select
        coalesce(cast(m.transaction_id as string), cast(g.transaction_id as string)) as transaction_id,
        case
            when m.transaction_id is null then 'missing_in_dbt'
            when g.transaction_id is null then 'missing_in_sas'
            when not (m.account_id <=> g.account_id) then 'account_id'
            when not (m.anomaly_type <=> g.anomaly_type) then 'anomaly_type'
            when not {{ approx_equal('cast(m.running_balance as double)', 'cast(g.running_balance as double)') }} then 'running_balance'
            when not {{ approx_equal('cast(m.pre_txn_balance as double)', 'cast(g.pre_txn_balance as double)') }} then 'pre_txn_balance'
            when not {{ approx_equal_rel('cast(m.z_score as double)', 'cast(g.z_score as double)') }} then 'z_score'
            else null
        end as mismatch,
        m.account_id as dbt_account_id,
        g.account_id as sas_account_id,
        m.anomaly_type as dbt_anomaly_type,
        g.anomaly_type as sas_anomaly_type,
        m.running_balance as dbt_running_balance,
        g.running_balance as sas_running_balance,
        m.pre_txn_balance as dbt_pre_txn_balance,
        g.pre_txn_balance as sas_pre_txn_balance,
        m.z_score as dbt_z_score,
        g.z_score as sas_z_score
    from m
    full outer join g
        on m.transaction_id <=> g.transaction_id
)

select * from compared
where mismatch is not null
