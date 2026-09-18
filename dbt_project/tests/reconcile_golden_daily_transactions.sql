/*
  Golden parity: CURATED.DAILY_TRANSACTIONS  <->  mart_daily_transactions_curated

  Row-by-row comparison of the migrated model against the SAS golden output
  (seed sas_golden_daily_transactions, business date 31JAN2024). Keys: transaction_id.
  Exact columns: account_id, transaction_date, transaction_type.
  Tolerance columns: transaction_amount (macros/approx_equal.sql).

  Fails on any row present on one side only, or any compared column that
  differs. Enabled only when the batch is replayed for the golden business
  date: dbt build --vars '{curr_dt: "2024-01-31", report_month: "202401", sas_golden_parity: true}'
*/
{{ config(enabled=sas_var_is_true('sas_golden_parity'), tags=['reconcile', 'golden']) }}

with m as (
    select * from {{ ref('mart_daily_transactions_curated') }}
),

g as (
    select * from {{ ref('sas_golden_daily_transactions') }}
),

compared as (
    select
        coalesce(cast(m.transaction_id as string), cast(g.transaction_id as string)) as transaction_id,
        case
            when m.transaction_id is null then 'missing_in_dbt'
            when g.transaction_id is null then 'missing_in_sas'
            when not (m.account_id <=> g.account_id) then 'account_id'
            when not (m.transaction_date <=> g.transaction_date) then 'transaction_date'
            when not (m.transaction_type <=> g.transaction_type) then 'transaction_type'
            when not {{ approx_equal('cast(m.transaction_amount as double)', 'cast(g.transaction_amount as double)') }} then 'transaction_amount'
            else null
        end as mismatch,
        m.account_id as dbt_account_id,
        g.account_id as sas_account_id,
        m.transaction_date as dbt_transaction_date,
        g.transaction_date as sas_transaction_date,
        m.transaction_type as dbt_transaction_type,
        g.transaction_type as sas_transaction_type,
        m.transaction_amount as dbt_transaction_amount,
        g.transaction_amount as sas_transaction_amount
    from m
    full outer join g
        on m.transaction_id <=> g.transaction_id
)

select * from compared
where mismatch is not null
