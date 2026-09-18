/*
  Mapping parity: ANOMALY_TYPE follows the SAS rule order for every anomaly
  row, and no validated transaction that meets a rule is missing from the
  anomaly table (completeness of the HAVING ANOMALY_TYPE ne '' filter).
*/
with expected as (
    select
        transaction_id,
        case
            when z_score > 3 then 'HIGH_AMOUNT'
            when running_balance < 0 or running_balance is null then 'OVERDRAFT'
            when transaction_type = 'WDR'
                and (abs(transaction_amount) > pre_txn_balance * 0.9 or pre_txn_balance is null)
                then 'LARGE_WITHDRAWAL'
            when customer_id is null then 'ORPHAN_ACCOUNT'
            else ''
        end as expected_type,
        anomaly_type
    from {{ ref('mart_transaction_anomalies') }}
    where transaction_date = {{ sas_run_date() }}
),

wrong_type as (
    select transaction_id, 'anomaly_type mismatch' as issue, expected_type, anomaly_type
    from expected
    where expected_type <> anomaly_type or expected_type = ''
),

-- rules that can be evaluated without the 90-day statistics
missing_rows as (
    select
        t.transaction_id,
        'qualifying transaction missing from anomalies' as issue,
        case
            when t.running_balance < 0 or t.running_balance is null then 'OVERDRAFT'
            else 'LARGE_WITHDRAWAL'
        end as expected_type,
        cast(null as string) as anomaly_type
    from {{ ref('mart_daily_transactions') }} t
    left join {{ ref('mart_transaction_anomalies') }} a
        on t.transaction_id = a.transaction_id
    where t.transaction_date = {{ sas_run_date() }}
      and a.transaction_id is null
      and (
          t.running_balance < 0 or t.running_balance is null
          or (t.transaction_type = 'WDR'
              and (abs(t.transaction_amount) > t.pre_txn_balance * 0.9 or t.pre_txn_balance is null))
      )
)

select * from wrong_type
union all
select * from missing_rows
