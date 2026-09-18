/*
  Parity control: the anomaly classification CASE, branch by branch.

  daily_transaction_processing.sas classifies each enriched transaction in a
  strictly ordered CASE:
      Z_SCORE > 3                                              -> HIGH_AMOUNT
      RUNNING_BALANCE < 0                                      -> OVERDRAFT
      TRANSACTION_TYPE = 'WDR' and abs(amount) > PRE * 0.9     -> LARGE_WITHDRAWAL
      missing(CUSTOMER_ID)                                     -> ORPHAN_ACCOUNT
      otherwise                                                -> '' (dropped)
  with Z_SCORE computed only where STD_TXN_AMT > 0, from the 90-day baseline
  in the curated history as it stands before the day's append.

  This control recomputes the expected label for every business-date
  transaction straight from the raw feed and history, then full-joins it to
  the mart. A row reaching the wrong branch, an inverted precedence, a missing
  branch, or a row wrongly kept or dropped all surface here -- the aggregate
  anomaly counts can tie out while individual rows are on the wrong branch.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with baseline as (
    select
        account_id,
        avg(abs(transaction_amount)) as avg_txn_amt,
        stddev_samp(abs(transaction_amount)) as std_txn_amt
    from {{ source('banking_raw', 'curated_daily_transactions_history') }}
    where transaction_date >= date_add({{ sas_run_date() }}, -90)
    group by account_id
),

feed as (
    select *
    from {{ source('banking_raw', 'daily_transactions') }}
    where transaction_id is not null and trim(transaction_id) <> ''
      and account_id is not null and trim(account_id) <> ''
      and transaction_amount is not null
      and abs(transaction_amount) <= 10000000
      and transaction_type in (
          'DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF'
      )
      and transaction_date <= {{ sas_run_date() }}
),

accounts as (
    select * from {{ ref('int_account_metrics') }}
),

enriched as (
    select
        f.transaction_id,
        f.transaction_type,
        f.transaction_amount,
        a.customer_id,
        a.current_balance as pre_txn_balance,
        a.current_balance + sum(
            case
                when f.transaction_type in ('DEP', 'INT', 'REF', 'REV')
                    then f.transaction_amount
                when f.transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                    then -abs(f.transaction_amount)
                when f.transaction_type in ('TRF', 'ADJ')
                    then f.transaction_amount
                else 0
            end
        ) over (
            partition by f.account_id
            order by f.transaction_date, f.transaction_id
            rows unbounded preceding
        ) as running_balance,
        b.avg_txn_amt,
        b.std_txn_amt
    from feed f
    left join accounts a on f.account_id = a.account_id
    left join baseline b on f.account_id = b.account_id
),

expected as (
    select
        transaction_id,
        case
            when std_txn_amt > 0
                 and (abs(transaction_amount) - avg_txn_amt) / std_txn_amt > 3
                then 'HIGH_AMOUNT'
            -- a missing RUNNING_BALANCE is below zero in SAS
            when running_balance is null or running_balance < 0 then 'OVERDRAFT'
            when transaction_type = 'WDR'
                 and abs(transaction_amount) > pre_txn_balance * 0.9
                then 'LARGE_WITHDRAWAL'
            when customer_id is null then 'ORPHAN_ACCOUNT'
        end as expected_anomaly_type
    from enriched
),

expected_anomalies as (
    select transaction_id, expected_anomaly_type
    from expected
    where expected_anomaly_type is not null
),

actual_anomalies as (
    select transaction_id, anomaly_type as actual_anomaly_type
    from {{ ref('mart_transaction_anomalies') }}
)

select
    coalesce(e.transaction_id, a.transaction_id) as transaction_id,
    e.expected_anomaly_type,
    a.actual_anomaly_type
from expected_anomalies e
full outer join actual_anomalies a
    on e.transaction_id = a.transaction_id
where e.transaction_id is null
   or a.transaction_id is null
   or e.expected_anomaly_type <> a.actual_anomaly_type
