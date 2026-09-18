/*
  mart_transaction_anomalies.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 4)

  SAS Original:
    PROC SQL creating WORK.TXN_STATS (per-account mean/std of abs amount over
    the 90 days up to the business date, read from CURATED.DAILY_TRANSACTIONS
    *before* the day's PROC APPEND), then PROC SQL creating WORK.TXN_ANOMALIES
    from WORK.TXN_WITH_BALANCE with a Z-score and a CASE classification,
    keeping only rows with a non-blank ANOMALY_TYPE.

  dbt Equivalent:
    Aggregation over the curated history for the stats, LEFT JOIN + CASE for
    the classification.

  Source-faithful quirks reproduced here (flagged, not endorsed):
    - The 90-day baseline is read from the curated history only. The business
      date's own transactions are not in it, so they never influence their own
      Z-score, and an account with no history has no baseline at all.
    - SAS treats a missing RUNNING_BALANCE as less than zero, so a transaction
      on an account that is absent from the account snapshot is classified
      OVERDRAFT (the branch above ORPHAN_ACCOUNT), not ORPHAN_ACCOUNT.
    - The branches are evaluated in order, so an overdrawing transaction with a
      Z-score above 3 is reported only as HIGH_AMOUNT.
    - LARGE_WITHDRAWAL compares the amount to PRE_TXN_BALANCE, which is the
      account's start-of-day snapshot balance on every row, not the balance
      before this transaction.
*/

with txn_with_balance as (
    select * from {{ ref('int_txn_with_balance') }}
),

-- SAS: WORK.TXN_STATS from CURATED.DAILY_TRANSACTIONS pre-append.
-- SAS std() is the sample standard deviation and is missing for a single
-- observation, which stddev_samp also returns as null.
account_stats as (
    select
        account_id,
        avg(abs(transaction_amount)) as avg_txn_amt,
        stddev_samp(abs(transaction_amount)) as std_txn_amt,
        count(*) as txn_count
    from {{ source('banking_raw', 'curated_daily_transactions_history') }}
    where transaction_date >= date_add({{ sas_run_date() }}, -90)
    group by account_id
),

scored as (
    select
        e.*,
        s.avg_txn_amt,
        s.std_txn_amt,
        case
            when s.std_txn_amt > 0
                then (abs(e.transaction_amount) - s.avg_txn_amt) / s.std_txn_amt
        end as z_score
    from txn_with_balance e
    left join account_stats s
        on e.account_id = s.account_id
),

classified as (
    select
        *,
        case
            when z_score > 3 then 'HIGH_AMOUNT'
            -- SAS missing RUNNING_BALANCE sorts below zero and lands here
            when running_balance is null or running_balance < 0 then 'OVERDRAFT'
            when transaction_type = 'WDR'
                 and abs(transaction_amount) > pre_txn_balance * 0.9
                then 'LARGE_WITHDRAWAL'
            when customer_id is null then 'ORPHAN_ACCOUNT'
        end as anomaly_type
    from scored
)

-- SAS: having ANOMALY_TYPE ne ''
select * from classified
where anomaly_type is not null
