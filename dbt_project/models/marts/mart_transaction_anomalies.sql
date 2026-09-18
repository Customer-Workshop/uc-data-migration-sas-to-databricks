/*
  mart_transaction_anomalies.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 4)

  SAS Original:
    PROC SQL computing 90-day per-account statistics from
    CURATED.DAILY_TRANSACTIONS, then a Z-score / rule-based classification
    over WORK.TXN_WITH_BALANCE with `having ANOMALY_TYPE ne ''`.

  dbt Equivalent:
    SQL aggregation + CASE. Two SAS behaviours are reproduced deliberately:
      * the statistics read the curated table *before* the day's append, so the
        batch being scored is not in its own baseline;
      * SAS treats a missing value as smaller than any number, so a row whose
        RUNNING_BALANCE is missing (an account with no master record) satisfies
        `RUNNING_BALANCE < 0` and is classified OVERDRAFT, never ORPHAN_ACCOUNT.
*/

with transactions as (
    select * from {{ ref('int_txn_enriched') }}
),

-- SAS: WORK.TXN_STATS. `std` is the sample standard deviation and is missing
-- for accounts with a single transaction, which makes Z_SCORE missing too.
account_stats as (
    select
        account_id,
        avg(abs(transaction_amount)) as avg_txn_amt,
        stddev(abs(transaction_amount)) as std_txn_amt,
        count(*) as txn_count
    from ({{ curated_txn_history() }})
    where transaction_date >= date_add({{ sas_run_date() }}, -90)
    group by account_id
),

-- SAS: WORK.TXN_ANOMALIES
anomalies as (
    select
        t.*,
        s.avg_txn_amt,
        s.std_txn_amt,
        case
            when s.std_txn_amt > 0
                then (abs(t.transaction_amount) - s.avg_txn_amt) / s.std_txn_amt
        end as z_score,
        case
            when s.std_txn_amt > 0
                 and (abs(t.transaction_amount) - s.avg_txn_amt) / s.std_txn_amt > 3
                then 'HIGH_AMOUNT'
            -- missing running balance sorts below zero in SAS
            when t.running_balance < 0 or t.running_balance is null
                then 'OVERDRAFT'
            when t.transaction_type = 'WDR'
                 and (
                     t.pre_txn_balance is null
                     or abs(t.transaction_amount) > t.pre_txn_balance * 0.9
                 )
                then 'LARGE_WITHDRAWAL'
            when t.customer_id is null
                then 'ORPHAN_ACCOUNT'
        end as anomaly_type
    from transactions t
    left join account_stats s
        on t.account_id = s.account_id
)

select * from anomalies
where anomaly_type is not null
