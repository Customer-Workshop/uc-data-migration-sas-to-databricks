/*
  mart_transaction_anomalies.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 4)

  SAS Original:
    WORK.TXN_STATS — PROC SQL computing mean/std/count of abs(TRANSACTION_AMOUNT)
    per ACCOUNT_ID from CURATED.DAILY_TRANSACTIONS for the trailing 90 days,
    then WORK.TXN_ANOMALIES joining WORK.TXN_WITH_BALANCE to those stats,
    deriving Z_SCORE and classifying ANOMALY_TYPE, `having ANOMALY_TYPE ne ''`.

  dbt Equivalent:
    SQL aggregation replaces the first PROC SQL pass; the classification CASE is
    reproduced branch-for-branch in the same precedence order; the HAVING
    becomes a WHERE on the derived column.

  Source-faithful notes:
    * The SAS stats read CURATED.DAILY_TRANSACTIONS *before* the current batch is
      appended (Step 5 runs after Step 4), so the account statistics deliberately
      exclude the transactions being scored. Reproduced by excluding the current
      run date from the stats population.
    * SAS std() is the sample standard deviation and a single-observation account
      yields a missing STD_TXN_AMT, so Z_SCORE is missing and HIGH_AMOUNT cannot
      fire — reproduced by SQL stddev() (stddev_samp) returning null.
    * Branch precedence matters: HIGH_AMOUNT outranks OVERDRAFT, which outranks
      LARGE_WITHDRAWAL, which outranks ORPHAN_ACCOUNT.
    * OVERDRAFT tests the cumulative RUNNING_BALANCE, while LARGE_WITHDRAWAL
      tests the static PRE_TXN_BALANCE (the account's CURRENT_BALANCE) — the
      two thresholds use different balances in the source, and that asymmetry
      is preserved here.
*/

with transactions as (
    select * from {{ ref('mart_daily_transactions') }}
),

-- SAS: PROC SQL creating WORK.TXN_STATS (trailing 90-day stats per account,
-- from already-loaded history only)
account_stats as (
    select
        account_id,
        avg(abs(transaction_amount)) as avg_txn_amt,
        stddev(abs(transaction_amount)) as std_txn_amt,
        count(*) as txn_count
    from {{ ref('mart_daily_transactions') }}
    where transaction_date >= date_add(current_date(), -90)
      and transaction_date < current_date()
    group by account_id
),

-- SAS: PROC SQL creating WORK.TXN_ANOMALIES with Z-score and classification
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
            when t.running_balance < 0
                then 'OVERDRAFT'
            when t.transaction_type = 'WDR'
                 and abs(t.transaction_amount) > t.pre_txn_balance * 0.9
                then 'LARGE_WITHDRAWAL'
            when t.customer_id is null
                then 'ORPHAN_ACCOUNT'
        end as anomaly_type
    from transactions t
    left join account_stats s
        on t.account_id = s.account_id
)

-- SAS: having ANOMALY_TYPE ne ''
select * from anomalies
where anomaly_type is not null
