/*
  Reconciliation control: per-row PARITY of the anomaly classification CASE.

  Source of truth: daily_transaction_processing.sas Step 4.
      case
        when calculated Z_SCORE > 3 then 'HIGH_AMOUNT'
        when e.RUNNING_BALANCE < 0 then 'OVERDRAFT'
        when e.TRANSACTION_TYPE = 'WDR'
             and abs(e.TRANSACTION_AMOUNT) > e.PRE_TXN_BALANCE * 0.9
          then 'LARGE_WITHDRAWAL'
        when missing(e.CUSTOMER_ID) then 'ORPHAN_ACCOUNT'
        else ''
      end as ANOMALY_TYPE
      ... having ANOMALY_TYPE ne ''

  Branch precedence is part of the contract: a transaction that is both a
  Z-score outlier and an overdraft is HIGH_AMOUNT in the source. This control
  re-derives the classification for *every* transaction in the mart from the
  same inputs the source used (cumulative RUNNING_BALANCE for OVERDRAFT, static
  PRE_TXN_BALANCE for LARGE_WITHDRAWAL) and requires:
    * every non-blank classification to appear in mart_transaction_anomalies with
      the same value (no branch silently reassigned or reordered), and
    * no extra rows to be flagged (blank classifications must not appear).

  Stats population mirrors Step 4: trailing 90 days of already-loaded history,
  excluding the batch being scored (Step 5 appends only afterwards).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with txns as (
    select * from {{ ref('mart_daily_transactions') }}
),

stats as (
    select
        account_id,
        avg(abs(transaction_amount)) as avg_txn_amt,
        stddev(abs(transaction_amount)) as std_txn_amt
    from {{ ref('mart_daily_transactions') }}
    where transaction_date >= date_add(current_date(), -90)
      and transaction_date < current_date()
    group by account_id
),

expected as (
    select
        t.transaction_id,
        case
            when s.std_txn_amt > 0
                 and (abs(t.transaction_amount) - s.avg_txn_amt) / s.std_txn_amt > 3
                then 'HIGH_AMOUNT'
            when t.running_balance < 0 then 'OVERDRAFT'
            when t.transaction_type = 'WDR'
                 and abs(t.transaction_amount) > t.pre_txn_balance * 0.9
                then 'LARGE_WITHDRAWAL'
            when t.customer_id is null then 'ORPHAN_ACCOUNT'
        end as expected_anomaly_type
    from txns t
    left join stats s
        on t.account_id = s.account_id
),

actual as (
    select
        transaction_id,
        anomaly_type as actual_anomaly_type
    from {{ ref('mart_transaction_anomalies') }}
)

select
    e.transaction_id,
    e.expected_anomaly_type,
    a.actual_anomaly_type
from expected e
full outer join actual a
    on e.transaction_id = a.transaction_id
where coalesce(e.expected_anomaly_type, '') <> coalesce(a.actual_anomaly_type, '')
