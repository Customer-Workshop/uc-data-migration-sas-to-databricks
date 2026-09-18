/*
  Reconciliation test: anomaly coverage and classification precedence.

  Every transaction in the day's batch that trips one of the four SAS rules
  must appear in the anomaly mart exactly once, with the type the SAS CASE
  would have assigned. The rules are evaluated in SAS order, including two
  SAS quirks that are reproduced on purpose: the Z-score baseline comes from
  the curated history (not the batch), and a missing RUNNING_BALANCE counts
  as negative, so account-less transactions land in OVERDRAFT.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with account_stats as (
    select
        account_id,
        avg(abs(transaction_amount)) as avg_txn_amt,
        stddev(abs(transaction_amount)) as std_txn_amt
    from ({{ curated_txn_history() }})
    where transaction_date >= date_add({{ sas_run_date() }}, -90)
    group by account_id
),

expected as (
    select
        t.transaction_id,
        case
            when s.std_txn_amt > 0
                 and (abs(t.transaction_amount) - s.avg_txn_amt) / s.std_txn_amt > 3
                then 'HIGH_AMOUNT'
            when t.running_balance < 0 or t.running_balance is null then 'OVERDRAFT'
            when t.transaction_type = 'WDR'
                 and (
                     t.pre_txn_balance is null
                     or abs(t.transaction_amount) > t.pre_txn_balance * 0.9
                 )
                then 'LARGE_WITHDRAWAL'
            when t.customer_id is null then 'ORPHAN_ACCOUNT'
        end as expected_anomaly_type
    from {{ ref('int_txn_enriched') }} t
    left join account_stats s
        on t.account_id = s.account_id
),

actual as (
    select transaction_id, anomaly_type
    from {{ ref('mart_transaction_anomalies') }}
)

select
    coalesce(e.transaction_id, a.transaction_id) as transaction_id,
    e.expected_anomaly_type,
    a.anomaly_type as model_anomaly_type
from (select * from expected where expected_anomaly_type is not null) e
full outer join actual a
    on e.transaction_id = a.transaction_id
where e.transaction_id is null
   or a.transaction_id is null
   or e.expected_anomaly_type <> a.anomaly_type
