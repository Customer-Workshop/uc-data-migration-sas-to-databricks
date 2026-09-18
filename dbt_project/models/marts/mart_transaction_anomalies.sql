/*
  mart_transaction_anomalies.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Steps 4-5)
  Output contract: CURATED.TXN_ANOMALIES

  SAS Original:
    WORK.TXN_STATS  — per-account mean/std of abs(TRANSACTION_AMOUNT) and count
                      over CURATED.DAILY_TRANSACTIONS where
                      TRANSACTION_DATE >= intnx('day', "&txn_date"d, -90)
                      (read BEFORE today's append, so history only).
    WORK.TXN_ANOMALIES — TXN_WITH_BALANCE left join TXN_STATS, Z_SCORE when
                      STD > 0, ANOMALY_TYPE by first matching rule:
                        Z_SCORE > 3                                  -> HIGH_AMOUNT
                        RUNNING_BALANCE < 0                          -> OVERDRAFT
                        WDR and abs(amount) > PRE_TXN_BALANCE * 0.9  -> LARGE_WITHDRAWAL
                        CUSTOMER_ID missing (no snapshot match)      -> ORPHAN_ACCOUNT
                      having ANOMALY_TYPE ne ''.
    proc append base=CURATED.TXN_ANOMALIES data=WORK.TXN_ANOMALIES force;

  dbt Equivalent:
    SAS std() is the sample standard deviation -> stddev_samp(). SAS mean()
    ignores missing -> avg(). SAS SQL treats a missing value as lower than any
    number, so the comparisons on RUNNING_BALANCE / PRE_TXN_BALANCE are made
    null-aware to reproduce that (see the CASE below). The history is the estate's extract of the
    curated table before the batch (source curated_daily_transactions_history).
    Incremental MERGE on TRANSACTION_ID replaces PROC APPEND.
*/

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

with enriched as (
    select * from {{ ref('int_txn_enriched') }}
),

-- SAS: WORK.TXN_STATS
txn_stats as (
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
        e.transaction_id,
        e.account_id,
        e.transaction_date,
        e.transaction_type,
        e.transaction_amount,
        e.channel,
        e.merchant_category,
        e.description,
        e.post_date,
        e.currency_code,
        e.account_type,
        e.customer_id,
        e.customer_segment,
        e.region_code,
        e.branch_id,
        e.pre_txn_balance,
        e.post_txn_balance,
        e.risk_rating,
        e.running_balance,
        s.avg_txn_amt,
        s.std_txn_amt,
        case
            when s.std_txn_amt > 0
                then (abs(e.transaction_amount) - s.avg_txn_amt) / s.std_txn_amt
            else null
        end as z_score
    from enriched e
    left join txn_stats s
        on e.account_id = s.account_id
),

classified as (
    select
        *,
        case
            when z_score > 3 then 'HIGH_AMOUNT'
            -- SAS PROC SQL ranks missing below every number, so an account with no
            -- snapshot match (RUNNING_BALANCE missing) satisfies RUNNING_BALANCE < 0
            -- and is reported as OVERDRAFT before the ORPHAN_ACCOUNT rule is reached.
            when running_balance < 0 or running_balance is null then 'OVERDRAFT'
            when transaction_type = 'WDR'
                and (abs(transaction_amount) > pre_txn_balance * 0.9 or pre_txn_balance is null)
                then 'LARGE_WITHDRAWAL'
            when customer_id is null then 'ORPHAN_ACCOUNT'
            else ''
        end as anomaly_type
    from scored
)

select
    *,
    {{ format_txn_category('transaction_type') }} as transaction_type_desc
from classified
where anomaly_type <> ''

{% if is_incremental() %}
  and transaction_date >= (select max(transaction_date) from {{ this }})
{% endif %}
