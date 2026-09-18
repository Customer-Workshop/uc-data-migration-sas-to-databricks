/*
  mart_daily_transactions_curated.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 5)
  Output contract: CURATED.DAILY_TRANSACTIONS

  SAS Original:
    proc append base=CURATED.DAILY_TRANSACTIONS data=WORK.TXN_WITH_BALANCE force;
    The base table carries the ten feed columns only, so FORCE drops the
    enrichment columns (ACCOUNT_TYPE, CUSTOMER_ID, ..., RUNNING_BALANCE) with
    a WARNING and the appended rows keep just the feed shape. Prior content is
    retained — the table is the bank's rolling transaction history and is what
    the next day's 90-day anomaly statistics read.

  dbt Equivalent:
    Prior history (source curated_daily_transactions_history, the estate's
    extract of the table before the batch) UNION ALL today's validated rows,
    projected to the feed columns. Row count = history + validated feed.
*/

with history as (
    select
        transaction_id,
        account_id,
        transaction_date,
        transaction_type,
        transaction_amount,
        channel,
        merchant_category,
        description,
        post_date,
        currency_code
    from {{ source('banking_raw', 'curated_daily_transactions_history') }}
),

today as (
    select
        transaction_id,
        account_id,
        transaction_date,
        transaction_type,
        transaction_amount,
        channel,
        merchant_category,
        description,
        post_date,
        currency_code
    from {{ ref('int_txn_enriched') }}
)

select * from history
union all
select * from today
