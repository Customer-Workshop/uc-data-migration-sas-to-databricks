/*
  mart_daily_transactions.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 5)

  SAS Original:
    PROC APPEND base=CURATED.DAILY_TRANSACTIONS data=WORK.TXN_WITH_BALANCE force

  dbt Equivalent:
    The curated table is the prior history plus the day's validated batch.
    PROC APPEND ... FORCE keeps only the columns the base table already has, so
    the enrichment columns (balances, customer attributes) are dropped here —
    they live on int_txn_enriched, the equivalent of WORK.TXN_WITH_BALANCE.
*/

with history as (
    {{ curated_txn_history() }}
),

todays_batch as (
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
select * from todays_batch
