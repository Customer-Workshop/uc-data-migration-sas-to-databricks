/*
  mart_daily_transactions.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 5)

  SAS Original:
      proc append base=CURATED.DAILY_TRANSACTIONS
                  data=WORK.TXN_WITH_BALANCE force;

    CURATED.DAILY_TRANSACTIONS already holds the transaction history; the daily
    run appends the business date's validated feed to it.

  dbt Equivalent:
    A table that unions the curated history with the validated feed for the
    business date. Rebuilding produces exactly the same set, where re-running
    the SAS job would append the same rows twice.

  Source-faithful quirk reproduced here (flagged, not endorsed):
    PROC APPEND ... FORCE drops the nine enrichment columns that
    WORK.TXN_WITH_BALANCE carries (ACCOUNT_TYPE, CUSTOMER_ID,
    PRE_TXN_BALANCE, RUNNING_BALANCE, ...), because the history table does not
    have them. The curated table therefore holds only the ten feed columns, and
    that is what this mart holds. The enrichment and running balances live in
    int_txn_with_balance and mart_running_balances.
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

todays_feed as (
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
    from {{ ref('stg_daily_transactions') }}
)

select * from history
union all
select * from todays_feed
