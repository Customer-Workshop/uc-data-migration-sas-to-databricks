/*
  stg_daily_transactions.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 1)

  SAS Original:
    DATA step reading RAW_BANK.TXN_FEED_YYYYMMDD and routing each row to
    WORK.TXN_VALIDATED or WORK.TXN_REJECTED (first failing rule wins, `return`
    stops further checks).

  dbt Equivalent:
    One CASE expression evaluated in the same order as the SAS IF chain gives
    the first failing rule; this model is the TXN_VALIDATED branch (rows with
    no reject reason) and stg_txn_rejected is the TXN_REJECTED branch.
*/

with source as (
    select * from {{ source('banking_raw', 'daily_transactions') }}
),

validated as (
    select
        *,
        case
            when transaction_id is null or transaction_id = '' then 'Missing TRANSACTION_ID'
            when account_id is null or account_id = '' then 'Missing ACCOUNT_ID'
            when transaction_amount is null then 'Missing TRANSACTION_AMOUNT'
            when abs(transaction_amount) > 10000000 then 'Amount exceeds threshold'
            when
                transaction_type not in ('DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF')
                or transaction_type is null
                then 'Invalid transaction type'
            -- SAS: if TRANSACTION_DATE > "&txn_date"d
            when transaction_date > {{ sas_run_date() }} then 'Future dated'
            else null
        end as rejection_reason
    from source
),

-- Equivalent of the SAS "output WORK.TXN_VALIDATED" path
accepted as (
    select * from validated
    where rejection_reason is null
)

select * from accepted
