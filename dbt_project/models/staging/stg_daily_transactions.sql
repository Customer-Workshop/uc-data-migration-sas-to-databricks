/*
  stg_daily_transactions.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 1)

  SAS Original:
    DATA step splitting RAW_BANK.TXN_FEED_<yyyymmdd> into WORK.TXN_VALIDATED
    and WORK.TXN_REJECTED, with REJECT_REASON set by the first failing rule.

  dbt Equivalent:
    The same rules as an ordered CASE producing rejection_reason; rows with no
    reason are the validated feed. SAS missing() is true for a blank character
    value as well as a null, so both are treated as missing here.
*/

with source as (
    select * from {{ source('banking_raw', 'daily_transactions') }}
),

validated as (
    select
        *,
        case
            when transaction_id is null or trim(transaction_id) = ''
                then 'Missing TRANSACTION_ID'
            when account_id is null or trim(account_id) = ''
                then 'Missing ACCOUNT_ID'
            when transaction_amount is null
                then 'Missing TRANSACTION_AMOUNT'
            when abs(transaction_amount) > 10000000
                then 'Amount exceeds threshold'
            when transaction_type
                not in ('DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF')
                then 'Invalid transaction type'
            when transaction_date > {{ sas_run_date() }}
                then 'Future dated'
        end as rejection_reason
    from source
),

-- Equivalent of the SAS "output WORK.TXN_VALIDATED" path
accepted as (
    select * from validated
    where rejection_reason is null
)

select * from accepted
