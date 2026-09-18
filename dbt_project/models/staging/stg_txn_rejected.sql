/*
  stg_txn_rejected.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 1)

  SAS Original:
    WORK.TXN_REJECTED — feed rows that failed validation, with REJECT_REASON.
    SAS only logs the count (%nobs) and deletes the WORK table at the end; the
    rows never reach a permanent library.

  dbt Equivalent:
    Kept as a view so the completeness control can prove
    feed rows = validated rows + rejected rows.
    REJECT_REASON text mirrors the SAS catx() messages minus the formatted
    value suffix (put(..., dollar18.2) / put(..., date9.)).
*/

with source as (
    select * from {{ source('banking_raw', 'daily_transactions') }}
),

rejected as (
    select
        *,
        case
            when transaction_id is null or transaction_id = '' then 'Missing TRANSACTION_ID'
            when account_id is null or account_id = '' then 'Missing ACCOUNT_ID'
            when transaction_amount is null then 'Missing TRANSACTION_AMOUNT'
            when abs(transaction_amount) > 10000000
                then concat('Amount exceeds threshold: ', cast(transaction_amount as string))
            when
                transaction_type not in ('DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF')
                or transaction_type is null
                then concat('Invalid transaction type: ', coalesce(transaction_type, ''))
            when transaction_date > {{ sas_run_date() }}
                then concat('Future dated: ', date_format(transaction_date, 'ddMMMyyyy'))
            else null
        end as reject_reason
    from source
)

select * from rejected
where reject_reason is not null
