/*
  Mapping parity: each rejected row carries the reason SAS would assign and the
  validated branch contains no row that violates any reject rule.
*/
with rejected as (
    select
        transaction_id,
        reject_reason,
        case
            when transaction_id is null or transaction_id = '' then 'Missing TRANSACTION_ID'
            when account_id is null or account_id = '' then 'Missing ACCOUNT_ID'
            when transaction_amount is null then 'Missing TRANSACTION_AMOUNT'
            when abs(transaction_amount) > 10000000 then 'Amount exceeds threshold'
            when transaction_type is null
                or transaction_type not in ('DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF')
                then 'Invalid transaction type'
            when transaction_date > {{ sas_run_date() }} then 'Future dated'
        end as expected_prefix
    from {{ ref('stg_txn_rejected') }}
),

bad_reject as (
    select transaction_id, 'reject reason mismatch' as issue, reject_reason as detail
    from rejected
    where expected_prefix is null or not startswith(reject_reason, expected_prefix)
),

bad_valid as (
    select transaction_id, 'validated row violates a reject rule' as issue, transaction_type as detail
    from {{ ref('stg_daily_transactions') }}
    where transaction_id is null or transaction_id = ''
       or account_id is null or account_id = ''
       or transaction_amount is null
       or abs(transaction_amount) > 10000000
       or transaction_type not in ('DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF')
       or transaction_date > {{ sas_run_date() }}
)

select * from bad_reject
union all
select * from bad_valid
