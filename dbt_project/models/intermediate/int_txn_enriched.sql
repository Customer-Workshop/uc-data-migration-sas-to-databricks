/*
  int_txn_enriched.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Steps 2-3)

  SAS Original:
    PROC SQL creating WORK.TXN_ENRICHED (validated feed left joined to
    STG_BANK.CUST_ACCOUNTS_DAILY) followed by the DATA step creating
    WORK.TXN_WITH_BALANCE (RETAIN RUNNING_BALANCE, reset at first.ACCOUNT_ID).

  dbt Equivalent:
    LEFT JOIN replaces PROC SQL; a cumulative window sum seeded with the
    account's CURRENT_BALANCE replaces RETAIN + BY-group processing, using the
    same BY order (ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID).
*/

with transactions as (
    select * from {{ ref('stg_daily_transactions') }}
),

accounts as (
    select * from {{ ref('int_account_metrics') }}
),

-- SAS: WORK.TXN_ENRICHED
enriched as (
    select
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.transaction_type,
        t.transaction_amount,
        t.channel,
        t.merchant_category,
        t.description,
        t.post_date,
        t.currency_code,
        a.account_type,
        a.customer_id,
        a.customer_segment,
        a.region_code,
        a.branch_id,

        -- SAS: a.CURRENT_BALANCE as PRE_TXN_BALANCE (the account's opening
        -- balance, not the balance after the previous transaction)
        a.current_balance as pre_txn_balance,

        case
            when t.transaction_type in ('DEP', 'INT', 'REF', 'REV')
                then a.current_balance + t.transaction_amount
            when t.transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                then a.current_balance - abs(t.transaction_amount)
            when t.transaction_type in ('TRF', 'ADJ')
                then a.current_balance + t.transaction_amount
            else a.current_balance
        end as post_txn_balance,

        a.risk_rating,

        -- SAS: RETAIN RUNNING_BALANCE, reset to PRE_TXN_BALANCE on
        -- first.ACCOUNT_ID, then the same type arithmetic per row
        a.current_balance + sum(
            case
                when t.transaction_type in ('DEP', 'INT', 'REF', 'REV')
                    then t.transaction_amount
                when t.transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                    then -abs(t.transaction_amount)
                when t.transaction_type in ('TRF', 'ADJ')
                    then t.transaction_amount
                else 0
            end
        ) over (
            partition by t.account_id
            order by t.transaction_date, t.transaction_id
            rows unbounded preceding
        ) as running_balance,

        {{ format_account_type('a.account_type') }} as account_type_desc,
        {{ format_txn_category('t.transaction_type') }} as transaction_type_desc

    from transactions t
    left join accounts a
        on t.account_id = a.account_id
)

select * from enriched
