/*
  int_txn_with_balance.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas
                 (Step 2 PROC SQL enrichment + Step 3 DATA step RETAIN)

  SAS Original:
    PROC SQL creating WORK.TXN_ENRICHED (left join of the validated feed to
    STG_BANK.CUST_ACCOUNTS_DAILY, with POST_TXN_BALANCE computed per row from
    the account snapshot balance), then a DATA step creating
    WORK.TXN_WITH_BALANCE that RETAINs RUNNING_BALANCE across the
    BY ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID group.

  dbt Equivalent:
    LEFT JOIN for the enrichment, a running-sum window function for the
    RETAIN + BY-group running balance.

  Source-faithful quirks reproduced here (see PR notes, not endorsements):
    - POST_TXN_BALANCE is derived from the account's start-of-day
      CURRENT_BALANCE on every row, so it is NOT the cumulative balance and
      disagrees with RUNNING_BALANCE for accounts with more than one
      transaction in the day.
    - PRE_TXN_BALANCE is likewise the start-of-day snapshot balance on every
      row, not the balance before this particular transaction.
    - Deposit-like types add TRANSACTION_AMOUNT as signed, withdrawal-like
      types subtract abs(TRANSACTION_AMOUNT), TRF/ADJ add as signed, and any
      other type (unreachable after validation) moves the balance by zero.
*/

with transactions as (
    select * from {{ ref('stg_daily_transactions') }}
),

-- SAS: STG_BANK.CUST_ACCOUNTS_DAILY, the snapshot written by
-- load_customer_accounts.sas earlier in the same batch.
accounts as (
    select * from {{ ref('int_account_metrics') }}
),

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
        a.risk_rating,
        a.current_balance as pre_txn_balance,

        -- SAS Step 2: POST_TXN_BALANCE off the snapshot balance, per row
        case
            when t.transaction_type in ('DEP', 'INT', 'REF', 'REV')
                then a.current_balance + t.transaction_amount
            when t.transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                then a.current_balance - abs(t.transaction_amount)
            when t.transaction_type in ('TRF', 'ADJ')
                then a.current_balance + t.transaction_amount
            else a.current_balance
        end as post_txn_balance,

        -- SAS Step 3: RETAIN RUNNING_BALANCE, seeded at first.ACCOUNT_ID with
        -- PRE_TXN_BALANCE and moved by the same signed amounts.
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
