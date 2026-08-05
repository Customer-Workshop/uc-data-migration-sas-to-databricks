/*
  mart_daily_transactions.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Steps 2, 3, 5)

  SAS Original:
    Step 2 — PROC SQL creating WORK.TXN_ENRICHED: left join of the validated
             feed to STG_BANK.CUST_ACCOUNTS_DAILY, carrying
             a.CURRENT_BALANCE as PRE_TXN_BALANCE and a CASE-based
             POST_TXN_BALANCE, ordered by ACCOUNT_ID / TRANSACTION_DATE /
             TRANSACTION_ID.
    Step 3 — DATA step with RETAIN RUNNING_BALANCE + BY-group processing
             (reset to PRE_TXN_BALANCE on first.ACCOUNT_ID).
    Step 5 — PROC APPEND base=CURATED.DAILY_TRANSACTIONS.

  dbt Equivalent:
    SQL JOIN replaces PROC SQL, window function replaces RETAIN + BY-group
    logic, incremental materialization replaces PROC APPEND.

  Source-faithful quirks reproduced here (NOT corrected — see PR notes):
    * PRE_TXN_BALANCE is the account's static CURRENT_BALANCE for *every*
      transaction of the account, not the balance immediately preceding the
      transaction. SAS: `a.CURRENT_BALANCE as PRE_TXN_BALANCE`.
    * POST_TXN_BALANCE is likewise derived from the static CURRENT_BALANCE
      (single-transaction arithmetic), so for an account with several
      transactions POST_TXN_BALANCE does not equal RUNNING_BALANCE. Only
      RUNNING_BALANCE is cumulative.
    * TRF and ADJ add the *signed* amount (a transfer out is not subtracted),
      unlike WDR/PMT/FEE/CHG which subtract abs(amount).
    * The catch-all `else` leaves the balance unchanged (delta 0).
*/

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

with transactions as (
    select * from {{ ref('stg_daily_transactions') }}
),

accounts as (
    select * from {{ ref('int_account_metrics') }}
),

-- SAS: PROC SQL creating WORK.TXN_ENRICHED (join transactions to accounts)
base as (
    select
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.transaction_type,
        t.transaction_amount,
        t.description,
        a.account_type,
        a.customer_id,
        a.customer_segment,
        a.region_code,
        a.branch_id,
        a.current_balance,
        a.risk_rating,

        -- SAS Step 2/3: the signed balance movement per transaction type.
        -- Mirrors the CASE in WORK.TXN_ENRICHED and the IF/ELSE chain in the
        -- RETAIN DATA step value-for-value, including the catch-all else.
        case
            when t.transaction_type in ('DEP', 'INT', 'REF', 'REV')
                then t.transaction_amount
            when t.transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                then -abs(t.transaction_amount)
            when t.transaction_type in ('TRF', 'ADJ')
                then t.transaction_amount
            else 0
        end as balance_delta,

        -- SAS: a.CURRENT_BALANCE as PRE_TXN_BALANCE (static per account)
        a.current_balance as pre_txn_balance,

        {{ format_account_type('a.account_type') }} as account_type_desc,
        {{ format_txn_category('t.transaction_type') }} as transaction_type_desc

    from transactions t
    left join accounts a
        on t.account_id = a.account_id
),

enriched as (
    select
        *,

        -- SAS Step 2: POST_TXN_BALANCE = CURRENT_BALANCE +/- this transaction
        -- only (source-faithful: not cumulative).
        pre_txn_balance + balance_delta as post_txn_balance,

        -- SAS Step 3: RETAIN RUNNING_BALANCE reset to PRE_TXN_BALANCE on
        -- first.ACCOUNT_ID, then accumulated in BY order — replaced by a
        -- cumulative window sum over the same ordering.
        pre_txn_balance + sum(balance_delta) over (
            partition by account_id
            order by transaction_date, transaction_id
            rows unbounded preceding
        ) as running_balance

    from base
)

select * from enriched

{% if is_incremental() %}
where transaction_date >= (select max(transaction_date) from {{ this }})
{% endif %}
