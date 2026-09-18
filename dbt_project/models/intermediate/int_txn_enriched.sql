/*
  int_txn_enriched.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Steps 2-3)

  SAS Original:
    Step 2  PROC SQL WORK.TXN_ENRICHED — validated feed LEFT JOIN
            STG_BANK.CUST_ACCOUNTS_DAILY (today's snapshot) adding
            ACCOUNT_TYPE, CUSTOMER_ID, CUSTOMER_SEGMENT, REGION_CODE, BRANCH_ID,
            PRE_TXN_BALANCE (= snapshot CURRENT_BALANCE), POST_TXN_BALANCE
            (= CURRENT_BALANCE +/- this one transaction) and RISK_RATING,
            ordered by ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID.
    Step 3  DATA WORK.TXN_WITH_BALANCE — BY ACCOUNT_ID TRANSACTION_DATE
            TRANSACTION_ID; RETAIN RUNNING_BALANCE; reset to PRE_TXN_BALANCE
            on first.ACCOUNT_ID, then cumulative +/- per transaction type.

  dbt Equivalent:
    PRE_TXN_BALANCE and POST_TXN_BALANCE are per-row against the *snapshot*
    balance exactly as SAS computes them (they do NOT chain through earlier
    transactions of the same account). RUNNING_BALANCE is the chained value,
    expressed as snapshot balance + cumulative window SUM in the SAS BY order.
    For an account missing from the snapshot (ORPHAN_ACCOUNT) all three are
    null in SAS as well (missing + x = missing).
*/

with transactions as (
    select * from {{ ref('stg_daily_transactions') }}
),

accounts as (
    select * from {{ ref('int_account_metrics') }}
),

enriched as (
    select
        -- t.* (feed columns)
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
        -- account attributes
        a.account_type,
        a.customer_id,
        a.customer_segment,
        a.region_code,
        a.branch_id,
        a.current_balance as pre_txn_balance,
        a.current_balance + {{ sas_txn_balance_effect('t.transaction_type', 't.transaction_amount') }}
            as post_txn_balance,
        a.risk_rating,
        -- SAS: RETAIN RUNNING_BALANCE; if first.ACCOUNT_ID then RUNNING_BALANCE = PRE_TXN_BALANCE
        a.current_balance + sum(
            {{ sas_txn_balance_effect('t.transaction_type', 't.transaction_amount') }}
        ) over (
            partition by t.account_id
            order by t.transaction_date, t.transaction_id
            rows between unbounded preceding and current row
        ) as running_balance,
        {{ format_account_type('a.account_type') }} as account_type_desc,
        {{ format_txn_category('t.transaction_type') }} as transaction_type_desc
    from transactions t
    left join accounts a
        on t.account_id = a.account_id
)

select * from enriched
