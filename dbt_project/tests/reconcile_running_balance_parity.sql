/*
  Reconciliation control: per-row PARITY of the RETAIN-emulating running balance.

  Source of truth: daily_transaction_processing.sas Step 3.
      retain RUNNING_BALANCE;
      if first.ACCOUNT_ID then RUNNING_BALANCE = PRE_TXN_BALANCE;   /* = CURRENT_BALANCE */
      if  TRANSACTION_TYPE in ('DEP','INT','REF','REV') then RUNNING_BALANCE + amount;
      else if TRANSACTION_TYPE in ('WDR','PMT','FEE','CHG') then RUNNING_BALANCE - abs(amount);
      else if TRANSACTION_TYPE in ('TRF','ADJ') then RUNNING_BALANCE + amount;
  BY order: ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID (the PROC SQL ORDER BY
  in Step 2).

  The control recomputes the sequence independently of the mart — from the
  validated feed joined to the account balance — and compares row by row, so an
  off-by-one window frame, a wrong partition/order, or a mis-signed type surfaces
  on the offending transaction instead of netting out.

  It also asserts the source-faithful relationship between the two balances:
  PRE_TXN_BALANCE is the account's static CURRENT_BALANCE and POST_TXN_BALANCE is
  single-transaction arithmetic on it, so POST_TXN_BALANCE equals RUNNING_BALANCE
  only for an account's first transaction. That quirk is reproduced deliberately,
  not corrected.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select
        t.transaction_id,
        a.current_balance,
        a.current_balance
            + sum(
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
                rows between unbounded preceding and current row
            ) as expected_running_balance,
        a.current_balance
            + case
                when t.transaction_type in ('DEP', 'INT', 'REF', 'REV')
                    then t.transaction_amount
                when t.transaction_type in ('WDR', 'PMT', 'FEE', 'CHG')
                    then -abs(t.transaction_amount)
                when t.transaction_type in ('TRF', 'ADJ')
                    then t.transaction_amount
                else 0
            end as expected_post_txn_balance
    from {{ ref('stg_daily_transactions') }} t
    left join {{ ref('int_account_metrics') }} a
        on t.account_id = a.account_id
),

actual as (
    select
        transaction_id,
        current_balance,
        pre_txn_balance,
        post_txn_balance,
        running_balance
    from {{ ref('mart_daily_transactions') }}
)

select
    a.transaction_id,
    a.running_balance,
    e.expected_running_balance,
    a.post_txn_balance,
    e.expected_post_txn_balance
from actual a
inner join expected e
    on a.transaction_id = e.transaction_id
where round(a.running_balance, 2) <> round(e.expected_running_balance, 2)
   or round(a.post_txn_balance, 2) <> round(e.expected_post_txn_balance, 2)
   or round(a.pre_txn_balance, 2) <> round(a.current_balance, 2)
