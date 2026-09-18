/*
  Reconciliation test: balance arithmetic parity with the SAS DATA step.

  Two things must hold for every enriched transaction:

    * POST_TXN_BALANCE follows the SAS CASE branch for its transaction type,
      applied to the account's opening balance (PRE_TXN_BALANCE);
    * RUNNING_BALANCE equals the opening balance plus the signed amounts of
      every earlier transaction on the account in BY order
      (ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID), which is what SAS
      RETAIN + first.ACCOUNT_ID produces.

  The expected values are rebuilt here with an ordered self-join rather than
  the model's window function, so an error in the window frame shows up.
  Rows whose account has no master record carry missing balances in SAS; they
  are compared as missing on both sides.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with txns as (
    select
        transaction_id,
        account_id,
        transaction_date,
        transaction_type,
        transaction_amount,
        pre_txn_balance,
        post_txn_balance,
        running_balance,
        case
            when transaction_type in ('DEP', 'INT', 'REF', 'REV') then transaction_amount
            when transaction_type in ('WDR', 'PMT', 'FEE', 'CHG') then -abs(transaction_amount)
            when transaction_type in ('TRF', 'ADJ') then transaction_amount
            else 0
        end as signed_amount
    from {{ ref('int_txn_enriched') }}
),

expected as (
    select
        t.transaction_id,
        t.post_txn_balance,
        t.running_balance,
        t.pre_txn_balance + t.signed_amount as expected_post_txn_balance,
        t.pre_txn_balance + (
            select sum(p.signed_amount)
            from txns p
            where p.account_id = t.account_id
              and (
                  p.transaction_date < t.transaction_date
                  or (
                      p.transaction_date = t.transaction_date
                      and p.transaction_id <= t.transaction_id
                  )
              )
        ) as expected_running_balance
    from txns t
)

select *
from expected
where round(post_txn_balance, 6) is distinct from round(expected_post_txn_balance, 6)
   or round(running_balance, 6) is distinct from round(expected_running_balance, 6)
