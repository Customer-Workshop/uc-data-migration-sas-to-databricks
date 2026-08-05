/*
  Reconciliation control: COMPLETENESS of the persisted running-balance table.

  Source of truth: daily_transaction_processing.sas Step 6, which writes
  CURATED.RUNNING_BALANCES from WORK.TXN_WITH_BALANCE keeping ACCOUNT_ID,
  TRANSACTION_DATE, TRANSACTION_ID and RUNNING_BALANCE — one row per processed
  transaction, no filtering and no aggregation.

  So mart_running_balances must carry exactly the transaction keys of
  mart_daily_transactions with identical RUNNING_BALANCE values: any missing,
  extra, or altered row fails.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with txns as (
    select
        transaction_id,
        account_id,
        transaction_date,
        running_balance
    from {{ ref('mart_daily_transactions') }}
),

balances as (
    select
        transaction_id,
        account_id,
        transaction_date,
        running_balance
    from {{ ref('mart_running_balances') }}
)

select
    coalesce(t.transaction_id, b.transaction_id) as transaction_id,
    t.running_balance as txn_running_balance,
    b.running_balance as persisted_running_balance
from txns t
full outer join balances b
    on t.transaction_id = b.transaction_id
where t.transaction_id is null
   or b.transaction_id is null
   or t.account_id <> b.account_id
   or t.transaction_date <> b.transaction_date
   or round(t.running_balance, 2) <> round(b.running_balance, 2)
