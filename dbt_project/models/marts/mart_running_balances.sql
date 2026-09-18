/*
  mart_running_balances.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 6)

  SAS Original:
      data CURATED.RUNNING_BALANCES;
        set WORK.TXN_WITH_BALANCE;
        keep ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID RUNNING_BALANCE;
      run;

    A full replace (not an append), keeping four columns of the business
    date's transactions.
*/

select
    account_id,
    transaction_date,
    transaction_id,
    running_balance
from {{ ref('int_txn_with_balance') }}
