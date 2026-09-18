/*
  mart_running_balances.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 6)

  SAS Original:
    data CURATED.RUNNING_BALANCES; set WORK.TXN_WITH_BALANCE;
      keep ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID RUNNING_BALANCE; run;

  dbt Equivalent:
    A projection of int_txn_enriched. SAS rewrites this table each run, so it
    holds the current batch only.
*/

select
    account_id,
    transaction_date,
    transaction_id,
    running_balance
from {{ ref('int_txn_enriched') }}
