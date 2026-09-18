/*
  mart_running_balances.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 6)
  Output contract: CURATED.RUNNING_BALANCES

  SAS Original:
    data CURATED.RUNNING_BALANCES;
      set WORK.TXN_WITH_BALANCE;
      keep ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID RUNNING_BALANCE;
    run;
    A DATA step *replaces* the table each night (unlike the PROC APPENDs).

  dbt Equivalent:
    Table materialisation (full replace) projecting the four kept columns.
*/

select
    account_id,
    transaction_date,
    transaction_id,
    running_balance
from {{ ref('int_txn_enriched') }}
