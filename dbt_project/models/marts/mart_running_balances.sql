/*
  mart_running_balances.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Step 6)

  SAS Original:
    data CURATED.RUNNING_BALANCES;
      set WORK.TXN_WITH_BALANCE;
      keep ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID RUNNING_BALANCE;
    run;

  dbt Equivalent:
    A projection of the transaction mart carrying exactly the four kept
    columns. The SAS DATA step *replaces* CURATED.RUNNING_BALANCES on every
    run (unlike the PROC APPEND used for DAILY_TRANSACTIONS in Step 5), which
    maps to a full-refresh table materialization.

  Source-faithful note:
    RUNNING_BALANCE is the cumulative BY-group balance from Step 3 (reset to the
    account's CURRENT_BALANCE at first.ACCOUNT_ID), not the per-transaction
    POST_TXN_BALANCE — see mart_daily_transactions.
*/

{{ config(materialized='table') }}

select
    account_id,
    transaction_date,
    transaction_id,
    running_balance
from {{ ref('mart_daily_transactions') }}
