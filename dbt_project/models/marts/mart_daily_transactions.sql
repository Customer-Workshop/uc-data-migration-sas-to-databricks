/*
  mart_daily_transactions.sql
  Migrated from: Programs/Banking/daily_transaction_processing.sas (Steps 2, 3, 5)

  SAS Original:
    WORK.TXN_WITH_BALANCE — today's validated feed enriched with the account
    snapshot plus PRE/POST/RUNNING balance — is PROC APPENDed (FORCE) onto
    CURATED.DAILY_TRANSACTIONS.

  dbt Equivalent:
    The enriched daily slice, kept with all its balance columns and
    materialised incrementally (MERGE on TRANSACTION_ID replaces PROC APPEND,
    and makes a same-day re-run idempotent). The permanent-table contract of
    CURATED.DAILY_TRANSACTIONS (FORCE drops the enrichment columns because
    the base table only has the feed columns) is mart_daily_transactions_curated.
*/

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

select * from {{ ref('int_txn_enriched') }}

{% if is_incremental() %}
where transaction_date >= (select max(transaction_date) from {{ this }})
{% endif %}
