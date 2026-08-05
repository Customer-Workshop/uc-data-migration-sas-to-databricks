/*
  Reconciliation control: COMPLETENESS of the converted transaction feed.

  Source of truth: daily_transaction_processing.sas Step 1. The DATA step routes
  a feed record to WORK.TXN_REJECTED (and returns) when any of the following
  holds, otherwise it is output to WORK.TXN_VALIDATED:
      missing TRANSACTION_ID / ACCOUNT_ID / TRANSACTION_AMOUNT
      abs(TRANSACTION_AMOUNT) > 10000000
      TRANSACTION_TYPE not in the ten valid codes
      TRANSACTION_DATE > &txn_date
  Only the validated records are appended to CURATED.DAILY_TRANSACTIONS (Step 5).

  This control recomputes that in-scope population straight from the raw feed and
  requires the mart to carry exactly those rows — no silent row loss and no
  fan-out from the account join (which is why distinct transaction_id is checked
  alongside the row count).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('banking_raw', 'daily_transactions') }}
    where transaction_id is not null
      and account_id is not null
      and transaction_amount is not null
      and abs(transaction_amount) <= 10000000
      and transaction_type in (
          'DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF'
      )
      and transaction_date <= current_date()
),

model_txns as (
    select
        count(*) as n,
        count(distinct transaction_id) as n_distinct
    from {{ ref('mart_daily_transactions') }}
)

select
    e.n as expected_in_scope_txns,
    m.n as model_txns,
    m.n_distinct as model_distinct_txns,
    m.n - e.n as difference
from expected_in_scope e
cross join model_txns m
where e.n <> m.n or m.n <> m.n_distinct
