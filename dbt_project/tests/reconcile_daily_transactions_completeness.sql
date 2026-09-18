/*
  Reconciliation test: curated transaction completeness.

  daily_transaction_processing.sas appends every validated feed record to
  CURATED.DAILY_TRANSACTIONS and drops the rest into WORK.TXN_REJECTED. The
  curated table must therefore hold exactly the prior history plus the feed
  rows that pass all six validation rules — no silent row loss, no join
  fan-out. The expected count is recomputed here straight from the raw feed
  rather than from the staging model, so a change to the staging validation
  logic cannot quietly move both sides of the control.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select
        (select count(*) from ({{ curated_txn_history() }}))
        + (
            select count(*)
            from {{ source('banking_raw', 'daily_transactions') }}
            where transaction_id is not null and trim(transaction_id) <> ''
              and account_id is not null and trim(account_id) <> ''
              and transaction_amount is not null
              and abs(transaction_amount) <= 10000000
              and transaction_type in (
                  'DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF'
              )
              and (transaction_date <= {{ sas_run_date() }} or transaction_date is null)
        ) as n
),

actual as (
    select count(*) as n from {{ ref('mart_daily_transactions') }}
)

select
    e.n as expected_rows,
    a.n as model_rows,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
