/*
  Reconciliation control: completeness of the curated transaction table.

  Scope contract from daily_transaction_processing.sas: the curated table is
  the transaction history it already held, plus the business date's feed minus
  the rows the DATA step routed to WORK.TXN_REJECTED (missing key fields,
  abs(amount) > 10,000,000, transaction type outside the ten valid codes, or a
  transaction date after the business date).

  This control counts that population straight from the raw feed and history —
  independently of the staging model — so a lost row, a duplicated row or a
  fanned-out join in the conversion shows up here.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select
        (select count(*) from {{ source('banking_raw', 'curated_daily_transactions_history') }})
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
              and transaction_date <= {{ sas_run_date() }}
        ) as n
),

actual as (
    select count(*) as n from {{ ref('mart_daily_transactions') }}
)

select
    e.n as expected_curated_rows,
    a.n as model_rows,
    a.n - e.n as difference
from expected e
cross join actual a
where e.n <> a.n
