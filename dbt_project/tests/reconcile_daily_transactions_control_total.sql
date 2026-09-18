/*
  Reconciliation test: transaction amount control total.

  A row count alone does not prove the right rows landed. This control ties
  the curated table's total transaction amount back to the source: the prior
  history total plus the total of the feed rows that pass SAS validation. Any
  duplicated, dropped or rescaled amount moves this total.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected as (
    select
        round(
            (select coalesce(sum(transaction_amount), 0) from ({{ curated_txn_history() }}))
            + (
                select coalesce(sum(transaction_amount), 0)
                from {{ source('banking_raw', 'daily_transactions') }}
                where transaction_id is not null and trim(transaction_id) <> ''
                  and account_id is not null and trim(account_id) <> ''
                  and transaction_amount is not null
                  and abs(transaction_amount) <= 10000000
                  and transaction_type in (
                      'DEP', 'WDR', 'TRF', 'PMT', 'FEE', 'INT', 'ADJ', 'REV', 'CHG', 'REF'
                  )
                  and (transaction_date <= {{ sas_run_date() }} or transaction_date is null)
            ), 2
        ) as total
),

actual as (
    select round(coalesce(sum(transaction_amount), 0), 2) as total
    from {{ ref('mart_daily_transactions') }}
)

select
    e.total as expected_total,
    a.total as model_total,
    a.total - e.total as difference
from expected e
cross join actual a
where e.total <> a.total
