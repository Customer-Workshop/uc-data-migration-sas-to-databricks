/*
  Reconciliation test: total_relationship control total.

  SAS Step 1 computes total_relationship = sum(CURRENT_BALANCE) by customer.
  The mart's aggregate must tie out to the source account population — proving
  no balances were silently dropped or double-counted by the join logic.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_total as (
    select cast(sum(current_balance) as decimal(38, 2)) as total
    from {{ ref('int_account_metrics') }}
),

mart_total as (
    select cast(sum(total_relationship) as decimal(38, 2)) as total
    from {{ ref('mart_customer_pnl') }}
)

select
    s.total as source_balance_total,
    m.total as mart_relationship_total,
    m.total - s.total as difference
from source_total s
cross join mart_total m
where abs(m.total - s.total) > 0.01
