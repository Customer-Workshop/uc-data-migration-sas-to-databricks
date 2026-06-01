/*
  Reconciliation test: RWA exposure control total.

  Total exposure in mart_regulatory_rwa must tie out to the sum of
  current_balance across all in-scope accounts from the staging layer.
  A mismatch indicates row loss, fan-out, or a balance computation error
  in the conversion.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with mart_total as (
    select coalesce(sum(total_exposure), 0) as mart_exposure
    from {{ ref('mart_regulatory_rwa') }}
),

source_total as (
    select coalesce(sum(current_balance), 0) as source_exposure
    from {{ ref('stg_cust_accounts') }}
)

select
    s.source_exposure,
    m.mart_exposure,
    abs(m.mart_exposure - s.source_exposure) as difference
from source_total s
cross join mart_total m
where abs(m.mart_exposure - s.source_exposure) > 0.01
