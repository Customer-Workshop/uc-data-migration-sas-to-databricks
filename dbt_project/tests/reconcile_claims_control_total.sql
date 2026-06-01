/*
  Reconciliation test: claims control total.

  Total claimed_amount in the adjudicated output must tie back to the
  staged valid claims. No money should appear or vanish during fraud
  screening and adjudication — those steps classify and route but do
  not alter claimed_amount.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with staging_total as (
    select
        round(sum(claimed_amount), 2) as total_claimed
    from {{ ref('stg_claims') }}
),

adjudicated_total as (
    select
        round(sum(claimed_amount), 2) as total_claimed
    from {{ ref('int_claims_adjudication') }}
)

select
    s.total_claimed as staging_total,
    a.total_claimed as adjudicated_total,
    round(a.total_claimed - s.total_claimed, 2) as difference
from staging_total s
cross join adjudicated_total a
where round(s.total_claimed - a.total_claimed, 2) <> 0
