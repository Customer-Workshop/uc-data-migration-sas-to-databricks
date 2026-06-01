/*
  Reconciliation test: control total — sum of claimed_amount must tie out between
  stg_claims (validated input) and int_claims_adjudication (adjudicated output).

  The SAS auto-adjudication step (Step 3) does NOT drop any rows — every valid
  claim is either auto-approved, denied, or sent to manual review. Therefore the
  total claimed_amount entering the adjudication step must equal the total exiting.

  This catches accidental row loss or duplication in the adjudication logic.

  dbt singular test convention: returns rows on FAILURE.
*/
with staging_total as (
    select
        count(*) as n_rows,
        cast(sum(claimed_amount) as decimal(18, 2)) as total_claimed
    from {{ ref('stg_claims') }}
),

adjudication_total as (
    select
        count(*) as n_rows,
        cast(sum(claimed_amount) as decimal(18, 2)) as total_claimed
    from {{ ref('int_claims_adjudication') }}
)

select
    s.n_rows as staging_rows,
    a.n_rows as adjudication_rows,
    s.total_claimed as staging_total_claimed,
    a.total_claimed as adjudication_total_claimed,
    a.n_rows - s.n_rows as row_difference,
    a.total_claimed - s.total_claimed as amount_difference
from staging_total s
cross join adjudication_total a
where s.n_rows <> a.n_rows
   or s.total_claimed <> a.total_claimed
