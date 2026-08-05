/*
  Reconciliation test: claims register completeness and no fan-out.

  The expected population independently re-derives the SAS Step 1 active
  policy lookup and both validation rules from raw sources. Invalid claims
  are intentionally absent because SAS only persists CLAIMS_VALID.
*/
with expected_claims as (
    select c.claim_id
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
       and p.policy_status = 'ACTIVE'
    where c.loss_date is not null
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and (
          c.claimed_amount is null
          or c.claimed_amount <= p.sum_insured
      )
),

expected as (
    select
        count(*) as expected_rows,
        count(distinct claim_id) as expected_distinct_claims
    from expected_claims
),

actual as (
    select
        count(*) as actual_rows,
        count(distinct claim_id) as actual_distinct_claims
    from {{ ref('mart_claims_register') }}
)

select
    e.expected_rows,
    a.actual_rows,
    e.expected_distinct_claims,
    a.actual_distinct_claims
from expected e
cross join actual a
where e.expected_rows <> a.actual_rows
   or a.actual_rows <> a.actual_distinct_claims
   or e.expected_distinct_claims <> a.actual_distinct_claims
