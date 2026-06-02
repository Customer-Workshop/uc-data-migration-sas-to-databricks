/*
  Reconciliation test: claims control total.

  The total claimed_amount in the intermediate adjudication model must tie out
  to the total claimed_amount of in-scope claims from the source. Any mismatch
  means the conversion introduced or lost monetary value — an unacceptable
  divergence for a financial register.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with source_total as (
    select coalesce(sum(c.claimed_amount), 0) as total_claimed
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
    where p.status = 'ACTIVE'
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiration_date
      and c.claimed_amount <= p.sum_insured
),

model_total as (
    select coalesce(sum(claimed_amount), 0) as total_claimed
    from {{ ref('int_claims_adjudication') }}
)

select
    s.total_claimed as source_total_claimed,
    m.total_claimed as model_total_claimed,
    m.total_claimed - s.total_claimed as difference
from source_total s
cross join model_total m
where abs(s.total_claimed - m.total_claimed) > 0.01
