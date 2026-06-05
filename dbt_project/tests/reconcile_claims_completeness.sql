/*
  Reconciliation test: stg_claims contains all valid claims.

  The SAS claims processing (claims_processing.sas, Step 1) validates
  claims against active policies. Valid claims must have:
    - An active policy match
    - Loss date within policy period
    - Claimed amount <= sum insured

  The dbt model (stg_claims) must contain exactly the claims that pass
  all three checks — no silent row loss, no fan-out from the join.

  Fails if this query returns any rows.
*/
with expected_valid as (
    select count(*) as n
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
    where p.policy_status = 'ACTIVE'
      and c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and c.claimed_amount <= p.sum_insured
),

model_claims as (
    select count(*) as n from {{ ref('stg_claims') }}
)

select
    e.n as expected_valid_claims,
    m.n as model_claims,
    m.n - e.n as difference
from expected_valid e
cross join model_claims m
where e.n <> m.n
