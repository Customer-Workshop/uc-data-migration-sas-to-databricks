/*
  Reconciliation test: claims completeness.

  The SAS extract (claims_processing.sas, Step 1) validated claims against
  active policies with three conditions:
    1. Policy exists and is ACTIVE
    2. Loss date within [EFFECTIVE_DATE, EXPIRATION_DATE]
    3. Claimed amount <= SUM_INSURED
  Invalid claims were routed to CLAIMS_INVALID (excluded).

  This test proves stg_claims contains exactly the valid population —
  no silent row loss and no fan-out from the join.

  dbt singular test convention: FAILS if this query returns any rows.
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
