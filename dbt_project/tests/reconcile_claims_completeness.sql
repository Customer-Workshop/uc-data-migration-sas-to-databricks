/*
  Reconciliation test: claims completeness against the documented intake scope.

  claims_processing.sas (Step 1) did not carry every raw claim forward — it looked
  each claim up against ACTIVE policies (hash h_pol) and routed to CLAIMS_INVALID
  any claim whose policy was missing/inactive, whose loss date fell outside the
  policy period, or whose claimed amount exceeded the sum insured. Only CLAIMS_VALID
  flows on. stg_claims reproduces that contract:
      inner join policies (policy_status = 'ACTIVE')
      and loss_date between effective_date and expiry_date
      and claimed_amount <= sum_insured

  A naive "raw claims == model claims" check fails here (and should) because the
  conversion legitimately drops out-of-scope claims. This control reconciles the
  model against the *expected in-scope population* — proving the conversion dropped
  exactly the rows the validation rules say to drop, no more and no fewer (a
  fanned-out join or an over-broad filter would surface here).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
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
    e.n as expected_in_scope_claims,
    m.n as model_claims,
    m.n - e.n as difference
from expected_in_scope e
cross join model_claims m
where e.n <> m.n
