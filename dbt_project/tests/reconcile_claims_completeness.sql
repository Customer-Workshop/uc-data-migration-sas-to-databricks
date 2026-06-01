/*
  Reconciliation control: claims completeness against the documented scope.

  claims_processing.sas (Step 1) does NOT carry every raw claim forward. The
  CLAIMS_VALID branch keeps only claims whose policy resolves in the ACTIVE
  policy master (hash find rc=0), whose LOSS_DATE is inside the policy period,
  and whose CLAIMED_AMOUNT does not exceed SUM_INSURED. Everything else is
  routed to CLAIMS_INVALID and dropped.

  This control recomputes that exact in-scope population directly from raw and
  asserts the converted model (int_claims_adjudication, which keeps one row per
  valid claim through to adjudication) has neither lost rows nor fanned out.

  A naive "raw claims count == model count" check would (correctly) fail,
  because the conversion legitimately drops out-of-scope claims. We reconcile
  against the *expected in-scope* count instead.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with active_policies as (
    select
        policy_id,
        effective_date,
        expiry_date,
        sum_insured
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
),

expected_in_scope as (
    select count(*) as n
    from {{ source('insurance_raw', 'claims') }} c
    inner join active_policies p
        on c.policy_id = p.policy_id
    where c.loss_date >= p.effective_date
      and c.loss_date <= p.expiry_date
      and c.claimed_amount <= p.sum_insured
),

model_claims as (
    select count(*) as n from {{ ref('int_claims_adjudication') }}
)

select
    e.n as expected_in_scope_claims,
    m.n as model_claims,
    (m.n - e.n) as difference
from expected_in_scope e
cross join model_claims m
where e.n <> m.n
