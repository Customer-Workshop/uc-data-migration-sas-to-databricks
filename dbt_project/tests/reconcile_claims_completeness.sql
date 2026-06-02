/*
  Reconciliation test: claims completeness against the documented validation scope.

  The SAS program (claims_processing.sas, Step 1) validates claims against active
  policies via a hash-object lookup with three filters:
    1. Policy must be ACTIVE (h_pol loaded with where=(STATUS='ACTIVE'))
    2. LOSS_DATE must be within the policy period (>= effective, <= expiry)
    3. CLAIMED_AMOUNT must not exceed SUM_INSURED

  The dbt staging model (stg_claims) reproduces this contract via INNER JOIN to
  active policies + WHERE clauses. Only "valid" claims pass through; invalid
  claims (out-of-period, over-insured, inactive policy) are excluded.

  This control proves no silent row loss AND no fan-out: the model row count must
  exactly equal the in-scope source population derived by applying the same
  SAS Step 1 filters to raw data independently.

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
    select count(*) as n from {{ ref('int_claims_adjudication') }}
)

select
    e.n as expected_valid_claims,
    m.n as model_claims,
    m.n - e.n as difference
from expected_in_scope e
cross join model_claims m
where e.n <> m.n
