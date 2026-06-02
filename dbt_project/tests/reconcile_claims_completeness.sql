/*
  Reconciliation test: claims completeness against the documented extract scope.

  The SAS extract (claims_processing.sas, Step 1) validated claims via a hash
  object lookup to RAW_INS.POLICIES(where=(STATUS='ACTIVE')). Valid claims met
  three gates:
    1. Active policy found (h_pol.find() rc = 0)
    2. LOSS_DATE within [EFFECTIVE_DATE, EXPIRATION_DATE]
    3. CLAIMED_AMOUNT <= SUM_INSURED

  The dbt staging model (stg_claims) reproduces that contract with an INNER JOIN
  and WHERE. This test proves the model retains exactly the in-scope population
  — no silent row loss (over-broad filter) and no fan-out (duplicate join).

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
