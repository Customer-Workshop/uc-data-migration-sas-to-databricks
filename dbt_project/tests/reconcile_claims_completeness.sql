/*
  Reconciliation test: claims completeness against the documented validation scope.

  The SAS program (claims_processing.sas, Step 1) validates claims against active
  policies with three checks: policy active (STATUS='ACTIVE'), loss date within
  policy period, and claimed amount not exceeding sum insured. The dbt staging
  model (stg_claims) reproduces this contract via INNER JOIN + WHERE clauses.

  This control verifies no silent row loss or fan-out: the model row count must
  match the expected in-scope population derived from the same raw-data filters.

  dbt singular test convention: the test FAILS if this query returns any rows.
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
    select count(*) as n from {{ ref('int_claims_adjudication') }}
)

select
    e.n as expected_valid_claims,
    m.n as model_claims,
    m.n - e.n as difference
from expected_valid e
cross join model_claims m
where e.n <> m.n
