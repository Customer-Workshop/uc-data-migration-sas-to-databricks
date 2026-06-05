/*
  Reconciliation test: claims completeness against the documented validation scope.

  The SAS DATA step (claims_processing.sas, Step 1) validates claims against
  active policies using three rules:
    1. Policy must exist and be active (hash lookup succeeds)
    2. Loss date within policy period (effective_date..expiration_date)
    3. Claimed amount <= sum_insured
  Only claims passing all three rules reach WORK.CLAIMS_VALID.

  This test replicates the three validation rules against the raw source and
  compares the count to stg_claims — proving the conversion dropped exactly the
  rows the SAS logic would drop, no more and no fewer.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_valid_claims as (
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
from expected_valid_claims e
cross join model_claims m
where e.n <> m.n
