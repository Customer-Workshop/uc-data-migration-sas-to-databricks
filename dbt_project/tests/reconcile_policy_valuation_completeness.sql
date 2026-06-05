/*
  Reconciliation test: int_policy_valuation covers all in-force policies.

  The SAS program (policy_valuation.sas, Step 1) extracts active policies
  where effective_date <= valuation date and expiration_date >= valuation
  date. The dbt model (int_policy_valuation) must value the same population.

  Fails if this query returns any rows.
*/
with expected_inforce as (
    select count(*) as n
    from {{ source('insurance_raw', 'policies') }}
    where status = 'ACTIVE'
      and effective_date <= current_date()
      and expiration_date >= current_date()
),

model_valued as (
    select count(*) as n from {{ ref('int_policy_valuation') }}
)

select
    e.n as expected_inforce_policies,
    m.n as model_valued_policies,
    m.n - e.n as difference
from expected_inforce e
cross join model_valued m
where e.n <> m.n
