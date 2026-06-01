/*
  Reconciliation test: policy valuation completeness.

  The SAS extract (policy_valuation.sas, Step 1) selects in-force policies:
      STATUS = 'ACTIVE'  →  policy_status = 'ACTIVE'
      EFFECTIVE_DATE <= val_date
      EXPIRATION_DATE >= val_date  →  expiry_date >= current_date()

  The dbt model (int_policy_valuation) reproduces this WHERE exactly.
  This control proves no silent row loss or fan-out occurred.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
      and effective_date <= current_date()
      and expiry_date >= current_date()
),

model_policies as (
    select count(*) as n from {{ ref('int_policy_valuation') }}
)

select
    e.n as expected_in_scope_policies,
    m.n as model_policies,
    m.n - e.n as difference
from expected_in_scope e
cross join model_policies m
where e.n <> m.n
