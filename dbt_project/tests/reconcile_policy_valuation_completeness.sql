/*
  Reconciliation test: policy valuation completeness.

  The SAS extract (policy_valuation.sas, Step 1) selects in-force policies:
    STATUS = 'ACTIVE'
    AND EFFECTIVE_DATE <= val_date
    AND EXPIRATION_DATE >= val_date

  In the seed data the column names are policy_status / expiry_date.
  This test verifies the dbt model carries forward exactly the same
  population — no silent row loss and no fan-out from the JOINs.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_inforce as (
    select count(*) as n
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
      and effective_date <= '{{ var("curr_dt") }}'
      and expiry_date   >= '{{ var("curr_dt") }}'
),

model_policies as (
    select count(*) as n from {{ ref('int_policy_valuation') }}
)

select
    e.n as expected_inforce_policies,
    m.n as model_policies,
    m.n - e.n as difference
from expected_inforce e
cross join model_policies m
where e.n <> m.n
