/*
  Reconciliation test: policy valuation completeness.

  The SAS extract (policy_valuation.sas, Step 1) filters in-force policies:
      WHERE STATUS = 'ACTIVE'
        AND EFFECTIVE_DATE <= val_date
        AND EXPIRATION_DATE >= val_date

  The dbt model (int_policy_valuation) reproduces that contract. This control
  verifies no silent row loss or fan-out: model row count must equal the
  expected in-scope population from the raw source.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('insurance_raw', 'policies') }}
    where status = 'ACTIVE'
        and effective_date <= current_date()
        and expiration_date >= current_date()
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
