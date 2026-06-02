/*
  Reconciliation control: policy valuation completeness.

  policy_valuation.sas (Step 1) extracts the in-force policy population:
      STATUS = 'ACTIVE'
      AND EFFECTIVE_DATE   <= val_date
      AND EXPIRATION_DATE  >= val_date

  In the migrated source the columns are policy_status / effective_date /
  expiry_date. This control recomputes that in-scope population directly from
  the raw source and asserts int_policy_valuation carries forward exactly the
  same row count -- no silent row loss and no fan-out from the LEFT JOINs onto
  claims / premiums.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_inforce as (
    select count(*) as n
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
      and effective_date <= cast('{{ var("curr_dt") }}' as date)
      and expiry_date >= cast('{{ var("curr_dt") }}' as date)
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
