/*
  Reconciliation control: policy completeness (no silent row loss / no fan-out).

  policy_valuation.sas Step 1 (WORK.INFORCE) extracts only in-force policies:
      where STATUS='ACTIVE'
        and EFFECTIVE_DATE  <= val_date
        and EXPIRATION_DATE >= val_date
  Step 4 then keeps only those rows ("if a"). int_policy_valuation must contain
  exactly that population — no more (over-broad filter / join fan-out) and no
  fewer (silent drop). Source bindings: STATUS->policy_status,
  EXPIRATION_DATE->expiry_date.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_in_scope as (
    select count(*) as n
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
      and effective_date <= current_date()
      and expiry_date >= current_date()
),

model_rows as (
    select
        count(*) as n,
        count(distinct policy_id) as n_distinct
    from {{ ref('int_policy_valuation') }}
)

select
    e.n as expected_in_scope_policies,
    m.n as model_policies,
    m.n_distinct as model_distinct_policies
from expected_in_scope e
cross join model_rows m
where e.n <> m.n          -- completeness: population must match exactly
   or m.n <> m.n_distinct  -- no fan-out: policy_id is the grain
