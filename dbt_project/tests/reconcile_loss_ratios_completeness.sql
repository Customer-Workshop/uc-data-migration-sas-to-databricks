/*
  Reconciliation control: loss-ratio summary completeness.

  policy_valuation.sas Step 5 (PROC MEANS ... class POLICY_TYPE) produces one
  summary row per distinct in-force policy type. This control asserts
  mart_loss_ratios has exactly one row per distinct policy_type present in
  int_policy_valuation -- no dropped lines of business and no duplicate /
  fanned-out grouping rows.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with expected_groups as (
    select count(distinct policy_type) as n
    from {{ ref('int_policy_valuation') }}
),

mart_rows as (
    select count(*) as n from {{ ref('mart_loss_ratios') }}
)

select
    e.n as expected_policy_types,
    m.n as mart_rows,
    m.n - e.n as difference
from expected_groups e
cross join mart_rows m
where e.n <> m.n
