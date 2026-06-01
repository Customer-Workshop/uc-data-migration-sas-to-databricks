/*
  Reconciliation control: control totals tie out int -> mart.

  policy_valuation.sas Step 5 (PROC MEANS) aggregates the per-policy valuation
  into the loss-ratio summary. The mart must conserve the per-policy control
  totals exactly:
    - sum(n_policies)    over the mart  == count(*)             in the int model
    - sum(total_earned)  over the mart  == sum(ytd_earned_premium) in the int model
    - sum(total_incurred) over the mart == sum(total_incurred)  in the int model
  A drift here means the GROUP BY lost, duplicated, or mis-bucketed rows.

  dbt singular test convention: FAILS if this query returns any rows.
  Money tolerance 0.01; counts must match exactly.
*/
with int_totals as (
    select
        count(*) as n_policies,
        sum(ytd_earned_premium) as total_earned,
        sum(total_incurred) as total_incurred
    from {{ ref('int_policy_valuation') }}
),

mart_totals as (
    select
        sum(n_policies) as n_policies,
        sum(total_earned) as total_earned,
        sum(total_incurred) as total_incurred
    from {{ ref('mart_loss_ratios') }}
)

select
    i.n_policies as int_n_policies,
    m.n_policies as mart_n_policies,
    i.total_earned as int_total_earned,
    m.total_earned as mart_total_earned,
    i.total_incurred as int_total_incurred,
    m.total_incurred as mart_total_incurred
from int_totals i
cross join mart_totals m
where i.n_policies <> m.n_policies
   or abs(coalesce(i.total_earned, 0) - coalesce(m.total_earned, 0)) > 0.01
   or abs(coalesce(i.total_incurred, 0) - coalesce(m.total_incurred, 0)) > 0.01
