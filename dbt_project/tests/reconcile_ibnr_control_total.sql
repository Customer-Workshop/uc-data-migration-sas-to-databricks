/*
  Reconciliation test: IBNR control total.

  Total IBNR across all policies in int_policy_valuation must tie out to
  the formula applied independently to raw data:
    IBNR per policy = max(0, ytd_earned_premium * 0.15 - coalesce(total_paid, 0))
    Total IBNR = sum of per-policy IBNR

  This proves the reserve estimates are not inflated or deflated by the
  conversion. A tolerance of 0.01 accommodates floating-point rounding.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with raw_earned as (
    select
        p.policy_id,
        p.annual_premium / 12 * least(
            12,
            months_between(
                least(current_date(), p.expiration_date),
                greatest(p.effective_date, date_trunc('year', current_date()))
            )
        ) as ytd_earned_premium
    from {{ source('insurance_raw', 'policies') }} p
    where p.status = 'ACTIVE'
        and p.effective_date <= current_date()
        and p.expiration_date >= current_date()
),

raw_paid as (
    select
        c.policy_id,
        sum(c.paid_amount) as total_paid
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
        and c.loss_date <= current_date()
    group by c.policy_id
),

expected_ibnr as (
    select sum(
        greatest(0, e.ytd_earned_premium * 0.15 - coalesce(rp.total_paid, 0))
    ) as total_ibnr
    from raw_earned e
    left join raw_paid rp
        on e.policy_id = rp.policy_id
),

model_ibnr as (
    select sum(ibnr_estimate) as total_ibnr
    from {{ ref('int_policy_valuation') }}
)

select
    ei.total_ibnr as expected_total_ibnr,
    mi.total_ibnr as model_total_ibnr,
    abs(coalesce(ei.total_ibnr, 0) - coalesce(mi.total_ibnr, 0)) as abs_diff
from expected_ibnr ei
cross join model_ibnr mi
where abs(coalesce(ei.total_ibnr, 0) - coalesce(mi.total_ibnr, 0)) > 0.01
