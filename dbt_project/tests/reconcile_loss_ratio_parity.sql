/*
  Reconciliation test: loss ratio parity by policy type.

  For each policy_type in mart_loss_ratios, verifies agg_loss_ratio equals
  sum(incurred) / sum(earned) computed independently from raw data through
  the same transformation pipeline (in-force filter + claims 12-month window).

  Raw schema: policies.policy_status, policies.expiry_date;
              claims.claimed_amount (proxy for SAS incurred_amount).

  dbt singular test convention: FAILS if this query returns any rows.
*/
with raw_earned as (
    select
        p.policy_type,
        sum(
            p.annual_premium / 12 * least(
                12,
                months_between(
                    least(current_date(), p.expiry_date),
                    greatest(p.effective_date, date_trunc('year', current_date()))
                )
            )
        ) as total_earned
    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
        and p.effective_date <= current_date()
        and p.expiry_date >= current_date()
    group by p.policy_type
),

raw_incurred as (
    select
        p.policy_type,
        sum(c.claimed_amount) as total_incurred
    from {{ source('insurance_raw', 'claims') }} c
    inner join {{ source('insurance_raw', 'policies') }} p
        on c.policy_id = p.policy_id
    where p.policy_status = 'ACTIVE'
        and p.effective_date <= current_date()
        and p.expiry_date >= current_date()
        and c.loss_date >= add_months(current_date(), -12)
        and c.loss_date <= current_date()
    group by p.policy_type
),

expected as (
    select
        e.policy_type,
        case
            when e.total_earned > 0
                then coalesce(i.total_incurred, 0) / e.total_earned
            else null
        end as expected_loss_ratio
    from raw_earned e
    left join raw_incurred i
        on e.policy_type = i.policy_type
),

mart as (
    select
        policy_type,
        agg_loss_ratio
    from {{ ref('mart_loss_ratios') }}
)

select
    coalesce(e.policy_type, m.policy_type) as policy_type,
    e.expected_loss_ratio,
    m.agg_loss_ratio as actual_loss_ratio,
    abs(coalesce(e.expected_loss_ratio, 0) - coalesce(m.agg_loss_ratio, 0)) as abs_diff
from expected e
full outer join mart m
    on e.policy_type = m.policy_type
where abs(coalesce(e.expected_loss_ratio, 0) - coalesce(m.agg_loss_ratio, 0)) > 0.0001
    or e.policy_type is null
    or m.policy_type is null
