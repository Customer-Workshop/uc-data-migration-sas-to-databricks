/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1 — PROC SQL extracting in-force policies (via stg_policies).
    Step 2 — PROC SQL aggregating claims experience (12-month window).
    Step 3 — PROC SQL aggregating premium collections (YTD).
    Step 4 — DATA step MERGE BY POLICY_ID computing loss_ratio,
    combined_ratio, premium_adequate flag, IBNR_estimate, total_reserve.

  dbt Equivalent:
    stg_policies provides the filtered in-force base.
    SQL subqueries replace PROC SQL aggregations.
    LEFT JOINs replace SAS MERGE BY.
    CASE expressions replace IF/THEN metric derivations.
*/

with policies as (
    select * from {{ ref('stg_policies') }}
),

-- SAS: Step 2 — Claims experience (12-month window)
claims_exp as (
    select
        policy_id,
        count(distinct claim_id)    as num_claims,
        sum(incurred_amount)        as total_incurred,
        sum(paid_amount)            as total_paid,
        sum(reserved_amount)        as total_reserved,
        max(loss_date)              as last_claim_date,
        sum(case
            when claim_status in ('OPEN', 'PENDING', 'REOPENED')
            then reserved_amount else 0
        end)                        as open_reserves,
        sum(case
            when claim_status = 'DENIED' then 1 else 0
        end)                        as denied_claims
    from {{ source('insurance_raw', 'claims') }}
    where loss_date >= add_months(current_date(), -12)
      and loss_date <= current_date()
    group by policy_id
),

-- SAS: Step 3 — Premium collections (YTD)
premium_coll as (
    select
        policy_id,
        sum(premium_amount) as collected_premium,
        sum(case
            when payment_status = 'RETURNED'
            then premium_amount else 0
        end) as returned_premium,
        max(payment_date)   as last_payment_date,
        count(case when payment_status = 'LATE' then 1 end) as late_payments
    from {{ source('insurance_raw', 'premiums') }}
    where payment_date >= trunc(current_date(), 'YEAR')
      and payment_date <= current_date()
    group by policy_id
),

-- SAS: Step 4 — Merge and compute valuation metrics
valued as (
    select
        p.*,
        c.num_claims,
        c.total_incurred,
        c.total_paid,
        c.total_reserved,
        c.last_claim_date,
        c.open_reserves,
        c.denied_claims,
        pr.collected_premium,
        pr.returned_premium,
        pr.last_payment_date,
        pr.late_payments,

        -- SAS: Loss Ratio
        case
            when p.ytd_earned_premium > 0
            then coalesce(c.total_incurred, 0) / p.ytd_earned_premium
            else null
        end as loss_ratio,

        -- SAS: Combined Ratio (loss + 30% expense load)
        case
            when p.ytd_earned_premium > 0
            then (coalesce(c.total_incurred, 0) / p.ytd_earned_premium) + 0.30
            else null
        end as combined_ratio,

        -- SAS: Premium Adequacy Flag
        case
            when p.ytd_earned_premium <= 0 then 'N'
            when (coalesce(c.total_incurred, 0) / p.ytd_earned_premium) + 0.30 > 1.0
                then 'N'
            else 'Y'
        end as premium_adequate,

        -- SAS: IBNR Estimate (basic: 15% of earned premium - paid)
        greatest(0,
            p.ytd_earned_premium * 0.15 - coalesce(c.total_paid, 0)
        ) as ibnr_estimate,

        -- SAS: Total Reserve = Open Case + IBNR
        coalesce(c.open_reserves, 0)
            + greatest(0, p.ytd_earned_premium * 0.15 - coalesce(c.total_paid, 0))
        as total_reserve,

        current_date() as valuation_date

    from policies p
    left join claims_exp c
        on p.policy_id = c.policy_id
    left join premium_coll pr
        on p.policy_id = pr.policy_id
)

select * from valued
