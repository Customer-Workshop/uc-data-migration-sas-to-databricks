/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL extract of in-force policies with age, months to
            expiry, renewal flag, YTD earned premium (intck/intnx)
    Step 2: PROC SQL claims experience aggregation (12-month window)
    Step 3: PROC SQL premium collections aggregation (YTD)
    Step 4: DATA step MERGE BY POLICY_ID + valuation metric calculations
            (loss ratio, combined ratio, premium adequacy, IBNR, reserves)

  dbt Equivalent:
    CTEs replace WORK temporary tables.
    SQL LEFT JOINs replace SAS MERGE BY.
    SQL CASE replaces IF/THEN metric assignments.
    SAS intck/intnx → months_between/add_months/trunc.
    policyholder_id maps to SAS CUSTOMER_ID.
*/

with inforce as (
    -- SAS Step 1: In-force policy extract
    select
        p.policy_id,
        p.policyholder_id as customer_id,
        p.policy_type,
        p.effective_date,
        p.expiry_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,

        -- SAS: intck('month', EFFECTIVE_DATE, val_date)
        months_between(current_date(), p.effective_date) as policy_age_months,

        -- SAS: intck('month', val_date, EXPIRATION_DATE)
        months_between(p.expiry_date, current_date()) as months_to_expiry,

        -- SAS: renewal due within 3 months
        case
            when p.expiry_date <= add_months(current_date(), 3)
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: YTD earned premium (monthly pro-rata)
        p.annual_premium / 12
            * least(12, months_between(
                least(current_date(), p.expiry_date),
                greatest(p.effective_date, trunc(current_date(), 'YEAR'))
            )) as ytd_earned_premium

    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
      and p.effective_date <= current_date()
      and p.expiry_date >= current_date()
),

-- SAS Step 2: Claims experience (12-month window)
claims_exp as (
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.claimed_amount) as total_incurred,
        sum(case
            when c.claim_status in ('SETTLED', 'CLOSED')
                then c.claimed_amount else 0
        end) as total_paid,
        max(c.loss_date) as last_claim_date,
        sum(case
            when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
                then c.claimed_amount else 0
        end) as open_reserves,
        sum(case when c.claim_status = 'DENIED' then 1 else 0 end)
            as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
      and c.loss_date <= current_date()
    group by c.policy_id
),

-- SAS Step 3: Premium collections (YTD)
premium_coll as (
    select
        policy_id,
        sum(premium_paid) as collected_premium,
        sum(premium_due - premium_paid) as outstanding_premium
    from {{ source('insurance_raw', 'premiums') }}
    group by policy_id
),

-- SAS Step 4: MERGE BY + valuation metric calculations
merged as (
    select
        i.*,
        ce.num_claims,
        ce.total_incurred,
        ce.total_paid,
        ce.last_claim_date,
        ce.open_reserves,
        ce.denied_claims,
        pc.collected_premium,
        pc.outstanding_premium,

        -- SAS: Loss Ratio = TOTAL_INCURRED / YTD_EARNED_PREMIUM
        case
            when i.ytd_earned_premium > 0
                then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
            else null
        end as loss_ratio,

        -- SAS: Combined Ratio = LOSS_RATIO + 0.30 (expense load)
        case
            when i.ytd_earned_premium > 0
                then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
                     + 0.30
            else null
        end as combined_ratio,

        -- SAS: Premium Adequacy Flag
        case
            when i.ytd_earned_premium <= 0 then 'N'
            when coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
                 + 0.30 > 1.0
                then 'N'
            else 'Y'
        end as premium_adequate,

        -- SAS: IBNR Estimate (15% of earned premium - paid)
        greatest(
            0,
            i.ytd_earned_premium * 0.15
                - coalesce(ce.total_paid, 0)
        ) as ibnr_estimate,

        -- SAS: Total Reserve = Open Case + IBNR
        coalesce(ce.open_reserves, 0)
            + greatest(
                0,
                i.ytd_earned_premium * 0.15
                    - coalesce(ce.total_paid, 0)
            ) as total_reserve,

        current_date() as valuation_date

    from inforce i
    left join claims_exp ce
        on i.policy_id = ce.policy_id
    left join premium_coll pc
        on i.policy_id = pc.policy_id
)

select * from merged
