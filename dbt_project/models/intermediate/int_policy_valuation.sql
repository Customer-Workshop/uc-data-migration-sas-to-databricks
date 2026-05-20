/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL extracting in-force policies with earned premium calc
    Step 2: PROC SQL aggregating 12-month claims experience per policy
    Step 3: PROC SQL aggregating YTD premium collections per policy
    Step 4: DATA step MERGE BY POLICY_ID combining all three datasets,
            computing loss ratio, combined ratio, IBNR, reserves

  dbt Equivalent:
    CTEs replace WORK tables; LEFT JOINs replace SAS MERGE BY
    SQL date functions replace SAS intck/intnx
    SQL CASE replaces DATA step IF/THEN business logic
*/

with inforce as (
    -- SAS Step 1: PROC SQL creating WORK.INFORCE
    select
        p.policy_id,
        p.customer_id,
        p.policy_type,
        p.effective_date,
        p.expiration_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,
        p.risk_category,
        p.underwriting_class,
        p.agent_id,
        p.branch_code,

        -- SAS: intck('month', EFFECTIVE_DATE, "&val_date"d)
        months_between(current_date(), p.effective_date) as policy_age_months,

        -- SAS: intck('month', "&val_date"d, EXPIRATION_DATE)
        months_between(p.expiration_date, current_date()) as months_to_expiry,

        -- SAS: RENEWAL_DUE_FLAG
        case
            when p.expiration_date <= add_months(current_date(), 3)
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: YTD earned premium (monthly pro-rata)
        p.annual_premium / 12
            * least(12, months_between(
                least(current_date(), p.expiration_date),
                greatest(p.effective_date, trunc(current_date(), 'YEAR'))
            )) as ytd_earned_premium

    from {{ source('insurance_raw', 'policies') }} p
    where p.status = 'ACTIVE'
      and p.effective_date <= current_date()
      and p.expiration_date >= current_date()
),

claims_exp as (
    -- SAS Step 2: PROC SQL creating WORK.CLAIMS_EXP (12-month window)
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.incurred_amount) as total_incurred,
        sum(c.paid_amount) as total_paid,
        sum(c.reserved_amount) as total_reserved,
        max(c.loss_date) as last_claim_date,
        sum(case when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
            then c.reserved_amount else 0 end) as open_reserves,
        sum(case when c.claim_status = 'DENIED' then 1 else 0 end)
            as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
      and c.loss_date <= current_date()
    group by c.policy_id
),

premium_coll as (
    -- SAS Step 3: PROC SQL creating WORK.PREMIUM_COLL
    select
        policy_id,
        sum(premium_amount) as collected_premium,
        sum(case when payment_status = 'RETURNED'
            then premium_amount else 0 end) as returned_premium,
        max(payment_date) as last_payment_date,
        count(case when payment_status = 'LATE' then 1 end)
            as late_payments
    from {{ source('insurance_raw', 'premiums') }}
    where payment_date >= trunc(current_date(), 'YEAR')
      and payment_date <= current_date()
    group by policy_id
),

-- SAS Step 4: DATA step MERGE BY POLICY_ID
merged as (
    select
        i.*,
        ce.num_claims,
        ce.total_incurred,
        ce.total_paid,
        ce.total_reserved,
        ce.last_claim_date,
        ce.open_reserves,
        ce.denied_claims,
        pc.collected_premium,
        pc.returned_premium,
        pc.last_payment_date,
        pc.late_payments,

        -- SAS: LOSS_RATIO = TOTAL_INCURRED / YTD_EARNED_PREMIUM
        case
            when i.ytd_earned_premium > 0
            then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
            else null
        end as loss_ratio,

        -- SAS: COMBINED_RATIO = LOSS_RATIO + 0.30
        case
            when i.ytd_earned_premium > 0
            then (coalesce(ce.total_incurred, 0) / i.ytd_earned_premium) + 0.30
            else null
        end as combined_ratio,

        -- SAS: PREMIUM_ADEQUATE flag
        case
            when i.ytd_earned_premium <= 0 then 'N'
            when (coalesce(ce.total_incurred, 0) / i.ytd_earned_premium) + 0.30 > 1.0 then 'N'
            else 'Y'
        end as premium_adequate,

        -- SAS: IBNR_ESTIMATE = max(0, earned * 0.15 - paid)
        greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
            as ibnr_estimate,

        -- SAS: TOTAL_RESERVE = OPEN_RESERVES + IBNR
        coalesce(ce.open_reserves, 0)
            + greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
            as total_reserve,

        current_date() as valuation_date,
        current_timestamp() as load_timestamp

    from inforce i
    left join claims_exp ce
        on i.policy_id = ce.policy_id
    left join premium_coll pc
        on i.policy_id = pc.policy_id
)

select * from merged
