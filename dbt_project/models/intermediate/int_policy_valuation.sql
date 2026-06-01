/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1 PROC SQL extract of in-force policies (with intck/intnx date math),
    Step 2 claims experience aggregation, Step 3 premium collections, and a
    Step 4 DATA step MERGE BY POLICY_ID computing loss ratio / combined ratio /
    premium adequacy.

  dbt Equivalent:
    The SAS MERGE BY becomes LEFT JOINs; intck/intnx month math becomes
    months_between / add_months; the IF/THEN flags become CASE expressions.
*/

with policies as (
    select * from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
),

-- SAS Step 2: claims experience (incurred losses per policy)
claims_exp as (
    select
        policy_id,
        count(distinct claim_id) as num_claims,
        sum(claimed_amount) as total_incurred
    from {{ source('insurance_raw', 'claims') }}
    group by policy_id
),

-- SAS Step 3: premium collections
premium_coll as (
    select
        policy_id,
        sum(premium_paid) as collected_premium
    from {{ source('insurance_raw', 'premiums') }}
    group by policy_id
),

valued as (
    select
        p.policy_id,
        p.policy_type,
        p.annual_premium,
        p.sum_insured,
        p.deductible,
        p.effective_date,
        p.expiry_date,
        months_between(p.expiry_date, current_date()) as months_to_expiry,
        -- Earned premium proxy: collected premium, falling back to annual premium
        coalesce(pc.collected_premium, p.annual_premium) as ytd_earned_premium,
        coalesce(ce.num_claims, 0) as num_claims,
        coalesce(ce.total_incurred, 0) as total_incurred
    from policies p
    left join claims_exp ce on p.policy_id = ce.policy_id
    left join premium_coll pc on p.policy_id = pc.policy_id
)

select
    policy_id,
    policy_type,
    annual_premium,
    sum_insured,
    deductible,
    ytd_earned_premium,
    num_claims,
    total_incurred,
    -- SAS: LOSS_RATIO = TOTAL_INCURRED / YTD_EARNED_PREMIUM
    case
        when ytd_earned_premium > 0 then total_incurred / ytd_earned_premium
        else null
    end as loss_ratio,
    -- combined ratio = loss ratio + 30% expense load
    case
        when ytd_earned_premium > 0
            then (total_incurred / ytd_earned_premium) + 0.30
        else null
    end as combined_ratio,
    -- SAS: premium adequate when combined ratio <= 1.0
    case
        when ytd_earned_premium > 0
             and (total_incurred / ytd_earned_premium) + 0.30 <= 1.0
            then 'Y'
        else 'N'
    end as premium_adequate,
    -- SAS: RENEWAL_DUE_FLAG when expiry within 3 months
    case
        when expiry_date <= add_months(current_date(), 3) then 'Y'
        else 'N'
    end as renewal_due_flag
from valued
