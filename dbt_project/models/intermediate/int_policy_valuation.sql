/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL extract of in-force policies from RAW_INS.POLICIES
    Step 2: PROC SQL claims experience (12-month trailing window)
    Step 3: PROC SQL premium collections (YTD)
    Step 4: DATA step MERGE BY POLICY_ID + derived metrics
            (loss ratio, combined ratio, IBNR, reserves)

  dbt Equivalent:
    CTEs replace the four SAS work tables; LEFT JOIN replaces MERGE BY.
    SAS DATA step IF/THEN logic → SQL CASE expressions.
    SAS FORMAT $POLTYPE/$RISKCAT → format_policy_type() Jinja macro.

  Schema adaptation notes (SAS → Databricks raw):
    - RAW_INS.POLICIES.CUSTOMER_ID      → policyholder_id
    - RAW_INS.POLICIES.EXPIRATION_DATE   → expiry_date
    - RAW_INS.POLICIES.STATUS            → policy_status
    - RAW_INS.POLICIES.RISK_CATEGORY     → not present in DB; omitted
    - RAW_INS.POLICIES.UNDERWRITING_CLASS→ not present in DB; omitted
    - RAW_INS.POLICIES.AGENT_ID          → not present in DB; omitted
    - RAW_INS.POLICIES.BRANCH_CODE       → not present in DB; omitted
    - RAW_INS.CLAIMS.INCURRED_AMOUNT     → claimed_amount (proxy)
    - RAW_INS.CLAIMS.PAID_AMOUNT         → derived: claimed_amount
                                           where status in (CLOSED, SETTLED)
    - RAW_INS.CLAIMS.RESERVED_AMOUNT     → derived: claimed_amount
                                           where status in (OPEN, PENDING, REOPENED)
    - RAW_INS.PREMIUMS.PREMIUM_AMOUNT    → premium_paid
    - RAW_INS.PREMIUMS.PAYMENT_STATUS    → not present; RETURNED/LATE tracking unavailable
    - RAW_INS.PREMIUMS.PAYMENT_DATE      → due_date (proxy)

  Quirks reproduced from source (flagged, not fixed):
    [Q1] Combined ratio uses a hard-coded 30% expense load (line 144 in SAS).
    [Q2] IBNR uses a hard-coded 15% factor (line 155 in SAS).
    [Q3] When YTD_EARNED_PREMIUM = 0, LOSS_RATIO/COMBINED_RATIO are NULL
         and PREMIUM_ADEQUATE defaults to 'N' — penalising zero-premium policies.
    [Q4] The SAS header lists TERA_DW.ACTUARIAL_TABLES as an input, but the
         code never references it (dead dependency).
*/

-- Step 1: In-force policies (SAS WORK.INFORCE)
with inforce as (
    select
        p.policy_id,
        p.policyholder_id as customer_id,
        p.policy_type,
        p.effective_date,
        p.expiry_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,

        -- SAS: intck('month', p.EFFECTIVE_DATE, "&val_date"d)
        cast(months_between(current_date(), p.effective_date) as int)
            as policy_age_months,

        -- SAS: intck('month', "&val_date"d, p.EXPIRATION_DATE)
        cast(months_between(p.expiry_date, current_date()) as int)
            as months_to_expiry,

        -- SAS: RENEWAL_DUE_FLAG
        case
            when p.expiry_date <= add_months(current_date(), 3)
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: YTD_EARNED_PREMIUM (monthly pro-rata)
        -- annual_premium / 12 * min(12, months from max(eff, year_start) to min(val, exp))
        p.annual_premium / 12.0
            * least(
                12,
                greatest(
                    0,
                    months_between(
                        least(current_date(), p.expiry_date),
                        greatest(p.effective_date, date_trunc('year', current_date()))
                    )
                )
            ) as ytd_earned_premium

    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
      and p.effective_date <= current_date()
      and p.expiry_date >= current_date()
),

-- Step 2: Claims experience — 12-month trailing window (SAS WORK.CLAIMS_EXP)
-- Adaptation: claimed_amount proxies for INCURRED; PAID/RESERVED derived from status
claims_exp as (
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.claimed_amount) as total_incurred,
        sum(
            case
                when c.claim_status in ('CLOSED', 'SETTLED')
                    then c.claimed_amount
                else 0
            end
        ) as total_paid,
        sum(
            case
                when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
                    then c.claimed_amount
                else 0
            end
        ) as total_reserved,
        max(c.loss_date) as last_claim_date,
        -- SAS: OPEN_RESERVES — sum reserved where status is unresolved
        sum(
            case
                when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
                    then c.claimed_amount
                else 0
            end
        ) as open_reserves,
        -- SAS: DENIED_CLAIMS count
        sum(
            case when c.claim_status = 'DENIED' then 1 else 0 end
        ) as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
      and c.loss_date <= current_date()
    group by c.policy_id
),

-- Step 3: Premium collections — YTD (SAS WORK.PREMIUM_COLL)
-- Adaptation: premium_paid proxies for PREMIUM_AMOUNT; due_date for PAYMENT_DATE
premium_coll as (
    select
        policy_id,
        sum(premium_paid) as collected_premium,
        -- SAS: RETURNED_PREMIUM — not trackable in DB schema; zeroed
        cast(0 as double) as returned_premium,
        max(due_date) as last_payment_date,
        -- SAS: LATE_PAYMENTS — not trackable in DB schema; zeroed
        0 as late_payments
    from {{ source('insurance_raw', 'premiums') }}
    where due_date >= date_trunc('year', current_date())
      and due_date <= current_date()
    group by policy_id
),

-- Step 4: Merge + derived metrics (SAS DATA step)
merged as (
    select
        i.policy_id,
        i.customer_id,
        i.policy_type,
        i.effective_date,
        i.expiry_date,
        i.annual_premium,
        i.sum_insured,
        i.deductible,
        i.policy_age_months,
        i.months_to_expiry,
        i.renewal_due_flag,
        i.ytd_earned_premium,

        -- Claims experience (coalesce NULLs for policies with no claims)
        coalesce(ce.num_claims, 0) as num_claims,
        coalesce(ce.total_incurred, 0) as total_incurred,
        coalesce(ce.total_paid, 0) as total_paid,
        coalesce(ce.total_reserved, 0) as total_reserved,
        ce.last_claim_date,
        coalesce(ce.open_reserves, 0) as open_reserves,
        coalesce(ce.denied_claims, 0) as denied_claims,

        -- Premium collections
        coalesce(pc.collected_premium, 0) as collected_premium,
        coalesce(pc.returned_premium, 0) as returned_premium,
        pc.last_payment_date,
        coalesce(pc.late_payments, 0) as late_payments,

        -- SAS: LOSS_RATIO [Q3: NULL when earned = 0]
        case
            when i.ytd_earned_premium > 0
                then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
            else null
        end as loss_ratio,

        -- SAS: COMBINED_RATIO [Q1: hard-coded 30% expense load] [Q3]
        case
            when i.ytd_earned_premium > 0
                then (coalesce(ce.total_incurred, 0) / i.ytd_earned_premium) + 0.30
            else null
        end as combined_ratio,

        -- SAS: PREMIUM_ADEQUATE [Q3: null combined → 'N']
        case
            when i.ytd_earned_premium <= 0 then 'N'
            when (coalesce(ce.total_incurred, 0) / i.ytd_earned_premium) + 0.30 > 1.0
                then 'N'
            else 'Y'
        end as premium_adequate,

        -- SAS: IBNR_ESTIMATE [Q2: hard-coded 15% factor]
        greatest(
            0,
            i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0)
        ) as ibnr_estimate,

        -- SAS: TOTAL_RESERVE = OPEN_RESERVES + IBNR
        coalesce(ce.open_reserves, 0)
            + greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
            as total_reserve,

        -- Format descriptions (SAS FORMAT $POLTYPE.)
        {{ format_policy_type('i.policy_type') }} as policy_type_desc,

        current_date() as valuation_date,
        current_timestamp() as load_timestamp

    from inforce i
    left join claims_exp ce
        on i.policy_id = ce.policy_id
    left join premium_coll pc
        on i.policy_id = pc.policy_id
)

select * from merged
