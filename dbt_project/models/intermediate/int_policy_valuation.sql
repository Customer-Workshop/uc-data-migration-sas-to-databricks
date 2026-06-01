/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1 — PROC SQL: extract in-force policies (STATUS='ACTIVE',
             EFFECTIVE_DATE <= val_date, EXPIRATION_DATE >= val_date),
             compute POLICY_AGE_MONTHS, MONTHS_TO_EXPIRY, RENEWAL_DUE_FLAG,
             YTD_EARNED_PREMIUM (monthly pro-rata).
    Step 2 — PROC SQL: claims experience in 12-month window grouped by
             POLICY_ID (count claims, sum incurred/paid/reserved, open
             reserves, denied count).
    Step 3 — PROC SQL: premium collections YTD grouped by POLICY_ID.
    Step 4 — DATA step MERGE BY POLICY_ID: compute LOSS_RATIO,
             COMBINED_RATIO (+ 0.30 expense load), PREMIUM_ADEQUATE flag,
             IBNR_ESTIMATE, TOTAL_RESERVE.

  dbt Equivalent:
    Three CTEs (one per SAS step 1-3) joined in a final SELECT (step 4).
    SAS MERGE BY → LEFT JOIN on policy_id.
    SAS intck/intnx → Databricks months_between / add_months / trunc.
    SAS PROC FORMAT $POLTYPE/$RISKCAT → Jinja macros.

  Column-name mapping (SAS → seed/source):
    STATUS           → policy_status
    EXPIRATION_DATE  → expiry_date
    CUSTOMER_ID      → policyholder_id
    INCURRED_AMOUNT  → claimed_amount  (seed has single amount column)
    PAID_AMOUNT      → derived from claimed_amount + claim_status
    RESERVED_AMOUNT  → derived from claimed_amount + claim_status
    PREMIUM_AMOUNT   → premium_paid
    PAYMENT_DATE     → due_date
    PAYMENT_STATUS   → not available in seed (returned/late defaulted)

  Claim-status mapping (SAS → seed):
    OPEN             → OPEN
    INV/ADJ/PEND     → PENDING, REOPENED  (closest seed equivalents)
    DENY             → DENIED

  ⚠ FLAGGED BUSINESS ASSUMPTIONS (source-faithful, not endorsements):
    1. 30% expense load in combined ratio is hard-coded (SAS line 144).
    2. 15% IBNR factor is hard-coded (SAS line 155).
    Both are reproduced exactly from the SAS source.
*/

{% set val_date = var('curr_dt') %}

with inforce as (
    /* Step 1: In-force policies (SAS PROC SQL → WORK.INFORCE) */
    select
        p.policy_id,
        p.policyholder_id,
        p.policy_type,
        p.effective_date,
        p.expiry_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,

        /* SAS: intck('month', EFFECTIVE_DATE, val_date) */
        cast(months_between('{{ val_date }}', p.effective_date) as int)
            as policy_age_months,

        /* SAS: intck('month', val_date, EXPIRATION_DATE) */
        cast(months_between(p.expiry_date, '{{ val_date }}') as int)
            as months_to_expiry,

        /* SAS: EXPIRATION_DATE <= intnx('month', val_date, 3) → 'Y' */
        case
            when p.expiry_date <= add_months('{{ val_date }}', 3)
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        /* SAS: ANNUAL_PREMIUM / 12 * min(12, intck('month',
               max(EFFECTIVE_DATE, intnx('year', val_date, 0, 'B')),
               min(val_date, EXPIRATION_DATE)))
           Earned-premium pro-rata: months from max(effective, year-start)
           to min(val_date, expiry), capped at 12. */
        p.annual_premium / 12.0
            * least(12,
                greatest(0,
                    cast(months_between(
                        least(cast('{{ val_date }}' as date), p.expiry_date),
                        greatest(p.effective_date, trunc(cast('{{ val_date }}' as date), 'YEAR'))
                    ) as int)
                )
            ) as ytd_earned_premium

    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
      and p.effective_date <= '{{ val_date }}'
      and p.expiry_date   >= '{{ val_date }}'
    order by p.policy_id
),

claims_exp as (
    /* Step 2: Claims experience — 12-month lookback (SAS → WORK.CLAIMS_EXP)
       Seed data has a single claimed_amount column; SAS had separate
       INCURRED / PAID / RESERVED.  We derive the split from claim_status:
         CLOSED / SETTLED → treat claimed_amount as paid
         OPEN / PENDING / REOPENED → treat claimed_amount as reserved  */
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.claimed_amount) as total_incurred,
        sum(case when c.claim_status in ('CLOSED', 'SETTLED')
            then c.claimed_amount else 0 end) as total_paid,
        sum(case when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
            then c.claimed_amount else 0 end) as total_reserved,
        max(c.loss_date) as last_claim_date,
        /* SAS: OPEN_RESERVES = sum(case when STATUS in ('OPEN','INV','ADJ','PEND'))
           Seed equivalents: OPEN, PENDING, REOPENED */
        sum(case when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
            then c.claimed_amount else 0 end) as open_reserves,
        sum(case when c.claim_status = 'DENIED' then 1 else 0 end)
            as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months('{{ val_date }}', -12)
      and c.loss_date <= '{{ val_date }}'
    group by c.policy_id
),

premium_coll as (
    /* Step 3: Premium collections — YTD (SAS → WORK.PREMIUM_COLL)
       Seed has premium_paid / due_date but no payment_status.
       returned_premium and late_payments default to 0. */
    select
        pr.policy_id,
        sum(pr.premium_paid) as collected_premium,
        /* SAS: sum(case when PAYMENT_STATUS='RETURNED' ...) — not in seed */
        cast(0 as double) as returned_premium,
        max(pr.due_date) as last_payment_date,
        /* SAS: count(case when PAYMENT_STATUS='LATE' ...) — not in seed */
        cast(0 as int) as late_payments
    from {{ source('insurance_raw', 'premiums') }} pr
    where pr.due_date >= trunc(cast('{{ val_date }}' as date), 'YEAR')
      and pr.due_date <= '{{ val_date }}'
    group by pr.policy_id
)

/* Step 4: Merge and calculate valuation metrics (SAS DATA step MERGE BY) */
select
    i.policy_id,
    i.policyholder_id,
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

    /* Claims experience */
    coalesce(ce.num_claims, 0) as num_claims,
    coalesce(ce.total_incurred, 0) as total_incurred,
    coalesce(ce.total_paid, 0) as total_paid,
    coalesce(ce.total_reserved, 0) as total_reserved,
    ce.last_claim_date,
    coalesce(ce.open_reserves, 0) as open_reserves,
    coalesce(ce.denied_claims, 0) as denied_claims,

    /* Premium collections */
    coalesce(pc.collected_premium, 0) as collected_premium,
    coalesce(pc.returned_premium, 0) as returned_premium,
    pc.last_payment_date,
    coalesce(pc.late_payments, 0) as late_payments,

    /* Loss ratio (SAS line 136-139): TOTAL_INCURRED / YTD_EARNED_PREMIUM */
    case
        when i.ytd_earned_premium > 0
            then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
        else null
    end as loss_ratio,

    /* ⚠ Combined ratio = loss_ratio + 0.30 (hard-coded 30% expense load,
       SAS line 144).  This is a business assumption reproduced from
       the source — NOT an endorsement of its accuracy. */
    case
        when i.ytd_earned_premium > 0
            then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium + 0.30
        else null
    end as combined_ratio,

    /* Premium adequacy (SAS lines 150-152):
       N if combined_ratio is null OR > 1.0, else Y */
    case
        when i.ytd_earned_premium > 0
             and (coalesce(ce.total_incurred, 0) / i.ytd_earned_premium + 0.30) <= 1.0
            then 'Y'
        else 'N'
    end as premium_adequate,

    /* ⚠ IBNR estimate = max(0, earned_premium * 0.15 - paid)
       (hard-coded 15% factor, SAS line 155).  Reproduced exactly from
       the source — flagged as a business assumption. */
    greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
        as ibnr_estimate,

    /* Total reserve = open case reserves + IBNR (SAS line 159) */
    coalesce(ce.open_reserves, 0)
        + greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
        as total_reserve,

    cast('{{ val_date }}' as date) as valuation_date,
    current_timestamp() as load_timestamp

from inforce i
left join claims_exp ce on i.policy_id = ce.policy_id
left join premium_coll pc on i.policy_id = pc.policy_id
