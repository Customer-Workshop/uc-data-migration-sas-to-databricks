/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1: PROC SQL → WORK.INFORCE (in-force policy extract)
    Step 2: PROC SQL → WORK.CLAIMS_EXP (12-month claims experience)
    Step 3: PROC SQL → WORK.PREMIUM_COLL (YTD premium collections)
    Step 4: DATA step MERGE BY policy_id, IF a → STG_INS.POLICY_VALUATION

  dbt Equivalent:
    CTEs replace WORK tables, LEFT JOINs replace MERGE BY + IF a,
    CASE expressions replace DATA step IF/THEN/ELSE logic.
    intck('month',...) → months_between(); intnx('month',...,'B') → date_trunc + add_months.
*/

with inforce as (
    /* Step 1: Extract in-force policies (SAS PROC SQL → WORK.INFORCE) */
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

        -- SAS: EXPIRATION_DATE <= intnx('month', "&val_date"d, 3)
        -- Note: SAS intnx default alignment is 'B' (beginning of month)
        case
            when p.expiration_date <= date_trunc('month', add_months(current_date(), 3))
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: ANNUAL_PREMIUM / 12 * min(12, intck('month',
        --   max(EFFECTIVE_DATE, intnx('year',val_date,0,'B')),
        --   min(val_date, EXPIRATION_DATE)))
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

claims_exp as (
    /* Step 2: Claims experience — 12-month lookback (SAS PROC SQL → WORK.CLAIMS_EXP) */
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.incurred_amount) as total_incurred,
        sum(c.paid_amount) as total_paid,
        sum(c.reserved_amount) as total_reserved,
        max(c.loss_date) as last_claim_date,
        sum(case
            when c.claim_status in ('OPEN', 'INV', 'ADJ', 'PEND')
                then c.reserved_amount
            else 0
        end) as open_reserves,
        sum(case when c.claim_status = 'DENY' then 1 else 0 end) as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
        and c.loss_date <= current_date()
    group by c.policy_id
),

premium_coll as (
    /* Step 3: Premium collections — current year (SAS PROC SQL → WORK.PREMIUM_COLL) */
    select
        policy_id,
        sum(premium_amount) as collected_premium,
        sum(case
            when payment_status = 'RETURNED'
                then premium_amount
            else 0
        end) as returned_premium,
        max(payment_date) as last_payment_date,
        count(case when payment_status = 'LATE' then 1 end) as late_payments
    from {{ source('insurance_raw', 'premiums') }}
    where payment_date >= date_trunc('year', current_date())
        and payment_date <= current_date()
    group by policy_id
),

/*
  Step 4: Merge and calculate valuation metrics
  SAS: MERGE INFORCE(in=a) CLAIMS_EXP(in=b) PREMIUM_COLL(in=c); BY POLICY_ID; IF a;
  dbt: LEFT JOINs preserve all in-force rows (IF a equivalent).
*/
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
        pc.late_payments
    from inforce i
    left join claims_exp ce
        on i.policy_id = ce.policy_id
    left join premium_coll pc
        on i.policy_id = pc.policy_id
),

valued as (
    select
        *,

        -- SAS: if YTD_EARNED_PREMIUM > 0 then LOSS_RATIO = coalesce(TOTAL_INCURRED,0)/YTD_EARNED_PREMIUM
        case
            when ytd_earned_premium > 0
                then coalesce(total_incurred, 0) / ytd_earned_premium
            else null
        end as loss_ratio,

        -- SAS: COMBINED_RATIO = LOSS_RATIO + 0.30 (30% expense load)
        case
            when ytd_earned_premium > 0
                then coalesce(total_incurred, 0) / ytd_earned_premium + 0.30
            else null
        end as combined_ratio,

        -- SAS: PREMIUM_ADEQUATE — source-faithful reproduction
        -- SAS logic: if COMBINED_RATIO = . then 'N'; else if > 1.0 then 'N'; else 'Y'
        case
            when ytd_earned_premium > 0
                and coalesce(total_incurred, 0) / ytd_earned_premium + 0.30 <= 1.0
                then 'Y'
            else 'N'
        end as premium_adequate,

        -- SAS: IBNR_ESTIMATE = max(0, YTD_EARNED_PREMIUM * 0.15 - coalesce(TOTAL_PAID,0))
        greatest(0, ytd_earned_premium * 0.15 - coalesce(total_paid, 0)) as ibnr_estimate,

        -- SAS: TOTAL_RESERVE = coalesce(OPEN_RESERVES,0) + IBNR_ESTIMATE
        coalesce(open_reserves, 0)
            + greatest(0, ytd_earned_premium * 0.15 - coalesce(total_paid, 0)) as total_reserve,

        current_date() as valuation_date

    from merged
)

select * from valued
