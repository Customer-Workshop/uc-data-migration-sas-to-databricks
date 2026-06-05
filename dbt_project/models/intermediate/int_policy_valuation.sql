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

  Schema mapping (SAS RAW_INS → Databricks insurance_raw):
    policies: STATUS → policy_status, EXPIRATION_DATE → expiry_date,
              CUSTOMER_ID → policyholder_id.
              Columns not in raw: risk_category, underwriting_class, agent_id, branch_code.
    claims:   INCURRED_AMOUNT → claimed_amount (source-faithful: single monetary field in raw).
              PAID_AMOUNT / RESERVED_AMOUNT not in raw — derived from claimed_amount + status.
              Status mapping: SAS OPEN/INV/ADJ/PEND → raw OPEN/PENDING/REOPENED;
                              SAS DENY → raw DENIED.
    premiums: PREMIUM_AMOUNT → premium_paid, PAYMENT_DATE → due_date.
              PAYMENT_STATUS not in raw — returned_premium / late_payments always 0.
*/

with inforce as (
    /* Step 1: Extract in-force policies (SAS PROC SQL → WORK.INFORCE) */
    select
        p.policy_id,
        p.policyholder_id,
        p.policy_type,
        p.effective_date,
        p.expiry_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,

        -- SAS: intck('month', EFFECTIVE_DATE, "&val_date"d)
        months_between(current_date(), p.effective_date) as policy_age_months,

        -- SAS: intck('month', "&val_date"d, EXPIRATION_DATE)
        months_between(p.expiry_date, current_date()) as months_to_expiry,

        -- SAS: EXPIRATION_DATE <= intnx('month', "&val_date"d, 3)
        -- Source-faithful: SAS intnx default alignment is 'B' (beginning of month)
        case
            when p.expiry_date <= date_trunc('month', add_months(current_date(), 3))
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: ANNUAL_PREMIUM / 12 * min(12, intck('month',
        --   max(EFFECTIVE_DATE, intnx('year',val_date,0,'B')),
        --   min(val_date, EXPIRATION_DATE)))
        p.annual_premium / 12 * least(
            12,
            months_between(
                least(current_date(), p.expiry_date),
                greatest(p.effective_date, date_trunc('year', current_date()))
            )
        ) as ytd_earned_premium

    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
        and p.effective_date <= current_date()
        and p.expiry_date >= current_date()
),

claims_exp as (
    /*
      Step 2: Claims experience — 12-month lookback (SAS PROC SQL → WORK.CLAIMS_EXP)
      Divergence: raw table has claimed_amount only (no incurred/paid/reserved split).
      total_incurred maps to claimed_amount (source-faithful proxy).
      total_paid derived from claimed_amount for CLOSED/SETTLED claims.
      open_reserves derived from claimed_amount for open-status claims.
      SAS status codes mapped: OPEN/INV/ADJ/PEND → OPEN/PENDING/REOPENED; DENY → DENIED.
    */
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.claimed_amount) as total_incurred,
        sum(case
            when c.claim_status in ('CLOSED', 'SETTLED')
                then c.claimed_amount
            else 0
        end) as total_paid,
        sum(case
            when c.claim_status not in ('CLOSED', 'SETTLED', 'DENIED')
                then c.claimed_amount
            else 0
        end) as total_reserved,
        max(c.loss_date) as last_claim_date,
        -- SAS: CLAIM_STATUS in ('OPEN','INV','ADJ','PEND') → mapped to raw equivalents
        sum(case
            when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
                then c.claimed_amount
            else 0
        end) as open_reserves,
        sum(case when c.claim_status = 'DENIED' then 1 else 0 end) as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
        and c.loss_date <= current_date()
    group by c.policy_id
),

premium_coll as (
    /*
      Step 3: Premium collections — current year (SAS PROC SQL → WORK.PREMIUM_COLL)
      Divergence: raw premiums table has premium_paid/due_date only (no payment_status).
      returned_premium and late_payments always 0 (raw lacks status field).
    */
    select
        policy_id,
        sum(premium_paid) as collected_premium,
        cast(0 as double) as returned_premium,
        max(due_date) as last_payment_date,
        cast(0 as int) as late_payments
    from {{ source('insurance_raw', 'premiums') }}
    where due_date >= date_trunc('year', current_date())
        and due_date <= current_date()
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
