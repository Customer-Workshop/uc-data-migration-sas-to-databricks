/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1 — PROC SQL extract of in-force policies from RAW_INS.POLICIES
    Step 2 — PROC SQL 12-month claims experience from RAW_INS.CLAIMS
    Step 3 — PROC SQL YTD premium collections from RAW_INS.PREMIUMS
    Step 4 — DATA step MERGE BY POLICY_ID + derived valuation metrics

  dbt Equivalent:
    Three source CTEs (inforce / claims_exp / premium_coll) → LEFT JOIN
    SAS MERGE BY → SQL LEFT JOIN on policy_id (if a → inner via inforce base)
    SAS IF/THEN metrics → SQL CASE expressions
    SAS FORMAT $POLTYPE / $RISKCAT → Jinja macros format_policy_type / format_risk_category

  Column mapping (SAS → Databricks raw):
    RAW_INS.POLICIES.STATUS          → insurance_raw.policies.policy_status
    RAW_INS.POLICIES.EXPIRATION_DATE → insurance_raw.policies.expiry_date
    RAW_INS.POLICIES.CUSTOMER_ID     → insurance_raw.policies.policyholder_id
    RAW_INS.CLAIMS.INCURRED_AMOUNT   → insurance_raw.claims.claimed_amount
    RAW_INS.CLAIMS.PAID_AMOUNT       → derived from claimed_amount + claim_status
    RAW_INS.CLAIMS.RESERVED_AMOUNT   → derived from claimed_amount + claim_status
    RAW_INS.CLAIMS.CLAIM_STATUS 'DENY' → 'DENIED'; 'OPEN','INV','ADJ','PEND'
                                        → 'OPEN','PENDING','REOPENED'
    RAW_INS.PREMIUMS.PREMIUM_AMOUNT  → insurance_raw.premiums.premium_paid
    RAW_INS.PREMIUMS.PAYMENT_DATE    → insurance_raw.premiums.due_date
    (RISK_CATEGORY, UNDERWRITING_CLASS, AGENT_ID, BRANCH_CODE not in raw)
    (PAYMENT_STATUS not in raw; returned/late premium logic omitted)

  Quirks reproduced from source (flagged, not fixed):
    1. Combined ratio uses a hard-coded 30% expense load (line 144 in SAS).
    2. PREMIUM_ADEQUATE = 'N' when COMBINED_RATIO is null (i.e. when
       YTD_EARNED_PREMIUM = 0). Source-faithful.
    3. IBNR estimate is a simplified formula (15% of earned premium minus paid).
       Real actuarial IBNR would use development triangles. Source-faithful.
*/

with inforce as (
    /* Step 1: In-force policy extract (SAS: WORK.INFORCE) */
    select
        p.policy_id,
        p.policyholder_id,
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

        -- SAS: YTD earned premium (monthly pro-rata)
        -- annual_premium / 12 * min(12, months from max(eff, start_of_year)
        --   to min(val_date, expiry))
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
      Step 2: 12-month claims experience (SAS: WORK.CLAIMS_EXP)
      SAS uses INCURRED_AMOUNT / PAID_AMOUNT / RESERVED_AMOUNT columns;
      raw data has only claimed_amount. We derive paid/reserved from
      claim_status: settled/closed → paid; open/pending/reopened → reserved.
    */
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
        -- SAS: OPEN_RESERVES (status in OPEN/INV/ADJ/PEND → reserved)
        -- mapped: OPEN/PENDING/REOPENED → reserved amount
        sum(
            case
                when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
                    then c.claimed_amount
                else 0
            end
        ) as open_reserves,
        -- SAS: DENIED_CLAIMS (status = 'DENY')
        -- mapped: claim_status = 'DENIED'
        sum(
            case when c.claim_status = 'DENIED' then 1 else 0 end
        ) as denied_claims
    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months(current_date(), -12)
      and c.loss_date <= current_date()
    group by c.policy_id
),

premium_coll as (
    /*
      Step 3: YTD premium collections (SAS: WORK.PREMIUM_COLL)
      SAS uses PREMIUM_AMOUNT / PAYMENT_DATE / PAYMENT_STATUS;
      raw data has premium_paid / due_date (no payment_status).
      RETURNED_PREMIUM and LATE_PAYMENTS cannot be derived without
      payment_status — set to 0 / null.
    */
    select
        pm.policy_id,
        sum(pm.premium_paid) as collected_premium,
        -- SAS: RETURNED_PREMIUM (PAYMENT_STATUS='RETURNED')
        -- not derivable from raw; default to 0
        cast(0 as double) as returned_premium,
        max(pm.due_date) as last_payment_date,
        -- SAS: LATE_PAYMENTS (PAYMENT_STATUS='LATE')
        -- not derivable from raw; default to 0
        cast(0 as int) as late_payments
    from {{ source('insurance_raw', 'premiums') }} pm
    where pm.due_date >= date_trunc('year', current_date())
      and pm.due_date <= current_date()
    group by pm.policy_id
),

merged as (
    /*
      Step 4: Merge + valuation metrics (SAS: DATA STG_INS.POLICY_VALUATION)
      SAS "if a;" → keep only in-force policies (LEFT JOINs from inforce)
    */
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

        -- Claims experience (nulls coalesced per SAS logic)
        c.num_claims,
        c.total_incurred,
        c.total_paid,
        c.total_reserved,
        c.last_claim_date,
        c.open_reserves,
        c.denied_claims,

        -- Premium collections
        p.collected_premium,
        p.returned_premium,
        p.last_payment_date,
        p.late_payments,

        -- SAS: LOSS_RATIO = coalesce(TOTAL_INCURRED, 0) / YTD_EARNED_PREMIUM
        case
            when i.ytd_earned_premium > 0
                then coalesce(c.total_incurred, 0) / i.ytd_earned_premium
            else null
        end as loss_ratio,

        -- SAS: COMBINED_RATIO = LOSS_RATIO + 0.30
        -- QUIRK: hard-coded 30% expense load (source-faithful)
        case
            when i.ytd_earned_premium > 0
                then coalesce(c.total_incurred, 0) / i.ytd_earned_premium + 0.30
            else null
        end as combined_ratio,

        -- SAS: PREMIUM_ADEQUATE flag
        -- QUIRK: null combined_ratio → 'N' (source-faithful; policies with
        -- zero earned premium are always flagged as inadequate)
        case
            when i.ytd_earned_premium <= 0 or i.ytd_earned_premium is null
                then 'N'
            when coalesce(c.total_incurred, 0) / i.ytd_earned_premium + 0.30 > 1.0
                then 'N'
            else 'Y'
        end as premium_adequate,

        -- SAS: IBNR_ESTIMATE = max(0, YTD_EARNED_PREMIUM * 0.15 - coalesce(TOTAL_PAID, 0))
        -- QUIRK: simplified actuarial estimate (source-faithful)
        greatest(
            0,
            i.ytd_earned_premium * 0.15 - coalesce(c.total_paid, 0)
        ) as ibnr_estimate,

        -- SAS: TOTAL_RESERVE = coalesce(OPEN_RESERVES, 0) + IBNR_ESTIMATE
        coalesce(c.open_reserves, 0)
            + greatest(0, i.ytd_earned_premium * 0.15 - coalesce(c.total_paid, 0))
            as total_reserve,

        -- SAS FORMAT replacement
        {{ format_policy_type('i.policy_type') }} as policy_type_desc,

        current_date() as valuation_date,
        current_timestamp() as load_timestamp

    from inforce i
    left join claims_exp c
        on i.policy_id = c.policy_id
    left join premium_coll p
        on i.policy_id = p.policy_id
)

select * from merged
