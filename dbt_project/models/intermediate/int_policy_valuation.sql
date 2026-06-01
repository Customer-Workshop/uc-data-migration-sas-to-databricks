/*
  int_policy_valuation.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Steps 1-4)

  SAS Original:
    Step 1  PROC SQL  -> WORK.INFORCE      (in-force policy extract + derivations)
    Step 2  PROC SQL  -> WORK.CLAIMS_EXP   (12-month claims experience by policy)
    Step 3  PROC SQL  -> WORK.PREMIUM_COLL (premium collections by policy)
    Step 4  DATA step -> STG_INS.POLICY_VALUATION (MERGE BY POLICY_ID; if a;
            loss ratio / combined ratio / premium adequacy / IBNR / reserves)

  dbt Equivalent:
    PROC SQL extracts          -> source() CTEs (inforce, claims_exp)
    SAS MERGE BY (if a)        -> LEFT JOIN keyed on policy_id (keep in-force only)
    DATA step IF/THEN          -> SQL CASE expressions
    intck / intnx date math    -> sas_intck_month() macro + trunc()/add_months()
    format= POLTYPE.          -> format_policy_type() macro

  Valuation date:
    SAS &val_date defaults to &CURR_DT (the run date). We use current_date(),
    matching the repo convention (int_account_metrics, reconcile_*). Every
    reconciliation control recomputes against the same current_date(), so the
    controls tie out deterministically within a run.

  Source-schema bindings (the synthetic RAW_INS does not carry the exact SAS
  column names - see note in the model body). These are name bindings to the
  available source, NOT changes to the SAS business logic.

  Source-schema GAPS reproduced honestly, NOT fabricated:
    The SAS program also computes TOTAL_PAID, TOTAL_RESERVED, OPEN_RESERVES,
    DENIED_CLAIMS, IBNR_ESTIMATE, TOTAL_RESERVE and the Step-3 premium
    collection metrics. Those depend on RAW_INS.CLAIMS.PAID_AMOUNT /
    RESERVED_AMOUNT and RAW_INS.PREMIUMS.PAYMENT_STATUS / PAYMENT_DATE columns
    that DO NOT EXIST in the synthetic source (banking_analytics.raw.claims has
    only claimed_amount; banking_analytics.raw.premiums has premium_due /
    premium_paid / due_date with no payment status). Per "the SAS source is the
    source of truth", we reproduce only what the source supports and FLAG the
    rest for source enrichment rather than inventing values. See the PR.
*/

with policies as (
    select * from {{ source('insurance_raw', 'policies') }}
),

claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

valuation as (
    select current_date() as valuation_date
),

-- -- SAS Step 1: WORK.INFORCE -----------------------------------------------
-- SAS: where STATUS='ACTIVE' and EFFECTIVE_DATE <= val_date and EXPIRATION_DATE >= val_date
-- Source bindings: STATUS->policy_status, EXPIRATION_DATE->expiry_date, CUSTOMER_ID->policyholder_id
inforce as (
    select
        p.policy_id,
        p.policyholder_id as customer_id,
        p.policy_type,
        p.effective_date,
        p.expiry_date as expiration_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,
        v.valuation_date,

        -- SAS: POLICY_AGE_MONTHS = intck('month', EFFECTIVE_DATE, val_date)
        {{ sas_intck_month('p.effective_date', 'v.valuation_date') }} as policy_age_months,

        -- SAS: MONTHS_TO_EXPIRY = intck('month', val_date, EXPIRATION_DATE)
        {{ sas_intck_month('v.valuation_date', 'p.expiry_date') }} as months_to_expiry,

        -- SAS: RENEWAL_DUE_FLAG = 'Y' when EXPIRATION_DATE <= intnx('month', val_date, 3) else 'N'
        -- intnx default alignment is BEGINNING, so the 3-month horizon snaps to the
        -- first day of that month: trunc(add_months(val_date, 3), 'MM'). Reproduced faithfully.
        case
            when p.expiry_date <= trunc(add_months(v.valuation_date, 3), 'MM')
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: YTD_EARNED_PREMIUM = ANNUAL_PREMIUM/12 *
        --        min(12, intck('month',
        --          max(EFFECTIVE_DATE, intnx('year', val_date, 0, 'B')),
        --          min(val_date, EXPIRATION_DATE)))
        -- intnx('year', val_date, 0, 'B') = beginning of val_date's year = trunc(val_date,'YYYY').
        p.annual_premium / 12 * least(
            12,
            {{ sas_intck_month(
                "greatest(p.effective_date, trunc(v.valuation_date, 'YYYY'))",
                "least(v.valuation_date, p.expiry_date)"
            ) }}
        ) as ytd_earned_premium

    from policies p
    cross join valuation v
    where p.policy_status = 'ACTIVE'
      and p.effective_date <= v.valuation_date
      and p.expiry_date >= v.valuation_date
),

-- -- SAS Step 2: WORK.CLAIMS_EXP (12-month window) --------------------------
-- SAS: where LOSS_DATE >= intnx('month', val_date, -12) and LOSS_DATE <= val_date
-- intnx default BEGINNING -> trunc(add_months(val_date, -12), 'MM'). Reproduced faithfully.
-- Source binding: SAS sum(INCURRED_AMOUNT) -> sum(claimed_amount) (the only claim
-- amount the synthetic RAW_INS.CLAIMS carries). TOTAL_PAID / TOTAL_RESERVED /
-- OPEN_RESERVES / DENIED_CLAIMS are omitted: no source columns (see header).
claims_exp as (
    select
        c.policy_id,
        count(distinct c.claim_id) as num_claims,
        sum(c.claimed_amount) as total_incurred,
        max(c.loss_date) as last_claim_date
    from claims c
    cross join valuation v
    where c.loss_date >= trunc(add_months(v.valuation_date, -12), 'MM')
      and c.loss_date <= v.valuation_date
    group by c.policy_id
),

-- -- SAS Step 4: MERGE WORK.INFORCE(a) WORK.CLAIMS_EXP(b) BY POLICY_ID; if a --
-- "if a" keeps in-force policies only -> LEFT JOIN from inforce.
valued as (
    select
        a.policy_id,
        a.customer_id,
        a.policy_type,
        {{ format_policy_type('a.policy_type') }} as policy_type_desc,
        a.effective_date,
        a.expiration_date,
        a.annual_premium,
        a.sum_insured,
        a.deductible,
        a.policy_age_months,
        a.months_to_expiry,
        a.renewal_due_flag,
        a.ytd_earned_premium,
        coalesce(b.num_claims, 0) as num_claims,
        b.total_incurred,
        b.last_claim_date,

        -- SAS: if YTD_EARNED_PREMIUM > 0 then LOSS_RATIO = coalesce(TOTAL_INCURRED,0)/YTD_EARNED_PREMIUM; else .
        case
            when a.ytd_earned_premium > 0
                then coalesce(b.total_incurred, 0) / a.ytd_earned_premium
        end as loss_ratio,

        -- SAS: if YTD_EARNED_PREMIUM > 0 then COMBINED_RATIO = LOSS_RATIO + 0.30; else .
        case
            when a.ytd_earned_premium > 0
                then coalesce(b.total_incurred, 0) / a.ytd_earned_premium + 0.30
        end as combined_ratio,

        a.valuation_date
    from inforce a
    left join claims_exp b
        on a.policy_id = b.policy_id
)

select
    *,

    -- SAS: if COMBINED_RATIO = . then 'N'; else if COMBINED_RATIO > 1.0 then 'N'; else 'Y'
    case
        when combined_ratio is null then 'N'
        when combined_ratio > 1.0 then 'N'
        else 'Y'
    end as premium_adequate

from valued
