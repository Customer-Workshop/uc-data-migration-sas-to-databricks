/*
  int_policy_valuation.sql
  Migrated from: ts-sas-legacy-analytics/Programs/Insurance/policy_valuation.sas
                 (Steps 1-4)

  SAS Original:
    Step 1 — PROC SQL: extract in-force policies
             (STATUS='ACTIVE', EFFECTIVE_DATE <= val_date,
              EXPIRATION_DATE >= val_date); compute POLICY_AGE_MONTHS,
              MONTHS_TO_EXPIRY, RENEWAL_DUE_FLAG and YTD_EARNED_PREMIUM
              (monthly pro-rata earned premium).
    Step 2 — PROC SQL: claims experience over a 12-month lookback window
             grouped by POLICY_ID (count claims, sum incurred/paid/reserved,
             open reserves, denied count).
    Step 3 — PROC SQL: premium collections YTD grouped by POLICY_ID.
    Step 4 — DATA step MERGE BY POLICY_ID (keep in-force `if a;`): compute
             LOSS_RATIO, COMBINED_RATIO (+0.30 expense load),
             PREMIUM_ADEQUATE flag, IBNR_ESTIMATE, TOTAL_RESERVE.

  dbt Equivalent:
    Three CTEs (one per SAS step 1-3) joined in a final SELECT (step 4).
    SAS MERGE BY POLICY_ID with `if a;`  ->  LEFT JOIN keyed on policy_id.
    SAS intck('month', ...)              ->  intck_month() macro (exact
                                             month-boundary count; see macro).
    SAS intnx('month', d, 3) (beginning) ->  trunc(add_months(d, 3), 'MONTH').
    SAS intnx('year',  d, 0, 'B')        ->  trunc(d, 'YEAR').
    SAS PROC FORMAT $POLTYPE             ->  format_policy_type() macro.

  Column-name mapping (SAS RAW_INS.* -> migrated seed/source columns):
    STATUS           -> policy_status
    EXPIRATION_DATE  -> expiry_date
    CUSTOMER_ID      -> policyholder_id
    INCURRED_AMOUNT  -> claimed_amount       (seed has a single amount column)
    PAID_AMOUNT      -> claimed_amount, split by claim_status (see below)
    RESERVED_AMOUNT  -> claimed_amount, split by claim_status (see below)
    PREMIUM_AMOUNT   -> premium_paid
    PAYMENT_DATE     -> due_date
    PAYMENT_STATUS   -> not present in seed (RETURNED / LATE default to 0)

  Claim-status mapping (SAS code -> migrated seed code):
    OPEN              -> OPEN
    INV / ADJ / PEND  -> PENDING, REOPENED   (closest in-flight equivalents)
    DENY              -> DENIED
    (SETTLED / CLOSED in the seed are treated as paid-out, closed claims.)

  FLAGGED, source-faithful business quirks (reproduced, NOT endorsed — any
  remediation is a separate, deliberate decision with the business):
    Q1. Combined ratio adds a hard-coded 30% expense load (SAS line 144 / 188).
    Q2. IBNR estimate uses a hard-coded 15% factor (SAS line 155).
    Q3. SAS does NOT guard the earned-premium month count at 0; for in-force
        policies the count is always >= 0 (EFFECTIVE_DATE <= val_date), so the
        guard is unnecessary and is intentionally omitted to match the source.
    Q4. RISK_CATEGORY / $RISKCAT are referenced by the SAS `format` statement
        but RISK_CATEGORY does not exist in the migrated source, so it is not
        carried forward (no silent substitution).
*/

{% set val_date = var('curr_dt') %}
{% set vdate = "cast('" ~ val_date ~ "' as date)" %}
{% set ep_from = "greatest(p.effective_date, trunc(" ~ vdate ~ ", 'YEAR'))" %}
{% set ep_to = "least(" ~ vdate ~ ", p.expiry_date)" %}

with inforce as (
    /* Step 1: In-force policies (SAS PROC SQL -> WORK.INFORCE) */
    select
        p.policy_id,
        p.policyholder_id,
        p.policy_type,
        {{ format_policy_type('p.policy_type') }} as policy_type_desc,
        p.effective_date,
        p.expiry_date,
        p.annual_premium,
        p.sum_insured,
        p.deductible,

        -- SAS: intck('month', EFFECTIVE_DATE, "&val_date"d)
        {{ intck_month('p.effective_date', vdate) }} as policy_age_months,

        -- SAS: intck('month', "&val_date"d, EXPIRATION_DATE)
        {{ intck_month(vdate, 'p.expiry_date') }} as months_to_expiry,

        -- SAS: EXPIRATION_DATE <= intnx('month', "&val_date"d, 3) -> 'Y'
        --      intnx default alignment is BEGINNING -> first day of the month
        --      three months ahead.
        case
            when p.expiry_date <= trunc(add_months({{ vdate }}, 3), 'MONTH')
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: ANNUAL_PREMIUM / 12 * min(12, intck('month',
        --        max(EFFECTIVE_DATE, intnx('year', "&val_date"d, 0, 'B')),
        --        min("&val_date"d, EXPIRATION_DATE)))
        --      Monthly pro-rata earned premium: months from
        --      max(effective, start-of-year) to min(val_date, expiry), capped
        --      at 12 (no lower guard in the source -- see quirk Q3).
        p.annual_premium / 12.0
            * least(12, {{ intck_month(ep_from, ep_to) }})
            as ytd_earned_premium

    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
      and p.effective_date <= {{ vdate }}
      and p.expiry_date >= {{ vdate }}
),

claims_exp as (
    /* Step 2: Claims experience -- 12-month lookback (SAS -> WORK.CLAIMS_EXP).
       The migrated CLAIMS source has a single claimed_amount column where SAS
       had separate INCURRED / PAID / RESERVED amounts. The split is derived
       from claim_status (see header mapping):
         CLOSED / SETTLED          -> paid
         OPEN / PENDING / REOPENED -> reserved (in-flight) */
    select
        c.policy_id,

        -- SAS: count(distinct CLAIM_ID)
        count(distinct c.claim_id) as num_claims,

        -- SAS: sum(INCURRED_AMOUNT)  (INCURRED_AMOUNT -> claimed_amount)
        sum(c.claimed_amount) as total_incurred,

        -- SAS: sum(PAID_AMOUNT)      (derived: settled/closed claims)
        sum(case when c.claim_status in ('CLOSED', 'SETTLED')
            then c.claimed_amount else 0 end) as total_paid,

        -- SAS: sum(RESERVED_AMOUNT)  (derived: in-flight claims)
        sum(case when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
            then c.claimed_amount else 0 end) as total_reserved,

        -- SAS: max(LOSS_DATE)
        max(c.loss_date) as last_claim_date,

        -- SAS: OPEN_RESERVES = sum(case when CLAIM_STATUS in
        --        ('OPEN','INV','ADJ','PEND') then RESERVED_AMOUNT else 0)
        --      Migrated in-flight statuses: OPEN, PENDING, REOPENED.
        sum(case when c.claim_status in ('OPEN', 'PENDING', 'REOPENED')
            then c.claimed_amount else 0 end) as open_reserves,

        -- SAS: DENIED_CLAIMS = sum(case when CLAIM_STATUS='DENY' then 1 else 0)
        --      Migrated denial status code: DENIED.
        sum(case when c.claim_status = 'DENIED' then 1 else 0 end)
            as denied_claims

    from {{ source('insurance_raw', 'claims') }} c
    where c.loss_date >= add_months({{ vdate }}, -12)
      and c.loss_date <= {{ vdate }}
    group by c.policy_id
),

premium_coll as (
    /* Step 3: Premium collections -- YTD (SAS -> WORK.PREMIUM_COLL).
       Migrated PREMIUMS source has premium_paid / due_date but no
       PAYMENT_STATUS, so RETURNED_PREMIUM and LATE_PAYMENTS default to 0
       (not available in source -- see quirk in header). */
    select
        pr.policy_id,

        -- SAS: sum(PREMIUM_AMOUNT)  (PREMIUM_AMOUNT -> premium_paid)
        sum(pr.premium_paid) as collected_premium,

        -- SAS: sum(case when PAYMENT_STATUS='RETURNED' ...) -- not in source
        cast(0 as double) as returned_premium,

        -- SAS: max(PAYMENT_DATE)    (PAYMENT_DATE -> due_date)
        max(pr.due_date) as last_payment_date,

        -- SAS: count(case when PAYMENT_STATUS='LATE' ...) -- not in source
        cast(0 as int) as late_payments

    from {{ source('insurance_raw', 'premiums') }} pr
    where pr.due_date >= trunc({{ vdate }}, 'YEAR')
      and pr.due_date <= {{ vdate }}
    group by pr.policy_id
)

/* Step 4: Merge and calculate valuation metrics (SAS DATA step MERGE BY).
   `if a;` keeps only in-force policies -> LEFT JOIN from inforce. */
select
    i.policy_id,
    i.policyholder_id,
    i.policy_type,
    i.policy_type_desc,
    i.effective_date,
    i.expiry_date,
    i.annual_premium,
    i.sum_insured,
    i.deductible,
    i.policy_age_months,
    i.months_to_expiry,
    i.renewal_due_flag,
    i.ytd_earned_premium,

    -- Claims experience (coalesce: unmatched in-force policies -> 0)
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

    -- SAS (lines 136-139): LOSS_RATIO = coalesce(TOTAL_INCURRED,0)
    --                                    / YTD_EARNED_PREMIUM, else missing.
    case
        when i.ytd_earned_premium > 0
            then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium
        else null
    end as loss_ratio,

    -- SAS (lines 142-147): COMBINED_RATIO = LOSS_RATIO + 0.30, else missing.
    -- Quirk Q1: hard-coded 30% expense load reproduced from the source.
    case
        when i.ytd_earned_premium > 0
            then coalesce(ce.total_incurred, 0) / i.ytd_earned_premium + 0.30
        else null
    end as combined_ratio,

    -- SAS (lines 149-152): PREMIUM_ADEQUATE = 'N' when COMBINED_RATIO is
    --      missing or > 1.0, else 'Y'  (i.e. 'Y' iff earned>0 and combined<=1).
    case
        when i.ytd_earned_premium > 0
             and (coalesce(ce.total_incurred, 0) / i.ytd_earned_premium + 0.30)
                 <= 1.0
            then 'Y'
        else 'N'
    end as premium_adequate,

    -- SAS (line 155): IBNR_ESTIMATE = max(0, YTD_EARNED_PREMIUM * 0.15
    --                                       - coalesce(TOTAL_PAID, 0)).
    -- Quirk Q2: hard-coded 15% IBNR factor reproduced from the source.
    greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
        as ibnr_estimate,

    -- SAS (line 159): TOTAL_RESERVE = coalesce(OPEN_RESERVES,0) + IBNR_ESTIMATE.
    coalesce(ce.open_reserves, 0)
        + greatest(0, i.ytd_earned_premium * 0.15 - coalesce(ce.total_paid, 0))
        as total_reserve,

    -- SAS (line 162): VALUATION_DATE = "&val_date"d.
    {{ vdate }} as valuation_date,
    current_timestamp() as load_timestamp

from inforce i
left join claims_exp ce on i.policy_id = ce.policy_id
left join premium_coll pc on i.policy_id = pc.policy_id
