/*
  Reconciliation control: per-policy parity for every CASE/derivation in
  policy_valuation.sas Step 1 + Step 4. Parity is per-VALUE, not aggregate: a
  total can tie out while an individual branch is wrong, so we recompute each
  rule independently from the source/inputs and compare it to the stored value
  for EVERY policy.

  Rules reconciled (SAS source -> recomputation):
    1. YTD_EARNED_PREMIUM = ANNUAL_PREMIUM/12 *
         min(12, intck('month', max(EFFECTIVE_DATE, BOY), min(val_date, EXPIRATION_DATE)))
       (BOY = intnx('year', val_date, 0, 'B') = trunc(val_date,'YYYY'))
    2. RENEWAL_DUE_FLAG = 'Y' if EXPIRATION_DATE <= intnx('month', val_date, 3) else 'N'
       (intnx default BEGINNING -> trunc(add_months(val_date,3),'MM'))
    3. COMBINED_RATIO = LOSS_RATIO + 0.30 when YTD_EARNED_PREMIUM > 0 else missing
    4. PREMIUM_ADEQUATE = 'N' if COMBINED_RATIO missing; 'N' if COMBINED_RATIO > 1.0; else 'Y'

  dbt singular test convention: FAILS if this query returns any rows (i.e. any
  policy whose stored value diverges from the source rule).
*/
with model as (
    select * from {{ ref('int_policy_valuation') }}
),

src as (
    select
        p.policy_id,
        p.annual_premium,
        p.effective_date,
        p.expiry_date,
        current_date() as valuation_date
    from {{ source('insurance_raw', 'policies') }} p
    where p.policy_status = 'ACTIVE'
      and p.effective_date <= current_date()
      and p.expiry_date >= current_date()
),

recomputed as (
    select
        s.policy_id,
        s.annual_premium / 12 * least(
            12,
            {{ sas_intck_month(
                "greatest(s.effective_date, trunc(s.valuation_date, 'YYYY'))",
                "least(s.valuation_date, s.expiry_date)"
            ) }}
        ) as exp_earned,
        case
            when s.expiry_date <= trunc(add_months(s.valuation_date, 3), 'MM') then 'Y'
            else 'N'
        end as exp_renewal_flag
    from src s
)

select
    m.policy_id,
    m.ytd_earned_premium,
    r.exp_earned,
    m.renewal_due_flag,
    r.exp_renewal_flag,
    m.combined_ratio,
    m.loss_ratio,
    m.premium_adequate
from model m
inner join recomputed r
    on m.policy_id = r.policy_id
where
    -- 1. earned premium parity
    abs(coalesce(m.ytd_earned_premium, 0) - coalesce(r.exp_earned, 0)) > 0.01

    -- 2. renewal-due flag parity
    or m.renewal_due_flag <> r.exp_renewal_flag

    -- 3. combined ratio = loss ratio + 0.30 (or both missing) parity
    or (
        case when m.ytd_earned_premium > 0 then 1 else 0 end = 1
        and abs(coalesce(m.combined_ratio, 0) - (coalesce(m.loss_ratio, 0) + 0.30)) > 1e-9
    )
    or (m.ytd_earned_premium > 0 and m.combined_ratio is null)
    or (not (m.ytd_earned_premium > 0) and m.combined_ratio is not null)

    -- 4. premium adequacy flag parity
    or m.premium_adequate <> case
        when m.combined_ratio is null then 'N'
        when m.combined_ratio > 1.0 then 'N'
        else 'Y'
    end
