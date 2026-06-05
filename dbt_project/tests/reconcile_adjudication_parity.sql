/*
  Reconciliation test: adjudication parity — every adjudication_result in
  int_claims_adjudication matches the SAS routing rules exactly.

  The SAS DATA step (claims_processing.sas, Step 3) routes claims via sequential
  IF/THEN/RETURN:
    1. FRAUD_RISK='HIGH' → DENY
    2. FRAUD_RISK='LOW' AND claimed_amount<=5000 AND policy_type in (AUTO,HOME,RENT) → APPR
    3. FRAUD_RISK='LOW' AND claimed_amount<=sum_insured*0.25 AND claimed_amount<=50000 → APPR
    4. Everything else → PEND

  This test independently derives the expected adjudication_result from the raw
  source tables (claims, policies, fraud_indicators) — reproducing the full
  pipeline from scratch — then compares against the model output. Each returned
  row is a parity violation.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with raw_claims as (
    select * from {{ source('insurance_raw', 'claims') }}
),

active_policies as (
    select
        policy_id,
        policy_type,
        effective_date,
        expiry_date,
        sum_insured,
        deductible
    from {{ source('insurance_raw', 'policies') }}
    where policy_status = 'ACTIVE'
),

valid_claims as (
    -- Reproduce stg_claims validation from raw sources
    select
        c.claim_id,
        c.policy_id,
        c.claimed_amount,
        p.policy_type,
        p.sum_insured,
        p.deductible
    from raw_claims c
    inner join active_policies p
        on c.policy_id = p.policy_id
    where c.loss_date >= p.effective_date
        and c.loss_date <= p.expiry_date
        and c.claimed_amount <= p.sum_insured
),

fraud_joined as (
    -- Reproduce fraud screening from raw fraud_indicators
    select
        v.*,
        case
            when f.fraud_score >= 0.80 then 'HIGH'
            when f.fraud_score >= 0.50 then 'MEDIUM'
            else 'LOW'
        end as expected_fraud_risk
    from valid_claims v
    left join {{ source('insurance_raw', 'fraud_indicators') }} f
        on v.claim_id = f.claim_id
),

expected as (
    -- Reproduce adjudication routing (SAS sequential IF/THEN/RETURN)
    select
        claim_id,
        case
            when expected_fraud_risk = 'HIGH' then 'DENY'
            when expected_fraud_risk = 'LOW'
                and claimed_amount <= 5000
                and policy_type in ('AUTO', 'HOME', 'RENT') then 'APPR'
            when expected_fraud_risk = 'LOW'
                and claimed_amount <= sum_insured * 0.25
                and claimed_amount <= 50000 then 'APPR'
            else 'PEND'
        end as expected_result
    from fraud_joined
),

parity_check as (
    select
        e.claim_id,
        a.adjudication_result as actual_result,
        e.expected_result
    from expected e
    inner join {{ ref('int_claims_adjudication') }} a
        on e.claim_id = a.claim_id
)

select
    claim_id,
    actual_result,
    expected_result
from parity_check
where actual_result <> expected_result
