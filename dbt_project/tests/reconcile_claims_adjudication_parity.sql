/*
  Reconciliation test: adjudication result parity per CASE branch.

  Verifies that the auto-adjudication CASE logic in int_claims_adjudication
  reproduces the SAS DATA step IF/THEN/RETURN routing faithfully for every
  combination of fraud_risk, claimed_amount, and policy_type.

  Each branch of the SAS routing is checked individually:
    1. HIGH fraud → must be DENY
    2. LOW + amount <= 5000 + type in (AUTO,HOME,RENT) → must be APPR
    3. LOW + amount <= 25% sum_insured + amount <= 50000 (not rule 2) → must be APPR
    4. Everything else → must be PEND

  A single mismatch means the conversion diverged from the source.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
select *
from {{ ref('int_claims_adjudication') }}
where
    -- Branch 1: HIGH fraud must always produce DENY
    (fraud_risk = 'HIGH' and adjudication_result <> 'DENY')

    -- Branch 2: LOW + small claim on eligible policy types must produce APPR
    or (fraud_risk = 'LOW'
        and claimed_amount <= 5000
        and policy_type in ('AUTO', 'HOME', 'RENT')
        and adjudication_result <> 'APPR')

    -- Branch 3: LOW + within 25% threshold + under 50k (not caught by rule 2)
    or (fraud_risk = 'LOW'
        and not (claimed_amount <= 5000 and policy_type in ('AUTO', 'HOME', 'RENT'))
        and claimed_amount <= sum_insured * 0.25
        and claimed_amount <= 50000
        and adjudication_result <> 'APPR')

    -- Branch 4: everything else must produce PEND
    or (fraud_risk <> 'HIGH'
        and not (
            fraud_risk = 'LOW'
            and claimed_amount <= 5000
            and policy_type in ('AUTO', 'HOME', 'RENT')
        )
        and not (
            fraud_risk = 'LOW'
            and claimed_amount <= sum_insured * 0.25
            and claimed_amount <= 50000
        )
        and adjudication_result <> 'PEND')
