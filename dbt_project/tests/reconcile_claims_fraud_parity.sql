/*
  Reconciliation control: per-value parity of the fraud-risk CASE.

  claims_processing.sas (Step 2):
      case when f.FRAUD_SCORE >= 80 then 'HIGH'
           when f.FRAUD_SCORE >= 50 then 'MEDIUM'
           else 'LOW' end as FRAUD_RISK
  applied after a LEFT JOIN to TERA_DW.FRAUD_INDICATORS on (POLICY_ID,
  CLAIMANT_ID). A claim with no fraud row has a missing score, which SAS
  compares as < 50, so it buckets to LOW.

  Parity is per value, not aggregate: this control recomputes the expected
  bucket for every claim straight from the raw fraud score and fails on ANY
  claim whose model fraud_risk differs -- including the missing-score -> LOW
  branch. (A correct overall HIGH/MEDIUM/LOW distribution can still hide an
  individual mis-bucketed claim; this catches that.)

  dbt singular test convention: FAILS if this query returns any rows.
*/
with model as (
    select
        claim_id,
        policy_id,
        claimant_id,
        fraud_risk
    from {{ ref('int_claims_adjudication') }}
),

src_fraud as (
    select
        policy_id,
        claimant_id,
        fraud_score
    from {{ source('insurance_raw', 'fraud_indicators') }}
),

compared as (
    select
        m.claim_id,
        m.fraud_risk as model_fraud_risk,
        case
            when f.fraud_score >= 80 then 'HIGH'
            when f.fraud_score >= 50 then 'MEDIUM'
            else 'LOW'
        end as expected_fraud_risk
    from model m
    left join src_fraud f
        on m.policy_id = f.policy_id
        and m.claimant_id = f.claimant_id
)

select
    claim_id,
    model_fraud_risk,
    expected_fraud_risk
from compared
where model_fraud_risk <> expected_fraud_risk
