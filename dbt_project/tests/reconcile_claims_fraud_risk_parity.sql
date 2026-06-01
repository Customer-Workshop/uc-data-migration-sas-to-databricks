/*
  Reconciliation test: parity check — fraud_risk CASE mapping.

  The SAS source (claims_processing.sas, Step 2) defines:
    fraud_score >= 80  -> 'HIGH'
    fraud_score >= 50  -> 'MEDIUM'
    else               -> 'LOW'

  This test verifies that the dbt model's fraud_risk assignment matches the
  source mapping for every row. Any row where the assigned fraud_risk does not
  agree with the score thresholds represents a divergence from the source.

  dbt singular test convention: returns rows on FAILURE.
*/
select
    claim_id,
    fraud_score,
    fraud_risk,
    case
        when fraud_score >= 80 then 'HIGH'
        when fraud_score >= 50 then 'MEDIUM'
        else 'LOW'
    end as expected_fraud_risk
from {{ ref('int_claims_adjudication') }}
where fraud_risk <> (
    case
        when fraud_score >= 80 then 'HIGH'
        when fraud_score >= 50 then 'MEDIUM'
        else 'LOW'
    end
)
