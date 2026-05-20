/*
  Business rule: All claims with HIGH fraud risk must be denied.
  Mirrors SAS: IF FRAUD_RISK = 'HIGH' → ADJUDICATION_RESULT = 'DENY'.
*/
select *
from {{ ref('int_claims_adjudication') }}
where fraud_risk = 'HIGH'
  and adjudication_result != 'DENY'
