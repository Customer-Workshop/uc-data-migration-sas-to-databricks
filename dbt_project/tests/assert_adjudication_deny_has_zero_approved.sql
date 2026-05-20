/*
  Business rule: Claims denied (adjudication_result = 'DENY') must have
  approved_amount = 0. Mirrors the SAS routing where HIGH fraud risk
  forces APPROVED_AMOUNT = 0.
*/
select *
from {{ ref('int_claims_adjudication') }}
where adjudication_result = 'DENY'
  and approved_amount != 0
