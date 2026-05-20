/*
  Business rule: Approved amount must never exceed claimed amount.
  In the SAS logic, APPROVED_AMOUNT = max(0, CLAIMED_AMOUNT - DEDUCTIBLE),
  which is always <= CLAIMED_AMOUNT.
*/
select *
from {{ ref('int_claims_adjudication') }}
where approved_amount is not null
  and approved_amount > claimed_amount
