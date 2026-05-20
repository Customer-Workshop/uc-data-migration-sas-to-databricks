/*
  Business rule: Basel III risk weights must be between 0.00 and 1.00 inclusive.
*/
select *
from {{ ref('mart_regulatory_rwa') }}
where risk_weight < 0 or risk_weight > 1.00
