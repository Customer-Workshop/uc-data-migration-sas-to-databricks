/*
  Business rule: Risk-weighted assets must be non-negative.
  Basel III risk weights range from 0.00 to 1.00, so RWA cannot be negative.
*/
select *
from {{ ref('mart_regulatory_rwa') }}
where rwa < 0
