/*
  Reconciliation: profit-tier CASE parity.

  The SAS program assigns PROFIT_TIER via four IF/THEN/ELSE branches:
    NET_PROFIT >= 500   → 'Highly Profitable'
    NET_PROFIT >= 100   → 'Profitable'
    NET_PROFIT >= 0     → 'Marginal'
    else                → 'Unprofitable'

  This check re-derives the tier from the mart's own NET_PROFIT column and
  compares it to the stored PROFIT_TIER.  Any mismatch means the CASE
  expression in the model has drifted from the SAS thresholds.

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    customer_id,
    net_profit,
    profit_tier as stored_tier,
    case
        when net_profit >= 500 then 'Highly Profitable'
        when net_profit >= 100 then 'Profitable'
        when net_profit >= 0   then 'Marginal'
        else 'Unprofitable'
    end as expected_tier
from {{ ref('mart_customer_pnl') }}
where profit_tier <> case
    when net_profit >= 500 then 'Highly Profitable'
    when net_profit >= 100 then 'Profitable'
    when net_profit >= 0   then 'Marginal'
    else 'Unprofitable'
end
