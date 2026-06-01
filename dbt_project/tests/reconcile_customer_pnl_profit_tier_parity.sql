/*
  Reconciliation test: profit_tier CASE parity.

  Verifies that profit_tier assignments in the mart match the SAS thresholds
  exactly (500 / 100 / 0). Any row where the tier does not agree with a
  re-derivation from net_profit using the SAS thresholds indicates a CASE
  branch divergence.

  SAS source (customer_profitability.sas lines 128-132):
    if NET_PROFIT >= 500    then PROFIT_TIER = 'Highly Profitable';
    else if NET_PROFIT >= 100 then PROFIT_TIER = 'Profitable';
    else if NET_PROFIT >= 0   then PROFIT_TIER = 'Marginal';
    else PROFIT_TIER = 'Unprofitable';

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    customer_id,
    net_profit,
    profit_tier as actual_tier,
    case
        when net_profit >= 500 then 'Highly Profitable'
        when net_profit >= 100 then 'Profitable'
        when net_profit >= 0 then 'Marginal'
        else 'Unprofitable'
    end as expected_tier
from {{ ref('mart_customer_pnl') }}
where profit_tier <> case
    when net_profit >= 500 then 'Highly Profitable'
    when net_profit >= 100 then 'Profitable'
    when net_profit >= 0 then 'Marginal'
    else 'Unprofitable'
end
