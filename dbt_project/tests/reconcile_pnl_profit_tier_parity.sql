/*
  Reconciliation test: profit_tier parity against the SAS threshold rules.

  SAS customer_profitability.sas (Step 4) assigns profit_tier via:
    if NET_PROFIT >= 500    then PROFIT_TIER = 'Highly Profitable';
    else if NET_PROFIT >= 100 then PROFIT_TIER = 'Profitable';
    else if NET_PROFIT >= 0   then PROFIT_TIER = 'Marginal';
    else PROFIT_TIER = 'Unprofitable';

  This control verifies value-for-value: no customer has a tier inconsistent
  with the thresholds applied to their net_profit.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
select
    customer_id,
    net_profit,
    profit_tier,
    case
        when net_profit >= 500 then 'Highly Profitable'
        when net_profit >= 100 then 'Profitable'
        when net_profit >= 0 then 'Marginal'
        else 'Unprofitable'
    end as expected_tier
from {{ ref('mart_customer_pnl') }}
where profit_tier != case
    when net_profit >= 500 then 'Highly Profitable'
    when net_profit >= 100 then 'Profitable'
    when net_profit >= 0 then 'Marginal'
    else 'Unprofitable'
end
