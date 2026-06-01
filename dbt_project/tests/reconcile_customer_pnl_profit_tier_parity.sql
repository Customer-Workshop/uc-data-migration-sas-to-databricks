/*
  Reconciliation test: profit tier parity against SAS source thresholds.

  The SAS program (customer_profitability.sas, Step 4) assigns:
    IF NET_PROFIT >= 500    THEN PROFIT_TIER = 'Highly Profitable'
    ELSE IF NET_PROFIT >= 100 THEN PROFIT_TIER = 'Profitable'
    ELSE IF NET_PROFIT >= 0   THEN PROFIT_TIER = 'Marginal'
    ELSE                           PROFIT_TIER = 'Unprofitable'

  This per-value parity control checks every row's profit_tier against its
  net_profit, catching any branch that diverges from the source mapping.

  dbt singular test convention: FAILS if this query returns any rows.
*/
select
    customer_id,
    net_profit,
    profit_tier as actual_tier,
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
