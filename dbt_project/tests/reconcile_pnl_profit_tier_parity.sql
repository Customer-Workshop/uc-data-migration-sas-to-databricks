/*
  Reconciliation test: profit_tier CASE parity.

  SAS Step 4 assigns profit_tier via IF/THEN/ELSE on net_profit thresholds:
    >= 500  → 'Highly Profitable'
    >= 100  → 'Profitable'
    >= 0    → 'Marginal'
    else    → 'Unprofitable'

  This test re-derives the tier from net_profit and compares to the model's
  assignment. Any mismatch means the CASE logic has drifted from the source.

  dbt singular test convention: the test FAILS if this query returns any rows.
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
