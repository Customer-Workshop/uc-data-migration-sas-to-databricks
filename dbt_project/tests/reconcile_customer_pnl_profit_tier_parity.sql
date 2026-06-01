/*
  Reconciliation test (PARITY — profit-tier mapping):
  customer_profitability.sas, Step 4 IF/THEN ladder.

  SAS assigns PROFIT_TIER from NET_PROFIT value-for-value:
      NET_PROFIT >= 500 -> 'Highly Profitable'
      NET_PROFIT >= 100 -> 'Profitable'
      NET_PROFIT >=   0 -> 'Marginal'
      else              -> 'Unprofitable'

  Aggregate totals can tie out while an individual tier boundary is wrong, so
  this control re-derives the tier from the mart's own net_profit using the SAS
  thresholds and compares it to the stored profit_tier for EVERY row. Any
  mismatched boundary (e.g. `>` vs `>=`, a shifted cutoff) returns a row.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with mart as (
    select customer_id, net_profit, profit_tier
    from {{ ref('mart_customer_pnl') }}
),

expected as (
    select
        customer_id,
        net_profit,
        profit_tier,
        case
            when net_profit >= 500 then 'Highly Profitable'
            when net_profit >= 100 then 'Profitable'
            when net_profit >= 0   then 'Marginal'
            else 'Unprofitable'
        end as expected_tier
    from mart
)

select
    customer_id,
    net_profit,
    profit_tier as actual_tier,
    expected_tier
from expected
where profit_tier is distinct from expected_tier
