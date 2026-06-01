/*
  mart_segment_profitability.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 5, PROC MEANS #1)

  SAS Original:
    PROC MEANS with CLASS CUSTOMER_SEGMENT producing N, SUM, and MEAN aggregates.

  dbt Equivalent:
    SQL GROUP BY replacing PROC MEANS CLASS statement.
*/

select
    customer_segment,
    {{ format_customer_segment('customer_segment') }} as customer_segment_desc,
    count(*) as n_customers,
    sum(total_revenue) as total_revenue,
    sum(operating_cost) as operating_cost,
    sum(total_ecl) as total_ecl,
    sum(net_profit) as net_profit,
    sum(total_relationship) as total_relationship,
    avg(net_profit) as avg_profit_per_customer,
    report_month

from {{ ref('mart_customer_pnl') }}
group by customer_segment, report_month
