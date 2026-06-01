/*
  mart_branch_profitability.sql
  Migrated from: Programs/Reports/customer_profitability.sas (Step 5, PROC MEANS #2)

  SAS Original:
    PROC MEANS with CLASS BRANCH_ID REGION_CODE producing N and SUM aggregates.

  dbt Equivalent:
    SQL GROUP BY replacing PROC MEANS CLASS statement.
*/

select
    branch_id,
    region_code,
    count(*) as n_customers,
    sum(total_revenue) as total_revenue,
    sum(operating_cost) as operating_cost,
    sum(total_ecl) as total_ecl,
    sum(net_profit) as net_profit,
    report_month

from {{ ref('mart_customer_pnl') }}
group by branch_id, region_code, report_month
