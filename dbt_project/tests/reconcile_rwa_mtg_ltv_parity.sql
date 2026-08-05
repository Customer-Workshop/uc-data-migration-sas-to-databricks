/*
  Reconciliation control: mortgage risk-weight parity against source LTV.

  The two LTV-dependent branches of monthly_regulatory_reporting.sas Step 1:

      ACCOUNT_TYPE = 'MTG' and LTV <= 0.80 -> 0.35
      ACCOUNT_TYPE = 'MTG' and LTV >  0.80 -> 0.50

  A missing LTV on LOAN_DETAILS satisfies `LTV <= 0.80` in SAS (missing sorts
  below every number), so those mortgages are weighted 0.35 in the source. SQL
  would drop them to the catch-all 1.00 instead, so the model matches NULL
  explicitly and this control counts the SAS way — a source-faithful quirk, not
  an endorsement.

  Counts come from the raw tables, so the check is independent of the model's
  join path: a wrong boundary (<= vs <), a dropped NULL, or a fanned-out join
  moves accounts between bands and fails here.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with in_scope_mortgages as (
    select
        a.account_id,
        l.ltv
    from {{ source('banking_raw', 'cust_accounts') }} a
    inner join {{ source('banking_raw', 'cust_demographics') }} d
        on a.customer_id = d.customer_id
    left join {{ source('banking_raw', 'loan_details') }} l
        on a.account_id = l.account_id
    where a.account_status not in ('W', 'C')
      and a.open_date <= current_date()
      and a.account_type = 'MTG'
),

expected as (
    select
        case when ltv is null or ltv <= 0.80 then 0.35 else 0.50 end as risk_weight,
        count(*) as n_accounts
    from in_scope_mortgages
    group by 1
),

actual as (
    select
        risk_weight,
        sum(n_accounts) as n_accounts
    from {{ ref('mart_regulatory_rwa') }}
    where account_type = 'MTG'
    group by 1
)

select
    coalesce(e.risk_weight, a.risk_weight) as risk_weight,
    e.n_accounts as expected_accounts,
    a.n_accounts as actual_accounts
from expected e
full outer join actual a
    on cast(e.risk_weight as decimal(5, 2)) = cast(a.risk_weight as decimal(5, 2))
where coalesce(e.n_accounts, -1) <> coalesce(a.n_accounts, -1)
