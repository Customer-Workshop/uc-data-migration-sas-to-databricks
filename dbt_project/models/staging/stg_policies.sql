/*
  stg_policies.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 1)

  SAS Original:
    PROC SQL extracting in-force policies from RAW_INS.POLICIES with
    derived fields: POLICY_AGE_MONTHS, MONTHS_TO_EXPIRY,
    RENEWAL_DUE_FLAG, YTD_EARNED_PREMIUM.

  dbt Equivalent:
    Staging model reading from Databricks external table (Unity Catalog).
    SAS intck()/intnx() date functions replaced by months_between()/add_months().
*/

with source as (
    select * from {{ source('insurance_raw', 'policies') }}
),

in_force as (
    select
        policy_id,
        customer_id,
        policy_type,
        effective_date,
        expiration_date,
        annual_premium,
        sum_insured,
        deductible,
        risk_category,
        underwriting_class,
        agent_id,
        branch_code,

        -- SAS: intck('month', p.EFFECTIVE_DATE, "&val_date"d)
        months_between(current_date(), effective_date) as policy_age_months,

        -- SAS: intck('month', "&val_date"d, p.EXPIRATION_DATE)
        months_between(expiration_date, current_date()) as months_to_expiry,

        -- SAS: RENEWAL_DUE_FLAG (expires within 3 months)
        case
            when expiration_date <= add_months(current_date(), 3)
                then 'Y'
            else 'N'
        end as renewal_due_flag,

        -- SAS: YTD earned premium (monthly pro-rata)
        annual_premium / 12
            * least(12, months_between(
                least(current_date(), expiration_date),
                greatest(effective_date, trunc(current_date(), 'YEAR'))
            )) as ytd_earned_premium

    from source
    where status = 'ACTIVE'
      and effective_date <= current_date()
      and expiration_date >= current_date()
)

select * from in_force
