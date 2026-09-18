/*
  int_account_metrics.sql
  Migrated from: Programs/Banking/load_customer_accounts.sas (Step 2)

  SAS Original:
    DATA STG_BANK.CUST_ACCOUNTS_DAILY / WORK.ACCT_EXCEPTIONS step computing
    ACCT_AGE_MONTHS, DAYS_INACTIVE, UTILIZATION_PCT, DORMANCY_FLAG,
    HIGH_BALANCE_FLAG, SNAPSHOT_DATE, LOAD_TIMESTAMP and routing exception
    rows to WORK.ACCT_EXCEPTIONS (see int_acct_exceptions).

  dbt Equivalent:
    Same expressions as SQL CASE / date arithmetic. The business date is
    var('curr_dt') ("&run_date"d), not current_date(), so a batch can be
    re-run for a fixed date. intck('month') counts month boundaries, so it is
    expressed with the sas_intck_month macro rather than months_between().
    The *_desc columns apply the BANKING format catalog ($ACCTTYPE, $ACCTSTAT,
    $CUSTSEG, RISKRATE, $REGION) that SAS attached with FORMAT statements.
*/

with accounts as (
    select * from {{ ref('stg_cust_accounts') }}
),

enriched as (
    select
        *,
        -- SAS: ACCT_AGE_MONTHS = intck('month', OPEN_DATE, "&run_date"d)
        {{ sas_intck_month('open_date', sas_run_date()) }} as acct_age_months,
        -- SAS: DAYS_INACTIVE = "&run_date"d - LAST_ACTIVITY_DATE
        datediff({{ sas_run_date() }}, last_activity_date) as days_inactive,
        -- SAS: if ACCOUNT_TYPE in ('CC','LOC','HELC') and CREDIT_LIMIT > 0
        --        then UTILIZATION_PCT = CURRENT_BALANCE / CREDIT_LIMIT * 100
        case
            when account_type in {{ sas_revolving_products() }} and credit_limit > 0
                then current_balance / credit_limit * 100
            else null
        end as utilization_pct,
        -- SAS: if DAYS_INACTIVE > 365 and ACCOUNT_STATUS = 'A' then DORMANCY_FLAG = 'Y'
        case
            when datediff({{ sas_run_date() }}, last_activity_date) > 365
                and account_status = 'A'
                then 'Y'
            else 'N'
        end as dormancy_flag,
        -- SAS: if CURRENT_BALANCE >= 250000 then HIGH_BALANCE_FLAG = 'Y'
        case
            when current_balance >= 250000 then 'Y' else 'N'
        end as high_balance_flag,
        {{ format_account_type('account_type') }} as account_type_desc,
        {{ format_account_status('account_status') }} as account_status_desc,
        {{ format_customer_segment('customer_segment') }} as customer_segment_desc,
        {{ format_risk_rating('risk_rating') }} as risk_rating_desc,
        {{ format_region('region_code') }} as region_desc,
        -- SAS: SNAPSHOT_DATE = "&run_date"d; LOAD_TIMESTAMP = datetime()
        {{ sas_run_date() }} as snapshot_date,
        current_timestamp() as load_timestamp
    from accounts
)

select * from enriched
