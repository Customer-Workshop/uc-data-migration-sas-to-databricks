/*
  int_account_exceptions.sql
  Migrated from: Programs/Banking/load_customer_accounts.sas (Step 2, exception
  branch) + Step 3 (insert into STG_BANK.ACCT_EXCEPTIONS).

  SAS original:
    The DATA step writes two datasets. Alongside each account snapshot row it
    emits a data-quality exception row to WORK.ACCT_EXCEPTIONS for every rule
    that fires, in this order:

      1. NEG_BAL   ACCOUNT_TYPE in ('CHK','SAV','MMA','CD') and CURRENT_BALANCE < 0
      2. HIGH_UTIL UTILIZATION_PCT > 95
      3. NO_RISK   RISK_RATING = .

    Step 3 then appends WORK.ACCT_EXCEPTIONS to STG_BANK.ACCT_EXCEPTIONS.

  dbt equivalent:
    One `output WORK.ACCT_EXCEPTIONS` statement per rule becomes one branch of a
    union all over int_account_metrics (which already holds the derived columns
    the rules read: UTILIZATION_PCT and friends). An account that trips two
    rules produces two rows, so the grain is (account_id, exception_code) —
    the same grain the DATA step emits.

  Source quirks reproduced (flagged, not fixed):
    Q1  The step's `drop EXCEPTION_CODE EXCEPTION_DESC;` has no dataset option,
        so in SAS it applies to BOTH output datasets: the rows that land in
        STG_BANK.ACCT_EXCEPTIONS carry no code and no description, which makes
        the legacy exception table unusable for triage. This model keeps both
        columns (the values the DATA step computed) so the exceptions can be
        reconciled and acted on; the row count and grain are unchanged.
    Q2  SNAPSHOT_DATE and LOAD_TIMESTAMP are assigned *after* the three
        `output WORK.ACCT_EXCEPTIONS` statements, so every exception row leaves
        the DATA step with both values missing. Reproduced as nulls below.
    Q3  The NEG_BAL rule covers deposit types only ('CHK','SAV','MMA','CD'), so
        a negative balance on any other type raises nothing. HIGH_UTIL reads
        UTILIZATION_PCT, which is null for non-revolving accounts and for
        revolving accounts with CREDIT_LIMIT <= 0; in SAS a missing value is
        never > 95, and in SQL a null comparison is never true, so both engines
        emit nothing for those accounts.
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

-- SAS: if ACCOUNT_TYPE in ('CHK','SAV','MMA','CD') and CURRENT_BALANCE < 0
neg_bal as (
    select
        *,
        1 as exception_seq,
        'NEG_BAL' as exception_code,
        -- SAS: catx(' ', 'Negative balance', put(CURRENT_BALANCE, dollar18.2),
        --            'on deposit account', ACCOUNT_ID)
        -- dollar18.2 renders a negative as -$1,234.56 (sign before the symbol).
        concat_ws(
            ' ',
            'Negative balance',
            concat('-$', format_number(abs(current_balance), 2)),
            'on deposit account',
            account_id
        ) as exception_desc
    from accounts
    where account_type in ('CHK', 'SAV', 'MMA', 'CD')
      and current_balance < 0
),

-- SAS: if UTILIZATION_PCT > 95
high_util as (
    select
        *,
        2 as exception_seq,
        'HIGH_UTIL' as exception_code,
        -- SAS: catx(' ', 'Utilization at', put(UTILIZATION_PCT, 5.1), '%',
        --            'for account', ACCOUNT_ID)
        -- catx inserts a space before '%', as the source does.
        concat_ws(
            ' ',
            'Utilization at',
            format_number(utilization_pct, 1),
            '%',
            'for account',
            account_id
        ) as exception_desc
    from accounts
    where utilization_pct > 95
),

-- SAS: if RISK_RATING = .  (numeric missing)
no_risk as (
    select
        *,
        3 as exception_seq,
        'NO_RISK' as exception_code,
        -- SAS: catx(' ', 'Missing risk rating for customer', CUSTOMER_ID)
        concat_ws(' ', 'Missing risk rating for customer', customer_id) as exception_desc
    from accounts
    where risk_rating is null
),

emitted as (
    select * from neg_bal
    union all
    select * from high_util
    union all
    select * from no_risk
)

select
    account_id,
    customer_id,
    account_type,
    account_status,
    open_date,
    close_date,
    current_balance,
    available_balance,
    credit_limit,
    interest_rate,
    branch_id,
    officer_id,
    last_activity_date,
    first_name,
    last_name,
    ssn_hash,
    date_of_birth,
    customer_segment,
    risk_rating,
    region_code,
    primary_email,
    phone_number,
    acct_age_months,
    days_inactive,
    utilization_pct,
    dormancy_flag,
    high_balance_flag,
    exception_code,
    exception_desc,
    exception_seq,
    -- Q2: missing in the SAS rows; assigned after the exception outputs.
    cast(null as date) as snapshot_date,
    cast(null as timestamp) as load_timestamp
from emitted
