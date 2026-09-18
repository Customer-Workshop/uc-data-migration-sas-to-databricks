/*
  int_acct_exceptions.sql
  Migrated from: Programs/Banking/load_customer_accounts.sas (Step 2, exception
  routing) and Step 3 (INSERT INTO STG_BANK.ACCT_EXCEPTIONS).

  SAS Original:
    Inside the snapshot DATA step three *independent* IF blocks each do
    `output WORK.ACCT_EXCEPTIONS`, so one account can produce up to three
    exception rows:
      NEG_BAL   — ACCOUNT_TYPE in ('CHK','SAV','MMA','CD') and CURRENT_BALANCE < 0
      HIGH_UTIL — UTILIZATION_PCT > 95
      NO_RISK   — RISK_RATING = .
    The row is still written to the snapshot afterwards.

    Step 3 inserts WORK.ACCT_EXCEPTIONS into STG_BANK.ACCT_EXCEPTIONS. The
    permanent table in the estate has the snapshot shape (29 columns, without
    EXCEPTION_CODE / EXCEPTION_DESC), so those two columns are not persisted
    and the golden export carries the snapshot columns only. They are kept
    here so each rule branch can be reconciled (mapping-parity control).

  dbt Equivalent:
    UNION ALL of the three rule branches over int_account_metrics.
    EXCEPTION_DESC mirrors the SAS catx() text; the SAS dollar18.2 / 5.1
    picture formats are approximated with format_number().
*/

with accounts as (
    select * from {{ ref('int_account_metrics') }}
),

neg_bal as (
    select
        *,
        'NEG_BAL' as exception_code,
        concat(
            'Negative balance $', format_number(current_balance, 2),
            ' on deposit account ', account_id
        ) as exception_desc
    from accounts
    where account_type in {{ sas_deposit_products() }}
      and current_balance < 0
),

high_util as (
    select
        *,
        'HIGH_UTIL' as exception_code,
        concat(
            'Utilization at ', format_number(utilization_pct, 1),
            ' % for account ', account_id
        ) as exception_desc
    from accounts
    where utilization_pct > 95
),

no_risk as (
    select
        *,
        'NO_RISK' as exception_code,
        concat('Missing risk rating for customer ', customer_id) as exception_desc
    from accounts
    where risk_rating is null
)

select * from neg_bal
union all
select * from high_util
union all
select * from no_risk
