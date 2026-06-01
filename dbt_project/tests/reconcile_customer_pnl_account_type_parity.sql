/*
  Reconciliation test (PARITY — account-type income mapping):
  customer_profitability.sas, Step 1.

  The SAS source classifies every account type into exactly one income class,
  value-for-value:
      LENDING (income) : MTG, AUTO, PERS, CC, LOC, HELC
      DEPOSIT (cost)   : CHK, SAV, MMA, CD, IRA
  Anything outside both lists falls to the CASE `else 0` and is silently dropped
  from net interest income.

  This is the per-value parity guard for that mapping (mirrors the LOC risk-weight
  defect in the playbook): for every account type that actually appears in the
  source, assert it is covered by the SAS classification and is not claimed by
  both lists. An account type omitted from the dbt CASE (falling to `else`) or a
  type duplicated across both branches surfaces here as a returned row.

  dbt singular test convention: FAILS if this query returns any rows.
*/
with source_types as (
    select distinct account_type
    from {{ ref('int_account_metrics') }}
),

classified as (
    select
        account_type,
        case when account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
             then 1 else 0 end as is_lending,
        case when account_type in ('CHK','SAV','MMA','CD','IRA')
             then 1 else 0 end as is_deposit
    from source_types
)

select
    account_type,
    is_lending,
    is_deposit,
    case
        when is_lending = 0 and is_deposit = 0
            then 'uncovered: falls through SAS CASE to else (silently zeroed)'
        when is_lending = 1 and is_deposit = 1
            then 'double-counted: present in both lending and deposit lists'
    end as parity_violation
from classified
where is_lending + is_deposit <> 1
