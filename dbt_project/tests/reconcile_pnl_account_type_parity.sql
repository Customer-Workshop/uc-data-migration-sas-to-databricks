/*
  Reconciliation test: account type → lending/deposit CASE parity.

  SAS Step 1 classifies every account into exactly one revenue bucket:
    Lending: MTG, AUTO, PERS, CC, LOC, HELC
    Deposit: CHK, SAV, MMA, CD, IRA

  If an account type exists in the source that is not covered by either
  CASE branch, its balance × rate contribution silently falls to 0 —
  a revenue leak the SAS had and the dbt model must also have (but we
  want to know about it).

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with account_types_in_source as (
    select distinct account_type
    from {{ ref('int_account_metrics') }}
),

classified as (
    select
        account_type,
        case
            when account_type in ('MTG', 'AUTO', 'PERS', 'CC', 'LOC', 'HELC')
                then 'LENDING'
            when account_type in ('CHK', 'SAV', 'MMA', 'CD', 'IRA')
                then 'DEPOSIT'
            else 'UNCLASSIFIED'
        end as classification
    from account_types_in_source
)

select *
from classified
where classification = 'UNCLASSIFIED'
