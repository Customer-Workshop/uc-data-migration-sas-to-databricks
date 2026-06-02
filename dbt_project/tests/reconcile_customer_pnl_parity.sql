/*
  Reconciliation test: per-account-type interest classification parity vs SAS.

  customer_profitability.sas Step 1 classifies each account type into exactly one
  interest bucket via two IN-lists:
      LENDING : ('MTG','AUTO','PERS','CC','LOC','HELC')
      DEPOSIT : ('CHK','SAV','MMA','CD','IRA')
  anything else contributes to neither (NEITHER).

  Parity is per-value, not aggregate (the playbook's LOC worked example): a mapping
  can produce a correct grand total while an individual branch is wrong. This control
  reads the model's ACTUAL classification — the classify_interest_bucket macro that
  the income model is built from — for every account type present in the source, and
  compares it branch-by-branch to the SAS mapping transcribed from the source file.

  If the model's mapping ever diverges (a type moved between lists, dropped to the
  catch-all, etc.), this control fails with the offending type. Do NOT relax it —
  fix the macro/model to match the SAS source.

  dbt singular test convention: the test FAILS if this query returns any rows.
*/
with sas_truth as (
    select
        t.account_type,
        t.expected_bucket
    from (
        values
            ('MTG', 'LENDING'),
            ('AUTO', 'LENDING'),
            ('PERS', 'LENDING'),
            ('CC', 'LENDING'),
            ('LOC', 'LENDING'),
            ('HELC', 'LENDING'),
            ('CHK', 'DEPOSIT'),
            ('SAV', 'DEPOSIT'),
            ('MMA', 'DEPOSIT'),
            ('CD', 'DEPOSIT'),
            ('IRA', 'DEPOSIT')
    ) t (account_type, expected_bucket)
),

model_mapping as (
    select distinct
        account_type,
        {{ classify_interest_bucket('account_type') }} as model_bucket
    from {{ ref('int_account_metrics') }}
)

select
    m.account_type,
    m.model_bucket,
    coalesce(s.expected_bucket, 'NEITHER') as expected_bucket
from model_mapping m
left join sas_truth s
    on m.account_type = s.account_type
where m.model_bucket <> coalesce(s.expected_bucket, 'NEITHER')
