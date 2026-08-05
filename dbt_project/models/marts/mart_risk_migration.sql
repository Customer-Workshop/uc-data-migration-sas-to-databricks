/*
  mart_risk_migration.sql
  Migrated from: Programs/Banking/credit_risk_scoring.sas (Step 3)

  Conversion decision / source-data divergence:
    SAS compares numeric CUST_ACCOUNTS_DAILY.RISK_RATING (1–7) to
    NEW_RISK_RATING. The migrated raw estate carries only categorical
    cust_demographics.risk_rating (LOW/MEDIUM/HIGH), surfaced through
    int_account_metrics. Therefore this model compares a common three-band
    ordinal scale: previous LOW=1, MEDIUM=2, HIGH=3; current numeric ratings
    1–2=LOW, 3–5=MEDIUM, and 6–7=HIGH. These band cutoffs are a conversion
    decision required by the source-data difference, are not present in SAS,
    and are flagged for business confirmation.

  Source-faithful CASE:
    previous rating null -> NEW; current rank < previous rank -> UPGRADE;
    current rank > previous rank -> DOWNGRADE; else -> STABLE.
    The SAS WHERE (previous <> new or previous is null) is preserved, making
    STABLE unreachable dead code in the source rather than removing it.
*/
with scored as (
    select *
    from {{ ref('mart_risk_scores') }}
    where score_date = current_date()
),

with_previous as (
    select
        s.score_date,
        s.account_id,
        a.risk_rating as prev_rating,
        case upper(a.risk_rating)
            when 'LOW' then 1
            when 'MEDIUM' then 2
            when 'HIGH' then 3
        end as prev_rank,
        s.risk_rating as curr_rating,
        case
            when s.risk_rating in (1, 2) then 1
            when s.risk_rating in (3, 4, 5) then 2
            when s.risk_rating in (6, 7) then 3
        end as curr_rank,
        s.pd,
        s.expected_loss
    from scored s
    inner join {{ ref('int_account_metrics') }} a
        on s.account_id = a.account_id
        and a.snapshot_date = current_date()
)

select
    score_date,
    account_id,
    prev_rating,
    case prev_rank
        when 1 then 'LOW'
        when 2 then 'MEDIUM'
        when 3 then 'HIGH'
    end as prev_rating_band,
    curr_rating,
    case curr_rank
        when 1 then 'LOW'
        when 2 then 'MEDIUM'
        when 3 then 'HIGH'
    end as curr_rating_band,
    case
        when prev_rating is null then 'NEW'
        when curr_rank < prev_rank then 'UPGRADE'
        when curr_rank > prev_rank then 'DOWNGRADE'
        else 'STABLE'
    end as migration_direction,
    pd,
    expected_loss
from with_previous
where prev_rating is null
   or prev_rank <> curr_rank
