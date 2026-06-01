/*
  mart_loss_ratios.sql
  Migrated from: Programs/Insurance/policy_valuation.sas (Step 5)

  SAS Original:
    proc means data=STG_INS.POLICY_VALUATION noprint nway;
      class POLICY_TYPE;
      var YTD_EARNED_PREMIUM TOTAL_INCURRED ...;
      output out=REPORTS.LOSS_RATIO_SUMMARY
        n=N_POLICIES sum(YTD_EARNED_PREMIUM)=TOTAL_EARNED
        sum(TOTAL_INCURRED)=TOTAL_INCURRED ...;
    /* then */ AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED (if TOTAL_EARNED > 0);
               AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30;

  dbt Equivalent:
    PROC MEANS NWAY CLASS  -> GROUP BY
    n=/sum=                -> count(*)/sum()  (SUM ignores NULLs, matching SAS
                              PROC MEANS which ignores missing in VAR sums)
    derived ratio DATA step-> SQL CASE

  FLAGGED source-parity divergence (PROC MEANS formatted CLASS grouping):
    SAS PROC MEANS groups a CLASS variable by its *formatted* value. Step 4
    applies `format POLICY_TYPE POLTYPE.`, so the SAS summary actually groups by
    the POLTYPE label, collapsing every code absent from the legacy catalog
    (here LIFE / HEALTH / COMMERCIAL, and HEALTH != HLTH) into one 'Unknown'
    bucket. We instead GROUP BY the stored POLICY_TYPE code to keep one row per
    actual line of business (the analytic intent and the dbt contract:
    policy_type is unique per LOB), and expose the faithful POLTYPE label as
    policy_type_desc. This is a deliberate, FLAGGED divergence from the PROC
    MEANS formatted grouping for a business decision - it does NOT alter mapping
    value (POLTYPE is reproduced value-for-value in format_policy_type).
*/

with valued as (
    select * from {{ ref('int_policy_valuation') }}
),

summary as (
    select
        policy_type,

        -- SAS: N_POLICIES = n
        count(*) as n_policies,

        -- SAS: TOTAL_EARNED = sum(YTD_EARNED_PREMIUM)
        sum(ytd_earned_premium) as total_earned,

        -- SAS: TOTAL_INCURRED = sum(TOTAL_INCURRED)  (NULL = no claims, ignored by SUM)
        sum(total_incurred) as total_incurred

    from valued
    group by policy_type
)

select
    policy_type,
    {{ format_policy_type('policy_type') }} as policy_type_desc,
    n_policies,
    total_earned,
    total_incurred,

    -- SAS: if TOTAL_EARNED > 0 then AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED
    case
        when total_earned > 0 then total_incurred / total_earned
    end as agg_loss_ratio,

    -- SAS: AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30
    case
        when total_earned > 0 then total_incurred / total_earned + 0.30
    end as agg_combined_ratio

from summary
