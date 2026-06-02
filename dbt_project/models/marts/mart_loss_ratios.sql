/*
  mart_loss_ratios.sql
  Migrated from: ts-sas-legacy-analytics/Programs/Insurance/policy_valuation.sas
                 (Step 5)

  SAS Original:
    PROC MEANS data=STG_INS.POLICY_VALUATION noprint nway;
      class POLICY_TYPE;
      var YTD_EARNED_PREMIUM TOTAL_INCURRED TOTAL_PAID
          TOTAL_RESERVE IBNR_ESTIMATE;
      output out=REPORTS.LOSS_RATIO_SUMMARY(drop=_TYPE_ _FREQ_)
        n=N_POLICIES
        sum(YTD_EARNED_PREMIUM)=TOTAL_EARNED
        sum(TOTAL_INCURRED)=TOTAL_INCURRED
        sum(TOTAL_PAID)=TOTAL_PAID
        sum(TOTAL_RESERVE)=TOTAL_RESERVES
        sum(IBNR_ESTIMATE)=TOTAL_IBNR;
    run;
    Followed by a DATA step adding AGG_LOSS_RATIO and AGG_COMBINED_RATIO.

  dbt Equivalent:
    SQL GROUP BY policy_type replaces the PROC MEANS `class` statement.
    SAS n=        -> count(*).
    SAS sum(...)  -> sum(...).
    Aggregate loss / combined ratios computed inline.

  FLAGGED, source-faithful business quirk (reproduced, NOT endorsed):
    AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30  -- hard-coded 30% expense load
    (SAS line 188), identical to the per-policy combined ratio quirk.
*/

select
    policy_type,
    count(*) as n_policies,
    sum(ytd_earned_premium) as total_earned,
    sum(total_incurred) as total_incurred,
    sum(total_paid) as total_paid,
    sum(total_reserve) as total_reserves,
    sum(ibnr_estimate) as total_ibnr,

    -- SAS (line 187): AGG_LOSS_RATIO = TOTAL_INCURRED / TOTAL_EARNED.
    case
        when sum(ytd_earned_premium) > 0
            then sum(total_incurred) / sum(ytd_earned_premium)
        else null
    end as agg_loss_ratio,

    -- SAS (line 188): AGG_COMBINED_RATIO = AGG_LOSS_RATIO + 0.30.
    -- Hard-coded 30% expense load reproduced exactly from the source.
    case
        when sum(ytd_earned_premium) > 0
            then sum(total_incurred) / sum(ytd_earned_premium) + 0.30
        else null
    end as agg_combined_ratio

from {{ ref('int_policy_valuation') }}
group by policy_type
