/*
  mart_risk_summary.sql
  Migrated from: Programs/Banking/credit_risk_scoring.sas (Step 5)
  Output contract: REPORTS.RISK_SUMMARY (replaced each run)

  SAS Original:
    proc means data=WORK.SCORED noprint nway;
      class ACCOUNT_TYPE NEW_RISK_RATING;
      var PD LGD EAD EXPECTED_LOSS;
      output out=REPORTS.RISK_SUMMARY(drop=_TYPE_ _FREQ_)
        n=N_ACCOUNTS mean(PD)=AVG_PD mean(LGD)=AVG_LGD
        sum(EAD)=TOTAL_EAD sum(EXPECTED_LOSS)=TOTAL_EL;
    run;

  SAS quirk preserved:
    `n=N_ACCOUNTS` names only the first analysis variable's N. PROC MEANS then
    emits the N of the remaining VAR variables under their own names, so the
    real output has columns LGD, EAD and EXPECTED_LOSS that hold *counts*, not
    amounts. The golden export confirms this; the columns are reproduced so the
    contract (and any downstream reader) is unchanged.
    PROC MEANS N counts non-missing values -> count(<col>).
*/

select
    account_type,
    new_risk_rating,
    count(pd) as n_accounts,
    count(lgd) as lgd,
    count(ead) as ead,
    count(expected_loss) as expected_loss,
    avg(pd) as avg_pd,
    avg(lgd) as avg_lgd,
    sum(ead) as total_ead,
    sum(expected_loss) as total_el,
    {{ format_account_type('account_type') }} as account_type_desc,
    {{ format_risk_rating('new_risk_rating') }} as new_risk_rating_desc
from {{ ref('mart_risk_scores') }}
where score_date = {{ sas_run_date() }}
group by account_type, new_risk_rating
