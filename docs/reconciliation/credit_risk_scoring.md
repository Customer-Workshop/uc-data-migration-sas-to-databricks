# Credit Risk Scoring — SAS to dbt Migration

This document records the conversion of
`Programs/Banking/credit_risk_scoring.sas` and the verification approach for
the dbt/Databricks implementation.

## SAS Steps to dbt Models

| SAS step | dbt model | Conversion scope |
|---|---|---|
| Steps 1 and 2 | `mart_risk_scores` | Assemble the current-date scoring population, apply WOE bins and logistic coefficients, and calculate PD, LGD, EAD, expected loss, and numeric risk rating. |
| Step 3 | `mart_risk_migration` | Compare previous categorical risk bands with the new numeric rating bands and preserve the SAS migration-direction CASE structure. |
| Step 5 | `mart_risk_summary` | Reproduce `PROC MEANS noprint nway` by grouping score date, account type, and risk rating. |

SAS Step 4 uses `PROC APPEND` to accumulate history into
`CURATED.RISK_SCORES` and `CURATED.RISK_MIGRATION`. The dbt table
materializations fully replace the current score date rather than accumulating
history.

## SAS Constructs to Databricks Constructs

| SAS construct | dbt/Databricks construct |
|---|---|
| `PROC SQL` joins and filters | dbt model CTEs with `ref()` and `source()` relations |
| SAS DATA step `IF`/`ELSE IF` branches | SQL `CASE` expressions, preserving branch order and catch-all values |
| WOE scorecard bins | Literal SQL CASE logic in the score model, with independent literal mapping tables in parity tests |
| `EXP()` logistic calculation | Databricks SQL `exp()` and the same intercept and coefficients |
| SAS missing numeric values | SQL `NULL` checks and source-faithful missing-value defaults |
| `MAX`/`MIN` LGD clamp | Databricks SQL `greatest()`/`least()` |
| `PROC MEANS ... NWAY` | SQL `GROUP BY`; `nway` is represented by emitting only full class cross-classifications |
| `PROC APPEND` history accumulation | Full-replace dbt table materialization for the current score date |
| SAS macro score date | `current_date()` for the current snapshot and score date |

## Reconciliation Control Inventory

Each dbt singular test fails if it returns rows.

| Control | What it proves |
|---|---|
| `reconcile_risk_score_completeness` | The score mart count equals the current-date in-scope lending-account count, proving no row loss or fan-out from bureau, payment, or collateral left joins. |
| `reconcile_risk_ead_control_total` | Mart current-balance and EAD sums tie to independently computed totals from `int_account_metrics`, within `0.01` for floating-point rounding. |
| `reconcile_risk_pd_parity` | Every score row matches an independently reconstructed set of SAS WOE bins, coefficients, intercept, and logistic PD within `1e-9`. |
| `reconcile_risk_lgd_ead_parity` | Every row matches the SAS secured/unsecured LGD rules and account-type-specific EAD rule within `1e-9`. |
| `reconcile_risk_rating_parity` | Every numeric risk rating matches the independent literal SAS PD-threshold table, including the catch-all rating `7`. |
| `reconcile_risk_migration_scope` | Migration rows equal exactly the scored accounts whose rating band changes or whose previous rating is null; direction is checked and zero `STABLE` rows are required. |
| `reconcile_risk_summary_totals` | Summary account counts, EAD, expected loss, and the number of account-type/rating groups tie to the scores mart. |
| `reconcile_risk_summary_grain` | The summary contains at most one row per score-date/account-type/risk-rating grain. |

The human-facing companion checks in `verify/reconcile.py` report the same
control family with actual values, including maximum PD difference and
mismatch counts.

## Source-Faithful Quirks

The following legacy behaviors are intentionally preserved rather than
corrected:

- Missing FICO maps to WOE_FICO `0.198` (the same as the 640–679 bin) while
  every other feature defaults to `0` when missing.
- A secured account with a known LTV ≤ `0.5` gets LGD `0.0` via
  `max(0,...)`, whereas a secured account with MISSING LTV gets `0.40`
  (lower risk assigned to better information).
- A secured account with a negative current balance yields a negative LTV,
  which falls into the best `ltv <= 0.60` bin.
- EAD applies a 50% credit-conversion factor to undrawn limit for CC/LOC/HELC
  only, so an over-limit balance (`credit_limit < current_balance`) produces
  EAD below the drawn balance.
- The rating CASE catch-all assigns `7` (worst) to any PD ≥ `0.30` or null PD.

## Documented Divergences

### Previous-rating band conversion

SAS compares numeric `CUST_ACCOUNTS_DAILY.RISK_RATING` values from 1 through 7
with `NEW_RISK_RATING`. The migrated raw estate carries only categorical
`cust_demographics.risk_rating` values (`LOW`, `MEDIUM`, `HIGH`), surfaced
through staging and intermediate models. `mart_risk_migration` therefore uses
a common three-band ordinal scale:

- Previous `LOW=1`, `MEDIUM=2`, `HIGH=3`.
- Current numeric ratings `1–2 → LOW`, `3–5 → MEDIUM`, `6–7 → HIGH`.

These band cutoffs are a conversion decision required by the source-data
difference. They are not present in the SAS source and require business
confirmation.

### Bureau row selection and absent SAS features

SAS selects the latest bureau row using
`b.SCORE_DATE = (select max(SCORE_DATE) ... <= &score_date)`. The migrated raw
table `banking_raw.bureau_scores` has no `score_date` and is one row per
customer, so latest-row selection is not reproducible. The score completeness
control guards against fan-out and row loss under this limitation.

The migrated raw estate does not contain
`VANTAGE_SCORE`, `BUREAU_TRADES_OPEN`, `BUREAU_UTIL_PCT`,
`BUREAU_OLDEST_TRADE_MO`, `PMT_ONTIME_12MO`, `PMT_LATE_30_12MO`,
`PMT_LATE_60_12MO`, `MONTHS_SINCE_LAST_DPD`, `AVG_PMT_RATIO_12MO`, or
`LAST_APPRAISAL_DATE`, so these features are not carried. None feed the
scorecard math, so there is no scoring impact.

## Verification Status

- `make lint`: PASS.
- `dbt parse` via `make ci`: PASS.
- `make ci`: PASS.
- `make demo-up NS=child1`: not executed because the provisioned Databricks
  token is rejected with `403 Invalid access token`.
- `make reconcile NS=child1`: not executed for the same rejected-token
  condition.
- Asset Bundle deploy: not executed.
- The reconciliation report output is not yet attached because no
  Databricks-backed build or reconciliation run was possible.
