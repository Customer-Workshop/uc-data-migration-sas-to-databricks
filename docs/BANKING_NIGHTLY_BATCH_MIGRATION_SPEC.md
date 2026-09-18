# Banking Nightly Batch — SAS → dbt + Databricks Migration Specification

Source estate: [`ts-sas-legacy-analytics`](https://github.com/Cognition-Partner-Workshops/ts-sas-legacy-analytics) (`main`, read-only).
Target: this repository — `dbt_project/` (models, macros, seeds, tests), `verify/reconcile.py`
(cross-engine parity report), `databricks.yml` + `resources/` (Asset Bundle and the scheduled
`daily_banking_pipeline` Workflow).

This document is the transcription of record for the four programs that make up the Banking
nightly batch. Every business rule is quoted from the SAS source, mapped to the dbt object that
implements it, and tied to the reconciliation control that proves it. Where the source has a
defect or an ambiguity, the SAS behaviour is preserved and the decision is recorded, not hidden.

---

## 1. Batch inventory

The batch orchestrator is `BatchJobs/run_daily_banking.sas` (Control-M `BANK_MASTER`, 05:45).
It `%include`s `autoexec.sas`, then runs the four programs in a fixed dependency chain via
`%run_step`; a failing step sets `_batch_abort=1` and every later step is skipped. All four
programs run every night even though two of them are documented for a slower cadence — the
orchestrator does not gate on the day of week or month.

| Step | Program (`Programs/Banking/`) | Control-M job / documented cadence | Inputs | Outputs |
|---|---|---|---|---|
| 1 | `load_customer_accounts.sas` | `BANK_DAILY_01`, daily 06:00 | `ORA_DW.CUST_ACCOUNTS`, `ORA_DW.CUST_DEMOGRAPHICS` (`RAW_BANK.DAILY_RATES` is listed in the header but never read) | `STG_BANK.CUST_ACCOUNTS_DAILY` (replace), `STG_BANK.ACCT_EXCEPTIONS` (insert) |
| 2 | `daily_transaction_processing.sas` | `BANK_DAILY_02`, daily 07:30, depends on step 1 | `RAW_BANK.TXN_FEED_YYYYMMDD`, `STG_BANK.CUST_ACCOUNTS_DAILY`, `CURATED.DAILY_TRANSACTIONS` (90-day history) | `CURATED.DAILY_TRANSACTIONS` (append), `CURATED.TXN_ANOMALIES` (append), `CURATED.RUNNING_BALANCES` (replace) |
| 3 | `credit_risk_scoring.sas` | `BANK_WEEKLY_01`, documented weekly Sunday 02:00 — but executed nightly by the orchestrator | `STG_BANK.CUST_ACCOUNTS_DAILY`, `ORA_DW.BUREAU_SCORES`, `ORA_DW.PAYMENT_HISTORY`, `ORA_DW.COLLATERAL` | `CURATED.RISK_SCORES` (append), `CURATED.RISK_MIGRATION` (append), `REPORTS.RISK_SUMMARY` (replace) |
| 4 | `monthly_regulatory_reporting.sas` | `BANK_MONTHLY_01`, documented monthly 3rd business day — but executed nightly by the orchestrator | `STG_BANK.CUST_ACCOUNTS_DAILY`, `ORA_DW.LOAN_DETAILS` (`CURATED.DAILY_TRANSACTIONS` and `ORA_DW.COLLATERAL` are listed in the header but never read) | `REPORTS.MONTHLY_RWA`, `REPORTS.DELINQUENCY_AGING`, `REPORTS.LLP_COVERAGE`, `REPORTS.CAPITAL_ADEQUACY` (all replace), `REG_REPORT_&report_month..xlsx` |

Each program is a single macro invoked once at the bottom of the file with the autoexec
defaults: `%load_customer_accounts(run_date=&CURR_DT)`, `%daily_transaction_processing(txn_date=&CURR_DT)`,
`%credit_risk_scoring(score_date=&CURR_DT)`, `%monthly_regulatory_reporting(report_month=&PREV_YM)`.

### 1.1 Migration map (SAS output → dbt model → parity control)

| SAS output | dbt model (`dbt_project/models/`) | Materialisation | Golden parity test (`dbt_project/tests/`) |
|---|---|---|---|
| `WORK.ACCT_RAW` | `staging/stg_cust_accounts` | view | (population control: `reconcile_account_completeness`) |
| `STG_BANK.CUST_ACCOUNTS_DAILY` | `intermediate/int_account_metrics` | table | `reconcile_golden_cust_accounts_daily` |
| `STG_BANK.ACCT_EXCEPTIONS` | `intermediate/int_acct_exceptions` | table | `reconcile_acct_exception_branches` |
| `WORK.TXN_VALIDATED` / `WORK.TXN_REJECTED` | `staging/stg_daily_transactions` / `staging/stg_txn_rejected` | view | `reconcile_txn_completeness`, `reconcile_txn_reject_reasons` |
| `WORK.TXN_WITH_BALANCE` | `intermediate/int_txn_enriched` | table | `reconcile_running_balance_chain` |
| `CURATED.DAILY_TRANSACTIONS` (appended slice, all columns) | `marts/mart_daily_transactions` | incremental merge on `TRANSACTION_ID` | `reconcile_golden_daily_transactions` |
| `CURATED.DAILY_TRANSACTIONS` (permanent-table contract) | `marts/mart_daily_transactions_curated` | table (history ∪ today) | `reconcile_golden_daily_transactions` |
| `CURATED.TXN_ANOMALIES` | `marts/mart_transaction_anomalies` | incremental merge | `reconcile_golden_txn_anomalies`, `reconcile_anomaly_precedence` |
| `CURATED.RUNNING_BALANCES` | `marts/mart_running_balances` | table (replace) | `reconcile_golden_running_balances` |
| `CURATED.RISK_SCORES` | `marts/mart_risk_scores` | incremental merge on (`ACCOUNT_ID`,`SCORE_DATE`) | `reconcile_golden_risk_scores`, `reconcile_risk_score_bands` |
| `CURATED.RISK_MIGRATION` | `marts/mart_risk_migration` | incremental merge on (`ACCOUNT_ID`,`SCORE_DATE`) | `reconcile_golden_risk_migration`, `reconcile_risk_migration_direction` |
| `REPORTS.RISK_SUMMARY` | `marts/mart_risk_summary` | table | `reconcile_golden_risk_summary` |
| `REPORTS.MONTHLY_RWA` | `marts/mart_regulatory_rwa` | table | `reconcile_golden_monthly_rwa`, `reconcile_rwa_controls` |
| `REPORTS.DELINQUENCY_AGING` | `marts/mart_delinquency_aging` | table | `reconcile_golden_delinquency_aging`, `reconcile_delinquency_controls` |
| `REPORTS.LLP_COVERAGE` | `marts/mart_llp_coverage` | table | `reconcile_golden_llp_coverage`, `reconcile_llp_controls` |
| `REPORTS.CAPITAL_ADEQUACY` | `marts/mart_capital_adequacy` | table | `reconcile_golden_capital_adequacy` |
| `ORA_DW.LOAN_DETAILS` (read) | `staging/stg_loan_details` | view | — |
| `BANKING.FORMATS` catalog | `macros/format_*.sql` + seed `sas_formats/sas_format_catalog.csv` | macro / seed | `reconcile_format_mappings` |

Column names are preserved in lower case (Databricks identifiers are case-insensitive; dbt
conventions in this repo are lower-case). Numeric SAS formats (`dollar18.2`, `dollar20.2`,
`8.2`, `8.4`, `percent8.4`) are display formats only — they do not round stored values — so
the models store the unformatted number and the parity tests compare with a `0.005` absolute
tolerance (`approx_equal`) or a relative tolerance for money totals (`approx_equal_rel`).

---

## 2. Shared dependencies

### 2.1 `Config/autoexec.sas`

Libraries (all relevant to Banking):

| Libref | Path / engine | Access | Target equivalent |
|---|---|---|---|
| `RAW_BANK` | `/data/sas/raw/banking` | readonly | source `banking_raw` (`${raw_catalog}.${raw_schema}`) |
| `STG_BANK` | `/data/sas/staging/banking` | rw | `<ns>_intermediate` (`int_account_metrics`, `int_acct_exceptions`) |
| `CURATED` | `/data/sas/curated` | rw | `<ns>_marts` (`mart_daily_transactions*`, `mart_transaction_anomalies`, `mart_running_balances`, `mart_risk_scores`, `mart_risk_migration`) |
| `REPORTS` | `/data/sas/reports` | rw | `<ns>_marts` (`mart_risk_summary`, `mart_regulatory_rwa`, `mart_delinquency_aging`, `mart_llp_coverage`, `mart_capital_adequacy`) |
| `ARCHIVE` | `/data/sas/archive` | rw | not migrated (`ARCHIVE.BATCH_HISTORY` is the orchestrator's control table; Databricks Workflows run history replaces it) |
| `ORA_DW` | Oracle `FINPROD`, schema `DW_BANKING` | readonly | source `banking_raw` tables `cust_accounts`, `cust_demographics`, `loan_details`, `bureau_scores`, `payment_history`, `collateral` |
| `BANKING`, `INSURANCE`, `COMMON` | format catalogs, `fmtsearch=(BANKING INSURANCE COMMON WORK LIBRARY)` | — | `format_*` macros + `sas_format_catalog` seed |

Global macro variables used by the batch:

| Variable | Definition | Target parameter |
|---|---|---|
| `&CURR_DT` | `%sysfunc(today(), date9.)` | dbt var `curr_dt` (ISO `YYYY-MM-DD`), job parameter `curr_dt`; `sas_run_date()` |
| `&PREV_YM` | `%sysfunc(intnx(month, today(), -1), yymmn6.)` | dbt var `report_month` (`YYYYMM`); blank → derived from `curr_dt` by `sas_report_month()` |
| `&REPORT_PATH` | `/data/sas/reports/output` | UC volume `${catalog}.reports.parity_reports` (bundle variable `parity_report_dir`) |
| `&EMAIL_ONCALL`, `&EMAIL_DL` | notification lists | Databricks job `email_notifications` / not migrated (see §7) |
| `&ENVIRONMENT`, `&CURR_YM`, `&FY_START`, `&MAX_OBS_WARN`, `&ABORT_ON_ERR` | set, not read by Banking programs | — |

System options that matter for translation: `validvarname=v7` (upper-case names, ≤32 chars),
`nofmterr`, `yearcutoff=1920`, `noerrorabend` (a failed step does not stop the SAS session — the
orchestrator's `%run_step` handles abort).

### 2.2 Macros

| Macro | Used by | Behaviour | Target |
|---|---|---|---|
| `Macro/parmv.sas` | all four | Parameter validation (`_req=1` → fail if empty; `_val=` list check). `region` in program 1 must be one of `ALL NE SE MW SW W NW`. | Compile-time: dbt `var()` defaults; runtime: job parameters. No runtime equivalent needed — the models fail to compile without `curr_dt`. |
| `Macro/nobs.sas` | all four | Row count for logging and for the "abort when 0 rows" / "insert exceptions when > 0" branches. | Row-count controls in `verify/reconcile.py`; `reconcile_*_completeness.sql`. |
| `Macro/lock.sas` | programs 1–3 (included), used in 2 and 3 | Table lock around `PROC APPEND`. | Delta ACID; not needed. |
| `Macro/sendmail.sas` | program 1 (only when exceptions > 100), orchestrator | Email. | Job `email_notifications` (bundle). |
| `Macro/export_xlsx.sas` → `Macro/export_dbms.sas` | program 4 | `%export_xlsx(DATA=, PATH=, REPLACE=N, LABEL=N)` wraps `%export_dbms(dbms=xlsx)` which runs `proc export data=&data outfile=&path dbms=xlsx replace`. There is **no `SHEET=` parameter and no `sheet=` statement** in either macro. | See §6.5 — the source call is defective. |

### 2.3 `Formats/banking_formats.sas` (catalog `BANKING.FORMATS`)

Exact value maps. Formats used by the Banking programs are marked ●; the others are in the
catalog but not referenced by the batch and are carried in the seed for completeness.

| Format | Kind | Map | Target macro |
|---|---|---|---|
| ● `$ACCTTYPE` | char | CHK=Checking, SAV=Savings, MMA=Money Market, CD=Certificate of Deposit, IRA=Individual Retirement, LOC=Line of Credit, MTG=Mortgage, AUTO=Auto Loan, PERS=Personal Loan, CC=Credit Card, HELC=Home Equity LOC, OTHER=Unknown | `format_account_type()` |
| ● `$ACCTSTAT` | char | A=Active, C=Closed, D=Dormant, F=Frozen, R=Restricted, S=Suspended, P=Pending, W=Written Off, OTHER=Unknown | `format_account_status()` |
| ● `RISKRATE` | numeric | 1=Minimal Risk, 2=Low Risk, 3=Moderate Risk, 4=Elevated Risk, 5=High Risk, 6=Very High Risk, 7=Loss Expected, OTHER=Not Rated | `format_risk_rating()` |
| `$TXNCAT` | char | DEP=Deposit, WDR=Withdrawal, TRF=Transfer, PMT=Payment, FEE=Fee, INT=Interest, ADJ=Adjustment, REV=Reversal, CHG=Charge, REF=Refund, OTHER=Other | `format_txn_category()` |
| `DELQBKT` | numeric range | 0=Current, 1-29=1-29 Days, 30-59=30-59 Days, 60-89=60-89 Days, 90-119=90-119 Days, 120-179=120-179 Days, 180-HIGH=180+ Days (no OTHER) | `format_delinq_bucket()` — note program 4 does **not** use this format; it hard-codes its own bucket labels (§6.3) |
| ● `$REGION` | char | NE=Northeast, SE=Southeast, MW=Midwest, SW=Southwest, W=West, NW=Northwest, HQ=Headquarters, OTHER=Unknown | `format_region()` |
| ● `$CUSTSEG` | char | RET=Retail, PREM=Premium, PB=Private Banking, SMB=Small Business, COMM=Commercial, CORP=Corporate, OTHER=Unclassified | `format_customer_segment()` |
| `BALRANGE`, `$LNPURP` | numeric range / char | not used by the batch | seed only |

In SAS these are *display* formats attached with `format ... $ACCTTYPE.` — the stored value stays
the code. The models therefore keep the code columns and expose the decoded label only where a
macro is applied; `reconcile_format_mappings` proves every macro branch against the seed
`sas_format_catalog` value-for-value.

---

## 3. Program 1 — `load_customer_accounts.sas`

Macro `%load_customer_accounts(run_date=&CURR_DT, region=ALL)`.

### 3.1 Data structures

Inputs: `ORA_DW.CUST_ACCOUNTS a` (ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS,
OPEN_DATE, CLOSE_DATE, CURRENT_BALANCE, AVAILABLE_BALANCE, CREDIT_LIMIT, INTEREST_RATE,
BRANCH_ID, OFFICER_ID, LAST_ACTIVITY_DATE) inner-joined to `ORA_DW.CUST_DEMOGRAPHICS d`
(FIRST_NAME, LAST_NAME, SSN_HASH, DATE_OF_BIRTH, CUSTOMER_SEGMENT, RISK_RATING, REGION_CODE,
PRIMARY_EMAIL, PHONE_NUMBER) on `CUSTOMER_ID`.

Outputs:

- `STG_BANK.CUST_ACCOUNTS_DAILY` — replaced nightly (`data STG_BANK.CUST_ACCOUNTS_DAILY`). All
  22 extracted columns + derived `ACCT_AGE_MONTHS`, `DAYS_INACTIVE`, `UTILIZATION_PCT`,
  `DORMANCY_FLAG $1`, `HIGH_BALANCE_FLAG $1`, `SNAPSHOT_DATE`, `LOAD_TIMESTAMP`.
  Formats: `ACCOUNT_TYPE $ACCTTYPE.`, `ACCOUNT_STATUS $ACCTSTAT.`, `RISK_RATING RISKRATE.`,
  `CUSTOMER_SEGMENT $CUSTSEG.`, `REGION_CODE $REGION.`, balances `dollar18.2`, dates `date9.`,
  `LOAD_TIMESTAMP datetime20.`. Key: `ACCOUNT_ID` (one row per account).
- `STG_BANK.ACCT_EXCEPTIONS` — appended via `proc sql; insert into ... select * from WORK.ACCT_EXCEPTIONS`
  only when `%nobs(WORK.ACCT_EXCEPTIONS) > 0`. Same columns as the snapshot plus
  `EXCEPTION_CODE $10`, `EXCEPTION_DESC $200`. The `drop EXCEPTION_CODE EXCEPTION_DESC` applies
  to both datasets in the DATA statement, so the SAS exceptions table has *no* code column —
  the code is only recoverable from `EXCEPTION_DESC`. Key: (`ACCOUNT_ID`, exception branch);
  one account can produce up to three rows.

`WORK.ACCT_SUMMARY` (PROC MEANS by ACCOUNT_TYPE × REGION_CODE) is computed for the log and
deleted at `%EXIT`; it is not an output and is not migrated.

### 3.2 Business rules (exact)

```
where a.ACCOUNT_STATUS not in ('W', 'C')
  and a.OPEN_DATE <= "&run_date"d
  [and d.REGION_CODE = "&region"  -- only when region ne ALL; default ALL]

ACCT_AGE_MONTHS = intck('month', OPEN_DATE, "&run_date"d)   -- calendar-month boundaries crossed
DAYS_INACTIVE   = "&run_date"d - LAST_ACTIVITY_DATE
UTILIZATION_PCT = (CURRENT_BALANCE / CREDIT_LIMIT) * 100  if ACCOUNT_TYPE in ('CC','LOC','HELC') and CREDIT_LIMIT > 0, else .
DORMANCY_FLAG   = 'Y' if DAYS_INACTIVE > 365 and ACCOUNT_STATUS = 'A', else 'N'
HIGH_BALANCE_FLAG = 'Y' if CURRENT_BALANCE >= 250000, else 'N'
SNAPSHOT_DATE   = "&run_date"d
LOAD_TIMESTAMP  = datetime()
```

Exception branches — three **independent** `if` blocks (not `else if`), each does its own
`output WORK.ACCT_EXCEPTIONS`:

| Code | Condition | `EXCEPTION_DESC` |
|---|---|---|
| `NEG_BAL` | `ACCOUNT_TYPE in ('CHK','SAV','MMA','CD') and CURRENT_BALANCE < 0` | `catx(' ', 'Negative balance', put(CURRENT_BALANCE, dollar18.2), 'on deposit account', ACCOUNT_ID)` |
| `HIGH_UTIL` | `UTILIZATION_PCT > 95` (missing compares false) | `catx(' ', 'Utilization at', put(UTILIZATION_PCT, 5.1), '%', 'for account', ACCOUNT_ID)` |
| `NO_RISK` | `RISK_RATING = .` | `catx(' ', 'Missing risk rating for customer', CUSTOMER_ID)` |

Control flow: `%goto EXIT` when the raw extract has 0 rows (nothing written); email when
exceptions > 100.

### 3.3 Translation notes

- `intck('month', …)` → `sas_intck_month()` = `(year(to)−year(from))*12 + (month(to)−month(from))`
  — SAS counts month *boundaries* crossed, not elapsed 30-day periods, so `months_between()`
  would be wrong.
- `int_acct_exceptions` is a `UNION ALL` of the three branches (first implementation used a
  single CASE — wrong, it collapsed multi-exception accounts). The dbt model keeps
  `EXCEPTION_CODE` as a real column because the SAS `drop` is an obvious source defect.
  Golden parity for this table is a per-account multiset comparison (row count per
  `ACCOUNT_ID`, in `verify/reconcile.py`) plus `reconcile_acct_exception_branches`, which
  recomputes every branch; `EXCEPTION_DESC` is *not* compared byte-for-byte because the SAS
  `put(..., dollar18.2)` / `put(..., 5.1)` picture formats are only approximated with
  `format_number()`.

---

## 4. Program 2 — `daily_transaction_processing.sas`

Macro `%daily_transaction_processing(txn_date=&CURR_DT)`; `txn_ds = TXN_FEED_<yymmddn8>`.

### 4.1 Data structures

Inputs: `RAW_BANK.TXN_FEED_YYYYMMDD` (TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_DATE,
TRANSACTION_TYPE, TRANSACTION_AMOUNT, + descriptive feed columns), `STG_BANK.CUST_ACCOUNTS_DAILY`
(left join for enrichment), `CURATED.DAILY_TRANSACTIONS` (90-day statistics, read *before* the
append).

Outputs:

| Table | Write mode | Columns / key |
|---|---|---|
| `CURATED.DAILY_TRANSACTIONS` | `proc append … force` | Base table has the feed columns only; FORCE drops the enrichment columns with a WARNING. Key `TRANSACTION_ID`. |
| `CURATED.TXN_ANOMALIES` | `proc append … force`, only if anomalies > 0 | `WORK.TXN_WITH_BALANCE.*`, `AVG_TXN_AMT`, `STD_TXN_AMT`, `Z_SCORE`, `ANOMALY_TYPE $20`. |
| `CURATED.RUNNING_BALANCES` | DATA step — **replace** | `ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID, RUNNING_BALANCE` only. |

Rejected rows (`WORK.TXN_REJECTED`) are counted, never persisted.

### 4.2 Business rules (exact)

Validation — first failing rule wins (`return` after each `output WORK.TXN_REJECTED`):

1. `missing(TRANSACTION_ID)` → `Missing TRANSACTION_ID`
2. `missing(ACCOUNT_ID)` → `Missing ACCOUNT_ID`
3. `missing(TRANSACTION_AMOUNT)` → `Missing TRANSACTION_AMOUNT`
4. `abs(TRANSACTION_AMOUNT) > 10000000` → `Amount exceeds threshold: <dollar18.2>`
5. `TRANSACTION_TYPE not in ('DEP','WDR','TRF','PMT','FEE','INT','ADJ','REV','CHG','REF')` → `Invalid transaction type: <type>`
6. `TRANSACTION_DATE > "&txn_date"d` → `Future dated: <date9.>`

Enrichment (`left join STG_BANK.CUST_ACCOUNTS_DAILY a on ACCOUNT_ID`, ordered by
`ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_ID`): adds ACCOUNT_TYPE, CUSTOMER_ID, CUSTOMER_SEGMENT,
REGION_CODE, BRANCH_ID, `PRE_TXN_BALANCE = a.CURRENT_BALANCE`, RISK_RATING and

```
POST_TXN_BALANCE = case
  when TRANSACTION_TYPE in ('DEP','INT','REF','REV') then a.CURRENT_BALANCE + TRANSACTION_AMOUNT
  when TRANSACTION_TYPE in ('WDR','PMT','FEE','CHG') then a.CURRENT_BALANCE - abs(TRANSACTION_AMOUNT)
  when TRANSACTION_TYPE in ('TRF','ADJ')             then a.CURRENT_BALANCE + TRANSACTION_AMOUNT
  else a.CURRENT_BALANCE end
```

`POST_TXN_BALANCE` is *per transaction from the snapshot balance* (not cumulative); orphan
accounts (no snapshot row) get null balances.

Running balance (`retain RUNNING_BALANCE; by ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID`):

```
if first.ACCOUNT_ID then RUNNING_BALANCE = PRE_TXN_BALANCE;
RUNNING_BALANCE = RUNNING_BALANCE ± effect   -- same three-way effect as POST_TXN_BALANCE
```

Anomaly statistics (`WORK.TXN_STATS`) — from `CURATED.DAILY_TRANSACTIONS` **before** today's
append, `where TRANSACTION_DATE >= intnx('day', "&txn_date"d, -90)`, per ACCOUNT_ID:
`AVG_TXN_AMT = mean(abs(amt))`, `STD_TXN_AMT = std(abs(amt))` (sample std, n−1), `TXN_COUNT`.

Anomaly classification — first match wins, `having ANOMALY_TYPE ne ''`:

```
Z_SCORE = (abs(amt) - AVG_TXN_AMT) / STD_TXN_AMT   when STD_TXN_AMT > 0 else .
1. Z_SCORE > 3                                              → 'HIGH_AMOUNT'
2. RUNNING_BALANCE < 0                                      → 'OVERDRAFT'
3. TRANSACTION_TYPE = 'WDR' and abs(amt) > PRE_TXN_BALANCE * 0.9 → 'LARGE_WITHDRAWAL'
4. missing(CUSTOMER_ID)                                     → 'ORPHAN_ACCOUNT'
```

### 4.3 Translation notes

- The `RETAIN`/`first.` accumulation is `sum(effect) over (partition by account_id order by
  transaction_date, transaction_id rows unbounded preceding) + pre_txn_balance`
  (`int_txn_enriched`). `reconcile_running_balance_chain` proves `running_balance(n) =
  running_balance(n−1) + effect(n)` and the first row equals `PRE_TXN_BALANCE + effect`.
- `std()` → Spark `stddev_samp`. Null propagation matches SAS missing arithmetic.
- Two models carry the one SAS table: `mart_daily_transactions` (the enriched slice the
  downstream models need, merged on `TRANSACTION_ID` so a same-day rerun is idempotent — the SAS
  append would duplicate) and `mart_daily_transactions_curated` (the feed-shape permanent table
  = source `curated_daily_transactions_history` ∪ today). Only the latter is what a SAS user
  would see as `CURATED.DAILY_TRANSACTIONS`.
- `stg_txn_rejected` is kept as a view so `reconcile_txn_completeness` can prove
  `feed rows = validated + rejected` and `reconcile_txn_reject_reasons` can prove the reason
  precedence. `REJECT_REASON` carries the SAS message text without the formatted value suffix
  (`put(..., dollar18.2)` / `put(..., date9.)`); the rows never reach a permanent SAS table, so
  there is no golden to compare the text against.

---

## 5. Program 3 — `credit_risk_scoring.sas`

Macro `%credit_risk_scoring(score_date=&CURR_DT, model_id=CRM-2023-Q4-v2)`.

### 5.1 Data structures

Input `WORK.SCORE_INPUT`: `STG_BANK.CUST_ACCOUNTS_DAILY a` where `SNAPSHOT_DATE = "&score_date"d
and ACCOUNT_TYPE in ('MTG','AUTO','PERS','CC','LOC','HELC')`, left-joined to

- `ORA_DW.BUREAU_SCORES b` on CUSTOMER_ID **and** `b.SCORE_DATE = (select max(SCORE_DATE) from
  BUREAU_SCORES where CUSTOMER_ID = b.CUSTOMER_ID and SCORE_DATE <= "&score_date"d)` — latest
  score on or before the score date; ties on that date fan out;
- `ORA_DW.PAYMENT_HISTORY p` on ACCOUNT_ID;
- `ORA_DW.COLLATERAL c` on ACCOUNT_ID, `LTV = CURRENT_BALANCE / COLLATERAL_VALUE when
  COLLATERAL_VALUE > 0 else .` (`format=8.4`).

Outputs:

| Table | Write mode | Columns |
|---|---|---|
| `CURATED.RISK_SCORES` | append force | SCORE_INPUT columns + `PD percent8.4`, `LGD percent8.4`, `EAD dollar18.2`, `EXPECTED_LOSS dollar18.2`, `NEW_RISK_RATING`, `SCORE_DATE date9.`, `MODEL_ID`, `SCORE_TIMESTAMP datetime20.`; `INTERCEPT WOE_* LOG_ODDS` dropped. Key (`ACCOUNT_ID`,`SCORE_DATE`). |
| `CURATED.RISK_MIGRATION` | append force | `SCORE_DATE, ACCOUNT_ID, PREV_RATING, CURR_RATING, MIGRATION_DIRECTION $10, PD, EXPECTED_LOSS` |
| `REPORTS.RISK_SUMMARY` | PROC MEANS `nway` — replace | class `ACCOUNT_TYPE NEW_RISK_RATING`; `N_ACCOUNTS=n`, `AVG_PD`, `AVG_LGD`, `TOTAL_EAD`, `TOTAL_EL` |

### 5.2 Business rules (exact)

Scorecard `CRM-2023-Q4-v2`, `INTERCEPT = -3.2145`:

| WOE | Bins (first match) | Missing |
|---|---|---|
| `WOE_FICO` | ≥760 −1.204; ≥720 −0.812; ≥680 −0.356; ≥640 0.198; ≥600 0.654; else 1.102 | 0.198 (population average) |
| `WOE_UTIL` | ≤10 −0.956; ≤30 −0.521; ≤50 −0.102; ≤70 0.334; ≤90 0.789; else 1.245 | 0 |
| `WOE_DPD` (PMT_LATE_90_12MO) | =0 −0.678; =1 0.445; else 1.567 | 0 |
| `WOE_AGE` (ACCT_AGE_MONTHS) | ≥120 −0.534; ≥60 −0.289; ≥24 0.045; else 0.456 | 0 |
| `WOE_LTV` — only `ACCOUNT_TYPE in ('MTG','AUTO','HELC')` | ≤0.60 −0.712; ≤0.80 −0.234; ≤1.00 0.356; else 0.889 | 0 (and 0 for unsecured) |

```
LOG_ODDS = INTERCEPT + 0.412*WOE_FICO + 0.198*WOE_UTIL + 0.289*WOE_DPD + 0.067*WOE_AGE + 0.134*WOE_LTV
PD  = 1 / (1 + exp(-LOG_ODDS))
LGD = secured (MTG/AUTO/HELC): LTV not missing → max(0, min(1, (LTV - 0.5) * 0.8)); LTV missing → 0.40
      CC → 0.75; else → 0.50
EAD = ACCOUNT_TYPE in ('CC','LOC','HELC') → CURRENT_BALANCE + 0.50 * (CREDIT_LIMIT - CURRENT_BALANCE); else CURRENT_BALANCE
EXPECTED_LOSS = PD * LGD * EAD
NEW_RISK_RATING = PD < 0.005 → 1; < 0.01 → 2; < 0.03 → 3; < 0.07 → 4; < 0.15 → 5; < 0.30 → 6; else 7
```

Migration (`WORK.SCORED s inner join STG_BANK.CUST_ACCOUNTS_DAILY a on ACCOUNT_ID`, same
snapshot date, `where a.RISK_RATING ne s.NEW_RISK_RATING or a.RISK_RATING is null`):

```
MIGRATION_DIRECTION = a.RISK_RATING is null → 'NEW'; NEW < prior → 'UPGRADE'; NEW > prior → 'DOWNGRADE'; else 'STABLE'
```

(`STABLE` is unreachable given the WHERE clause; the model keeps the branch verbatim.)

### 5.3 Translation notes

- Correlated sub-select → `max(score_date) over (partition by customer_id)` on the
  date-filtered bureau table, joined on equality (preserves the tie fan-out).
- `percent8.4` on PD/LGD is display-only (SAS stores 0.0123, shows `1.2300%`); stored values
  are compared at 1e-6 relative.
- The nightly re-execution of a "weekly" program is preserved: `mart_risk_scores` /
  `mart_risk_migration` are incremental on (`account_id`, `score_date`) so re-running the same
  night is idempotent, and each distinct night appends, as PROC APPEND did.

---

## 6. Program 4 — `monthly_regulatory_reporting.sas`

Macro `%monthly_regulatory_reporting(report_month=&PREV_YM)`.

### 6.1 Period setup

```
month_start = %sysfunc(inputn(&report_month.01, yymmdd8.), date9.)
month_end   = %sysfunc(intnx(month, "&month_start"d, 0, E), date9.)
rpt_label   = %substr(&report_month,1,4)-%substr(&report_month,5,2)     -- YYYY-MM
default report_month = &PREV_YM = intnx(month, today(), -1) formatted yymmn6.
```

All four queries filter `a.SNAPSHOT_DATE = "&month_end"d`. Because program 1 *replaces*
`STG_BANK.CUST_ACCOUNTS_DAILY` every night with `SNAPSHOT_DATE = &CURR_DT`, the month-end
snapshot only exists on the night `CURR_DT = month_end`. On any other night the three report
tables are rebuilt **empty** and `CAPITAL_ADEQUACY` has one row with `TOTAL_RWA` missing, the
ratios missing and every status `FAIL` (`sum()` of no rows is missing; `. = 0` is false,
`./.` is missing, `missing >= 4.5` is false → the `else 'FAIL'` branch — see §6.4). This is the
source behaviour and is preserved unchanged; Spark null semantics give the same result. The
golden run (`CURR_DT = 31JAN2024`, `report_month = 202401`) exercises the populated path.

Target: `sas_report_month()` returns `var('report_month')` or, when blank, the month before
`curr_dt` (deterministic — SAS derives `&PREV_YM` from `today()`, which would make the golden
comparison date-dependent). `sas_month_start()` / `sas_month_end()` give `to_date(report_month||'01')`
and `last_day(...)`.

### 6.2 `REPORTS.MONTHLY_RWA` — Basel III standardized risk weights

`STG_BANK.CUST_ACCOUNTS_DAILY a left join ORA_DW.LOAN_DETAILS l on ACCOUNT_ID`,
`where a.SNAPSHOT_DATE = month_end`, `group by 1,2,3,4`, `order by ACCOUNT_TYPE, CUSTOMER_SEGMENT`.

| Column | Definition | Format |
|---|---|---|
| `REPORT_MONTH` | `"&report_month"` | `$6` |
| `ACCOUNT_TYPE`, `CUSTOMER_SEGMENT` | group keys | |
| `RISK_WEIGHT` | `case when ACCOUNT_TYPE in ('CHK','SAV','MMA') then 0.00 when 'CD' then 0.00 when 'MTG' and LTV <= 0.80 then 0.35 when 'MTG' and LTV > 0.80 then 0.50 when 'HELC' then 0.50 when in ('AUTO','PERS') then 0.75 when 'CC' then 0.75 when 'LOC' then 1.00 else 1.00 end` | |
| `N_ACCOUNTS` | `count(*)` | |
| `TOTAL_EXPOSURE` | `sum(CURRENT_BALANCE)` | `dollar20.2` |
| `RWA` | `sum(CURRENT_BALANCE * calculated RISK_WEIGHT)` | `dollar20.2` |

Source quirks preserved: a `MTG` row with **missing** `LTV` (no `LOAN_DETAILS` row, or
`LTV` null) satisfies neither MTG branch and falls to `else 1.00`; every non-lending type not
listed (e.g. `IRA`) also gets `1.00`. `LTV` here is the `LOAN_DETAILS.LTV` column, *not* the
collateral-derived LTV of program 3. `RISK_WEIGHT` is a group key, so one
(`ACCOUNT_TYPE`,`CUSTOMER_SEGMENT`) can appear twice for MTG (0.35 and 0.50).

### 6.3 `REPORTS.DELINQUENCY_AGING`

Same join, plus `and a.ACCOUNT_TYPE in ('MTG','AUTO','PERS','CC','LOC','HELC')`, `group by 1,2,3,4`.

```
DELINQ_BUCKET (length 10) = case
  when DAYS_PAST_DUE = 0                 then 'Current'
  when DAYS_PAST_DUE between 1 and 29    then '1-29'
  when DAYS_PAST_DUE between 30 and 59   then '30-59'
  when DAYS_PAST_DUE between 60 and 89   then '60-89'
  when DAYS_PAST_DUE between 90 and 119  then '90-119'
  when DAYS_PAST_DUE between 120 and 179 then '120-179'
  when DAYS_PAST_DUE >= 180              then '180+'
  else 'Unknown' end            -- reached for missing DAYS_PAST_DUE (left join miss) and negatives
N_ACCOUNTS = count(*), TOTAL_BALANCE = sum(CURRENT_BALANCE) dollar20.2, TOTAL_PAST_DUE = sum(PAST_DUE_AMOUNT) dollar20.2
order by ACCOUNT_TYPE, REGION_CODE, severity(Current=0, 1-29=1, 30-59=2, 60-89=3, 90-119=4, 120-179=5, 180+=6, else 7)
```

The labels differ from the `DELQBKT` format (`'1-29'` vs `'1-29 Days'`); the program's literals
are what the report contains, so `mart_delinquency_aging` uses the literals and exposes
`bucket_sort_order` (0–7) as the ordering column. `reconcile_delinquency_controls` proves the
bucket ↔ DAYS_PAST_DUE ranges and severity ordering; `reconcile_golden_delinquency_aging`
compares all 70 golden rows.

### 6.4 `REPORTS.LLP_COVERAGE` and `REPORTS.CAPITAL_ADEQUACY`

LLP: `inner join ORA_DW.LOAN_DETAILS`, lending products only, `group by REPORT_MONTH, ACCOUNT_TYPE`.

```
N_LOANS          = count(*)
GROSS_LOANS      = sum(a.CURRENT_BALANCE)                              dollar20.2
TOTAL_ALLOWANCE  = sum(l.ALLOWANCE_AMT)                                dollar20.2
COVERAGE_PCT     = case when sum(CURRENT_BALANCE) > 0 then sum(ALLOWANCE_AMT)/sum(CURRENT_BALANCE)*100 else 0 end   8.2
NPL_BALANCE      = sum(case when l.DAYS_PAST_DUE >= 90 then a.CURRENT_BALANCE else 0 end)                          dollar20.2
NPL_COVERAGE_PCT = case when calculated NPL_BALANCE > 0 then sum(ALLOWANCE_AMT)/calculated NPL_BALANCE*100 else 0 end  8.2
```

Capital adequacy (one row, from `REPORTS.MONTHLY_RWA`):

```
TOTAL_RWA      = sum(RWA)                                    dollar20.2
CET1_CAPITAL   = 50000000, TIER1_CAPITAL = 65000000, TOTAL_CAPITAL = 80000000   (GL placeholders)
CET1_RATIO     = case when sum(RWA) > 0 then 50000000/sum(RWA)*100 else . end   8.2
TIER1_RATIO    = … 65000000 …;  TOTAL_CAPITAL_RATIO = … 80000000 …
CET1_STATUS    = case when sum(RWA) = 0 then 'PASS' when 50000000/sum(RWA)*100 >= 4.5 then 'PASS' else 'FAIL' end  $4
TIER1_STATUS   = … >= 6.0 …;  TOTAL_CAPITAL_STATUS = … >= 8.0 …
```

Zero-RWA behaviour: `sum(RWA) = 0` → ratios missing, statuses `PASS`. No rows (missing sum)
→ ratios missing, statuses `FAIL` (SAS missing-value logic; Spark `null` behaves the same way,
so `mart_capital_adequacy` needs no special casing and `reconcile_golden_capital_adequacy`
plus `reconcile_rwa_controls` (`TOTAL_RWA = Σ mart_regulatory_rwa.RWA`, ratio = capital /
TOTAL_RWA × 100, status thresholds) cover it).

### 6.5 Excel export contract — and the source defect

Intended contract (from the program): one workbook `&REPORT_PATH/REG_REPORT_&report_month..xlsx`
= `/data/sas/reports/output/REG_REPORT_YYYYMM.xlsx`, three sheets in this order:

| Sheet | Dataset | Columns |
|---|---|---|
| `RWA` | `REPORTS.MONTHLY_RWA` | REPORT_MONTH, ACCOUNT_TYPE, CUSTOMER_SEGMENT, RISK_WEIGHT, N_ACCOUNTS, TOTAL_EXPOSURE, RWA |
| `Delinquency` | `REPORTS.DELINQUENCY_AGING` | REPORT_MONTH, ACCOUNT_TYPE, REGION_CODE, DELINQ_BUCKET, N_ACCOUNTS, TOTAL_BALANCE, TOTAL_PAST_DUE |
| `LLP_Coverage` | `REPORTS.LLP_COVERAGE` | REPORT_MONTH, ACCOUNT_TYPE, N_LOANS, GROSS_LOANS, TOTAL_ALLOWANCE, COVERAGE_PCT, NPL_BALANCE, NPL_COVERAGE_PCT |

`CAPITAL_ADEQUACY` is computed *after* the export and is not in the workbook.

What the source actually does: the program calls `%export_xlsx(data=, file=, sheet=)`, but the
macro in `Macro/export_xlsx.sas` is defined with `(DATA=, PATH=, REPLACE=N, LABEL=N)`. `FILE`
and `SHEET` are not parameters of that macro, so SAS 9.4 rejects each call
("The keyword parameter FILE was not defined with the macro") and no workbook is written. Even
with the parameters fixed, `%export_dbms` runs `proc export … dbms=xlsx replace` with no
`sheet=` statement, so the three calls would overwrite one another and only `LLP_Coverage`
would survive. The Excel deliverable is therefore a latent defect in production, not a working
output. The containerised golden run uses `docker/shims/export_xlsx.sas`, which accepts the
program's parameters and writes one CSV per sheet (`REG_REPORT_202401_RWA.csv`,
`REG_REPORT_202401_Delinquency.csv`, `REG_REPORT_202401_LLP_Coverage.csv`, kept under
`verify/golden/`) so the sheet contents can still be reconciled.

Target decision: the three sheet datasets are delivered as governed Delta tables
(`mart_regulatory_rwa`, `mart_delinquency_aging`, `mart_llp_coverage`) and the batch writes the
parity report (`.md` + `.json`) to the UC volume `${catalog}.reports.parity_reports`. Producing
an `.xlsx` artifact from those tables is a follow-up (a small `spark_python_task` after
`dbt_marts` — see §9); it is not part of this PR because the source never produced one.

---

## 7. Orchestration

### 7.1 Source

Control-M `BANK_MASTER` 05:45 → `run_daily_banking.sas` → steps 1‒4 sequentially, abort chain
on first failure, `restart_from=` step number for reruns, control rows appended to
`ARCHIVE.BATCH_HISTORY`, summary email to `&EMAIL_DL`.

### 7.2 Target — Databricks Asset Bundle `sas_banking_nightly_batch`

Files: `databricks.yml` (bundle, variables, `dev`/`prod` targets),
`resources/daily_banking_pipeline.job.yml` (the Workflow), `resources/reports.schema.yml`
(UC schema `${catalog}.reports`), `resources/parity_reports.volume.yml` (managed volume for the
parity artifacts).

Job `daily_banking_pipeline`, schedule `0 0 6 * * ?` `America/New_York` (the 06:00 cadence of
`BANK_DAILY_01`), `max_concurrent_runs: 1`, serverless dbt tasks, `PAUSED` in `dev`
(development mode) and `UNPAUSED` in `prod`:

```
dbt_seed  →  dbt_staging  →  dbt_intermediate  →  dbt_marts  →  dbt_test  →  parity_report
```

| Task | Runs | Replaces |
|---|---|---|
| `dbt_seed` | `dbt seed` (SAS format catalog + golden reference tables) | `banking_formats.sas` catalog build |
| `dbt_staging` / `dbt_intermediate` / `dbt_marts` | `dbt run --select staging` / `intermediate` / `marts` | steps 1‒4 in dependency order (`ref()` gives the finer-grained DAG inside each layer) |
| `dbt_test` | `dbt test` — schema tests + every `reconcile_*.sql`; golden parity tests only when `golden_parity=true` | `%nobs` checks, the abort-on-failure semantics of `%run_step` |
| `parity_report` | `verify/reconcile.py` on the job's Spark session; writes `parity_<ts>.md/.json` to `/Volumes/${catalog}/reports/parity_reports/` | `ARCHIVE.BATCH_HISTORY` + the summary email body |

Job parameters: `curr_dt` (= `&CURR_DT`, default `{{job.start_time.iso_date}}` — the run date,
like `today()`; pass `curr_dt=2024-01-31,report_month=202401,golden_parity=true` to replay the
golden date), `report_month` (= `&PREV_YM`, blank → derived), `golden_parity`
(`true`/`false`). Bundle variables: `catalog`, `namespace`, `raw_catalog`, `raw_schema`,
`warehouse_id`, `schedule_timezone`, `dbt_databricks_version`, `parity_report_dir`.
Targets: `dev` → `namespace = ${workspace.current_user.short_name}`, `raw_schema = raw_sas`;
`prod` → `namespace = prod`, `raw_schema = raw`. Nothing in the bundle writes to
`banking_analytics.raw`.

`workflows/daily_banking_pipeline.json` is the pre-bundle hand-written Jobs-API definition of
the same job. It is kept as documentation of the Control-M mapping (`workflows/README.md`) but
is not deployed; the bundle is the single deployable definition.

Restart semantics: Databricks "Repair run" re-executes failed tasks and their dependents,
which is the equivalent of `restart_from=`.

---

## 8. Reconciliation / parity controls

### 8.1 Golden data — provenance and limitations

Neither repository contained SAS-produced outputs for the Banking batch. The golden set was
generated in this session by running the *unmodified* Banking programs in the estate's own
container (`docker/compose.yml` in `ts-sas-legacy-analytics`, runtime **OpenSAS**, not SAS 9.4)
at `CURR_DT = 31JAN2024`, `report_month = 202401`, over the estate's deterministic seed data
(`Data/load_seed_data.sas`). `verify/golden/manifest.json` records the estate commit, runtime
and row counts; `seed/import_sas_golden.py` loads the exports as dbt seeds
`dbt_project/seeds/sas_golden/sas_golden_*.csv` (SAS missing `.` → null).

| Golden table | Rows | Golden table | Rows |
|---|---|---|---|
| `CUST_ACCOUNTS_DAILY` | 466 | `RISK_SCORES` | 236 |
| `ACCT_EXCEPTIONS` | 32 | `RISK_MIGRATION` | 195 |
| `DAILY_TRANSACTIONS` | 18,903 | `RISK_SUMMARY` | 12 |
| `TXN_ANOMALIES` | 46 | `MONTHLY_RWA` | 59 |
| `RUNNING_BALANCES` | 610 | `DELINQUENCY_AGING` | 70 |
| `LLP_COVERAGE` | 6 | `CAPITAL_ADEQUACY` | 1 |

Limitations, stated plainly:

- The golden outputs are **OpenSAS-produced reference data, not SAS 9.4 production output**.
  Databricks tables derived from them (the `sas_golden_*` seeds and anything joined to them)
  must not be described as "SAS-produced".
- One known OpenSAS deviation from SAS 9.4 semantics is recorded in
  `verify/golden/known_deviations.json` (`KD-001`: `LLP_COVERAGE.NPL_COVERAGE_PCT` comes out
  `0` where the SAS formula gives `TOTAL_ALLOWANCE / NPL_BALANCE × 100`). The model implements
  the SAS formula; `reconcile_golden_llp_coverage` accepts the model value on exactly those
  cells only when it equals the formula applied to the golden's own `TOTAL_ALLOWANCE` and
  `NPL_BALANCE`, and the parity report lists the exception.
- To replace the reference set with real SAS output, run the same Docker route on a SAS 9.4
  image (or point `docker/compose.yml` at one), re-export, and re-run
  `python seed/import_sas_golden.py`; the parity tests need no change.

### 8.2 Control catalogue

`dbt build` fails if any `dbt_project/tests/reconcile_*.sql` returns rows (TDD: the controls are
the contract; a failing control means fix the model, never the test). Golden parity tests are
enabled with `--vars '{sas_golden_parity: true}'`.

| Control | Proves |
|---|---|
| `reconcile_account_completeness` | in-scope source population (status ∉ {W,C}, open ≤ run date, demographics match) = snapshot rows; no fan-out |
| `reconcile_acct_exception_branches` | each of `NEG_BAL` / `HIGH_UTIL` / `NO_RISK` matches an independent recomputation from the snapshot; multi-exception accounts keep all rows |
| `reconcile_txn_completeness` | feed = validated + rejected |
| `reconcile_txn_reject_reasons` | reject reason = first failing rule in SAS order |
| `reconcile_running_balance_chain` | `RUNNING_BALANCE(n) − RUNNING_BALANCE(n−1) = effect(n)`; first row = `PRE_TXN_BALANCE + effect` |
| `reconcile_anomaly_precedence` | `ANOMALY_TYPE` = first true condition in SAS order; no row with all conditions false |
| `reconcile_risk_score_bands` | every WOE bin, PD/LGD/EAD/EL formula and rating band recomputed and compared |
| `reconcile_risk_migration_direction` | NEW/UPGRADE/DOWNGRADE from (`PREV_RATING`, `CURR_RATING`); row set = rating changed or prior missing |
| `reconcile_rwa_controls` | every risk-weight CASE branch; `RWA = Σ balance × weight`; `TOTAL_RWA` ties to `mart_regulatory_rwa`; ratios and PASS/FAIL thresholds |
| `reconcile_delinquency_controls` | bucket ↔ DPD range for all 8 branches, lending-only population, severity ordering |
| `reconcile_llp_controls` | inner-join population, `COVERAGE_PCT` / `NPL_COVERAGE_PCT` formulas and divide guards |
| `reconcile_format_mappings` | every `format_*` macro branch = `sas_format_catalog` seed |
| `reconcile_golden_*` (11) | row-level parity with the golden table on its key, every column, `0.005` abs / `1e-6` rel tolerance, and equal row counts |

`verify/reconcile.py` (`make reconcile NS=<ns>` locally over a SQL warehouse; the
`parity_report` task on Spark) reruns the row-count, control-total (`verify/golden/controls.csv`:
`TXN_ANOMALIES.HIGH_AMOUNT=16`, `OVERDRAFT=30`, `DAILY_TRANSACTIONS.SUM_AMOUNT=8,231,436.81`,
`RISK_SCORES.N_ACCOUNTS=236`, `MONTHLY_RWA.SUM_RWA=14,156,900.58`) and per-table golden diffs
and writes the human-readable parity report with pass/fail per table and the first differing
rows.

---

## 9. Open items and follow-ups

1. **Golden data is OpenSAS-derived** (§8.1). Re-run on SAS 9.4 when a licence is available.
2. **Excel workbook** is not produced (source never produced it either — §6.5). If regulators
   need the `.xlsx`, add a `spark_python_task` after `dbt_marts` that writes the three sheets
   from the marts to `/Volumes/${catalog}/reports/`.
3. **Email notifications** (`&EMAIL_ONCALL` when exceptions > 100; batch summary to
   `&EMAIL_DL`) — set `email_notifications`/`notification_settings` on the job once the target
   distribution lists are known; the exception count is available from
   `int_acct_exceptions`.
4. **Production raw data**: `banking_analytics.raw` does not hold the estate's deterministic
   January-2024 inputs, so `dev` reads `raw_sas` (the seeded extract). Switching `prod` to
   `raw` is a bundle variable, not a code change; golden parity should be disabled there
   (`golden_parity=false`) because the golden set only matches the seeded inputs.
5. **Documented cadences** (weekly risk scoring, monthly regulatory reporting) are *not* what
   the source orchestrator does — it runs all four nightly. The bundle reproduces the
   orchestrator. If the business wants the documented cadences, split `dbt_marts` into a
   nightly selector and `if/else` tasks keyed on `{{job.start_time}}`.
