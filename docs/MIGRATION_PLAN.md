# SAS → dbt/Databricks Migration Plan

> Detailed program-by-program migration plan for the `ts-sas-legacy-analytics` estate to the `uc-data-migration-sas-to-databricks` dbt project. Each entry maps the SAS program to its dbt layer, catalogues every SAS construct requiring translation (per `docs/SAS_TO_DBT_MIGRATION_MAP.md`), and provides effort/risk estimates.

---

## Table of Contents

1. [Migration Scope Summary](#1-migration-scope-summary)
2. [Program Migration Details](#2-program-migration-details)
   - 2.1 [load_customer_accounts.sas](#21-load_customer_accountssas)
   - 2.2 [daily_transaction_processing.sas](#22-daily_transaction_processingsas)
   - 2.3 [credit_risk_scoring.sas](#23-credit_risk_scoringsas)
   - 2.4 [monthly_regulatory_reporting.sas](#24-monthly_regulatory_reportingsas)
   - 2.5 [claims_processing.sas](#25-claims_processingsas)
   - 2.6 [policy_valuation.sas](#26-policy_valuationsas)
   - 2.7 [customer_profitability.sas](#27-customer_profitabilitysas)
3. [Supporting Artefact Migrations](#3-supporting-artefact-migrations)
   - 3.1 [autoexec.sas (Config)](#31-autoexecsas-config)
   - 3.2 [PROC FORMAT Catalogs](#32-proc-format-catalogs)
   - 3.3 [SAS Macro Library](#33-sas-macro-library)
   - 3.4 [Batch Orchestration (BatchJobs)](#34-batch-orchestration-batchjobs)
4. [dbt DAG — Full Target State](#4-dbt-dag--full-target-state)
5. [Effort & Risk Summary Matrix](#5-effort--risk-summary-matrix)
6. [Migration Wave Plan](#6-migration-wave-plan)
7. [Validation Strategy](#7-validation-strategy)

---

## 1. Migration Scope Summary

| Category | Count | Notes |
|---|---|---|
| Core SAS programs | 7 | Banking (4), Insurance (2), Reports (1) |
| Batch orchestrators | 2 | `run_daily_banking.sas`, `run_daily_insurance.sas` |
| PROC FORMAT catalogs | 2 | `banking_formats.sas` (8 formats), `insurance_formats.sas` (4 formats) |
| SAS macros (Macro/) | 92 | Reusable utility library; subset needed in dbt |
| Config files | 1 | `autoexec.sas` — LIBNAMEs, global vars, DB connections |
| Target dbt models | 15 | 3 staging, 3 intermediate, 9 marts |
| Target dbt macros | 12+ | 4 existing format macros + new ones needed |
| Target Databricks Workflows | 2 | Banking daily/weekly, Insurance daily/monthly |

**Already migrated (implemented in dbt_project/):**

| dbt Model | Status | Source SAS |
|---|---|---|
| `stg_cust_accounts` | Done | `load_customer_accounts.sas` Step 1 |
| `stg_daily_transactions` | Done | `daily_transaction_processing.sas` Step 1 |
| `int_account_metrics` | Done | `load_customer_accounts.sas` Step 2 |
| `mart_daily_transactions` | Done | `daily_transaction_processing.sas` Steps 2-5 |
| `mart_risk_scores` | Done | `credit_risk_scoring.sas` |
| `mart_transaction_anomalies` | Done | `daily_transaction_processing.sas` Step 4 |

**Remaining (tests defined but models not yet implemented):**

| dbt Model | Status | Source SAS |
|---|---|---|
| `stg_claims` | Planned | `claims_processing.sas` Step 1 |
| `int_claims_adjudication` | Planned | `claims_processing.sas` Steps 2-4 |
| `int_policy_valuation` | Planned | `policy_valuation.sas` Steps 1-4 |
| `mart_regulatory_rwa` | Planned | `monthly_regulatory_reporting.sas` Step 1 |
| `mart_delinquency_aging` | Planned | `monthly_regulatory_reporting.sas` Step 2 |
| `mart_loss_ratios` | Planned | `policy_valuation.sas` Step 5 |
| `mart_customer_pnl` | Planned | `customer_profitability.sas` Steps 1-4 |
| `mart_llp_coverage` | New | `monthly_regulatory_reporting.sas` Step 3 |
| `mart_capital_adequacy` | New | `monthly_regulatory_reporting.sas` Step 5 |

---

## 2. Program Migration Details

### 2.1 load_customer_accounts.sas

**Source:** `Programs/Banking/load_customer_accounts.sas` (216 lines)
**Schedule:** Daily 06:00 — Control-M `BANK_DAILY_01`
**Inputs:** `ORA_DW.CUST_ACCOUNTS`, `ORA_DW.CUST_DEMOGRAPHICS`, `RAW_BANK.DAILY_RATES`
**Outputs:** `STG_BANK.CUST_ACCOUNTS_DAILY`, `STG_BANK.ACCT_EXCEPTIONS`

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: PROC SQL extract + JOIN | `stg_cust_accounts` | Staging | **Done** |
| Step 2: DATA step business rules | `int_account_metrics` | Intermediate | **Done** |
| Step 3: Exception routing | `int_account_exceptions` | Intermediate | **New — needs implementation** |
| Step 4: PROC MEANS summary | _(downstream reporting)_ | — | Captured in marts |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| `%include` (parmv, nobs, lock) | Lines 13-15 | dbt macro `ref()` DAG; lock unnecessary (ACID Delta) | §7: %INCLUDE → ref() |
| `LIBNAME ORA_DW oracle` | autoexec.sas | Unity Catalog external table + dbt `source()` | §1: LIBNAME → Unity Catalog |
| `PROC SQL ... INNER JOIN` | Lines 34-69 | SQL CTE in `stg_cust_accounts` | §3: PROC SQL → dbt SQL |
| `%if &region ne ALL` (conditional SQL) | Lines 64-66 | Jinja `{% if var('region') != 'ALL' %}` | §8: Macro vars → dbt vars |
| `format ... $ACCTTYPE. $ACCTSTAT.` | Lines 87-95 | `format_account_type()`, `format_account_status()` macros | §2: PROC FORMAT → dbt macros |
| `intck('month', ...)` | Line 100 | `months_between(current_date(), open_date)` | §4: DATA step → SQL |
| `DATA step IF/THEN → two outputs` | Lines 82-157 | SQL CASE expressions in `int_account_metrics`; exception model via `WHERE` | §3: DATA step → SQL CASE |
| `%nobs()` | Lines 71, 162, 186 | `{{ dbt_utils.get_single_value() }}` or dbt test row counts | §8: Macro → dbt macro |
| `%sendmail()` | Lines 175-179 | Databricks Alerts / PagerDuty webhook | §7: sendmail → Alerts |
| `PROC MEANS ... CLASS ... OUTPUT OUT=` | Lines 188-198 | SQL `GROUP BY` with `AVG()`, `SUM()`, `COUNT()` | §3: PROC MEANS → SQL GROUP BY |
| `%lock() / %lock(unlock)` | _(implicit)_ | Delta ACID transactions (no explicit locking) | §6: PROC APPEND → incremental |
| `PROC DATASETS ... DELETE` | Lines 209-211 | Not needed (dbt ephemeral CTEs / temp views) | N/A |

#### Effort: **Low** (already implemented)
#### Risk: **Low**

**Notes:**
- Exception routing (`ACCT_EXCEPTIONS`) not yet in dbt — add `int_account_exceptions` model filtering on negative balance, high utilization, and missing risk rating rules.
- The SAS `%sendmail()` notification on >100 exceptions needs a Databricks Alert or external webhook trigger configured at the Workflow level.

---

### 2.2 daily_transaction_processing.sas

**Source:** `Programs/Banking/daily_transaction_processing.sas` (246 lines)
**Schedule:** Daily 07:30 — Control-M `BANK_DAILY_02` (depends on `BANK_DAILY_01`)
**Inputs:** `RAW_BANK.TXN_FEED_YYYYMMDD`, `STG_BANK.CUST_ACCOUNTS_DAILY`, `BANKING.FORMATS`
**Outputs:** `CURATED.DAILY_TRANSACTIONS`, `CURATED.TXN_ANOMALIES`, `CURATED.RUNNING_BALANCES`

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: DATA step validation | `stg_daily_transactions` | Staging | **Done** |
| Step 2: PROC SQL enrichment join | `mart_daily_transactions` | Marts | **Done** |
| Step 3: DATA step RETAIN running balance | `mart_daily_transactions` | Marts | **Done** (window function) |
| Step 4: PROC SQL anomaly detection | `mart_transaction_anomalies` | Marts | **Done** |
| Step 5: PROC APPEND to curated | `mart_daily_transactions` | Marts | **Done** (incremental merge) |
| Step 6: Running balance persistence | _(in mart_daily_transactions)_ | Marts | **Done** |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| `%sysfunc(exist(RAW_BANK.&txn_ds))` | Line 36 | dbt source freshness check + `dbt source freshness` | §8: Macro → dbt macro |
| Dynamic dataset name `TXN_FEED_YYYYMMDD` | Line 25 | Single source table with date partitioning; filter via `WHERE transaction_date = ...` | §1: LIBNAME → Unity Catalog |
| DATA step validation with `RETURN` | Lines 45-97 | SQL `CASE WHEN ... THEN 'reason' ELSE NULL END` + `WHERE rejection_reason IS NULL` | §3: DATA step → SQL CASE |
| `PROC SQL ... ORDER BY ... LEFT JOIN` | Lines 105-130 | SQL CTE JOIN in `mart_daily_transactions` | §3: PROC SQL → dbt SQL |
| **`RETAIN RUNNING_BALANCE` + `BY` group** | Lines 137-153 | **Window function: `SUM(...) OVER (PARTITION BY account_id ORDER BY ... ROWS UNBOUNDED PRECEDING)`** | **§4: RETAIN → Window fn** |
| `first.ACCOUNT_ID` (BY-group processing) | Line 143 | `PARTITION BY account_id` boundary resets running sum | §4: RETAIN → Window fn |
| Z-score anomaly detection (two-pass SQL) | Lines 159-197 | Two CTEs: `account_stats` → `anomalies` with `z_score > 3` | §3: PROC SQL → dbt SQL |
| **`PROC APPEND ... FORCE`** + `%lock()` | Lines 205-216 | **`config(materialized='incremental', incremental_strategy='merge')`** | **§6: PROC APPEND → incremental** |
| `format ... dollar18.2` | Line 153 | Databricks column metadata / BI layer formatting | N/A |

#### Effort: **Low** (already implemented)
#### Risk: **Medium**

**Risk factors:**
- **Running balance correctness**: Window function order must exactly match SAS `BY ACCOUNT_ID TRANSACTION_DATE TRANSACTION_ID`. Any sort order mismatch produces different balances. Requires strict validation.
- **Incremental merge**: Must confirm `unique_key='transaction_id'` handles late-arriving or corrected transactions identically to the SAS `PROC APPEND FORCE` behavior (which simply appends, even duplicates). The dbt merge will upsert, which is safer but produces different behavior for duplicate transaction IDs.
- **90-day lookback for anomaly stats**: Relies on `mart_daily_transactions` having sufficient history. First-run seeding must backfill 90+ days.

---

### 2.3 credit_risk_scoring.sas

**Source:** `Programs/Banking/credit_risk_scoring.sas` (270 lines)
**Schedule:** Weekly Sunday 02:00 — Control-M `BANK_WEEKLY_01`
**Inputs:** `STG_BANK.CUST_ACCOUNTS_DAILY`, `ORA_DW.BUREAU_SCORES`, `ORA_DW.PAYMENT_HISTORY`, `ORA_DW.COLLATERAL`
**Outputs:** `CURATED.RISK_SCORES`, `CURATED.RISK_MIGRATION`, `REPORTS.RISK_SUMMARY`

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: PROC SQL feature assembly | `mart_risk_scores` (CTE: `score_input`) | Marts | **Done** |
| Step 2: DATA step WOE scorecard + PD/LGD/EAD | `mart_risk_scores` (CTEs: `woe_scored`, `pd_calc`) | Marts | **Done** |
| Step 3: Risk migration matrix | `mart_risk_migration` | Marts | **New — needs implementation** |
| Step 4: PROC APPEND scores | _(incremental model)_ | Marts | Via materialization |
| Step 5: PROC MEANS risk summary | `mart_risk_summary` | Marts | **New — needs implementation** |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| Multi-source PROC SQL with correlated subquery | Lines 32-86 | Multi-ref CTE JOINs; subquery for latest bureau score → `ROW_NUMBER()` or subquery | §3: PROC SQL → dbt SQL |
| **WOE binning (nested IF/THEN/ELSE)** | Lines 92-147 | **Nested SQL CASE expressions** (implemented in `woe_scored` CTE) | **§3: DATA step → SQL CASE** |
| `exp()` for logistic PD | Line 157 | Databricks SQL `exp()` function (identical syntax) | §3: DATA step → SQL |
| `max(0, min(1, ...))` for LGD | Line 163 | `greatest(0, least(1, ...))` in Databricks SQL | §3: DATA step → SQL |
| **Model coefficients hard-coded** | Lines 96-155 | dbt `var()` or seed CSV for model coefficients (recommended for auditability) | §8: Macro vars → dbt vars |
| Risk rating assignment (IF/THEN chain) | Lines 183-189 | SQL CASE with PD threshold bands | §3: DATA step → SQL CASE |
| **Risk migration: JOIN current vs previous** | Lines 202-223 | New model `mart_risk_migration` comparing current and prior scores via `LAG()` or self-join | New model needed |
| `PROC MEANS ... CLASS ... OUTPUT OUT=` | Lines 246-256 | SQL `GROUP BY account_type, risk_rating` with `AVG(pd)`, `SUM(ead)`, `SUM(expected_loss)` | §3: PROC MEANS → SQL GROUP BY |
| `PROC APPEND base=CURATED.RISK_SCORES` | Lines 229-234 | `config(materialized='incremental')` on `mart_risk_scores` | §6: PROC APPEND → incremental |

#### Effort: **Medium**
#### Risk: **High**

**Risk factors:**
- **Numerical precision**: The logistic regression coefficients, WOE bin boundaries, and `exp()` calculations must produce bit-identical PD values between SAS and Databricks. IEEE 754 double-precision differences can cause accounts to land in different risk rating bands at boundary values (e.g., PD = 0.029999 vs 0.030001 changes rating from 3 to 4). Requires sample-level validation with tolerance thresholds.
- **Correlated subquery for latest bureau score**: SAS uses `SCORE_DATE = (SELECT MAX(...))`. In Databricks, recommend rewriting with `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY score_date DESC)` for determinism and performance.
- **Risk migration matrix**: Not yet implemented. Requires comparing current run scores against previous run's `risk_rating` on the same account. Design choice: use `LAG()` on an ordered score history, or self-join `mart_risk_scores` on `account_id` with date filter.
- **Regulatory sensitivity**: This model feeds capital adequacy calculations. Any deviation in PD/LGD/EAD must be documented and explainable to regulators.

---

### 2.4 monthly_regulatory_reporting.sas

**Source:** `Programs/Banking/monthly_regulatory_reporting.sas` (199 lines)
**Schedule:** Monthly 3rd business day — Control-M `BANK_MONTHLY_01`
**Inputs:** `CURATED.DAILY_TRANSACTIONS`, `STG_BANK.CUST_ACCOUNTS_DAILY`, `ORA_DW.LOAN_DETAILS`, `ORA_DW.COLLATERAL`
**Outputs:** `REPORTS.MONTHLY_RWA`, `REPORTS.CAPITAL_ADEQUACY`, `REPORTS.DELINQUENCY_AGING`, `REPORTS.LLP_COVERAGE`, Excel file

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: RWA by category (Basel III) | `mart_regulatory_rwa` | Marts | **Planned** (tests defined) |
| Step 2: Delinquency aging buckets | `mart_delinquency_aging` | Marts | **Planned** (tests defined) |
| Step 3: Loan loss provision coverage | `mart_llp_coverage` | Marts | **New — needs model + tests** |
| Step 4: Excel export | Databricks notebook / Python | N/A | Out of dbt scope |
| Step 5: Capital adequacy summary | `mart_capital_adequacy` | Marts | **New — needs model + tests** |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| `PROC SQL ... CASE ... GROUP BY` (RWA) | Lines 40-67 | SQL model with Basel III risk weight CASE + GROUP BY | §3: PROC SQL → dbt SQL |
| `calculated RISK_WEIGHT` (SAS SQL extension) | Line 59 | Subquery or CTE (Databricks SQL doesn't support `calculated`) | §3: PROC SQL → dbt SQL |
| `BETWEEN` for delinquency buckets | Lines 79-86 | SQL `CASE WHEN days_past_due BETWEEN ...` (identical) | §3: DATA step → SQL CASE |
| Complex `ORDER BY CASE` | Lines 98-107 | `ORDER BY` with CASE for custom sort (identical syntax) | §3: PROC SQL → dbt SQL |
| `%export_xlsx()` macro | Lines 146-162 | Databricks notebook with `pandas.to_excel()` or Databricks dashboard export | §3: PROC EXPORT → notebook |
| Hardcoded capital figures (CET1, Tier1) | Lines 175-177 | dbt `var()` or seed table for capital inputs (need external GL feed in production) | §8: Macro vars → dbt vars |
| `PROC SQL ... SUM(RWA)` capital ratios | Lines 169-190 | SQL aggregation referencing `mart_regulatory_rwa` via `ref()` | §3: PROC SQL → dbt SQL |
| `%let month_start` / `%let month_end` date calc | Lines 27-28 | dbt `var('prev_ym')` or Jinja date math | §8: Macro vars → dbt vars |

#### Effort: **Medium**
#### Risk: **High**

**Risk factors:**
- **Regulatory accuracy**: RWA calculations feed Basel III capital adequacy ratios. Misclassification of risk weights directly impacts reported CET1/Tier1 ratios. Must validate against SAS output to the penny.
- **Excel export**: dbt cannot produce Excel files natively. Requires a separate Databricks notebook step in the Workflow. This is a process change that must be communicated to report consumers.
- **`calculated` keyword**: SAS PROC SQL supports `calculated` to reference computed columns within the same SELECT. Databricks does not. Must refactor into CTEs or subqueries.
- **Capital figures**: Currently hardcoded in SAS. Recommend parameterizing via dbt seed or external table for auditability.
- **LTV dependency**: RWA risk weights for mortgages depend on LTV from `ORA_DW.COLLATERAL`. Need to ensure `collateral` source table is available in Unity Catalog.

---

### 2.5 claims_processing.sas

**Source:** `Programs/Insurance/claims_processing.sas` (238 lines)
**Schedule:** Daily 08:00 — Control-M `INS_DAILY_01`
**Inputs:** `RAW_INS.CLAIMS_FEED_YYYYMMDD`, `RAW_INS.POLICIES`, `TERA_DW.FRAUD_INDICATORS`
**Outputs:** `STG_INS.CLAIMS_REGISTER`, `STG_INS.CLAIMS_REVIEW_QUEUE`, `STG_INS.FRAUD_ALERTS`

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: Ingest + validate (hash lookup) | `stg_claims` | Staging | **Planned** (tests defined) |
| Step 2: Fraud screening | `int_claims_adjudication` (CTE) | Intermediate | **Planned** (tests defined) |
| Step 3: Auto-adjudication rules | `int_claims_adjudication` | Intermediate | **Planned** (tests defined) |
| Step 4: Claims register update | `mart_claims_register` | Marts | **New — needs model + tests** |
| Fraud alerts + email | Databricks Alert | N/A | Workflow-level config |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| **`declare hash h_pol(dataset: ...)`** | Lines 46-52 | **`/*+ BROADCAST(p) */ LEFT JOIN policies p`** — broadcast join hint for small dimension | **§5: Hash object → Broadcast join** |
| `h_pol.definekey('POLICY_ID')` / `.find()` | Lines 48-57 | `ON c.policy_id = p.policy_id WHERE p.status = 'ACTIVE'` | §5: Hash object → Broadcast join |
| `if _N_ = 1 then do; ... end;` (one-time init) | Lines 46-52 | Not needed — JOIN is declarative | §5: Hash object → Broadcast join |
| DATA step validation with `RETURN` | Lines 43-84 | SQL `CASE WHEN ... THEN 'reason'` + filter | §3: DATA step → SQL CASE |
| `PROC SQL ... LEFT JOIN` (fraud check) | Lines 93-108 | SQL CTE with LEFT JOIN to `fraud_indicators` source | §3: PROC SQL → dbt SQL |
| DATA step IF/THEN routing (auto-adjudication) | Lines 127-176 | **SQL CASE chain for APPR/DENY/PEND routing** | §3: DATA step → SQL CASE |
| `catx(' ', ..., ifc(...))` | Lines 170-173 | `CONCAT_WS(' ', CASE ...)` in Databricks SQL | §3: DATA step → SQL |
| **`PROC APPEND base=STG_INS.CLAIMS_REGISTER`** | Lines 194-196 | **`config(materialized='incremental', incremental_strategy='merge', unique_key='claim_id')`** | **§6: PROC APPEND → incremental** |
| `%sendmail()` for SIU alerts | Lines 209-213 | Databricks Alert on `fraud_risk = 'HIGH'` count, routed to PagerDuty | §7: sendmail → Alerts |
| `format ... $CLMSTAT.` | Line 191 | New dbt macro `format_claim_status()` needed | §2: PROC FORMAT → dbt macros |
| `format ... $POLTYPE.` (from insurance_formats) | `policy_valuation.sas` | New dbt macro `format_policy_type()` needed | §2: PROC FORMAT → dbt macros |

#### Effort: **Medium**
#### Risk: **Medium**

**Risk factors:**
- **Hash object → broadcast join**: The SAS hash object loads `POLICIES` into memory once and does key lookups. The Databricks broadcast join (`/*+ BROADCAST(p) */`) is the equivalent for a small dimension table. If `POLICIES` grows beyond broadcast threshold (~10 MB default), the hint should be removed and Spark's join optimizer will handle it. Validate row counts match the SAS hash `.find()` hit/miss pattern.
- **Teradata source**: Fraud indicators come from `TERA_DW`. Need to confirm this data is landed in Unity Catalog (likely via separate ingestion pipeline) before the dbt model can reference it.
- **Auto-adjudication logic**: The routing rules (deny if fraud HIGH, approve if low risk + small claim, etc.) are critical business logic. Must be validated with exact same claim records to confirm identical routing decisions.

---

### 2.6 policy_valuation.sas

**Source:** `Programs/Insurance/policy_valuation.sas` (206 lines)
**Schedule:** Monthly 5th business day — Control-M `INS_MONTHLY_01`
**Inputs:** `RAW_INS.POLICIES`, `RAW_INS.CLAIMS`, `RAW_INS.PREMIUMS`, `TERA_DW.ACTUARIAL_TABLES`
**Outputs:** `STG_INS.POLICY_VALUATION`, `REPORTS.LOSS_RATIO_SUMMARY`, `REPORTS.RESERVE_ADEQUACY`

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: In-force policy extract | `int_policy_valuation` (CTE) | Intermediate | **Planned** (tests defined) |
| Step 2: Claims experience (12-month) | `int_policy_valuation` (CTE) | Intermediate | **Planned** |
| Step 3: Premium collections | `int_policy_valuation` (CTE) | Intermediate | **Planned** |
| Step 4: MERGE BY + valuation metrics | `int_policy_valuation` | Intermediate | **Planned** (tests defined) |
| Step 5: PROC MEANS loss ratio summary | `mart_loss_ratios` | Marts | **Planned** (tests defined) |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| **`DATA ... MERGE ... BY POLICY_ID`** | Lines 123-127 | **`LEFT JOIN` on three CTEs**: inforce, claims_exp, premium_coll | **§5: MERGE BY → SQL JOIN** |
| `if a;` (keep only matched from first dataset) | Line 129 | `FROM inforce LEFT JOIN claims LEFT JOIN premiums` (inforce drives) | §5: MERGE BY → SQL JOIN |
| `intck('month', ...)` for policy age | Line 49 | `months_between()` in Databricks SQL | §4: DATA step → SQL |
| `intnx('month', ..., 3)` for renewal flag | Line 52 | `add_months(current_date(), 3)` or `date_add()` | §4: DATA step → SQL |
| Earned premium calculation (date math) | Lines 57-60 | SQL `LEAST(12, months_between(...))` with `GREATEST()` bounds | §4: DATA step → SQL |
| `coalesce(TOTAL_INCURRED, 0) / YTD_EARNED_PREMIUM` | Line 137 | `COALESCE(total_incurred, 0) / NULLIF(ytd_earned_premium, 0)` (handle div-by-zero) | §3: DATA step → SQL |
| IBNR estimate formula | Line 155 | SQL `GREATEST(0, ytd_earned_premium * 0.15 - COALESCE(total_paid, 0))` | §3: DATA step → SQL |
| `format ... $POLTYPE. $RISKCAT.` | Lines 131-132 | New dbt macros: `format_policy_type()`, `format_risk_category()` | §2: PROC FORMAT → dbt macros |
| `PROC MEANS ... CLASS POLICY_TYPE` | Lines 169-181 | SQL `GROUP BY policy_type` with `SUM()`, `COUNT()` | §3: PROC MEANS → SQL GROUP BY |
| Post-aggregation DATA step (adding ratios) | Lines 184-193 | SQL window function or CTE computing `agg_loss_ratio` in same model | §3: DATA step → SQL |
| `%if &lob ne ALL` (conditional filter) | Lines 66-68 | Jinja `{% if var('lob', 'ALL') != 'ALL' %}` | §8: Macro vars → dbt vars |

#### Effort: **Medium**
#### Risk: **Medium**

**Risk factors:**
- **Three-way MERGE BY**: SAS `MERGE ... BY POLICY_ID` with three datasets is a full outer join variant where `if a;` keeps only rows from the first dataset (left join semantics). Must be implemented as `FROM inforce LEFT JOIN claims_exp USING (policy_id) LEFT JOIN premium_coll USING (policy_id)`. Verify no unintended row duplication from 1:N joins.
- **Earned premium pro-rata calculation**: Complex date arithmetic with `intck`/`intnx` boundaries. Must validate against SAS output for edge cases (policies spanning year boundaries, mid-month effective dates).
- **IBNR estimate**: Actuarial calculation (15% of earned premium minus paid) is simplified but still regulatory-relevant. Any rounding difference is visible to actuaries.
- **`TERA_DW.ACTUARIAL_TABLES`**: Referenced in the header but not directly used in the code. May be used in future enhancements — document as a dependency.

---

### 2.7 customer_profitability.sas

**Source:** `Programs/Reports/customer_profitability.sas` (176 lines)
**Schedule:** Monthly 10th business day — Control-M `BANK_MONTHLY_03`
**Inputs:** `STG_BANK.CUST_ACCOUNTS_DAILY`, `CURATED.DAILY_TRANSACTIONS`, `CURATED.RISK_SCORES`, `ORA_DW.COST_OF_FUNDS`
**Outputs:** `REPORTS.CUSTOMER_PNL`, `REPORTS.SEGMENT_PROFITABILITY`, `REPORTS.BRANCH_PROFITABILITY`

#### dbt Model Layer Mapping

| SAS Step | dbt Model | Layer | Status |
|---|---|---|---|
| Step 1: Interest income by customer | `mart_customer_pnl` (CTE) | Marts | **Planned** (tests defined) |
| Step 2: Fee income from transactions | `mart_customer_pnl` (CTE) | Marts | **Planned** |
| Step 3: Expected credit loss | `mart_customer_pnl` (CTE) | Marts | **Planned** |
| Step 4: P&L assembly + tier assignment | `mart_customer_pnl` | Marts | **Planned** (tests defined) |
| Step 5: Segment summary | `mart_segment_profitability` | Marts | **New — needs model + tests** |
| Step 5: Branch summary | `mart_branch_profitability` | Marts | **New — needs model + tests** |
| Excel export | Databricks notebook | N/A | Workflow-level |

#### SAS Constructs Requiring Translation

| SAS Construct | Location | dbt/Databricks Equivalent | Reference |
|---|---|---|---|
| **Multi-source `DATA ... MERGE ... BY`** | Lines 100-104 | **Multi-ref `LEFT JOIN` on `customer_id`**: interest_income, fee_income, ecl | **§5: MERGE BY → SQL JOIN** |
| `if a;` (keep only interest_income matches) | Line 106 | `FROM interest_income LEFT JOIN fee_income LEFT JOIN ecl` | §5: MERGE BY → SQL JOIN |
| `calculated LENDING_INCOME` (SAS SQL extension) | Line 52 | CTE or subquery (Databricks doesn't support `calculated`) | §3: PROC SQL → dbt SQL |
| Correlated subquery for latest risk scores | Lines 91-93 | `ROW_NUMBER() OVER (... ORDER BY score_date DESC) = 1` or subquery | §3: PROC SQL → dbt SQL |
| `sum(NET_INTEREST_INCOME, FEE_INCOME, 0)` (SAS SUM) | Line 113 | `COALESCE(net_interest_income, 0) + COALESCE(fee_income, 0)` (SAS `SUM()` ignores missing) | §3: DATA step → SQL |
| Profitability tier IF/THEN | Lines 128-132 | SQL CASE on `net_profit` thresholds | §3: DATA step → SQL CASE |
| `PROC MEANS ... CLASS CUSTOMER_SEGMENT` | Lines 140-148 | SQL `GROUP BY customer_segment` | §3: PROC MEANS → SQL GROUP BY |
| `PROC MEANS ... CLASS BRANCH_ID REGION_CODE` | Lines 150-157 | SQL `GROUP BY branch_id, region_code` | §3: PROC MEANS → SQL GROUP BY |
| `%export_xlsx()` | Lines 159-163 | Databricks notebook export step | §3: PROC EXPORT → notebook |

#### Effort: **Medium**
#### Risk: **Low-Medium**

**Risk factors:**
- **SAS `SUM()` vs SQL `+`**: SAS's `SUM()` function treats missing values as zero. Standard SQL `+` with a NULL operand returns NULL. Must use `COALESCE(..., 0)` wrappers consistently. This is a common migration pitfall.
- **Operating cost allocation**: Hardcoded `$15/account/month`. Recommend parameterizing via `dbt var()` or seed.
- **Multi-source merge**: Three WORK datasets merged by `CUSTOMER_ID`. Confirm 1:1 join cardinality — if a customer has records in `fee_income` but not `interest_income`, the `if a;` filter drops them. The dbt model must replicate this (interest_income is the driving table).

---

## 3. Supporting Artefact Migrations

### 3.1 autoexec.sas (Config)

**Source:** `Config/autoexec.sas` (118 lines)

| SAS Element | dbt/Databricks Equivalent | Status |
|---|---|---|
| `LIBNAME RAW_BANK`, `STG_BANK`, etc. | Unity Catalog schemas + dbt `source()` definitions in `_staging_sources.yml` | **Done** |
| `LIBNAME ORA_DW oracle` | Unity Catalog external table (Delta or Lakehouse Federation) | **Done** (sources defined) |
| `LIBNAME TERA_DW teradata` | Unity Catalog external table (requires Lakehouse Federation or ingestion pipeline) | **Planned** |
| `%let CURR_DT`, `%let PREV_YM` | `dbt_project.yml` vars: `curr_dt`, `prev_ym` | **Done** |
| `%let ENVIRONMENT`, `%let BASE_PATH` | `env_var('DBT_ENVIRONMENT')`, not needed (cloud paths) | **Done** |
| `%let EMAIL_DL`, `%let EMAIL_ONCALL` | Databricks Alerts / Workflow notification config | **Planned** |
| `options fmtsearch=(BANKING INSURANCE)` | dbt macro `dispatch` or direct macro calls | **Done** (macros implemented) |
| Oracle/Teradata credentials (`&ora_uid`, `&ora_pwd`) | Databricks Secrets + `env_var()` in `profiles.yml` | **Done** |

### 3.2 PROC FORMAT Catalogs

**Banking formats (`Formats/banking_formats.sas` — 8 formats):**

| SAS Format | dbt Macro | Status |
|---|---|---|
| `$ACCTTYPE` (11 values) | `macros/format_account_type.sql` | **Done** |
| `$ACCTSTAT` (8 values) | `macros/format_account_status.sql` | **Done** |
| `$CUSTSEG` (6 values) | `macros/format_customer_segment.sql` | **Done** |
| `$TXNCAT` (10 values) | `macros/format_txn_category.sql` | **Done** |
| `RISKRATE` (7 values) | `macros/format_risk_rating.sql` | **New — needs implementation** |
| `DELQBKT` (range-based) | Inline CASE in `mart_delinquency_aging` | **Planned** |
| `BALRANGE` (range-based) | `macros/format_balance_range.sql` | **New — needs implementation** |
| `$REGION` (7 values) | `macros/format_region.sql` | **New — needs implementation** |
| `$LNPURP` (8 values) | `macros/format_loan_purpose.sql` | **New — needs implementation** |

**Insurance formats (`Formats/insurance_formats.sas` — 4 formats):**

| SAS Format | dbt Macro | Status |
|---|---|---|
| `$POLTYPE` (13 values) | `macros/format_policy_type.sql` | **New — needs implementation** |
| `$CLMSTAT` (12 values) | `macros/format_claim_status.sql` | **New — needs implementation** |
| `$RISKCAT` (5 values) | `macros/format_risk_category.sql` | **New — needs implementation** |
| `$COVTYPE` (9 values) | `macros/format_coverage_type.sql` | **New — needs implementation** |
| `LOSSRANGE` (range-based) | `macros/format_loss_range.sql` | **New — needs implementation** |

**Total: 13 formats → 4 done, 9 new macros needed.**

### 3.3 SAS Macro Library

The `Macro/` directory contains 92 SAS macros. Most are general-purpose SAS utilities that have no dbt equivalent (because dbt/SQL handles them natively or they are not needed in a cloud environment). Below is the categorization:

#### Macros with dbt equivalents (migration required)

| SAS Macro | Used By | dbt Equivalent |
|---|---|---|
| `parmv.sas` | All programs | dbt model config + Jinja `{% if var(...) %}` validation |
| `nobs.sas` | All programs | dbt tests (`row_count`), or `{{ dbt_utils.get_single_value() }}` |
| `lock.sas` | Transaction, risk programs | Not needed — Delta Lake ACID transactions |
| `sendmail.sas` | Exceptions, claims, batch | Databricks Alerts / PagerDuty integration |
| `export_xlsx.sas` | Regulatory, profitability | Databricks notebook with pandas |
| `hash_define.sas` / `hash_lookup.sas` | Claims processing | Broadcast JOIN (SQL-native) |
| `seplist.sas` | Parent-Child-Index | Jinja `{{ "`, `".join(list) }}` or `dbt_utils.star()` |

#### Macros not needed in dbt (drop)

| Category | Examples | Why Not Needed |
|---|---|---|
| File I/O | `create_directory`, `delete_file`, `dirlist`, `execpath` | Cloud storage / Unity Catalog — no file system ops |
| SAS-specific | `optload`, `optsave`, `optval`, `fmtexist`, `fmtlist` | No SAS options in Databricks |
| Export utilities | `export_csv`, `export_dbms`, `export_dlm`, `export_sas`, `export_spss`, `export_stata` | dbt materializes tables; external export via notebooks |
| Data utilities | `get_data_attr`, `get_lib_attr`, `varexist`, `varlist` | dbt schema YAML / `information_schema` |
| Batch control | `batch_submit`, `RunAll`, `RunAll_ControlTable`, `loop`, `loop_control` | Databricks Workflows replaces batch orchestration |
| UI/Output | `log2pdf`, `txt2pdf`, `txt2rtf`, `pagexofy`, `reduce_pixel` | Not applicable in cloud pipeline |

### 3.4 Batch Orchestration (BatchJobs)

**SAS → Databricks Workflow Mapping:**

#### run_daily_banking.sas → `daily_banking_pipeline` Workflow

| SAS Step | dbt Task | Databricks Task Key | Depends On |
|---|---|---|---|
| Step 1: Load Customer Accounts | `dbt run --select stg_cust_accounts int_account_metrics` | `dbt_staging_accounts` | — |
| Step 2: Daily Transactions | `dbt run --select stg_daily_transactions mart_daily_transactions mart_transaction_anomalies` | `dbt_transactions` | `dbt_staging_accounts` |
| Step 3: Credit Risk Scoring | `dbt run --select mart_risk_scores` | `dbt_risk_scoring` | `dbt_staging_accounts` |
| Step 4: Monthly Regulatory | `dbt run --select mart_regulatory_rwa mart_delinquency_aging mart_llp_coverage mart_capital_adequacy` | `dbt_regulatory` | `dbt_staging_accounts` |
| — | `dbt test --select tag:staging tag:intermediate tag:marts` | `dbt_test` | All above |
| — | Notebook: Excel export for regulators | `export_regulatory_report` | `dbt_regulatory` |

**Control-M feature mapping:**

| Control-M Feature | Databricks Workflow Equivalent |
|---|---|
| `%run_step()` with error handling | Task-level `depends_on` + `on_failure` policy |
| `ABORT_ON_ERR = Y` | `max_retries: 0` + Workflow failure stops downstream |
| `restart_from=N` (restart mid-batch) | "Repair Run" — re-execute failed and downstream tasks |
| `BATCH_ID` tracking | Workflow run ID + run metadata |
| `BATCH_CONTROL` table | Workflow run history API + audit log |
| `%sendmail()` on failure | Email / webhook notification on Workflow task failure |
| Cron scheduling (Control-M calendar) | Databricks Workflow cron trigger (`0 45 5 * * ?`) |

#### run_daily_insurance.sas → `daily_insurance_pipeline` Workflow

| SAS Step | dbt Task | Databricks Task Key | Depends On |
|---|---|---|---|
| Step 1: Claims Processing | `dbt run --select stg_claims int_claims_adjudication` | `dbt_claims` | — |
| Step 2: Policy Valuation | `dbt run --select int_policy_valuation mart_loss_ratios` | `dbt_valuation` | — |
| — | `dbt test` | `dbt_test` | All above |

---

## 4. dbt DAG — Full Target State

```
Sources (Unity Catalog)
│
├─ banking_raw.cust_accounts ──┐
├─ banking_raw.cust_demographics ─┤
│                                 ▼
│                          stg_cust_accounts ─────────► int_account_metrics ──┬──► mart_daily_transactions (incremental)
│                                                                            │         │
├─ banking_raw.daily_transactions ──► stg_daily_transactions ────────────────┘         │
│                                                                                      ▼
│                                                                        mart_transaction_anomalies
│
├─ banking_raw.bureau_scores ───┐
├─ banking_raw.payment_history ─┤
├─ banking_raw.collateral ──────┤
│                               ▼
│                        int_account_metrics ──────────► mart_risk_scores (incremental)
│                                                              │
│                                                              ├──► mart_risk_migration [NEW]
│                                                              └──► mart_risk_summary [NEW]
│
├─ banking_raw.loan_details ────┐
│                               ▼
│                        int_account_metrics ──────────► mart_regulatory_rwa [PLANNED]
│                                                  ├──► mart_delinquency_aging [PLANNED]
│                                                  ├──► mart_llp_coverage [NEW]
│                                                  └──► mart_capital_adequacy [NEW]
│
│                        int_account_metrics ──┐
│                        mart_daily_transactions ─┤
│                        mart_risk_scores ────────┤
│                                                 ▼
│                                          mart_customer_pnl [PLANNED]
│                                                 ├──► mart_segment_profitability [NEW]
│                                                 └──► mart_branch_profitability [NEW]
│
├─ insurance_raw.policies ──────┐
├─ insurance_raw.claims ────────┤
├─ insurance_raw.fraud_indicators ─┤
│                                  ▼
│                           stg_claims [PLANNED] ──► int_claims_adjudication [PLANNED]
│                                                         └──► mart_claims_register [NEW]
│
├─ insurance_raw.policies ──────┐
├─ insurance_raw.claims ────────┤
├─ insurance_raw.premiums ──────┤
│                               ▼
│                        int_policy_valuation [PLANNED] ──► mart_loss_ratios [PLANNED]
```

---

## 5. Effort & Risk Summary Matrix

| SAS Program | dbt Layer(s) | Status | Effort | Risk | Key Risk Factor |
|---|---|---|---|---|---|
| `load_customer_accounts.sas` | Staging → Intermediate | **Done** | Low | Low | Exception model still needed |
| `daily_transaction_processing.sas` | Staging → Marts | **Done** | Low | Medium | Running balance precision; incremental merge semantics |
| `credit_risk_scoring.sas` | Marts | **Partial** | Medium | **High** | Numerical precision in PD/LGD; regulatory sensitivity |
| `monthly_regulatory_reporting.sas` | Marts | **Planned** | Medium | **High** | Basel III RWA accuracy; Excel export process change |
| `claims_processing.sas` | Staging → Intermediate | **Planned** | Medium | Medium | Hash → broadcast join; Teradata source availability |
| `policy_valuation.sas` | Intermediate → Marts | **Planned** | Medium | Medium | Three-way merge; earned premium date math |
| `customer_profitability.sas` | Marts | **Planned** | Medium | Low-Med | SAS SUM() null handling; multi-source merge |
| `banking_formats.sas` | Macros | **Partial** | Low | Low | 4 done, 5 remaining |
| `insurance_formats.sas` | Macros | **Not started** | Low | Low | 5 new macros needed |
| `autoexec.sas` | Config/Sources | **Done** | Low | Low | — |
| `run_daily_banking.sas` | Databricks Workflow | **Not started** | Medium | Medium | Scheduling, alerting, restart logic |
| `run_daily_insurance.sas` | Databricks Workflow | **Not started** | Low | Low | Simpler pipeline |

**Overall Totals:**
- **Models:** 15 total (6 done, 9 remaining)
- **Macros:** 13 total (4 done, 9 remaining)
- **Workflows:** 2 (not started)

---

## 6. Migration Wave Plan

### Wave 1 — Foundation (Complete)
**Scope:** Core banking staging/intermediate + existing marts
**Duration:** Done
**Deliverables:**
- `stg_cust_accounts`, `stg_daily_transactions`
- `int_account_metrics`
- `mart_daily_transactions`, `mart_risk_scores`, `mart_transaction_anomalies`
- 4 format macros (account type, account status, customer segment, txn category)
- dbt project config, profiles, CI/CD pipeline

### Wave 2 — Insurance Domain (2-3 weeks estimated)
**Scope:** Claims and policy valuation pipeline
**Priority:** Medium — independent from banking, can run in parallel with Wave 3
**Deliverables:**
- `stg_claims` → `int_claims_adjudication` → `mart_claims_register`
- `int_policy_valuation` → `mart_loss_ratios`
- 5 insurance format macros ($POLTYPE, $CLMSTAT, $RISKCAT, $COVTYPE, LOSSRANGE)
- Insurance Databricks Workflow definition

**Key dependencies:**
- Insurance source tables in Unity Catalog (policies, claims, premiums, fraud_indicators)
- Teradata → Unity Catalog ingestion pipeline for fraud indicators

### Wave 3 — Regulatory & Risk Expansion (2-3 weeks estimated)
**Scope:** Regulatory reporting models + risk scoring enhancements
**Priority:** High — regulatory deadlines drive this
**Deliverables:**
- `mart_regulatory_rwa`, `mart_delinquency_aging`
- `mart_llp_coverage`, `mart_capital_adequacy`
- `mart_risk_migration`, `mart_risk_summary`
- `int_account_exceptions`
- 5 remaining banking format macros (RISKRATE, BALRANGE, $REGION, $LNPURP, DELQBKT)
- Databricks notebook for Excel regulatory export

**Key dependencies:**
- `loan_details` source table in Unity Catalog (for RWA and LLP)
- Validation against SAS regulatory output (high-stakes accuracy requirement)

### Wave 4 — Profitability & Orchestration (1-2 weeks estimated)
**Scope:** Customer profitability + full Databricks Workflow deployment
**Priority:** Medium — depends on Waves 1-3 models being validated
**Deliverables:**
- `mart_customer_pnl`, `mart_segment_profitability`, `mart_branch_profitability`
- Banking Databricks Workflow definition (replacing `run_daily_banking.sas`)
- Insurance Databricks Workflow definition (replacing `run_daily_insurance.sas`)
- Databricks Alerts replacing `%sendmail()` notifications
- Databricks notebook for profitability Excel export

### Wave 5 — Validation & Cutover (2-3 weeks estimated)
**Scope:** SAS-to-dbt parity validation + parallel run + production cutover
**Deliverables:**
- Row count parity checks for all models
- Column-level checksums (SUM, COUNT DISTINCT)
- Sample record comparison (100 records per model)
- Business rule exception count matching
- Parallel run: both SAS and dbt producing outputs for same dates
- Sign-off from stakeholders (Risk, Finance, Actuarial, Ops)
- Production cutover + Control-M decommission plan

---

## 7. Validation Strategy

For each migrated model, validate equivalence against SAS output per the framework in `docs/SAS_TO_DBT_MIGRATION_MAP.md`:

| Check | Method | Tolerance |
|---|---|---|
| **Row count parity** | `SELECT COUNT(*)` in dbt vs SAS `%nobs()` from batch logs | Exact match |
| **Column checksums** | `SUM(amount)`, `COUNT(DISTINCT id)` compared across systems | Exact match for integers; ±0.01 for currency; ±0.0001 for ratios |
| **PD/LGD/EAD precision** | Compare 1,000 sample accounts at boundary PD values | ±0.0001 (4 decimal places) |
| **Risk rating distribution** | `GROUP BY risk_rating ORDER BY risk_rating` counts | Exact match (boundary accounts may differ — document any) |
| **Anomaly detection** | Compare anomaly counts and types between SAS `TXN_ANOMALIES` and dbt | Exact match on anomaly_type distribution |
| **Running balance** | Select 50 accounts, compare final running balance | Exact match (±$0.01 for floating point) |
| **Regulatory RWA** | Compare `TOTAL_RWA` and `CET1_RATIO` | Exact match (regulatory precision) |
| **Loss ratios** | Compare aggregate and per-policy loss ratios | ±0.01% |
| **Profitability tiers** | Compare tier distribution counts | Exact match |

The existing `config/validation_rule_config.json` in this repo and the Streamlit validation app (`app.py`) can be used to execute and review these checks interactively.
