# Claims Processing — Data Dictionary

> **Draft — pending data steward review**

Column-level documentation for the dbt models produced by the
`claims_processing.sas` migration.

---

## `stg_claims` — Validated Claims with Policy Enrichment

Staging model that ingests the daily claims feed, validates each claim
against the active policy register, and enriches with policy attributes.
Replaces SAS DATA step (Step 1) and the hash-object lookup to
`RAW_INS.POLICIES`.

| Column | Data Type | Business Definition | Source SAS Variable | Valid Values | Null Handling |
|---|---|---|---|---|---|
| `claim_id` | `STRING` | Unique identifier for the insurance claim. Primary key. | `CLAIM_ID` (CLAIMS_FEED) | Non-empty string | NOT NULL — rejected if missing. |
| `policy_id` | `STRING` | Policy under which the claim is filed. Validated as active via broadcast join to `policies`. | `POLICY_ID` (CLAIMS_FEED) | Must exist in `policies` with `STATUS = 'ACTIVE'` | NOT NULL — claims with invalid/inactive policies are excluded (SAS routed to `CLAIMS_INVALID`). |
| `claimant_id` | `STRING` | Identifier of the person or entity filing the claim. | `CLAIMANT_ID` (CLAIMS_FEED) | Non-empty string | NOT NULL. |
| `claim_type` | `STRING` | Classification of the claim (e.g., collision, liability, property). | `CLAIM_TYPE` (CLAIMS_FEED) | Domain-specific codes | NOT NULL. |
| `claimed_amount` | `DOUBLE` | Dollar amount claimed by the policyholder. Validated to not exceed `sum_insured`. | `CLAIMED_AMOUNT` (CLAIMS_FEED) | ≥ 0; ≤ `sum_insured` | NOT NULL — claims exceeding sum insured are excluded. |
| `loss_date` | `DATE` | Date the insured loss event occurred. Validated to fall within the policy effective period. | `LOSS_DATE` (CLAIMS_FEED) | Between `effective_date` and `expiration_date` | NOT NULL — out-of-period claims are excluded. |
| `reported_date` | `DATE` | Date the claim was reported to the insurer. | `REPORTED_DATE` (CLAIMS_FEED) | Valid date | NOT NULL. |
| `claim_status` | `STRING` | Current processing status of the claim. Formatted via `$CLMSTAT.` in legacy SAS. | `CLAIM_STATUS` (CLAIMS_FEED) | `OPEN`, `CLOSED`, `PENDING`, `DENIED`, `SETTLED`, `REOPENED` | NOT NULL. |
| `policy_type` | `STRING` | Line of business for the associated policy (e.g., AUTO, HOME, RENT). | `POLICY_TYPE` (POLICIES via hash) | Domain-specific codes (AUTO, HOME, RENT, …) | NOT NULL — only active policies are joined. |
| `effective_date` | `DATE` | Start date of the policy coverage period. | `EFFECTIVE_DATE` (POLICIES via hash) | Valid date | NOT NULL. |
| `expiration_date` | `DATE` | End date of the policy coverage period. | `EXPIRATION_DATE` (POLICIES via hash) | Valid date; ≥ `effective_date` | NOT NULL. |
| `sum_insured` | `DOUBLE` | Maximum coverage amount on the policy. | `SUM_INSURED` (POLICIES via hash) | > 0 | NOT NULL. |
| `deductible` | `DOUBLE` | Policyholder's deductible amount. Subtracted from approved claims. | `DEDUCTIBLE` (POLICIES via hash) | ≥ 0 | NOT NULL. |

---

## `int_claims_adjudication` — Fraud Screening and Auto-Adjudication

Intermediate model that applies fraud scoring, risk classification, and
auto-adjudication rules. Replaces SAS Steps 2–4 (PROC SQL fraud join,
DATA step IF/THEN routing, PROC APPEND to claims register).

| Column | Data Type | Business Definition | Source SAS Variable | Valid Values | Null Handling |
|---|---|---|---|---|---|
| `claim_id` | `STRING` | Primary key — carried from `stg_claims`. | `CLAIM_ID` | Non-empty string | NOT NULL. |
| `policy_id` | `STRING` | Policy identifier — carried from `stg_claims`. | `POLICY_ID` | Non-empty string | NOT NULL. |
| `claimant_id` | `STRING` | Claimant identifier — carried from `stg_claims`. | `CLAIMANT_ID` | Non-empty string | NOT NULL. |
| `claim_type` | `STRING` | Claim classification — carried from `stg_claims`. | `CLAIM_TYPE` | Domain-specific codes | NOT NULL. |
| `claimed_amount` | `DOUBLE` | Original claimed amount — carried from `stg_claims`. | `CLAIMED_AMOUNT` | ≥ 0 | NOT NULL. |
| `loss_date` | `DATE` | Date of loss — carried from `stg_claims`. | `LOSS_DATE` | Valid date | NOT NULL. |
| `reported_date` | `DATE` | Date reported — carried from `stg_claims`. | `REPORTED_DATE` | Valid date | NOT NULL. |
| `claim_status` | `STRING` | Claim status — carried from `stg_claims`. | `CLAIM_STATUS` | `OPEN`, `CLOSED`, `PENDING`, `DENIED`, `SETTLED`, `REOPENED` | NOT NULL. |
| `policy_type` | `STRING` | Policy line of business — carried from `stg_claims`. | `POLICY_TYPE` | AUTO, HOME, RENT, … | NOT NULL. |
| `sum_insured` | `DOUBLE` | Maximum policy coverage — used in adjudication threshold (25%). | `SUM_INSURED` | > 0 | NOT NULL. |
| `deductible` | `DOUBLE` | Policyholder deductible — subtracted from approved amount. | `DEDUCTIBLE` | ≥ 0 | NOT NULL. |
| `fraud_score` | `INT` | Numeric fraud propensity score from the fraud model. | `FRAUD_SCORE` (FRAUD_INDICATORS) | 0–100 (typical) | NULL if no fraud record matched (left join). Coalesced to 0 in risk derivation. |
| `indicator_flags` | `STRING` | Pipe- or semicolon-delimited fraud indicator codes. | `INDICATOR_FLAGS` (FRAUD_INDICATORS) | Free text | NULL if no fraud record matched. |
| `fraud_risk` | `STRING` | Risk tier derived from `fraud_score`: HIGH (≥ 80), MEDIUM (≥ 50), LOW (< 50). Replaces SAS `CASE` / `IF/THEN`. | *(derived)* | `HIGH`, `MEDIUM`, `LOW` | NOT NULL — defaults to `LOW` when `fraud_score` is NULL. |
| `adjudication_result` | `STRING` | Auto-adjudication outcome. `APPR` = auto-approved, `DENY` = auto-denied (high fraud), `PEND` = routed to manual review. | *(derived)* | `APPR`, `DENY`, `PEND` | NOT NULL. |
| `adjudication_reason` | `STRING` | Human-readable explanation of the adjudication decision. | *(derived)* | Free text | NOT NULL — always populated. |
| `approved_amount` | `DOUBLE` | Approved payout amount. `GREATEST(0, claimed_amount - deductible)` for APPR; `0` for DENY; `NULL` for PEND. | *(derived)* | ≥ 0 or NULL | NULL when `adjudication_result = 'PEND'`; NOT NULL otherwise. |
| `processing_date` | `DATE` | Date the claim was processed through the adjudication pipeline. | *(derived from `&proc_date`)* | Valid date | NOT NULL. |
