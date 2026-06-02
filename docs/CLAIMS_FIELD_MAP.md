# Claims Processing — Source-to-Target Field Map

> **Draft — pending data steward review**

This document maps every field used in `claims_processing.sas` to its
corresponding column in the target dbt models (`stg_claims`,
`int_claims_adjudication`).

## Field Map

| SAS Variable | SAS Source Dataset | dbt Column | dbt Model | Transform Notes |
|---|---|---|---|---|
| `CLAIM_ID` | `RAW_INS.CLAIMS_FEED` | `claim_id` | `stg_claims` | Direct rename (lower-case). SAS character → STRING. |
| `POLICY_ID` | `RAW_INS.CLAIMS_FEED` | `policy_id` | `stg_claims` | Direct rename. SAS character → STRING. Validated against `POLICIES` via broadcast join. |
| `CLAIMANT_ID` | `RAW_INS.CLAIMS_FEED` | `claimant_id` | `stg_claims` | Direct rename. SAS character → STRING. |
| `CLAIM_TYPE` | `RAW_INS.CLAIMS_FEED` | `claim_type` | `stg_claims` | Direct rename. SAS character → STRING. |
| `CLAIMED_AMOUNT` | `RAW_INS.CLAIMS_FEED` | `claimed_amount` | `stg_claims` | Direct rename. SAS numeric (8 bytes) → DOUBLE. Validated ≤ `SUM_INSURED`. |
| `LOSS_DATE` | `RAW_INS.CLAIMS_FEED` | `loss_date` | `stg_claims` | SAS date numeric → DATE. Validated within policy period (`EFFECTIVE_DATE` – `EXPIRATION_DATE`). |
| `REPORTED_DATE` | `RAW_INS.CLAIMS_FEED` | `reported_date` | `stg_claims` | SAS date numeric → DATE. |
| `CLAIM_STATUS` | `RAW_INS.CLAIMS_FEED` | `claim_status` | `stg_claims` | SAS character → STRING. Formatted via `$CLMSTAT.` in SAS; accepted values enforced by dbt test. |
| `POLICY_TYPE` | `RAW_INS.POLICIES` (hash) | `policy_type` | `stg_claims` | Retrieved via SAS hash lookup on `POLICY_ID` → broadcast join in dbt. SAS character → STRING. |
| `EFFECTIVE_DATE` | `RAW_INS.POLICIES` (hash) | `effective_date` | `stg_claims` | SAS date numeric → DATE. Used for loss-date validation window. |
| `EXPIRATION_DATE` | `RAW_INS.POLICIES` (hash) | `expiration_date` | `stg_claims` | SAS date numeric → DATE. Used for loss-date validation window. |
| `SUM_INSURED` | `RAW_INS.POLICIES` (hash) | `sum_insured` | `stg_claims` | SAS numeric → DOUBLE. Used in claimed-amount validation and adjudication thresholds. |
| `DEDUCTIBLE` | `RAW_INS.POLICIES` (hash) | `deductible` | `stg_claims` | SAS numeric → DOUBLE. Used to compute `APPROVED_AMOUNT`. |
| `FRAUD_SCORE` | `TERA_DW.FRAUD_INDICATORS` | `fraud_score` | `int_claims_adjudication` | SAS numeric → INT. Joined via `POLICY_ID` + `CLAIMANT_ID` (left join). |
| `INDICATOR_FLAGS` | `TERA_DW.FRAUD_INDICATORS` | `indicator_flags` | `int_claims_adjudication` | SAS character → STRING. Carried through for SIU alert narrative. |
| *(derived)* | — | `fraud_risk` | `int_claims_adjudication` | `CASE WHEN fraud_score >= 80 THEN 'HIGH' WHEN fraud_score >= 50 THEN 'MEDIUM' ELSE 'LOW' END`. SAS IF/THEN → SQL CASE. STRING. |
| *(derived)* | — | `adjudication_result` | `int_claims_adjudication` | `CASE WHEN` tree replicating SAS IF/THEN routing. Values: `APPR`, `DENY`, `PEND`. STRING. |
| *(derived)* | — | `adjudication_reason` | `int_claims_adjudication` | Concatenated reason text built from multiple conditions. SAS `catx()` → `concat_ws()`. STRING. |
| *(derived)* | — | `approved_amount` | `int_claims_adjudication` | `GREATEST(0, claimed_amount - deductible)` for approved; `0` for denied; `NULL` for pending. SAS numeric → DOUBLE. |
| *(derived)* | — | `processing_date` | `int_claims_adjudication` | SAS macro variable `&proc_date` → dbt `var('curr_dt')` or `current_date()`. SAS date numeric → DATE. |
