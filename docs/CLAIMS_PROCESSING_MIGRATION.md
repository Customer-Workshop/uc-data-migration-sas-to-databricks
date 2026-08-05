# Claims Processing SAS → dbt Migration

## Construct mapping

| SAS construct | dbt/Databricks implementation |
|---|---|
| `RAW_INS.CLAIMS_FEED_YYYYMMDD` | `source('insurance_raw', 'claims')`; the raw claims register stands in for the absent daily feed table |
| `RAW_INS.POLICIES(where=(STATUS='ACTIVE'))` | Active-policy inner join on `insurance_raw.policies.policy_status` |
| SAS hash lookup | `stg_claims` inner join |
| `WORK.CLAIMS_VALID` | `stg_claims` view |
| `WORK.FRAUD_CHECK` | `int_claims_adjudication` fraud join and risk band |
| Ordered DATA-step `IF/THEN` with `return` | Ordered SQL `case` expressions in `int_claims_adjudication` |
| `WORK.AUTO_ADJUDICATED` + `WORK.MANUAL_REVIEW` | `mart_claims_register` |
| `STG_INS.CLAIMS_REVIEW_QUEUE` | `mart_claims_review_queue` |
| `STG_INS.FRAUD_ALERTS` | `mart_fraud_alerts` |
| `$CLMSTAT` PROC FORMAT | `macros/format_claim_status.sql` |
| `PROC APPEND` | Full dbt tables; append persistence is documented but not invented |
| `&proc_date` | `current_date()` at dbt run time |

## Fidelity quirks and source divergences

- The daily SAS claims feed has no per-day Unity Catalog table; `raw.claims`
  is the documented stand-in.
- SAS policy `EXPIRATION_DATE` is represented by raw `expiry_date`.
- SAS fraud indicators join on `POLICY_ID` and `CLAIMANT_ID`. The Databricks
  table is keyed by `claim_id`, so the conversion joins on `claim_id`.
  This can differ from SAS composite-key fan-out behavior.
- `INDICATOR_FLAGS` is not present in the raw fraud table. Fraud alert reasons
  therefore retain only the SAS score component; no substitute was invented.
- Fraud thresholds remain exactly `>= 80` for HIGH and `>= 50` for MEDIUM.
  The synthetic seed currently emits scores from `0` through `1`; that is a
  source-data fidelity gap and is not rescaled by the models.
- SAS invalid claims are written only to transient `WORK.CLAIMS_INVALID` and
  are not persisted. The staging model silently drops them.
- SAS drops `VALIDATION_ERROR` and `rc` from every output dataset, including
  the invalid output.
- SAS missing `CLAIMED_AMOUNT` passes the `CLAIMED_AMOUNT > SUM_INSURED`
  validation check. The model preserves that behavior.
- SAS `max(0, x)` ignores missing arguments. Approved amount logic therefore
  coalesces missing arithmetic to zero rather than propagating NULL.
- A missing fraud score fails both threshold comparisons and falls through to
  LOW.
- High-risk claims are assigned `DENY` but written to `MANUAL_REVIEW`.
- SAS uses `put(FRAUD_SCORE, 4.)` in alert text; the model rounds to an
  integer before concatenating the score.
- SAS conditionally appends fraud alerts only when the set is nonempty. An
  empty filtered dbt table is the equivalent no-op.
- SAS uses `PROC APPEND` for the claims register and queues. dbt materializes
  full tables and does not fabricate append-history behavior.

## Verification limitation

The Databricks workspace token returns HTTP 403 / invalid access token.
Consequently, `make demo-up NS=child4`, `make reconcile NS=child4`, and bundle
deployment were not executed. No reconciliation report or live row counts are
fabricated. Static verification is limited to CI-equivalent `make lint` and
`make parse`.
