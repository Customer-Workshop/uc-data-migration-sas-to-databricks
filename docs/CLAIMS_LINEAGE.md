# Claims Processing — Data Lineage

> **Draft — pending data steward review**

End-to-end data lineage for the `claims_processing.sas` migration,
showing source tables, dbt models, join keys, and filters.

## Lineage Diagram

```mermaid
flowchart LR
    CF["RAW_INS.CLAIMS_FEED<br/><i>Source: daily claims intake</i>"]
    POL["RAW_INS.POLICIES<br/><i>Source: policy master</i>"]
    FI["TERA_DW.FRAUD_INDICATORS<br/><i>Source: fraud scoring model</i>"]

    STG["stg_claims<br/><i>Staging: validated &amp; enriched claims</i>"]
    INT["int_claims_adjudication<br/><i>Intermediate: fraud screen &amp; adjudication</i>"]

    CF -- "all columns<br/>filter: policy active &amp; loss_date in period &amp; amount ≤ sum_insured" --> STG
    POL -- "broadcast join on POLICY_ID<br/>filter: STATUS = 'ACTIVE'" --> STG
    STG -- "all stg columns" --> INT
    FI -- "left join on POLICY_ID + CLAIMANT_ID<br/>columns: FRAUD_SCORE, INDICATOR_FLAGS" --> INT
```

## Edge Details

| From | To | Join Key | Filter / Transform |
|---|---|---|---|
| `RAW_INS.CLAIMS_FEED` | `stg_claims` | *(direct ingest)* | Exclude claims where: policy not found/inactive, `loss_date` outside policy period, `claimed_amount` > `sum_insured`. |
| `RAW_INS.POLICIES` | `stg_claims` | `POLICY_ID` (broadcast join) | `STATUS = 'ACTIVE'` — only active policies are joined. Enriches with `POLICY_TYPE`, `EFFECTIVE_DATE`, `EXPIRATION_DATE`, `SUM_INSURED`, `DEDUCTIBLE`. |
| `stg_claims` | `int_claims_adjudication` | *(ref — all rows)* | All validated claims flow through. Adds derived columns: `fraud_risk`, `adjudication_result`, `adjudication_reason`, `approved_amount`, `processing_date`. |
| `TERA_DW.FRAUD_INDICATORS` | `int_claims_adjudication` | `POLICY_ID` + `CLAIMANT_ID` (left join) | No filter — all fraud indicators are joined. NULLs (no match) default to `fraud_risk = 'LOW'`. |
