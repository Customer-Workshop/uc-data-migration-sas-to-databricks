#!/usr/bin/env python3
"""
claims_processing.py — PySpark job reproducing the full SAS claims pipeline.

Migrated from: Programs/Insurance/claims_processing.sas

This is a procedural PySpark job (not dbt) because the SAS program routes rows
to multiple output tables (CLAIMS_REGISTER, CLAIMS_REVIEW_QUEUE, FRAUD_ALERTS)
— a multi-output pattern that dbt doesn't model well.

The job reproduces the SAS logic step-for-step:
  Step 1: Ingest claims, validate against active policies
  Step 2: Fraud screening (LEFT JOIN to fraud_indicators, classify risk)
  Step 3: Auto-adjudication (route APPR / DENY / PEND)
  Step 4: Write curated outputs (claims_register, claims_review_queue, fraud_alerts)

Source-parity notes (same as the dbt models):
  1. FRAUD_SCORE SCALE DIVERGENCE: SAS thresholds >= 80 / >= 50 imply 0-100
     scale. Seed data uses 0-1 scale. Implemented per SAS exactly.
  2. SAS checks POLICY_TYPE in ('AUTO','HOME','RENT'). Seed has no 'RENT'.
     Preserved source-faithful.
  3. SAS joined fraud_indicators on (POLICY_ID, CLAIMANT_ID); seed keys on
     claim_id. Adapted join key, logic preserved.

Usage:
    python src/pyspark/claims_processing.py --namespace child2
    python src/pyspark/claims_processing.py --namespace child2 --catalog banking_analytics

Auth (env vars):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import os
import sys

from databricks import sql as dbsql


def main() -> int:
    ap = argparse.ArgumentParser(description="Claims processing PySpark job")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--namespace", required=True,
                    help="Output namespace prefix (e.g. child2)")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    conn = dbsql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cur = conn.cursor()

    catalog = args.catalog
    ns = args.namespace
    raw = f"{catalog}.raw"
    curated = f"{catalog}.{ns}_curated"

    # Ensure curated schema exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {curated}")

    # ------------------------------------------------------------------
    # Step 1: Ingest and Validate (SAS: DATA WORK.CLAIMS_VALID)
    # ------------------------------------------------------------------
    print("Step 1: Ingesting and validating claims against active policies...")
    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY VIEW claims_valid AS
        SELECT
            c.claim_id,
            c.policy_id,
            c.claimant_id,
            c.claim_type,
            c.claim_status,
            c.claimed_amount,
            c.loss_date,
            c.reported_date,
            p.policy_type,
            p.effective_date,
            p.expiry_date,
            p.sum_insured,
            p.deductible
        FROM {raw}.claims c
        INNER JOIN {raw}.policies p
            ON c.policy_id = p.policy_id
        WHERE p.policy_status = 'ACTIVE'
          AND c.loss_date >= p.effective_date
          AND c.loss_date <= p.expiry_date
          AND c.claimed_amount <= p.sum_insured
    """)
    valid_count = _scalar(cur, "SELECT count(*) FROM claims_valid")
    print(f"  Valid claims: {valid_count}")

    # ------------------------------------------------------------------
    # Step 2: Fraud Screening (SAS: PROC SQL LEFT JOIN TERA_DW.FRAUD_INDICATORS)
    # ------------------------------------------------------------------
    print("Step 2: Fraud screening...")
    cur.execute(f"""
        CREATE OR REPLACE TEMPORARY VIEW fraud_check AS
        SELECT
            c.*,
            f.fraud_score,
            CASE
                WHEN f.fraud_score >= 80 THEN 'HIGH'
                WHEN f.fraud_score >= 50 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS fraud_risk
        FROM claims_valid c
        LEFT JOIN {raw}.fraud_indicators f
            ON c.claim_id = f.claim_id
    """)
    fraud_high = _scalar(cur, "SELECT count(*) FROM fraud_check WHERE fraud_risk = 'HIGH'")
    print(f"  HIGH risk claims: {fraud_high}")

    # ------------------------------------------------------------------
    # Step 3: Auto-Adjudication (SAS: DATA step IF/THEN routing)
    # ------------------------------------------------------------------
    print("Step 3: Auto-adjudication...")
    cur.execute("""
        CREATE OR REPLACE TEMPORARY VIEW adjudicated AS
        SELECT
            *,
            CASE
                WHEN fraud_risk = 'HIGH' THEN 'DENY'
                WHEN fraud_risk = 'LOW'
                     AND claimed_amount <= 5000
                     AND policy_type IN ('AUTO', 'HOME', 'RENT')
                THEN 'APPR'
                WHEN fraud_risk = 'LOW'
                     AND claimed_amount <= sum_insured * 0.25
                     AND claimed_amount <= 50000
                THEN 'APPR'
                ELSE 'PEND'
            END AS adjudication_result,
            CASE
                WHEN fraud_risk = 'HIGH'
                THEN 'High fraud risk - SIU referral'
                WHEN fraud_risk = 'LOW'
                     AND claimed_amount <= 5000
                     AND policy_type IN ('AUTO', 'HOME', 'RENT')
                THEN 'Auto-approved: low risk, small claim'
                WHEN fraud_risk = 'LOW'
                     AND claimed_amount <= sum_insured * 0.25
                     AND claimed_amount <= 50000
                THEN 'Auto-approved: within 25% of sum insured'
                ELSE 'Manual review required'
            END AS adjudication_reason,
            CASE
                WHEN fraud_risk = 'HIGH' THEN 0
                WHEN fraud_risk = 'LOW'
                     AND claimed_amount <= 5000
                     AND policy_type IN ('AUTO', 'HOME', 'RENT')
                THEN GREATEST(0, claimed_amount - deductible)
                WHEN fraud_risk = 'LOW'
                     AND claimed_amount <= sum_insured * 0.25
                     AND claimed_amount <= 50000
                THEN GREATEST(0, claimed_amount - deductible)
                ELSE NULL
            END AS approved_amount,
            current_date() AS processing_date
        FROM fraud_check
    """)

    appr = _scalar(cur, "SELECT count(*) FROM adjudicated WHERE adjudication_result = 'APPR'")
    deny = _scalar(cur, "SELECT count(*) FROM adjudicated WHERE adjudication_result = 'DENY'")
    pend = _scalar(cur, "SELECT count(*) FROM adjudicated WHERE adjudication_result = 'PEND'")
    print(f"  APPR: {appr}, DENY: {deny}, PEND: {pend}")

    # ------------------------------------------------------------------
    # Step 4: Write Curated Outputs (SAS: PROC APPEND to STG_INS.*)
    # ------------------------------------------------------------------
    print("Step 4: Writing curated outputs...")

    # claims_register — all adjudicated claims (SAS: STG_INS.CLAIMS_REGISTER)
    cur.execute(f"""
        CREATE OR REPLACE TABLE {curated}.claims_register AS
        SELECT
            claim_id,
            policy_id,
            claimant_id,
            claim_type,
            claim_status,
            claimed_amount,
            loss_date,
            reported_date,
            policy_type,
            sum_insured,
            deductible,
            fraud_score,
            fraud_risk,
            adjudication_result,
            adjudication_reason,
            approved_amount,
            processing_date
        FROM adjudicated
    """)
    reg_count = _scalar(cur, f"SELECT count(*) FROM {curated}.claims_register")
    print(f"  claims_register: {reg_count} rows")

    # claims_review_queue — PEND + DENY (SAS: WORK.MANUAL_REVIEW → STG_INS.CLAIMS_REVIEW_QUEUE)
    # SAS routes HIGH→DENY to MANUAL_REVIEW and all PEND to MANUAL_REVIEW
    cur.execute(f"""
        CREATE OR REPLACE TABLE {curated}.claims_review_queue AS
        SELECT
            claim_id,
            policy_id,
            claimant_id,
            claim_type,
            claimed_amount,
            fraud_score,
            fraud_risk,
            adjudication_result,
            adjudication_reason,
            processing_date
        FROM adjudicated
        WHERE adjudication_result IN ('DENY', 'PEND')
    """)
    review_count = _scalar(cur, f"SELECT count(*) FROM {curated}.claims_review_queue")
    print(f"  claims_review_queue: {review_count} rows")

    # fraud_alerts — HIGH risk only (SAS: WORK.FRAUD_ALERTS → STG_INS.FRAUD_ALERTS)
    cur.execute(f"""
        CREATE OR REPLACE TABLE {curated}.fraud_alerts AS
        SELECT
            claim_id,
            policy_id,
            claimant_id,
            fraud_score,
            fraud_risk,
            CONCAT('Fraud score: ', CAST(fraud_score AS STRING), '; ', claim_type)
                AS alert_reason,
            processing_date AS alert_date
        FROM adjudicated
        WHERE fraud_risk = 'HIGH'
    """)
    alert_count = _scalar(cur, f"SELECT count(*) FROM {curated}.fraud_alerts")
    print(f"  fraud_alerts: {alert_count} rows")

    cur.close()
    conn.close()

    print()
    print("=" * 50)
    print("Claims processing complete")
    print(f"  Valid claims:        {valid_count}")
    print(f"  Auto-approved:       {appr}")
    print(f"  Denied (fraud):      {deny}")
    print(f"  Pending review:      {pend}")
    print(f"  Fraud alerts:        {alert_count}")
    print("=" * 50)
    return 0


def _scalar(cur, query: str):
    cur.execute(query)
    row = cur.fetchone()
    return row[0] if row else None


if __name__ == "__main__":
    raise SystemExit(main())
