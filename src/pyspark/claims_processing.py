"""
claims_processing.py — PySpark job for claims processing (Step 4).

Migrated from: Programs/Insurance/claims_processing.sas (Step 4)

SAS Original:
  PROC APPEND to three output tables:
    STG_INS.CLAIMS_REGISTER   — all adjudicated claims (AUTO_ADJUDICATED + MANUAL_REVIEW)
    STG_INS.CLAIMS_REVIEW_QUEUE — PEND claims only (MANUAL_REVIEW)
    STG_INS.FRAUD_ALERTS       — HIGH fraud risk claims with alert_reason + alert_date

PySpark Equivalent:
  Reads from dbt intermediate output (int_claims_adjudication) and writes
  three curated tables. This is procedural/multi-output logic that is better
  suited to PySpark than dbt.

Usage:
  spark-submit src/pyspark/claims_processing.py --namespace child2
  # or via Databricks job configuration
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(namespace: str, catalog: str = "banking_analytics") -> int:
    spark = SparkSession.builder.appName(
        f"claims_processing_{namespace}"
    ).getOrCreate()

    intermediate_schema = f"{catalog}.{namespace}_intermediate"
    curated_schema = f"{catalog}.{namespace}_curated"

    # Ensure curated schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {curated_schema}")

    # Read from dbt intermediate output
    adjudicated = spark.table(f"{intermediate_schema}.int_claims_adjudication")

    # ---- Output 1: Claims Register (all claims) ----
    # SAS: PROC APPEND base=STG_INS.CLAIMS_REGISTER data=WORK.CLAIMS_COMBINED
    claims_register = adjudicated.select(
        "claim_id",
        "policy_id",
        "claimant_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "loss_date",
        "reported_date",
        "policy_type",
        "sum_insured",
        "deductible",
        "fraud_risk",
        "fraud_score",
        "adjudication_result",
        "adjudication_reason",
        "processing_date",
    )

    claims_register.write.mode("overwrite").saveAsTable(
        f"{curated_schema}.claims_register"
    )

    register_count = claims_register.count()
    print(f"NOTE: Claims register: {register_count} rows written")

    # ---- Output 2: Claims Review Queue (PEND only) ----
    # SAS: PROC APPEND base=STG_INS.CLAIMS_REVIEW_QUEUE data=WORK.MANUAL_REVIEW
    # Note: In SAS, MANUAL_REVIEW includes both DENY (HIGH fraud) and PEND.
    # The review queue is the PEND subset only.
    claims_review_queue = adjudicated.filter(
        F.col("adjudication_result") == "PEND"
    ).select(
        "claim_id",
        "policy_id",
        "claimant_id",
        "claim_type",
        "claimed_amount",
        "loss_date",
        "reported_date",
        "policy_type",
        "sum_insured",
        "deductible",
        "fraud_risk",
        "fraud_score",
        "adjudication_result",
        "adjudication_reason",
        "processing_date",
    )

    claims_review_queue.write.mode("overwrite").saveAsTable(
        f"{curated_schema}.claims_review_queue"
    )

    review_count = claims_review_queue.count()
    print(f"NOTE: Claims review queue: {review_count} rows written")

    # ---- Output 3: Fraud Alerts (HIGH fraud risk only) ----
    # SAS: PROC APPEND base=STG_INS.FRAUD_ALERTS data=WORK.FRAUD_ALERTS
    # SAS adds ALERT_REASON and ALERT_DATE to HIGH-risk claims.
    fraud_alerts = adjudicated.filter(
        F.col("fraud_risk") == "HIGH"
    ).select(
        "claim_id",
        "policy_id",
        "claimant_id",
        "claim_type",
        "claimed_amount",
        "loss_date",
        "fraud_score",
        F.col("indicator_flags"),
        # SAS: ALERT_REASON = catx('; ', catx(' ', 'Fraud score:', put(FRAUD_SCORE, 4.)), INDICATOR_FLAGS)
        F.concat_ws(
            "; ",
            F.concat(F.lit("Fraud score: "), F.col("fraud_score").cast("string")),
            F.col("indicator_flags"),
        ).alias("alert_reason"),
        # SAS: ALERT_DATE = "&proc_date"d
        F.col("processing_date").alias("alert_date"),
    )

    fraud_alerts.write.mode("overwrite").saveAsTable(
        f"{curated_schema}.fraud_alerts"
    )

    fraud_count = fraud_alerts.count()
    print(f"NOTE: Fraud alerts: {fraud_count} rows written")

    print("NOTE: ============================================")
    print("NOTE: claims_processing PySpark job completed")
    print(f"NOTE: Claims register: {register_count}")
    print(f"NOTE: Review queue:    {review_count}")
    print(f"NOTE: Fraud alerts:    {fraud_count}")
    print("NOTE: ============================================")

    spark.stop()
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Claims processing PySpark job")
    ap.add_argument(
        "--namespace",
        default=os.environ.get("DBT_SCHEMA", "dev"),
        help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')",
    )
    ap.add_argument(
        "--catalog",
        default="banking_analytics",
        help="Unity Catalog name (default: banking_analytics)",
    )
    args = ap.parse_args()
    raise SystemExit(main(args.namespace, args.catalog))
