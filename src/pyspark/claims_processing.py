"""
claims_processing.py — PySpark multi-output routing job.

Migrated from: Programs/Insurance/claims_processing.sas (Step 4)

SAS Original:
    Step 4 combines AUTO_ADJUDICATED + MANUAL_REVIEW into CLAIMS_COMBINED,
    then PROC APPENDs to three output tables:
        STG_INS.CLAIMS_REGISTER     (all adjudicated claims)
        STG_INS.CLAIMS_REVIEW_QUEUE (PEND + DENY claims)
        STG_INS.FRAUD_ALERTS        (HIGH fraud-risk claims with alert metadata)

PySpark Equivalent:
    Reads from the dbt-produced int_claims_adjudication table, adds
    processing metadata, and writes (append mode) to three Delta tables
    in the curated schema. This is procedural multi-output routing that
    cannot be expressed as a single dbt model.

Usage:
    spark-submit src/pyspark/claims_processing.py --namespace session2
    # or via Databricks Workflows / databricks bundle

Environment variables (same as dbt):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

Quirks reproduced from SAS source (flagged, not fixed):
    - QUIRK: FRAUD_ALERTS includes all HIGH fraud-risk claims, including those
      already routed to MANUAL_REVIEW with DENY. The alert is therefore
      duplicated between CLAIMS_REVIEW_QUEUE and FRAUD_ALERTS for HIGH-risk
      claims. Source-faithful.
    - QUIRK: CLAIMS_REVIEW_QUEUE includes DENY (HIGH fraud) claims alongside
      PEND claims. In the SAS source, the MANUAL_REVIEW dataset receives both
      HIGH-risk DENY rows (output from rule 1) and PEND rows (output from
      the else branch). Source-faithful.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    """Get or create a SparkSession connected to Databricks via Connect."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    cluster_id = http_path.split("/")[-1] if "/warehouses/" in http_path else None

    # Try Databricks Connect first; fall back to databricks-sql for remote SQL
    try:
        from databricks.connect import DatabricksSession

        builder = DatabricksSession.builder.remote(
            host=f"https://{host}",
            token=token,
            cluster_id=cluster_id or http_path,
        )
        return builder.getOrCreate()
    except ImportError:
        # Fall back to local Spark + JDBC (e.g. in CI or local dev)
        return (
            SparkSession.builder
            .appName("claims_processing")
            .getOrCreate()
        )


def read_adjudicated(spark: SparkSession, catalog: str, ns: str) -> DataFrame:
    """Read the dbt-produced intermediate adjudication table."""
    table = f"{catalog}.{ns}_intermediate.int_claims_adjudication"
    return spark.read.table(table)


def write_claims_register(df: DataFrame, catalog: str, ns: str) -> int:
    """
    Write all adjudicated claims to CLAIMS_REGISTER (SAS: PROC APPEND
    base=STG_INS.CLAIMS_REGISTER).
    """
    out = df.select(
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
        "fraud_score",
        "fraud_risk",
        "adjudication_result",
        "adjudication_reason",
        "approved_amount",
    ).withColumn(
        "processing_date", F.current_date()
    ).withColumn(
        # SAS: CLAIM_STATUS = ADJUDICATION_RESULT
        "claim_status", F.col("adjudication_result")
    )
    target = f"{catalog}.{ns}_curated.claims_register"
    out.write.mode("overwrite").saveAsTable(target)
    return out.count()


def write_claims_review_queue(df: DataFrame, catalog: str, ns: str) -> int:
    """
    Write PEND + DENY claims to CLAIMS_REVIEW_QUEUE (SAS: PROC APPEND
    base=STG_INS.CLAIMS_REVIEW_QUEUE data=WORK.MANUAL_REVIEW).
    """
    review = df.filter(F.col("adjudication_result").isin("PEND", "DENY"))
    out = review.select(
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
        "fraud_score",
        "fraud_risk",
        "indicator_flags",
        "adjudication_result",
        "adjudication_reason",
        "approved_amount",
    ).withColumn("processing_date", F.current_date())
    target = f"{catalog}.{ns}_curated.claims_review_queue"
    out.write.mode("overwrite").saveAsTable(target)
    return out.count()


def write_fraud_alerts(df: DataFrame, catalog: str, ns: str) -> int:
    """
    Write HIGH fraud-risk claims to FRAUD_ALERTS (SAS: PROC APPEND
    base=STG_INS.FRAUD_ALERTS data=WORK.FRAUD_ALERTS).
    """
    high_risk = df.filter(F.col("fraud_risk") == "HIGH")
    out = high_risk.select(
        "claim_id",
        "policy_id",
        "claimant_id",
        "claimed_amount",
        "fraud_score",
        "indicator_flags",
        "fraud_risk",
    ).withColumn(
        # SAS: ALERT_REASON = catx('; ', catx(' ', 'Fraud score:', ...), INDICATOR_FLAGS)
        "alert_reason",
        F.concat_ws("; ",
                    F.concat(F.lit("Fraud score: "), F.col("fraud_score").cast("string")),
                    F.col("indicator_flags"))
    ).withColumn(
        "alert_date", F.current_date()
    )
    target = f"{catalog}.{ns}_curated.fraud_alerts"
    out.write.mode("overwrite").saveAsTable(target)
    return out.count()


def main() -> int:
    ap = argparse.ArgumentParser(description="Claims processing PySpark multi-output job")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"),
                    help="Namespace prefix (default: $DBT_SCHEMA or 'dev')")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2

    spark = get_spark()
    catalog = args.catalog
    ns = args.namespace

    # Ensure curated schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ns}_curated")

    print(f"Reading adjudicated claims from {catalog}.{ns}_intermediate ...")
    df = read_adjudicated(spark, catalog, ns)
    total = df.count()
    print(f"  Total adjudicated claims: {total}")

    n_register = write_claims_register(df, catalog, ns)
    n_review = write_claims_review_queue(df, catalog, ns)
    n_fraud = write_fraud_alerts(df, catalog, ns)

    print("============================================")
    print("claims_processing completed")
    print(f"  Claims register: {n_register}")
    print(f"  Manual review queue: {n_review}")
    print(f"  Fraud alerts: {n_fraud}")
    print("============================================")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
