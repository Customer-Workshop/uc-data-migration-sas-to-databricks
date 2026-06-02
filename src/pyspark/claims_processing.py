"""
claims_processing.py — PySpark claims intake, fraud screening, and routing.

Migrated from: ts-sas-legacy-analytics/Programs/Insurance/claims_processing.sas

SAS Original
------------
The SAS macro %claims_processing ingests a daily claims feed, validates each
claim against policy data via a hash-object lookup, screens for fraud using a
Teradata-sourced fraud-indicators table, applies auto-adjudication rules (deny
high-fraud, auto-approve small/low-risk, else manual review), and routes output
to three destinations:

    STG_INS.CLAIMS_REGISTER       — all adjudicated claims
    STG_INS.CLAIMS_REVIEW_QUEUE   — pending manual review
    STG_INS.FRAUD_ALERTS          — high-risk SIU referrals

PySpark Equivalent
------------------
Hash-object lookup  →  broadcast join on policies
IF/THEN routing     →  DataFrame filter + union
PROC APPEND         →  Delta MERGE / overwrite into curated tables

Output tables (in <NS>_curated schema):
    claims_register       — all processed claims with adjudication result
    fraud_alerts          — high-risk claims flagged for SIU
    claims_review_queue   — claims pending manual review

This is a skeleton that Devin fills in during the live conversion demo. The
file path and structure are established so the Asset Bundle job definition
(resources/daily_banking_pipeline.job.yml) can reference it immediately.

Usage (via Databricks Workflows or local spark-submit):
    spark-submit src/pyspark/claims_processing.py --namespace dev --catalog banking_analytics
"""
from __future__ import annotations

import argparse
import sys


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="PySpark claims processing job")
    ap.add_argument("--namespace", default="dev",
                    help="Schema namespace prefix (e.g. dev, alice, ci_123)")
    ap.add_argument("--catalog", default="banking_analytics",
                    help="Unity Catalog name")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    catalog = args.catalog
    ns = args.namespace

    # Schema references following the namespace convention
    staging = f"{catalog}.{ns}_staging"
    intermediate = f"{catalog}.{ns}_intermediate"
    curated = f"{catalog}.{ns}_curated"

    # ── Spark session ────────────────────────────────────────────────────
    from pyspark.sql import SparkSession  # noqa: E402
    import pyspark.sql.functions as F  # noqa: E402

    spark = (
        SparkSession.builder
        .appName(f"claims_processing_{ns}")
        .config("spark.sql.catalog.banking_analytics", "com.databricks.sql.catalog.DatabricksCatalog")
        .getOrCreate()
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {curated}")

    # ── Step 1: Ingest claims feed and validate via broadcast join ────────
    # SAS: hash h_pol(dataset: "RAW_INS.POLICIES(where=(STATUS='ACTIVE'))");
    #      rc = h_pol.find();
    claims_raw = spark.table(f"{staging}.stg_claims")

    policies = (
        spark.table(f"{catalog}.raw.policies")
        .filter(F.col("policy_status") == "ACTIVE")
        .select(
            "policy_id", "policy_type", "effective_date",
            "expiry_date", "sum_insured", "deductible",
        )
    )

    claims_validated = (
        claims_raw
        .join(F.broadcast(policies), on="policy_id", how="inner")
        .filter(
            (F.col("loss_date") >= F.col("effective_date"))
            & (F.col("loss_date") <= F.col("expiry_date"))
            & (F.col("claimed_amount") <= F.col("sum_insured"))
        )
    )

    # ── Step 2: Fraud screening ──────────────────────────────────────────
    # SAS: left join TERA_DW.FRAUD_INDICATORS on POLICY_ID and CLAIMANT_ID
    # The fraud_indicators table is keyed by claim_id (not policy_id/claimant_id),
    # so we join through the claims table's claim_id.
    fraud_indicators = spark.table(f"{catalog}.raw.fraud_indicators")

    claims_screened = (
        claims_validated
        .join(
            fraud_indicators.select("claim_id", "fraud_score"),
            on="claim_id",
            how="left",
        )
        .withColumn(
            "fraud_risk",
            F.when(F.col("fraud_score") >= 80, "HIGH")
            .when(F.col("fraud_score") >= 50, "MEDIUM")
            .otherwise("LOW"),
        )
    )

    # ── Step 3: Auto-adjudication rules ──────────────────────────────────
    # SAS IF/THEN routing to AUTO_ADJUDICATED vs MANUAL_REVIEW
    claims_adjudicated = (
        claims_screened
        .withColumn(
            "adjudication_result",
            F.when(F.col("fraud_risk") == "HIGH", "DENY")
            .when(
                (F.col("fraud_risk") == "LOW")
                & (F.col("claimed_amount") <= 5000)
                & (F.col("policy_type").isin("AUTO", "HOME", "RENT")),
                "APPR",
            )
            .when(
                (F.col("fraud_risk") == "LOW")
                & (F.col("claimed_amount") <= F.col("sum_insured") * 0.25)
                & (F.col("claimed_amount") <= 50000),
                "APPR",
            )
            .otherwise("PEND"),
        )
        .withColumn(
            "approved_amount",
            F.when(
                F.col("adjudication_result") == "APPR",
                F.greatest(F.lit(0), F.col("claimed_amount") - F.col("deductible")),
            ).otherwise(F.lit(0)),
        )
        .withColumn("processing_date", F.current_date())
        .withColumn("claim_status", F.col("adjudication_result"))
    )

    # ── Step 4: Route to output tables ───────────────────────────────────
    # SAS: PROC APPEND base=STG_INS.CLAIMS_REGISTER / CLAIMS_REVIEW_QUEUE / FRAUD_ALERTS

    # Claims register — all adjudicated claims
    claims_adjudicated.write.mode("overwrite").saveAsTable(f"{curated}.claims_register")

    # Fraud alerts — high-risk SIU referrals
    (
        claims_adjudicated
        .filter(F.col("fraud_risk") == "HIGH")
        .withColumn("alert_date", F.current_date())
        .withColumn(
            "alert_reason",
            F.concat(F.lit("Fraud score: "), F.col("fraud_score").cast("string")),
        )
        .write.mode("overwrite")
        .saveAsTable(f"{curated}.fraud_alerts")
    )

    # Claims review queue — pending manual review
    (
        claims_adjudicated
        .filter(F.col("adjudication_result") == "PEND")
        .write.mode("overwrite")
        .saveAsTable(f"{curated}.claims_review_queue")
    )

    print(f"claims_processing completed for namespace '{ns}'")
    print(f"  claims_register     → {curated}.claims_register")
    print(f"  fraud_alerts        → {curated}.fraud_alerts")
    print(f"  claims_review_queue → {curated}.claims_review_queue")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
