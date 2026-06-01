"""
claims_processing.py — PySpark port of Programs/Insurance/claims_processing.sas

This is the *alternative* migration path to dbt: a custom PySpark ETL job that
replaces a procedural SAS DATA step program with hash-object lookups, fraud
screening, and IF/THEN auto-adjudication routing to multiple outputs.

Why PySpark here (vs. dbt elsewhere):
  claims_processing.sas is procedural and multi-output (CLAIMS_REGISTER,
  CLAIMS_REVIEW_QUEUE, FRAUD_ALERTS). When the SAS logic is row-oriented and
  fans out to several targets, a single imperative job is often a more faithful
  (and more readable) port than a chain of SQL models. The dbt project handles
  the set-based transformations; this job demonstrates the imperative pattern.

SAS construct -> PySpark mapping:
  declare hash h_pol(...)              -> broadcast join to active policies
  IF/THEN output CLAIMS_VALID/INVALID  -> filter() into separate DataFrames
  PROC SQL fraud join                  -> left join on claim_id
  DATA step adjudication IF/THEN       -> when().otherwise() column expression
  PROC APPEND base=STG_INS.*           -> DataFrame.write.saveAsTable(mode=append)

Run (Databricks job, serverless):
  spark_python_task with parameters --catalog <cat> --schema <prefix>
Outputs (created if missing): <catalog>.<prefix>_curated.{claims_register,
  claims_review_queue, fraud_alerts}
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SAS claims_processing.sas -> PySpark")
    parser.add_argument("--catalog", default="banking_analytics",
                        help="Unity Catalog catalog holding the raw schema and outputs")
    parser.add_argument("--raw_schema", default="raw",
                        help="Schema holding raw claims/policies/fraud feeds")
    parser.add_argument("--schema", default="dev",
                        help="Output schema prefix; outputs land in <schema>_curated")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    raw = f"{args.catalog}.{args.raw_schema}"
    out_schema = f"{args.catalog}.{args.schema}_curated"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {out_schema}")

    # ------------------------------------------------------------------
    # Step 1: Ingest and validate (SAS: hash lookup to active policies)
    # ------------------------------------------------------------------
    claims = spark.table(f"{raw}.claims")
    policies = (
        spark.table(f"{raw}.policies")
        .where(F.col("policy_status") == "ACTIVE")
        .select(
            "policy_id", "policy_type", "sum_insured", "deductible",
            "effective_date", "expiry_date",
        )
    )

    # SAS hash object h_pol.find() -> broadcast join (policy table is small)
    joined = claims.join(F.broadcast(policies), on="policy_id", how="left")

    valid = joined.where(
        F.col("policy_type").isNotNull()
        & (F.col("loss_date") >= F.col("effective_date"))
        & (F.col("loss_date") <= F.col("expiry_date"))
        & (F.col("claimed_amount") <= F.col("sum_insured"))
    )
    invalid_count = joined.count() - valid.count()

    # ------------------------------------------------------------------
    # Step 2: Fraud screening (SAS: PROC SQL join to FRAUD_INDICATORS)
    # ------------------------------------------------------------------
    fraud = (
        spark.table(f"{raw}.fraud_indicators")
        .select("claim_id", (F.col("fraud_score") * 100).alias("fraud_score"))
    )
    screened = valid.join(fraud, on="claim_id", how="left").withColumn(
        "fraud_score", F.coalesce(F.col("fraud_score"), F.lit(0.0))
    ).withColumn(
        "fraud_risk",
        F.when(F.col("fraud_score") >= 80, F.lit("HIGH"))
         .when(F.col("fraud_score") >= 50, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW")),
    )

    # ------------------------------------------------------------------
    # Step 3: Auto-adjudication rules (SAS DATA step IF/THEN routing)
    # ------------------------------------------------------------------
    adjudicated = screened.withColumn(
        "adjudication_result",
        F.when(F.col("fraud_risk") == "HIGH", F.lit("DENY"))
         .when(
             (F.col("fraud_risk") == "LOW")
             & (F.col("claimed_amount") <= 5000)
             & (F.col("policy_type").isin("AUTO", "HOME")),
             F.lit("APPR"),
         )
         .when(
             (F.col("fraud_risk") == "LOW")
             & (F.col("claimed_amount") <= F.col("sum_insured") * 0.25)
             & (F.col("claimed_amount") <= 50000),
             F.lit("APPR"),
         )
         .otherwise(F.lit("PEND")),
    ).withColumn(
        "approved_amount",
        F.when(F.col("adjudication_result") == "APPR",
               F.greatest(F.lit(0.0), F.col("claimed_amount") - F.col("deductible")))
         .when(F.col("adjudication_result") == "DENY", F.lit(0.0))
         .otherwise(F.lit(None).cast("double")),
    ).withColumn("processing_date", F.current_date())

    # ------------------------------------------------------------------
    # Step 4: Write outputs (SAS: PROC APPEND to STG_INS.* tables)
    # ------------------------------------------------------------------
    register_cols = [
        "claim_id", "policy_id", "claimant_id", "claim_type", "policy_type",
        "claimed_amount", "fraud_score", "fraud_risk", "adjudication_result",
        "approved_amount", "processing_date",
    ]
    register = adjudicated.select(*register_cols)
    register.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{out_schema}.claims_register"
    )

    review_queue = adjudicated.where(F.col("adjudication_result") == "PEND").select(*register_cols)
    review_queue.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{out_schema}.claims_review_queue"
    )

    fraud_alerts = (
        adjudicated.where(F.col("fraud_risk") == "HIGH")
        .select(
            "claim_id", "policy_id", "claimant_id", "fraud_score",
            F.col("claimed_amount"), F.current_date().alias("alert_date"),
        )
    )
    fraud_alerts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{out_schema}.fraud_alerts"
    )

    # SAS %put NOTE: summary counts
    print("=" * 50)
    print("claims_processing (PySpark) completed")
    print(f"  Valid claims:      {register.count()}")
    print(f"  Invalid (dropped): {invalid_count}")
    print(f"  Manual review:     {review_queue.count()}")
    print(f"  Fraud alerts:      {fraud_alerts.count()}")
    print(f"  Outputs written to {out_schema}.*")
    print("=" * 50)


if __name__ == "__main__":
    main()
