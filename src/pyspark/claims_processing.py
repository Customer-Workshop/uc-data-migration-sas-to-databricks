#!/usr/bin/env python3
"""
claims_processing.py — PySpark routing job for the daily claims pipeline.

Migrated from: ts-sas-legacy-analytics/Programs/Insurance/claims_processing.sas
               (Step 3 routing + Step 4 register/queue/alert appends).

Why PySpark (and not dbt) for this part
---------------------------------------
The SAS program is *procedural and multi-output*: a single DATA step reads one
input (WORK.FRAUD_CHECK) and routes each row to one of several physical outputs
via `output <dataset>; return;`, and Step 4 fans the results out to three
register tables (CLAIMS_REGISTER, CLAIMS_REVIEW_QUEUE, FRAUD_ALERTS). That
"read once, write to N sinks by rule" shape is exactly what the playbook calls
out as the PySpark path. The set-based *adjudication logic itself* lives in the
dbt model int_claims_adjudication (Steps 2-4 decisions); this job performs only
the row routing onto the curated tables, reading that model as its single input.

Outputs (namespaced, concurrent-safe -- land in <catalog>.<ns>_curated):
    claims_register      <- SAS STG_INS.CLAIMS_REGISTER     (all adjudicated claims)
    claims_review_queue  <- SAS STG_INS.CLAIMS_REVIEW_QUEUE  (MANUAL_REVIEW: DENY + PEND)
    fraud_alerts         <- SAS STG_INS.FRAUD_ALERTS         (FRAUD_RISK = 'HIGH')

SOURCE-FAITHFUL QUIRK (see int_claims_adjudication.sql header, quirk 1):
    DENY claims are part of MANUAL_REVIEW, not the auto-adjudicated set, because
    the SAS high-fraud branch does `output WORK.MANUAL_REVIEW`. routing_target
    (computed in the dbt model) encodes this, so the review-queue filter below is
    `routing_target = 'MANUAL_REVIEW'`, which includes the DENY rows.

Execution engines
-----------------
* engine=spark : real PySpark -- read the intermediate table into one DataFrame
  and route it to the three curated sinks with DataFrame transforms. This is the
  production path (Databricks job task / Asset Bundle).
* engine=sql   : equivalent execution over a Databricks SQL warehouse via
  databricks-sql-connector (CREATE OR REPLACE TABLE AS SELECT), for environments
  that have a warehouse but no Spark cluster (e.g. CI / the reconciliation demo).

Both engines consume the SAME routing predicates and projections defined in
ROUTES below, so they cannot diverge.

Usage:
    python src/pyspark/claims_processing.py --namespace child2 --engine sql
    python src/pyspark/claims_processing.py --namespace child2   # auto-detect

Credentials (engine=sql): DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field

SOURCE_TABLE = "int_claims_adjudication"  # in <catalog>.<ns>_intermediate


@dataclass
class Route:
    """One curated output: a WHERE predicate + a column projection.

    The same `where` / `select_exprs` strings drive both the Spark DataFrame
    path (df.where(...).selectExpr(...)) and the SQL-warehouse path
    (CREATE OR REPLACE TABLE ... AS SELECT ... WHERE ...), guaranteeing parity
    between the two engines.
    """

    name: str
    where: str
    select_exprs: list[str] = field(default_factory=list)


# Columns carried onto the claims register / review queue (mirror the SAS
# CLAIMS_COMBINED / MANUAL_REVIEW layout: identity + adjudication outcome).
_REGISTER_COLS = [
    "claim_id",
    "policy_id",
    "claimant_id",
    "claim_type",
    "claimed_amount",
    "approved_amount",
    "fraud_risk",
    "adjudication_result",
    "adjudication_reason",
    "routing_target",
    "claim_status",
    "claim_status_desc",
    "processing_date",
]

ROUTES: list[Route] = [
    # SAS Step 4: proc append base=STG_INS.CLAIMS_REGISTER data=CLAIMS_COMBINED
    # (CLAIMS_COMBINED = AUTO_ADJUDICATED + MANUAL_REVIEW = every adjudicated claim).
    Route(name="claims_register", where="1 = 1", select_exprs=_REGISTER_COLS),
    # SAS Step 4: proc append base=STG_INS.CLAIMS_REVIEW_QUEUE data=MANUAL_REVIEW.
    # MANUAL_REVIEW = high-fraud DENY (quirk 1) + everything routed to PEND.
    Route(
        name="claims_review_queue",
        where="routing_target = 'MANUAL_REVIEW'",
        select_exprs=_REGISTER_COLS,
    ),
    # SAS Step 2/4: WORK.FRAUD_ALERTS = where FRAUD_RISK='HIGH'; appended to
    # STG_INS.FRAUD_ALERTS. ALERT_DATE = &proc_date (here: processing_date).
    Route(
        name="fraud_alerts",
        where="fraud_alert_flag = true",
        select_exprs=[
            "claim_id",
            "policy_id",
            "claimant_id",
            "fraud_score",
            "indicator_flags",
            "alert_reason",
            "processing_date as alert_date",
        ],
    ),
]


# --------------------------------------------------------------------------- spark
def _get_spark():
    try:
        from databricks.connect import DatabricksSession  # type: ignore

        return DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession  # type: ignore

        return SparkSession.builder.appName("claims_processing").getOrCreate()


def run_spark(catalog: str, ns: str) -> dict:
    spark = _get_spark()
    intermediate = f"{catalog}.{ns}_intermediate"
    curated = f"{catalog}.{ns}_curated"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {curated}")

    # SAS read-once: WORK.FRAUD_CHECK / adjudicated claims -> one source DataFrame.
    src = spark.table(f"{intermediate}.{SOURCE_TABLE}")

    counts: dict = {}
    for route in ROUTES:
        out = src.where(route.where).selectExpr(*route.select_exprs)
        fq = f"{curated}.{route.name}"
        (
            out.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(fq)
        )
        counts[route.name] = out.count()
    return counts


# ----------------------------------------------------------------------------- sql
def run_sql(catalog: str, ns: str) -> dict:
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    con = sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    intermediate = f"{catalog}.{ns}_intermediate"
    curated = f"{catalog}.{ns}_curated"
    counts: dict = {}
    try:
        cur = con.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {curated}")
        for route in ROUTES:
            proj = ", ".join(route.select_exprs)
            fq = f"{curated}.{route.name}"
            cur.execute(
                f"CREATE OR REPLACE TABLE {fq} AS "
                f"SELECT {proj} FROM {intermediate}.{SOURCE_TABLE} "
                f"WHERE {route.where}"
            )
            cur.execute(f"SELECT count(*) FROM {fq}")
            counts[route.name] = cur.fetchone()[0]
        cur.close()
    finally:
        con.close()
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description="Claims processing routing job (PySpark)")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument(
        "--namespace",
        default=os.environ.get("DBT_SCHEMA", "dev"),
        help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')",
    )
    ap.add_argument(
        "--engine",
        choices=["auto", "spark", "sql"],
        default="auto",
        help="auto: use Spark if available, else the SQL warehouse",
    )
    args = ap.parse_args()

    engine = args.engine
    if engine == "auto":
        try:
            import pyspark  # noqa: F401

            engine = "spark"
        except Exception:
            engine = "sql"

    print(f"claims_processing: routing {args.catalog}.{args.namespace}_intermediate."
          f"{SOURCE_TABLE} -> {args.catalog}.{args.namespace}_curated (engine={engine})")
    counts = run_spark(args.catalog, args.namespace) if engine == "spark" \
        else run_sql(args.catalog, args.namespace)

    for name, n in counts.items():
        print(f"  {name}: {n} rows")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
