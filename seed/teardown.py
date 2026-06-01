#!/usr/bin/env python3
"""
teardown.py — drop the per-namespace output schemas for a demo run.

The "before" state (raw source tables in banking_analytics.raw) is durable and
is NOT touched by this script. Only the "after" output schemas a run created are
dropped, so you can reset (or clear one of several concurrent demo spaces)
without disturbing anything else.

For namespace NS, this drops (CASCADE):
    <catalog>.<NS>_staging
    <catalog>.<NS>_intermediate
    <catalog>.<NS>_marts
    <catalog>.<NS>_seeds
    <catalog>.<NS>_curated   (PySpark claims_processing outputs)

Usage:
    python seed/teardown.py --namespace alice
    python seed/teardown.py --namespace dev --catalog banking_analytics

Auth (same env vars dbt + the seeder use):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import os
import sys

from databricks import sql as dbsql

LAYERS = ["staging", "intermediate", "marts", "seeds", "curated"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", required=True,
                    help="Schema prefix used for the run (e.g. dev, alice, run2)")
    ap.add_argument("--catalog", default="banking_analytics")
    args = ap.parse_args()

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    conn = dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)
    cur = conn.cursor()
    for layer in LAYERS:
        schema = f"{args.catalog}.{args.namespace}_{layer}"
        print(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    cur.close()
    conn.close()
    print(f"Namespace '{args.namespace}' torn down. Raw 'before' data untouched.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
