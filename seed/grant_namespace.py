#!/usr/bin/env python3
"""
grant_namespace.py — Give a principal access to what a namespace built.

Schemas created by a dbt run are owned by the identity that ran it, so anyone
else (a presenter opening the catalog in the UI, a reviewer checking the marts)
sees nothing. This grants USE CATALOG on the catalog and USE SCHEMA + SELECT on
the namespace's four output schemas.

    python seed/grant_namespace.py --namespace w1u1 --principal user@example.com

Credentials come from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import os
import sys

from databricks import sql

LAYERS = ("staging", "intermediate", "marts", "curated")


def main() -> int:
    ap = argparse.ArgumentParser(description="Grant read access to a namespace's schemas")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"))
    ap.add_argument("--principal", default=os.environ.get("DEMO_GRANT_PRINCIPAL"),
                    help="User, group or service principal (default: $DEMO_GRANT_PRINCIPAL)")
    ap.add_argument("--raw-schema", default=os.environ.get("RAW_SCHEMA", "raw"),
                    help="Source schema to grant SELECT on as well")
    args = ap.parse_args()

    if not args.principal:
        print("nothing to grant: no --principal and no $DEMO_GRANT_PRINCIPAL")
        return 0

    con = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    principal = f"`{args.principal}`"
    statements = [f"grant use catalog on catalog {args.catalog} to {principal}"]
    schemas = [f"{args.namespace}_{layer}" for layer in LAYERS] + [args.raw_schema]
    for schema in schemas:
        target = f"{args.catalog}.{schema}"
        statements.append(f"grant use schema on schema {target} to {principal}")
        statements.append(f"grant select on schema {target} to {principal}")

    cur = con.cursor()
    failed = False
    try:
        for stmt in statements:
            try:
                cur.execute(stmt)
                print(f"OK    {stmt}")
            except Exception as exc:  # noqa: BLE001 - one missing schema must not stop the rest
                # A layer with no models in this namespace has no schema to grant on.
                if "SCHEMA_DOES_NOT_EXIST" in str(exc):
                    print(f"SKIP  {stmt} (schema not built)")
                    continue
                failed = True
                print(f"FAIL  {stmt}: {exc}", file=sys.stderr)
    finally:
        cur.close()
        con.close()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
