#!/usr/bin/env python3
"""
check_connection.py — Fail fast, and say which part is wrong.

A demo run that starts against the wrong workspace, a deleted SQL warehouse or a
catalog the token cannot see fails deep inside dbt with an opaque error. This
prints one line per prerequisite — workspace, warehouse, catalog, raw schema —
and, when the configured warehouse does not exist, lists the ones the token can
actually see so the fix is obvious.

    python verify/check_connection.py --catalog banking_analytics --raw-schema raw_sas

Credentials come from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request

from databricks import sql

REQUIRED_VARS = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")


def list_warehouses(host: str, token: str) -> list[tuple[str, str]]:
    req = urllib.request.Request(
        f"{host.rstrip('/')}/api/2.0/sql/warehouses",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            import json

            payload = json.load(resp)
    except (urllib.error.URLError, ValueError):
        return []
    return [(w.get("id", ""), w.get("name", "")) for w in payload.get("warehouses", [])]


def main() -> int:
    ap = argparse.ArgumentParser(description="Check the Databricks demo prerequisites")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--raw-schema", default=os.environ.get("RAW_SCHEMA", "raw"))
    args = ap.parse_args()

    missing = [v for v in REQUIRED_VARS if not os.environ.get(v)]
    if missing:
        print(f"FAIL  env: {', '.join(missing)} not set (see .env.example)", file=sys.stderr)
        return 2

    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]
    print(f"OK    workspace: {host}")

    try:
        con = sql.connect(
            server_hostname=host.replace("https://", ""),
            http_path=http_path,
            access_token=token,
        )
    except Exception as exc:  # noqa: BLE001 - surface the driver's own message
        print(f"FAIL  warehouse {http_path}: {exc}", file=sys.stderr)
        for wid, name in list_warehouses(host, token):
            print(f"      available: /sql/1.0/warehouses/{wid}  ({name})", file=sys.stderr)
        return 1
    print(f"OK    warehouse: {http_path}")

    cur = con.cursor()
    try:
        cur.execute(f"select count(*) from {args.catalog}.information_schema.tables "
                    f"where table_schema = '{args.raw_schema}'")
        row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL  catalog {args.catalog}: {exc}", file=sys.stderr)
        return 1
    finally:
        cur.close()
        con.close()

    n_tables = row[0] if row else 0
    print(f"OK    catalog:   {args.catalog}")
    if not n_tables:
        print(f"FAIL  raw schema {args.catalog}.{args.raw_schema}: no tables visible",
              file=sys.stderr)
        return 1
    print(f"OK    raw schema: {args.catalog}.{args.raw_schema} ({n_tables} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
