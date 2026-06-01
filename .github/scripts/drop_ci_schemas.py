#!/usr/bin/env python3
"""Drop the isolated per-run CI schemas created by `dbt build` in dbt_ci.yml.

PR CI builds into a unique namespace (``ci_<run_id>_<attempt>``) so it never
collides with the shared ``dev`` namespace that live demo sessions write to.
dbt materializes one schema per layer (``<prefix>_staging``, ``_intermediate``,
``_marts``, ``_seeds``), so this drops every schema in the catalog whose name
starts with the run's prefix. Runs in an ``always()`` cleanup job so isolated
schemas never accumulate in the shared catalog.
"""
from __future__ import annotations

import argparse
import os
import sys

from databricks import sql


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True, help="Per-run schema prefix, e.g. ci_123_1")
    ap.add_argument("--catalog", default="banking_analytics")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"missing required env var: {var}", file=sys.stderr)
            return 1

    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    con = sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cur = con.cursor()
    cur.execute(
        "select schema_name from {}.information_schema.schemata "
        "where schema_name like ?".format(args.catalog),
        [f"{args.prefix}%"],
    )
    schemas = [r[0] for r in cur.fetchall()]
    if not schemas:
        print(f"no schemas found with prefix {args.prefix!r} in {args.catalog}")
    for s in schemas:
        fqn = f"`{args.catalog}`.`{s}`"
        print(f"dropping {fqn}")
        cur.execute(f"drop schema if exists {fqn} cascade")
    cur.close()
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
