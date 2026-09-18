#!/usr/bin/env python3
"""
load_sas_estate.py — load the legacy SAS estate's own seed extracts into the
Databricks "before" schema, so the target runs on EXACTLY the data the SAS
container ran on.

Why this exists
---------------
`generate_and_load.py` fabricates synthetic raw data with Faker. That is fine for
exercising the dbt project, but it cannot be reconciled against a SAS run,
because SAS never saw that data. The legacy estate (`ts-sas-legacy-analytics`)
ships deterministic CSV extracts under `Data/csv/` that its local driver loads
into ORA_DW / RAW_BANK / CURATED before running the four banking programs. The
Dockerised SAS runtime runs on those CSVs and exports golden outputs; loading
the same CSVs here is what makes `verify/reconcile.py --sas-golden` a real
source-vs-target comparison instead of a plausibility check.

Column types follow the estate's own loader (`Data/load_seed_data.sas`): the
columns it reads with `$` are STRING, the ones it reads with `date9.` are DATE,
everything else is DOUBLE. Column names are lower-cased to match the dbt
sources; values are not transformed.

Usage
-----
    python seed/load_sas_estate.py --estate ../ts-sas-legacy-analytics
    python seed/load_sas_estate.py --estate ../ts-sas-legacy-analytics --schema raw

Auth (env vars, same ones dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
    DATABRICKS_CATALOG (optional, default banking_analytics)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from databricks import sql as dbsql


@dataclass(frozen=True)
class Extract:
    """One CSV extract in the estate and the raw table it becomes."""
    table: str          # target table name in <catalog>.<schema>
    path: str           # glob relative to <estate>/Data/csv
    sas_libref: str     # where the SAS estate loads it (for the report)
    char: frozenset[str] = field(default_factory=frozenset)
    date: frozenset[str] = field(default_factory=frozenset)


# Mirrors Data/load_seed_data.sas in the estate: `length … $` -> char, `informat … date9.` -> date.
EXTRACTS: list[Extract] = [
    Extract("cust_demographics", "oracle_dw/CUST_DEMOGRAPHICS.csv", "ORA_DW",
            char=frozenset({"customer_id", "first_name", "last_name", "ssn_hash", "customer_segment",
                            "region_code", "primary_email", "phone_number"}),
            date=frozenset({"date_of_birth"})),
    Extract("cust_accounts", "oracle_dw/CUST_ACCOUNTS.csv", "ORA_DW",
            char=frozenset({"account_id", "customer_id", "account_type", "account_status",
                            "branch_id", "officer_id"}),
            date=frozenset({"open_date", "close_date", "last_activity_date"})),
    Extract("bureau_scores", "oracle_dw/BUREAU_SCORES.csv", "ORA_DW",
            char=frozenset({"customer_id"}), date=frozenset({"score_date"})),
    Extract("payment_history", "oracle_dw/PAYMENT_HISTORY.csv", "ORA_DW",
            char=frozenset({"account_id"})),
    Extract("collateral", "oracle_dw/COLLATERAL.csv", "ORA_DW",
            char=frozenset({"account_id"}), date=frozenset({"last_appraisal_date"})),
    Extract("loan_details", "oracle_dw/LOAN_DETAILS.csv", "ORA_DW",
            char=frozenset({"account_id", "loan_purpose"}), date=frozenset({"orig_date"})),
    Extract("daily_rates", "raw_bank/DAILY_RATES.csv", "RAW_BANK",
            char=frozenset({"rate_type"}), date=frozenset({"rate_date"})),
    # The daily feed file(s) RAW_BANK.TXN_FEED_YYYYMMDD -> one raw.daily_transactions table.
    Extract("daily_transactions", "raw_bank/TXN_FEED_*.csv", "RAW_BANK",
            char=frozenset({"transaction_id", "account_id", "transaction_type", "channel",
                            "merchant_category", "description", "currency_code"}),
            date=frozenset({"transaction_date", "post_date"})),
    # The curated history the estate appends into; kept as a raw copy so the
    # target can reproduce the "history + today's feed" semantics if it wants to.
    Extract("curated_daily_transactions_history", "curated/DAILY_TRANSACTIONS.csv", "CURATED",
            char=frozenset({"transaction_id", "account_id", "transaction_type", "channel",
                            "merchant_category", "description", "currency_code"}),
            date=frozenset({"transaction_date", "post_date"})),
]


def sas_date(text: str) -> dt.date:
    """Parse a SAS `date9.` value such as 27NOV2011."""
    return dt.datetime.strptime(text, "%d%b%Y").date()


def read_extract(csv_root: Path, ex: Extract) -> tuple[list[str], list[dict]]:
    files = sorted(glob.glob(str(csv_root / ex.path)))
    if not files:
        raise FileNotFoundError(f"{ex.table}: no file matches {csv_root / ex.path}")
    header: list[str] | None = None
    rows: list[dict] = []
    for f in files:
        with open(f, newline="") as fh:
            rd = csv.DictReader(fh)
            cols = [c.lower() for c in rd.fieldnames or []]
            if header is None:
                header = cols
            elif cols != header:
                raise ValueError(f"{ex.table}: {f} has columns {cols}, expected {header}")
            for raw in rd:
                rows.append({k.lower(): v for k, v in raw.items()})
    assert header is not None
    unknown = (ex.char | ex.date) - set(header)
    if unknown:
        raise ValueError(f"{ex.table}: typed columns not in the CSV header: {sorted(unknown)}")
    return header, rows


def col_type(ex: Extract, col: str) -> str:
    if col in ex.char:
        return "STRING"
    if col in ex.date:
        return "DATE"
    return "DOUBLE"


def literal(ex: Extract, col: str, val: str | None) -> str:
    if val is None or val == "":
        return "NULL"
    t = col_type(ex, col)
    if t == "STRING":
        return "'" + val.replace("'", "''") + "'"
    if t == "DATE":
        return f"DATE'{sas_date(val).isoformat()}'"
    return repr(float(val))


def load(cur, full_schema: str, ex: Extract, header: list[str], rows: list[dict], batch: int = 1000) -> None:
    fq = f"{full_schema}.{ex.table}"
    ddl = ", ".join(f"{c} {col_type(ex, c)}" for c in header)
    cur.execute(f"CREATE OR REPLACE TABLE {fq} ({ddl}) USING DELTA "
                f"COMMENT 'SAS estate extract {ex.sas_libref}.{ex.table.upper()} (Data/csv/{ex.path})'")
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        values = ", ".join(
            "(" + ", ".join(literal(ex, c, r.get(c)) for c in header) + ")" for r in chunk
        )
        cur.execute(f"INSERT INTO {fq} ({', '.join(header)}) VALUES {values}")
    print(f"  {fq}: {len(rows)} rows  (SAS {ex.sas_libref}, {ex.path})")


def main() -> int:
    ap = argparse.ArgumentParser(description="Load the SAS estate's Data/csv extracts as the Databricks 'before' data")
    ap.add_argument("--estate", default=os.environ.get("SAS_ESTATE_DIR", "../ts-sas-legacy-analytics"),
                    help="Path to the ts-sas-legacy-analytics checkout (default: $SAS_ESTATE_DIR or ../ts-sas-legacy-analytics)")
    ap.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG", "banking_analytics"),
                    help="Unity Catalog catalog (default: $DATABRICKS_CATALOG or banking_analytics)")
    ap.add_argument("--schema", default=os.environ.get("DBT_RAW_SCHEMA", "raw"),
                    help="Schema for the 'before' tables (default: $DBT_RAW_SCHEMA or raw)")
    ap.add_argument("--dry-run", action="store_true", help="Read and type-check the CSVs without connecting")
    args = ap.parse_args()

    csv_root = Path(args.estate) / "Data" / "csv"
    if not csv_root.is_dir():
        print(f"ERROR: {csv_root} not found — point --estate at the ts-sas-legacy-analytics checkout", file=sys.stderr)
        return 2

    print(f"Reading SAS estate extracts from {csv_root} ...")
    loaded = [(ex, *read_extract(csv_root, ex)) for ex in EXTRACTS]
    for ex, header, rows in loaded:
        for r in rows:  # fail here, not mid-INSERT, on an unparsable date/number
            for c in header:
                literal(ex, c, r.get(c))
        print(f"  {ex.table}: {len(rows)} rows, {len(header)} columns")
    if args.dry_run:
        return 0

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    conn = dbsql.connect(server_hostname=host, http_path=os.environ["DATABRICKS_HTTP_PATH"],
                         access_token=os.environ["DATABRICKS_TOKEN"])
    cur = conn.cursor()
    full_schema = f"{args.catalog}.{args.schema}"
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
    print(f"Loading into {full_schema} ...")
    for ex, header, rows in loaded:
        load(cur, full_schema, ex, header, rows)
    cur.close()
    conn.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
