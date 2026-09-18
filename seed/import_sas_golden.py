#!/usr/bin/env python3
"""
import_sas_golden.py — turn the legacy estate's golden exports into dbt seeds.

The SAS estate (`ts-sas-legacy-analytics`) ships a Dockerised SAS runtime that
runs the four Banking programs on the estate's own CSV extracts for the fixed
business date 31JAN2024 and writes every output table to /data/sas/golden as
CSV (see docker/sas/export_golden.sas there). This script copies those exports
into dbt_project/seeds/sas_golden/ so the parity tests (dbt_project/tests/
reconcile_golden_*.sql) and verify/reconcile.py can compare the migrated marts
against what SAS actually produced.

Conversions applied (values are otherwise untouched):
  * column names lower-cased (dbt convention; SAS names are case-insensitive)
  * SAS date columns (days since 1960-01-01) -> ISO dates
  * SAS datetime columns (seconds since 1960-01-01) -> ISO timestamps

Usage
-----
    # 1. produce the golden outputs in the estate repo
    cd ../ts-sas-legacy-analytics
    ESTATE_COMMIT=$(git rev-parse HEAD) docker compose -f docker/compose.yml build
    docker compose -f docker/compose.yml run --rm sas
    docker run --rm -v ts-sas-legacy_sasdata:/data/sas -v "$PWD/../golden:/out" \
        debian:bookworm-slim cp -r /data/sas/golden/. /data/sas/reports/. /data/sas/curated/. /out/
    # 2. import them here
    python seed/import_sas_golden.py --golden-dir ../golden
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SEED_DIR = REPO_ROOT / "dbt_project" / "seeds" / "sas_golden"
META_DIR = REPO_ROOT / "verify" / "golden"

SAS_EPOCH = dt.date(1960, 1, 1)

# golden export file -> (seed name, SAS date columns, SAS datetime columns)
TABLES: dict[str, tuple[str, set[str], set[str]]] = {
    "stg_bank__cust_accounts_daily.csv": (
        "sas_golden_cust_accounts_daily",
        {"OPEN_DATE", "CLOSE_DATE", "LAST_ACTIVITY_DATE", "DATE_OF_BIRTH", "SNAPSHOT_DATE"},
        {"LOAD_TIMESTAMP"},
    ),
    "stg_bank__acct_exceptions.csv": (
        "sas_golden_acct_exceptions",
        {"OPEN_DATE", "CLOSE_DATE", "LAST_ACTIVITY_DATE", "DATE_OF_BIRTH", "SNAPSHOT_DATE"},
        {"LOAD_TIMESTAMP"},
    ),
    "curated__daily_transactions.csv": (
        "sas_golden_daily_transactions",
        {"TRANSACTION_DATE", "POST_DATE"},
        set(),
    ),
    "curated__txn_anomalies.csv": (
        "sas_golden_txn_anomalies",
        {"TRANSACTION_DATE", "POST_DATE"},
        set(),
    ),
    "RUNNING_BALANCES.csv": (
        "sas_golden_running_balances",
        {"TRANSACTION_DATE"},
        set(),
    ),
    "curated__risk_scores.csv": (
        "sas_golden_risk_scores",
        {"LAST_APPRAISAL_DATE", "SCORE_DATE"},
        {"SCORE_TIMESTAMP"},
    ),
    "RISK_MIGRATION.csv": (
        "sas_golden_risk_migration",
        {"SCORE_DATE"},
        set(),
    ),
    "RISK_SUMMARY.csv": ("sas_golden_risk_summary", set(), set()),
    "reports__monthly_rwa.csv": ("sas_golden_monthly_rwa", set(), set()),
    "reports__delinquency_aging.csv": ("sas_golden_delinquency_aging", set(), set()),
    "reports__llp_coverage.csv": ("sas_golden_llp_coverage", set(), set()),
    "CAPITAL_ADEQUACY.csv": ("sas_golden_capital_adequacy", set(), set()),
}


def sas_date(value: str) -> str:
    return "" if value == "" else (SAS_EPOCH + dt.timedelta(days=int(float(value)))).isoformat()


def sas_datetime(value: str) -> str:
    if value == "":
        return ""
    base = dt.datetime(1960, 1, 1)
    return (base + dt.timedelta(seconds=float(value))).strftime("%Y-%m-%d %H:%M:%S")


def find(golden_dir: Path, name: str) -> Path | None:
    hits = sorted(golden_dir.rglob(name))
    return hits[0] if hits else None


def convert(src: Path, dest: Path, date_cols: set[str], dt_cols: set[str]) -> int:
    with src.open(newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)
    date_idx = [i for i, c in enumerate(header) if c in date_cols]
    dt_idx = [i for i, c in enumerate(header) if c in dt_cols]
    for row in rows:
        # SAS writes a missing numeric as '.', which Databricks cannot cast
        for i, v in enumerate(row):
            if v == ".":
                row[i] = ""
        for i in date_idx:
            row[i] = sas_date(row[i])
        for i in dt_idx:
            row[i] = sas_datetime(row[i])
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow([c.lower() for c in header])
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Import SAS golden exports as dbt seeds")
    ap.add_argument("--golden-dir", required=True, type=Path,
                    help="Directory holding the estate's golden exports (searched recursively)")
    args = ap.parse_args()

    counts: dict[str, int] = {}
    for filename, (seed, date_cols, dt_cols) in TABLES.items():
        src = find(args.golden_dir, filename)
        if src is None:
            print(f"WARNING: {filename} not found under {args.golden_dir}; skipped")
            continue
        n = convert(src, SEED_DIR / f"{seed}.csv", date_cols, dt_cols)
        counts[seed] = n
        print(f"{filename} -> seeds/sas_golden/{seed}.csv ({n} rows)")

    META_DIR.mkdir(parents=True, exist_ok=True)
    for meta in ("manifest.json", "controls.csv", "row_counts.csv"):
        src = find(args.golden_dir, meta)
        if src is not None:
            shutil.copy(src, META_DIR / meta)
            print(f"{meta} -> verify/golden/{meta}")
    (META_DIR / "seed_row_counts.json").write_text(json.dumps(counts, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
