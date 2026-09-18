#!/usr/bin/env python3
"""
reconcile.py — Source -> target reconciliation report for the SAS -> Databricks
migration.

Why this exists
---------------
The point of the migration is not just to produce *some* output on Databricks —
it is to produce output we can *trust* matches what the legacy SAS estate would
have produced. Because there is no live SAS runtime here, "trust" is established
the same way a SAS analyst established it by hand: deterministic reconciliation
controls (row counts, control totals, domain coverage, referential integrity)
between the raw source and the converted marts.

dbt schema/singular tests already gate these invariants on every build (see
dbt_project/tests/reconcile_*.sql). This script is the human-facing companion:
it runs the same family of controls and prints a single reconciliation report you
can show live and attach to a PR. It exits non-zero if any control fails, so it
also works as a CI / pre-merge gate.

This is the harness *framework* with the banking-domain control
(`account_completeness`). When a new program is converted, its conversion adds
the matching controls here (e.g. risk-weight parity, control totals, cross-engine
PySpark checks) — see .workshop/playbooks/sas-to-databricks-conversion.devin.md
and .agents/skills/sas-to-databricks-conversion/SKILL.md for the reconciliation contract.

When the legacy estate has actually been run (the OpenSAS container in
ts-sas-legacy-analytics exports golden CSVs), `--sas-golden <dir>` adds a second
family of controls on top: SAS output vs Databricks output, table by table and
control total by control total. Golden tables whose SAS program has not been
converted yet are reported SKIP, not PASS.

Usage
-----
    python verify/reconcile.py --namespace dev
    python verify/reconcile.py --namespace run1 --catalog banking_analytics
    python verify/reconcile.py --namespace dev --report reconciliation_report.md
    python verify/reconcile.py --namespace baseline --raw-schema raw_sas \
        --sas-golden verify/fixtures/sas_golden

Credentials are read from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from databricks import sql


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | FAIL | SKIP
    detail: str = ""
    metrics: dict = field(default_factory=dict)
    program: str = ""  # SAS program the control belongs to


# SAS output table -> (converted relation attribute, model name, SAS program).
# A None relation means the program has not been converted yet: its golden rows
# are reported SKIP so the gap stays visible instead of silently passing.
GOLDEN_TABLE_MAP = {
    "STG_BANK.CUST_ACCOUNTS_DAILY": ("intermediate", "int_account_metrics",
                                     "load_customer_accounts.sas"),
    "STG_BANK.ACCT_EXCEPTIONS": (None, None, "load_customer_accounts.sas"),
    "CURATED.DAILY_TRANSACTIONS": ("marts", "mart_daily_transactions",
                                   "daily_transaction_processing.sas"),
    "CURATED.TXN_ANOMALIES": ("marts", "mart_transaction_anomalies",
                              "daily_transaction_processing.sas"),
    "CURATED.RISK_SCORES": ("marts", "mart_risk_scores", "credit_risk_scoring.sas"),
    "REPORTS.MONTHLY_RWA": ("marts", "mart_regulatory_rwa",
                            "monthly_regulatory_reporting.sas"),
    "REPORTS.DELINQUENCY_AGING": ("marts", "mart_delinquency_aging",
                                  "monthly_regulatory_reporting.sas"),
    "REPORTS.LLP_COVERAGE": ("marts", "mart_llp_coverage",
                             "monthly_regulatory_reporting.sas"),
}

# SAS control total -> (relation attribute, query template, SAS program).
GOLDEN_CONTROL_MAP = {
    "TXN_ANOMALIES.HIGH_AMOUNT": (
        "marts",
        "select count(*) from {rel}.mart_transaction_anomalies"
        " where anomaly_type = 'HIGH_AMOUNT'",
        "daily_transaction_processing.sas",
    ),
    "TXN_ANOMALIES.OVERDRAFT": (
        "marts",
        "select count(*) from {rel}.mart_transaction_anomalies"
        " where anomaly_type = 'OVERDRAFT'",
        "daily_transaction_processing.sas",
    ),
    "DAILY_TRANSACTIONS.SUM_AMOUNT": (
        "marts",
        "select round(sum(transaction_amount), 2) from {rel}.mart_daily_transactions",
        "daily_transaction_processing.sas",
    ),
    "RISK_SCORES.N_ACCOUNTS": (
        "marts",
        "select count(*) from {rel}.mart_risk_scores",
        "credit_risk_scoring.sas",
    ),
    "MONTHLY_RWA.SUM_RWA": (
        "marts",
        "select round(sum(rwa), 2) from {rel}.mart_regulatory_rwa",
        "monthly_regulatory_reporting.sas",
    ),
}


class Reconciler:
    def __init__(self, catalog: str, namespace: str, raw_schema: str = "raw",
                 sas_golden: str | None = None):
        host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
        self.con = sql.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        self.catalog = catalog
        self.ns = namespace
        self.raw_schema = raw_schema
        self.sas_golden = Path(sas_golden) if sas_golden else None
        self.raw = f"{catalog}.{raw_schema}"
        self.staging = f"{catalog}.{namespace}_staging"
        self.intermediate = f"{catalog}.{namespace}_intermediate"
        self.marts = f"{catalog}.{namespace}_marts"
        self.curated = f"{catalog}.{namespace}_curated"
        self.results: list[CheckResult] = []

    def _scalar(self, query: str):
        cur = self.con.cursor()
        try:
            cur.execute(query)
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()

    @property
    def _run_date_sql(self) -> str:
        """The business date the models were built as of (see macros/sas_run_date.sql)."""
        run_date = os.environ.get("RUN_DATE", "")
        return f"date '{run_date}'" if run_date else "current_date()"

    # ------------------------------------------------------------------ checks
    def check_account_completeness(self):
        """Model accounts must equal the documented in-scope raw population."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= {self._run_date_sql}
            """
        )
        actual = self._scalar(f"select count(*) from {self.intermediate}.int_account_metrics")
        ok = expected == actual
        self.results.append(
            CheckResult(
                "account_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope raw accounts = {expected}, model accounts = {actual}",
                {"expected": expected, "actual": actual},
                program="load_customer_accounts.sas",
            )
        )

    # ------------------------------------------------- SAS golden comparison
    def _read_golden_csv(self, name: str) -> list[dict]:
        path = self.sas_golden / name
        if not path.exists():
            return []
        with path.open(newline="") as fh:
            return list(csv.DictReader(fh))

    @staticmethod
    def _as_number(value: str):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def check_sas_row_counts(self):
        """Every SAS output table must have the same row count as its model."""
        rows = self._read_golden_csv("row_counts.csv")
        if not rows:
            self.results.append(CheckResult(
                "sas_row_counts", "SKIP",
                f"no row_counts.csv under {self.sas_golden}",
            ))
            return
        for row in rows:
            table = (row.get("TABLE_NAME") or "").strip().upper()
            expected = self._as_number(row.get("N_ROWS"))
            layer, model, program = GOLDEN_TABLE_MAP.get(table, (None, None, "unmapped"))
            name = f"sas_rows::{table.lower()}"
            if layer is None:
                self.results.append(CheckResult(
                    name, "SKIP",
                    f"SAS rows = {expected:.0f}; no converted model yet"
                    if expected is not None else "no converted model yet",
                    {"sas": expected},
                    program=program,
                ))
                continue
            actual = self._scalar(f"select count(*) from {getattr(self, layer)}.{model}")
            ok = expected is not None and float(actual) == expected
            self.results.append(CheckResult(
                name, "PASS" if ok else "FAIL",
                f"SAS {table} = {expected:.0f}, {model} = {actual}",
                {"sas": expected, "databricks": actual, "model": model},
                program=program,
            ))

    def check_sas_control_totals(self):
        """Control totals (sums, anomaly mix) must match SAS to the cent."""
        rows = self._read_golden_csv("controls.csv")
        if not rows:
            self.results.append(CheckResult(
                "sas_control_totals", "SKIP",
                f"no controls.csv under {self.sas_golden}",
            ))
            return
        for row in rows:
            control = (row.get("CONTROL") or "").strip().upper()
            expected = self._as_number(row.get("VALUE"))
            layer, query, program = GOLDEN_CONTROL_MAP.get(control, (None, None, "unmapped"))
            name = f"sas_control::{control.lower()}"
            if layer is None:
                self.results.append(CheckResult(
                    name, "SKIP", "no converted model yet",
                    {"sas": expected}, program=program,
                ))
                continue
            actual = self._as_number(str(self._scalar(query.format(rel=getattr(self, layer)))))
            ok = (expected is not None and actual is not None
                  and abs(actual - expected) <= 0.01)
            self.results.append(CheckResult(
                name, "PASS" if ok else "FAIL",
                f"SAS = {expected}, Databricks = {actual}",
                {"sas": expected, "databricks": actual},
                program=program,
            ))

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        if self.sas_golden:
            self.check_sas_row_counts()
            self.check_sas_control_totals()
        self.con.close()
        return all(r.status != "FAIL" for r in self.results)

    def render(self) -> str:
        icon = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "SKIP"}
        lines = [
            f"# Reconciliation Report — {self.catalog} / namespace `{self.ns}`",
            "",
            f"Raw source: `{self.raw}`"
            + (f" · SAS golden: `{self.sas_golden}`" if self.sas_golden else ""),
            "",
            "Source -> target controls proving the converted marts match the legacy",
            "SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite",
            "(e.g. an unconverted SAS program) has not been produced yet.",
            "",
            "| SAS program | Control | Result | Detail |",
            "|---|---|---|---|",
        ]
        for r in sorted(self.results, key=lambda r: (r.program, r.name)):
            lines.append(f"| {r.program or '—'} | `{r.name}` | {icon[r.status]} | {r.detail} |")
        passed = sum(r.status == "PASS" for r in self.results)
        failed = sum(r.status == "FAIL" for r in self.results)
        skipped = sum(r.status == "SKIP" for r in self.results)
        lines += ["", f"**{passed} passed, {failed} failed, {skipped} skipped**", ""]
        return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="SAS -> Databricks reconciliation report")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--raw-schema", default=os.environ.get("RAW_SCHEMA", "raw"),
                    help="Source schema the models read (default: $RAW_SCHEMA or 'raw')")
    ap.add_argument("--sas-golden", default=os.environ.get("SAS_GOLDEN"),
                    help="Directory of SAS golden CSVs to compare against")
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"),
                    help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')")
    ap.add_argument("--report", help="Optional path to write the markdown report")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2

    if args.sas_golden and not Path(args.sas_golden).is_dir():
        print(f"ERROR: --sas-golden {args.sas_golden} is not a directory", file=sys.stderr)
        return 2

    rec = Reconciler(args.catalog, args.namespace, args.raw_schema, args.sas_golden)
    ok = rec.run()
    report = rec.render()
    print(report)
    if args.report:
        with open(args.report, "w") as fh:
            fh.write(report + "\n")
        print(f"(report written to {args.report})")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
