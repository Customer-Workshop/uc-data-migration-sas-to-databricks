#!/usr/bin/env python3
"""
reconcile.py — Source -> target reconciliation report for the SAS -> Databricks
migration.

Why this exists
---------------
The point of the migration is not just to produce *some* output on Databricks —
it is to produce output we can *trust* matches what the legacy SAS estate
produced. Two families of controls establish that trust:

1. Source -> target invariants (row counts, control totals, domain coverage,
   referential integrity) between the raw source and the converted marts.
2. Legacy -> target parity against the SAS estate's *golden outputs*: the
   Dockerised SAS runtime (ts-sas-legacy-analytics/docker) runs the real
   programs on the real seed extracts and exports every output table plus
   `row_counts.csv` / `controls.csv`. Pass that directory with `--sas-golden`
   and each SAS output table is compared with the model that replaces it.
   A model that has not been converted yet is reported as SKIP ("not yet
   converted"), a converted model that disagrees with SAS is a FAIL.

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

Usage
-----
    python verify/reconcile.py --namespace dev
    python verify/reconcile.py --namespace run1 --catalog banking_analytics
    python verify/reconcile.py --namespace dev --report reconciliation_report.md
    python verify/reconcile.py --namespace dev --sas-golden /path/to/sas/golden

Credentials are read from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, DATABRICKS_CATALOG (optional)
"""
from __future__ import annotations

import argparse
import csv
import json
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


@dataclass(frozen=True)
class GoldenTable:
    """A SAS output table and the target model that replaces it."""
    sas: str        # e.g. CURATED.TXN_ANOMALIES (as in row_counts.csv)
    layer: str      # staging | intermediate | marts | curated
    model: str      # target table name inside <catalog>.<ns>_<layer>
    program: str    # SAS program that produces it (for the report)


# The SAS estate's eight banking outputs (docker/expected/row_counts.csv in
# ts-sas-legacy-analytics) and where each lands on the target. Adding a
# conversion = adding/renaming a line here so the harness starts gating it.
GOLDEN_TABLES: list[GoldenTable] = [
    GoldenTable("STG_BANK.CUST_ACCOUNTS_DAILY", "staging", "stg_cust_accounts", "load_customer_accounts.sas"),
    GoldenTable("STG_BANK.ACCT_EXCEPTIONS", "staging", "stg_acct_exceptions", "load_customer_accounts.sas"),
    GoldenTable("CURATED.DAILY_TRANSACTIONS", "marts", "mart_daily_transactions", "daily_transaction_processing.sas"),
    GoldenTable("CURATED.TXN_ANOMALIES", "marts", "mart_transaction_anomalies", "daily_transaction_processing.sas"),
    GoldenTable("CURATED.RISK_SCORES", "marts", "mart_risk_scores", "credit_risk_scoring.sas"),
    GoldenTable("REPORTS.MONTHLY_RWA", "marts", "mart_regulatory_rwa", "monthly_regulatory_reporting.sas"),
    GoldenTable("REPORTS.DELINQUENCY_AGING", "marts", "mart_delinquency_aging", "monthly_regulatory_reporting.sas"),
    GoldenTable("REPORTS.LLP_COVERAGE", "marts", "mart_llp_coverage", "monthly_regulatory_reporting.sas"),
]

# controls.csv rows from the SAS export -> the target SQL that must reproduce them.
# {marts} etc. are filled with the namespace's schemas. Money totals compare to 2dp.
GOLDEN_CONTROLS: dict[str, tuple[str, str]] = {
    "TXN_ANOMALIES.HIGH_AMOUNT": (
        "CURATED.TXN_ANOMALIES",
        "select count(*) from {marts}.mart_transaction_anomalies where anomaly_type = 'HIGH_AMOUNT'"),
    "TXN_ANOMALIES.OVERDRAFT": (
        "CURATED.TXN_ANOMALIES",
        "select count(*) from {marts}.mart_transaction_anomalies where anomaly_type = 'OVERDRAFT'"),
    "DAILY_TRANSACTIONS.SUM_AMOUNT": (
        "CURATED.DAILY_TRANSACTIONS",
        "select round(sum(transaction_amount), 2) from {marts}.mart_daily_transactions"),
    "RISK_SCORES.N_ACCOUNTS": (
        "CURATED.RISK_SCORES",
        "select count(*) from {marts}.mart_risk_scores"),
    "MONTHLY_RWA.SUM_RWA": (
        "REPORTS.MONTHLY_RWA",
        "select round(sum(rwa), 2) from {marts}.mart_regulatory_rwa"),
}


class TableNotFound(Exception):
    """The target table does not exist in this namespace (not yet converted)."""


def is_not_found(err: Exception) -> bool:
    msg = str(err)
    return "TABLE_OR_VIEW_NOT_FOUND" in msg or "SCHEMA_NOT_FOUND" in msg or "cannot be found" in msg


def read_golden(golden_dir: Path) -> tuple[dict[str, int], dict[str, float | None], dict]:
    """row_counts.csv, controls.csv and manifest.json from a SAS golden export."""
    counts: dict[str, int] = {}
    with open(golden_dir / "row_counts.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            counts[r["TABLE_NAME"].strip()] = int(float(r["N_ROWS"]))
    controls: dict[str, float | None] = {}
    with open(golden_dir / "controls.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            v = (r["VALUE"] or "").strip()
            controls[r["CONTROL"].strip()] = float(v) if v else None
    manifest = {}
    mp = golden_dir / "manifest.json"
    if mp.exists():
        with open(mp) as fh:
            manifest = json.load(fh)
    return counts, controls, manifest


def numbers_match(expected: float, actual: float, tol: float = 0.005) -> bool:
    return abs(float(expected) - float(actual)) <= tol


def fmt_num(value) -> str:
    """16 -> '16', 8231436.81 -> '8,231,436.81', None -> 'NULL'."""
    if value is None:
        return "NULL"
    f = float(value)
    return f"{int(f):,}" if f.is_integer() else f"{f:,.2f}"


class Reconciler:
    def __init__(self, catalog: str, namespace: str, con=None, sas_golden: Path | None = None,
                 raw_schema: str = "raw"):
        if con is None:
            host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
            con = sql.connect(
                server_hostname=host,
                http_path=os.environ["DATABRICKS_HTTP_PATH"],
                access_token=os.environ["DATABRICKS_TOKEN"],
            )
        self.con = con
        self.catalog = catalog
        self.ns = namespace
        self.raw = f"{catalog}.{raw_schema}"
        self.staging = f"{catalog}.{namespace}_staging"
        self.intermediate = f"{catalog}.{namespace}_intermediate"
        self.marts = f"{catalog}.{namespace}_marts"
        self.curated = f"{catalog}.{namespace}_curated"
        self.sas_golden = sas_golden
        self.manifest: dict = {}
        self.results: list[CheckResult] = []

    def _scalar(self, query: str):
        cur = self.con.cursor()
        try:
            cur.execute(query)
            row = cur.fetchone()
            return row[0] if row else None
        except Exception as err:  # the connector raises its own ServerOperationError
            if is_not_found(err):
                raise TableNotFound(query) from err
            raise
        finally:
            cur.close()

    def _schema(self, layer: str) -> str:
        return {"staging": self.staging, "intermediate": self.intermediate,
                "marts": self.marts, "curated": self.curated}[layer]

    # ------------------------------------------------------------------ checks
    def check_account_completeness(self):
        """Model accounts must equal the documented in-scope raw population."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.cust_accounts a
            inner join {self.raw}.cust_demographics d on a.customer_id = d.customer_id
            where a.account_status not in ('W', 'C')
              and a.open_date <= current_date()
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
            )
        )

    def check_sas_golden(self):
        """Every SAS output table must be reproduced by its replacement model."""
        assert self.sas_golden is not None
        counts, controls, self.manifest = read_golden(self.sas_golden)
        for gt in GOLDEN_TABLES:
            name = f"sas_rowcount:{gt.sas}"
            if gt.sas not in counts:
                self.results.append(CheckResult(name, "FAIL", f"{gt.sas} missing from row_counts.csv"))
                continue
            expected = counts[gt.sas]
            fq = f"{self._schema(gt.layer)}.{gt.model}"
            try:
                actual = self._scalar(f"select count(*) from {fq}")
            except TableNotFound:
                self.results.append(CheckResult(
                    name, "SKIP", f"{gt.program} not yet converted: `{fq}` does not exist (SAS rows = {expected})",
                    {"expected": expected}))
                continue
            ok = actual == expected
            self.results.append(CheckResult(
                name, "PASS" if ok else "FAIL",
                f"SAS {gt.sas} = {expected} rows, `{gt.model}` = {actual} rows",
                {"expected": expected, "actual": actual}))

        schemas = {"staging": self.staging, "intermediate": self.intermediate,
                   "marts": self.marts, "curated": self.curated, "raw": self.raw}
        for control, (sas_table, query) in GOLDEN_CONTROLS.items():
            name = f"sas_control:{control}"
            if control not in controls:
                self.results.append(CheckResult(name, "FAIL", f"{control} missing from controls.csv"))
                continue
            expected = controls[control]
            if expected is None:
                self.results.append(CheckResult(name, "FAIL", f"{control} is blank in controls.csv (SAS export problem)"))
                continue
            try:
                actual = self._scalar(query.format(**schemas))
            except TableNotFound:
                self.results.append(CheckResult(
                    name, "SKIP", f"{sas_table} not yet converted (SAS value = {fmt_num(expected)})",
                    {"expected": expected}))
                continue
            ok = actual is not None and numbers_match(expected, actual)
            self.results.append(CheckResult(
                name, "PASS" if ok else "FAIL",
                f"SAS = {fmt_num(expected)}, target = {fmt_num(actual)}",
                {"expected": expected, "actual": actual}))

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        if self.sas_golden is not None:
            self.check_sas_golden()
        self.con.close()
        return all(r.status != "FAIL" for r in self.results)

    def render(self) -> str:
        icon = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "SKIP"}
        lines = [
            f"# Reconciliation Report — {self.catalog} / namespace `{self.ns}` (source: `{self.raw}`)",
            "",
            "Source -> target controls proving the converted marts match the legacy",
            "SAS estate. FAIL blocks the migration; SKIP means the SAS output has no",
            "converted model in this namespace yet.",
            "",
        ]
        if self.sas_golden is not None:
            m = self.manifest
            lines += [
                f"SAS golden baseline: `{self.sas_golden}` — estate `{m.get('estate', '?')}`"
                f" @ `{m.get('estate_commit', '?')}`, runtime `{m.get('runtime', '?')}`,"
                f" business date `{m.get('business_date', '?')}`, exported {m.get('finished_utc', '?')}.",
                "",
            ]
        lines += ["| Control | Result | Detail |", "|---|---|---|"]
        for r in self.results:
            lines.append(f"| `{r.name}` | {icon[r.status]} | {r.detail} |")
        passed = sum(r.status == "PASS" for r in self.results)
        failed = sum(r.status == "FAIL" for r in self.results)
        skipped = sum(r.status == "SKIP" for r in self.results)
        lines += ["", f"**{passed} passed, {failed} failed, {skipped} skipped**", ""]
        return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="SAS -> Databricks reconciliation report")
    ap.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG", "banking_analytics"),
                    help="Unity Catalog catalog (default: $DATABRICKS_CATALOG or banking_analytics)")
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"),
                    help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')")
    ap.add_argument("--raw-schema", default=os.environ.get("DBT_RAW_SCHEMA", "raw"),
                    help="Schema holding the durable 'before' tables (default: $DBT_RAW_SCHEMA or raw)")
    ap.add_argument("--report", help="Optional path to write the markdown report")
    ap.add_argument("--sas-golden", type=Path, default=os.environ.get("SAS_GOLDEN_DIR") or None,
                    help="Directory exported by the SAS container (row_counts.csv, controls.csv, manifest.json);"
                         " default: $SAS_GOLDEN_DIR")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2
    if args.sas_golden is not None and not (args.sas_golden / "row_counts.csv").is_file():
        print(f"ERROR: {args.sas_golden} has no row_counts.csv — run the SAS container first", file=sys.stderr)
        return 2

    rec = Reconciler(args.catalog, args.namespace, sas_golden=args.sas_golden, raw_schema=args.raw_schema)
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
