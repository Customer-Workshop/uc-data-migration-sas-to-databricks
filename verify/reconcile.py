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
it runs the same family of controls plus the cross-engine PySpark checks that
dbt cannot express, and prints a single reconciliation report you can show live
and attach to a PR. It exits non-zero if any control fails, so it also works as
a CI / pre-merge gate.

Usage
-----
    python verify/reconcile.py --namespace dev
    python verify/reconcile.py --namespace run1 --catalog banking_analytics
    python verify/reconcile.py --namespace dev --report reconciliation_report.md

Credentials are read from the environment (same vars dbt uses):
    DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field

from databricks import sql


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | FAIL | SKIP
    detail: str = ""
    metrics: dict = field(default_factory=dict)


class Reconciler:
    def __init__(self, catalog: str, namespace: str):
        host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
        self.con = sql.connect(
            server_hostname=host,
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
        self.catalog = catalog
        self.ns = namespace
        self.raw = f"{catalog}.raw"
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

    def _schema_exists(self, schema: str) -> bool:
        name = schema.split(".")[-1]
        n = self._scalar(
            f"select count(*) from {self.catalog}.information_schema.schemata "
            f"where schema_name = '{name}'"
        )
        return bool(n)

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

    # SAS-source risk-weight mapping (monthly_regulatory_reporting.sas). MTG is
    # LTV-dependent and validated by allowed-set; everything else is an exact
    # value. IRA is unmapped in the source and lands on the else (1.00) branch.
    RWA_EXPECTED = {
        "CHK": {0.00}, "SAV": {0.00}, "MMA": {0.00}, "CD": {0.00},
        "HELC": {0.50}, "AUTO": {0.75}, "PERS": {0.75}, "CC": {0.75},
        "LOC": {1.00}, "IRA": {1.00}, "MTG": {0.35, 0.50},
    }

    def check_rwa_risk_weight_parity(self):
        """Every account_type's risk weight must match the SAS source mapping."""
        cur = self.con.cursor()
        try:
            cur.execute(
                f"select distinct account_type, risk_weight "
                f"from {self.marts}.mart_regulatory_rwa"
            )
            rows = cur.fetchall()
        finally:
            cur.close()
        mismatches = []
        for account_type, risk_weight in rows:
            allowed = self.RWA_EXPECTED.get(account_type)
            if allowed is None:
                mismatches.append(f"{account_type}={risk_weight} (not in source mapping)")
            elif round(float(risk_weight), 2) not in allowed:
                exp = "/".join(f"{v:.2f}" for v in sorted(allowed))
                mismatches.append(f"{account_type}={risk_weight} (source expects {exp})")
        ok = not mismatches
        detail = (
            "all account_types match the SAS risk-weight mapping"
            if ok else "; ".join(mismatches)
        )
        self.results.append(
            CheckResult(
                "rwa_risk_weight_parity",
                "PASS" if ok else "FAIL",
                detail,
                {"mismatches": mismatches},
            )
        )

    def check_rwa_exposure_control_total(self, tolerance: float = 0.01):
        """RWA mart total exposure must tie out to source balances."""
        src = self._scalar(
            f"select sum(current_balance) from {self.intermediate}.int_account_metrics"
        )
        mart = self._scalar(
            f"select sum(total_exposure) from {self.marts}.mart_regulatory_rwa"
        )
        src = float(src or 0)
        mart = float(mart or 0)
        diff = abs(src - mart)
        ok = diff <= tolerance
        self.results.append(
            CheckResult(
                "rwa_exposure_control_total",
                "PASS" if ok else "FAIL",
                f"source balance = {src:,.2f}, mart exposure = {mart:,.2f}, diff = {diff:,.2f}",
                {"source": src, "mart": mart, "diff": diff},
            )
        )

    def check_claims_pipeline(self):
        """Cross-engine: PySpark curated claims outputs are internally consistent."""
        if not self._schema_exists(self.curated):
            self.results.append(
                CheckResult(
                    "claims_register_within_source",
                    "SKIP",
                    f"{self.curated} not found — run the PySpark job first "
                    "(make run-job, or the claims_processing task)",
                )
            )
            self.results.append(CheckResult("fraud_alerts_referential", "SKIP", "curated schema absent"))
            self.results.append(CheckResult("review_queue_referential", "SKIP", "curated schema absent"))
            return

        raw_claims = self._scalar(f"select count(*) from {self.raw}.claims")
        register = self._scalar(f"select count(*) from {self.curated}.claims_register")
        null_ids = self._scalar(
            f"select count(*) from {self.curated}.claims_register where claim_id is null"
        )
        ok = register <= raw_claims and (null_ids or 0) == 0
        self.results.append(
            CheckResult(
                "claims_register_within_source",
                "PASS" if ok else "FAIL",
                f"raw claims = {raw_claims}, register = {register}, null claim_id = {null_ids or 0}",
                {"raw": raw_claims, "register": register, "null_ids": null_ids or 0},
            )
        )

        for tbl, label in (("fraud_alerts", "fraud_alerts_referential"),
                           ("claims_review_queue", "review_queue_referential")):
            orphans = self._scalar(
                f"""
                select count(*)
                from {self.curated}.{tbl} t
                left join {self.curated}.claims_register r on t.claim_id = r.claim_id
                where r.claim_id is null
                """
            )
            ok = (orphans or 0) == 0
            self.results.append(
                CheckResult(
                    label,
                    "PASS" if ok else "FAIL",
                    f"{tbl} rows with no matching claim in register = {orphans or 0} (expected 0)",
                    {"orphans": orphans or 0},
                )
            )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_rwa_risk_weight_parity()
        self.check_rwa_exposure_control_total()
        self.check_claims_pipeline()
        self.con.close()
        return all(r.status != "FAIL" for r in self.results)

    def render(self) -> str:
        width = max(len(r.name) for r in self.results) + 2
        icon = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "SKIP"}
        lines = [
            f"# Reconciliation Report — {self.catalog} / namespace `{self.ns}`",
            "",
            "Source -> target controls proving the converted marts match the legacy",
            "SAS extract's intent. FAIL blocks the migration; SKIP means a prerequisite",
            "(e.g. the PySpark curated outputs) has not been produced yet.",
            "",
            "| Control | Result | Detail |",
            "|---|---|---|",
        ]
        for r in self.results:
            lines.append(f"| `{r.name}` | {icon[r.status]} | {r.detail} |")
        passed = sum(r.status == "PASS" for r in self.results)
        failed = sum(r.status == "FAIL" for r in self.results)
        skipped = sum(r.status == "SKIP" for r in self.results)
        lines += ["", f"**{passed} passed, {failed} failed, {skipped} skipped**", ""]
        return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="SAS -> Databricks reconciliation report")
    ap.add_argument("--catalog", default="banking_analytics")
    ap.add_argument("--namespace", default=os.environ.get("DBT_SCHEMA", "dev"),
                    help="Output namespace prefix (default: $DBT_SCHEMA or 'dev')")
    ap.add_argument("--report", help="Optional path to write the markdown report")
    args = ap.parse_args()

    for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        if not os.environ.get(var):
            print(f"ERROR: {var} is not set", file=sys.stderr)
            return 2

    rec = Reconciler(args.catalog, args.namespace)
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
