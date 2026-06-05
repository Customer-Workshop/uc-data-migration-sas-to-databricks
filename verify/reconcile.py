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

    # ------------------------------------------------------------------ checks
    # monthly_regulatory_reporting.sas — Step 1: RWA
    def check_rwa_completeness(self):
        """mart_regulatory_rwa row count must match int_account_metrics."""
        expected = self._scalar(
            f"select count(*) from {self.intermediate}.int_account_metrics"
        )
        actual = self._scalar(
            f"select sum(n_accounts) from {self.marts}.mart_regulatory_rwa"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "rwa_completeness",
                "PASS" if ok else "FAIL",
                f"expected accounts = {expected}, mart sum(n_accounts) = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_rwa_risk_weight_parity(self):
        """Every account_type risk_weight must match the SAS CASE mapping."""
        cur = self.con.cursor()
        try:
            cur.execute(
                f"""
                select distinct account_type, risk_weight
                from {self.marts}.mart_regulatory_rwa
                """
            )
            rows = cur.fetchall()
        finally:
            cur.close()

        expected_map = {
            "CHK": {0.00}, "SAV": {0.00}, "MMA": {0.00}, "CD": {0.00},
            "MTG": {0.35, 0.50}, "HELC": {0.50},
            "AUTO": {0.75}, "PERS": {0.75}, "CC": {0.75}, "LOC": {1.00},
        }
        divergences = []
        for acct_type, weight in rows:
            allowed = expected_map.get(acct_type, {1.00})
            if round(float(weight), 3) not in {round(w, 3) for w in allowed}:
                divergences.append(f"{acct_type}={weight} (expected {allowed})")

        ok = len(divergences) == 0
        detail = "all weights match SAS source" if ok else "; ".join(divergences)
        self.results.append(
            CheckResult(
                "rwa_risk_weight_parity",
                "PASS" if ok else "FAIL",
                detail,
                {"divergences": divergences},
            )
        )

    def check_rwa_control_total(self):
        """Total RWA in mart must tie to independently computed total."""
        mart_rwa = self._scalar(
            f"select coalesce(sum(rwa), 0) from {self.marts}.mart_regulatory_rwa"
        )
        independent_rwa = self._scalar(
            f"""
            select coalesce(sum(
                a.current_balance * case
                    when a.account_type in ('CHK','SAV','MMA') then 0.00
                    when a.account_type = 'CD' then 0.00
                    when a.account_type = 'MTG'
                         and c.collateral_value > 0
                         and (a.current_balance / c.collateral_value) <= 0.80
                        then 0.35
                    when a.account_type = 'MTG' then 0.50
                    when a.account_type = 'HELC' then 0.50
                    when a.account_type in ('AUTO','PERS') then 0.75
                    when a.account_type = 'CC' then 0.75
                    when a.account_type = 'LOC' then 1.00
                    else 1.00
                end
            ), 0)
            from {self.intermediate}.int_account_metrics a
            left join {self.raw}.collateral c
                on a.account_id = c.account_id
            """
        )
        diff = abs(float(mart_rwa) - float(independent_rwa))
        ok = diff <= 0.01
        self.results.append(
            CheckResult(
                "rwa_control_total",
                "PASS" if ok else "FAIL",
                f"mart RWA = {mart_rwa}, independent = {independent_rwa}, diff = {diff:.2f}",
                {"mart_rwa": mart_rwa, "independent_rwa": independent_rwa, "diff": diff},
            )
        )

    # monthly_regulatory_reporting.sas — Step 2: Delinquency Aging
    def check_delinquency_completeness(self):
        """mart_delinquency_aging must cover all in-scope loan accounts."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.intermediate}.int_account_metrics
            where account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
            """
        )
        actual = self._scalar(
            f"select sum(n_accounts) from {self.marts}.mart_delinquency_aging"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "delinquency_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope loan accounts = {expected}, mart sum(n_accounts) = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_rwa_completeness()
        self.check_rwa_risk_weight_parity()
        self.check_rwa_control_total()
        self.check_delinquency_completeness()
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
