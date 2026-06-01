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

    # ---------------------------------------------- customer P&L checks
    def check_customer_pnl_completeness(self):
        """One P&L row per customer that has at least one in-scope account."""
        expected = self._scalar(
            f"select count(distinct customer_id) from {self.intermediate}.int_account_metrics"
        )
        actual = self._scalar(
            f"select count(*) from {self.marts}.mart_customer_pnl"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "customer_pnl_completeness",
                "PASS" if ok else "FAIL",
                f"expected customers = {expected}, model rows = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_customer_pnl_control_total(self):
        """Total revenue in the mart must tie to independently computed NII + fees."""
        nii = self._scalar(
            f"""
            select
                sum(case when account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
                    then current_balance * interest_rate / 12 else 0 end)
              - sum(case when account_type in ('CHK','SAV','MMA','CD','IRA')
                    then current_balance * interest_rate / 12 else 0 end)
            from {self.intermediate}.int_account_metrics
            """
        )
        fees = self._scalar(
            f"""
            select sum(case when t.transaction_type = 'FEE'
                then abs(t.transaction_amount) else 0 end)
            from {self.marts}.mart_daily_transactions t
            inner join (select distinct customer_id from {self.intermediate}.int_account_metrics) c
                on t.customer_id = c.customer_id
            """
        )
        expected = (nii or 0) + (fees or 0)
        actual = self._scalar(
            f"select sum(total_revenue) from {self.marts}.mart_customer_pnl"
        ) or 0
        ok = abs(actual - expected) <= 0.01
        self.results.append(
            CheckResult(
                "customer_pnl_control_total",
                "PASS" if ok else "FAIL",
                f"expected total_revenue = {expected:.2f}, model = {actual:.2f}, "
                f"diff = {abs(actual - expected):.4f}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_customer_pnl_profit_tier_parity(self):
        """Every row's profit_tier must match the SAS threshold mapping."""
        mismatches = self._scalar(
            f"""
            select count(*)
            from {self.marts}.mart_customer_pnl
            where profit_tier <> case
                when net_profit >= 500 then 'Highly Profitable'
                when net_profit >= 100 then 'Profitable'
                when net_profit >= 0   then 'Marginal'
                else 'Unprofitable'
            end
            """
        )
        ok = mismatches == 0
        self.results.append(
            CheckResult(
                "customer_pnl_profit_tier_parity",
                "PASS" if ok else "FAIL",
                f"rows with tier mismatch = {mismatches}",
                {"mismatches": mismatches},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_customer_pnl_completeness()
        self.check_customer_pnl_control_total()
        self.check_customer_pnl_profit_tier_parity()
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
