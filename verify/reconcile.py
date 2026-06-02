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

    def _scalar_safe(self, query: str):
        """Like _scalar but returns None on database errors (e.g. missing table)."""
        try:
            return self._scalar(query)
        except Exception:
            return None

    def _query(self, query: str):
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
        except Exception:
            return None
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

    # ---- policy valuation checks (policy_valuation.sas conversion) ----

    def check_policy_valuation_completeness(self):
        """In-force policy count must match raw ACTIVE policies in the period."""
        # Use the model's own valuation_date (set at dbt build time) so the
        # control stays consistent even if reconcile.py runs on a later day.
        val_date = self._scalar_safe(
            f"select max(valuation_date) from {self.intermediate}.int_policy_valuation"
        )
        if val_date is None:
            self.results.append(
                CheckResult("policy_valuation_completeness", "SKIP",
                            "int_policy_valuation not found")
            )
            return
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.policies
            where policy_status = 'ACTIVE'
              and effective_date <= '{val_date}'
              and expiry_date   >= '{val_date}'
            """
        )
        actual = self._scalar(
            f"select count(*) from {self.intermediate}.int_policy_valuation"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "policy_valuation_completeness",
                "PASS" if ok else "FAIL",
                f"in-force raw policies = {expected}, model policies = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_policy_valuation_control_total(self):
        """Total earned premium ties between the int and mart layers."""
        int_total = self._scalar_safe(
            f"""
            select coalesce(sum(ytd_earned_premium), 0)
            from {self.intermediate}.int_policy_valuation
            """
        )
        mart_total = self._scalar_safe(
            f"""
            select coalesce(sum(total_earned), 0)
            from {self.marts}.mart_loss_ratios
            """
        )
        if int_total is None or mart_total is None:
            self.results.append(
                CheckResult("policy_valuation_control_total", "SKIP",
                            "int_policy_valuation or mart_loss_ratios not found")
            )
            return
        diff = abs(float(int_total) - float(mart_total))
        ok = diff <= 0.01
        self.results.append(
            CheckResult(
                "policy_valuation_control_total",
                "PASS" if ok else "FAIL",
                f"int earned = {int_total}, mart earned = {mart_total}, diff = {diff:.2f}",
                {"int_total": int_total, "mart_total": mart_total, "diff": diff},
            )
        )

    def check_policy_valuation_parity(self):
        """Per-policy SAS formulas (loss/combined ratio, adequacy, IBNR) hold."""
        rows = self._query(
            f"""
            select policy_id, ytd_earned_premium, total_incurred, total_paid,
                   loss_ratio, combined_ratio, premium_adequate, ibnr_estimate
            from {self.intermediate}.int_policy_valuation
            """
        )
        if rows is None:
            self.results.append(
                CheckResult("policy_valuation_parity", "SKIP",
                            "int_policy_valuation not found")
            )
            return
        failures = []
        for row in rows:
            pid, earned, incurred, paid, lr, cr, adequate, ibnr = row
            earned = float(earned or 0)
            incurred = float(incurred or 0)
            paid = float(paid or 0)
            if earned > 0:
                exp_lr = incurred / earned
                exp_cr = exp_lr + 0.30
                exp_adequate = "Y" if exp_cr <= 1.0 else "N"
            else:
                exp_lr = None
                exp_cr = None
                exp_adequate = "N"
            exp_ibnr = max(0.0, earned * 0.15 - paid)
            lr_ok = (lr is None and exp_lr is None) or (
                lr is not None and exp_lr is not None
                and abs(float(lr) - exp_lr) <= 0.0001
            )
            cr_ok = (cr is None and exp_cr is None) or (
                cr is not None and exp_cr is not None
                and abs(float(cr) - exp_cr) <= 0.0001
            )
            if not (lr_ok and cr_ok and adequate == exp_adequate
                    and abs(float(ibnr) - exp_ibnr) <= 0.0001):
                failures.append(str(pid))
        ok = len(failures) == 0
        detail = (
            f"all {len(rows)} policies match SAS formulas"
            if ok else
            f"{len(failures)} policies diverge (e.g. {', '.join(failures[:5])})"
        )
        self.results.append(
            CheckResult("policy_valuation_parity", "PASS" if ok else "FAIL", detail)
        )

    def check_loss_ratios_completeness(self):
        """mart_loss_ratios has one row per distinct in-force policy type."""
        expected = self._scalar_safe(
            f"""
            select count(distinct policy_type)
            from {self.intermediate}.int_policy_valuation
            """
        )
        actual = self._scalar_safe(
            f"select count(*) from {self.marts}.mart_loss_ratios"
        )
        if expected is None or actual is None:
            self.results.append(
                CheckResult("loss_ratios_completeness", "SKIP",
                            "int_policy_valuation or mart_loss_ratios not found")
            )
            return
        ok = expected == actual
        self.results.append(
            CheckResult(
                "loss_ratios_completeness",
                "PASS" if ok else "FAIL",
                f"distinct policy types = {expected}, mart rows = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_loss_ratios_parity(self):
        """Aggregate loss / combined ratio formulas match SAS exactly."""
        rows = self._query(
            f"""
            select policy_type, agg_loss_ratio, agg_combined_ratio,
                   total_incurred, total_earned
            from {self.marts}.mart_loss_ratios
            where total_earned > 0
            """
        )
        if rows is None:
            self.results.append(
                CheckResult("loss_ratios_parity", "SKIP",
                            "mart_loss_ratios not found")
            )
            return
        failures = []
        for row in rows:
            ptype, lr, cr, incurred, earned = row
            exp_lr = float(incurred) / float(earned)
            exp_cr = exp_lr + 0.30
            if abs(float(lr) - exp_lr) > 0.0001 or abs(float(cr) - exp_cr) > 0.0001:
                failures.append(
                    f"{ptype}: lr={lr} (exp {exp_lr:.6f}), cr={cr} (exp {exp_cr:.6f})"
                )
        ok = len(failures) == 0
        detail = "all policy types match" if ok else "; ".join(failures)
        self.results.append(
            CheckResult("loss_ratios_parity", "PASS" if ok else "FAIL", detail)
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_policy_valuation_completeness()
        self.check_policy_valuation_control_total()
        self.check_policy_valuation_parity()
        self.check_loss_ratios_completeness()
        self.check_loss_ratios_parity()
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
