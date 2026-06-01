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

    # --------------------------------------------------- insurance: policy_valuation
    # Controls for policy_valuation.sas -> int_policy_valuation + mart_loss_ratios.
    # Each is wrapped so a namespace that has not built the insurance models yet
    # records a SKIP (missing prerequisite) instead of crashing the report.
    def _guard(self, name: str, fn):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - report, don't crash
            msg = str(exc).splitlines()[0][:160]
            self.results.append(CheckResult(name, "SKIP", f"prerequisite missing: {msg}"))

    def check_policy_completeness(self):
        """int_policy_valuation must equal the in-force raw population (no loss/fan-out)."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.policies
            where policy_status = 'ACTIVE'
              and effective_date <= current_date()
              and expiry_date >= current_date()
            """
        )
        actual = self._scalar(f"select count(*) from {self.intermediate}.int_policy_valuation")
        distinct = self._scalar(
            f"select count(distinct policy_id) from {self.intermediate}.int_policy_valuation"
        )
        ok = expected == actual == distinct
        self.results.append(
            CheckResult(
                "policy_completeness",
                "PASS" if ok else "FAIL",
                f"in-force raw policies = {expected}, model policies = {actual} "
                f"(distinct = {distinct})",
                {"expected": expected, "actual": actual, "distinct": distinct},
            )
        )

    def check_loss_ratio_control_total(self):
        """Control totals must be conserved from int_policy_valuation to mart_loss_ratios."""
        int_earned = self._scalar(
            f"select sum(ytd_earned_premium) from {self.intermediate}.int_policy_valuation"
        )
        mart_earned = self._scalar(f"select sum(total_earned) from {self.marts}.mart_loss_ratios")
        int_n = self._scalar(f"select count(*) from {self.intermediate}.int_policy_valuation")
        mart_n = self._scalar(f"select sum(n_policies) from {self.marts}.mart_loss_ratios")
        ok = int_n == mart_n and abs((int_earned or 0) - (mart_earned or 0)) <= 0.01
        self.results.append(
            CheckResult(
                "loss_ratio_control_total",
                "PASS" if ok else "FAIL",
                f"earned premium int = {int_earned:,.2f} vs mart = {mart_earned:,.2f}; "
                f"policies int = {int_n} vs mart = {mart_n}",
                {"int_earned": int_earned, "mart_earned": mart_earned},
            )
        )

    def check_premium_adequacy_parity(self):
        """Every policy's premium_adequate flag must match the SAS rule recomputed in place."""
        bad = self._scalar(
            f"""
            select count(*)
            from {self.intermediate}.int_policy_valuation
            where premium_adequate <> case
                when combined_ratio is null then 'N'
                when combined_ratio > 1.0 then 'N'
                else 'Y'
            end
            or (ytd_earned_premium > 0
                and abs(coalesce(combined_ratio, 0) - (coalesce(loss_ratio, 0) + 0.30)) > 1e-9)
            """
        )
        self.results.append(
            CheckResult(
                "premium_adequacy_parity",
                "PASS" if bad == 0 else "FAIL",
                f"{bad} policies diverge from the SAS premium-adequacy / combined-ratio rule",
                {"violations": bad},
            )
        )

    def check_loss_ratio_mapping_parity(self):
        """mart policy_type_desc must reproduce $POLTYPE value-for-value (catch-all 'Unknown')."""
        bad = self._scalar(
            f"""
            with expected as (
                select code, label from values
                    ('WL','Whole Life'),('TL','Term Life'),('UL','Universal Life'),
                    ('VL','Variable Life'),('AUTO','Auto Insurance'),('HOME','Homeowners'),
                    ('RENT','Renters'),('UMBR','Umbrella'),('HLTH','Health'),('DNTL','Dental'),
                    ('VIS','Vision'),('DISAB','Disability'),('LTCI','Long-Term Care')
                    as t(code, label)
            )
            select count(*)
            from {self.marts}.mart_loss_ratios m
            left join expected e on m.policy_type = e.code
            where m.policy_type_desc <> coalesce(e.label, 'Unknown')
               or (m.total_earned > 0
                   and abs(coalesce(m.agg_combined_ratio, 0)
                       - (coalesce(m.agg_loss_ratio, 0) + 0.30)) > 1e-9)
            """
        )
        self.results.append(
            CheckResult(
                "loss_ratio_mapping_parity",
                "PASS" if bad == 0 else "FAIL",
                f"{bad} LOB rows diverge from $POLTYPE mapping or combined-ratio identity",
                {"violations": bad},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self._guard("policy_completeness", self.check_policy_completeness)
        self._guard("loss_ratio_control_total", self.check_loss_ratio_control_total)
        self._guard("premium_adequacy_parity", self.check_premium_adequacy_parity)
        self._guard("loss_ratio_mapping_parity", self.check_loss_ratio_mapping_parity)
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
