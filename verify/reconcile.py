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

    def _rows(self, query: str) -> list[tuple]:
        cur = self.con.cursor()
        try:
            cur.execute(query)
            return cur.fetchall()
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

    # ── monthly_regulatory_reporting.sas Step 1 — RWA controls ──────────
    # SAS risk-weight CASE (source of truth), used to recompute expected values
    # independently of the mart.
    _RWA_CASE = """
        case
            when a.account_type in ('CHK','SAV','MMA') then 0.00
            when a.account_type = 'CD' then 0.00
            when a.account_type = 'MTG' and l.ltv <= 0.80 then 0.35
            when a.account_type = 'MTG' and l.ltv > 0.80 then 0.50
            when a.account_type = 'HELC' then 0.50
            when a.account_type in ('AUTO','PERS') then 0.75
            when a.account_type = 'CC' then 0.75
            when a.account_type = 'LOC' then 1.00
            else 1.00
        end
    """

    def check_rwa_completeness(self):
        """mart_regulatory_rwa must cover the whole daily-snapshot population."""
        expected = self._scalar(f"select count(*) from {self.intermediate}.int_account_metrics")
        actual = self._scalar(
            f"select coalesce(sum(n_accounts), 0) from {self.marts}.mart_regulatory_rwa"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "rwa_completeness",
                "PASS" if ok else "FAIL",
                f"source accounts = {expected}, mart accounts = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_rwa_control_total(self):
        """total_exposure and rwa must tie out to the source, recomputed via SAS CASE."""
        exp_exposure, exp_rwa = self._rows(
            f"""
            select
                sum(a.current_balance),
                sum(a.current_balance * ({self._RWA_CASE}))
            from {self.intermediate}.int_account_metrics a
            left join {self.raw}.loan_details l on a.account_id = l.account_id
            """
        )[0]
        act_exposure, act_rwa = self._rows(
            f"select coalesce(sum(total_exposure), 0), coalesce(sum(rwa), 0) "
            f"from {self.marts}.mart_regulatory_rwa"
        )[0]
        d_exp = abs(float(act_exposure or 0) - float(exp_exposure or 0))
        d_rwa = abs(float(act_rwa or 0) - float(exp_rwa or 0))
        ok = d_exp <= 0.01 and d_rwa <= 0.01
        self.results.append(
            CheckResult(
                "rwa_control_total",
                "PASS" if ok else "FAIL",
                f"exposure src={exp_exposure} mart={act_exposure} (diff {d_exp:.2f}); "
                f"rwa src={exp_rwa} mart={act_rwa} (diff {d_rwa:.2f})",
                {"d_exposure": d_exp, "d_rwa": d_rwa},
            )
        )

    def check_rwa_risk_weight_parity(self):
        """Per-value: every (account_type, risk_weight) count must match the SAS CASE."""
        expected = {
            (t, round(float(w), 2)): int(n)
            for t, w, n in self._rows(
                f"""
                select a.account_type, ({self._RWA_CASE}) as rw, count(*)
                from {self.intermediate}.int_account_metrics a
                left join {self.raw}.loan_details l on a.account_id = l.account_id
                group by a.account_type, rw
                """
            )
        }
        actual = {
            (t, round(float(w), 2)): int(n)
            for t, w, n in self._rows(
                f"select account_type, risk_weight, sum(n_accounts) "
                f"from {self.marts}.mart_regulatory_rwa group by account_type, risk_weight"
            )
        }
        bad = []
        for key in sorted(set(expected) | set(actual)):
            e, a = expected.get(key, 0), actual.get(key, 0)
            if e != a:
                bad.append(f"{key[0]}@{key[1]}: expected {e}, actual {a}")
        ok = not bad
        self.results.append(
            CheckResult(
                "rwa_risk_weight_parity",
                "PASS" if ok else "FAIL",
                "; ".join(bad) if bad else "all account_type risk weights match the SAS mapping",
                {"mismatches": bad},
            )
        )

    # ── monthly_regulatory_reporting.sas Step 2 — delinquency controls ──
    _DELINQ_TYPES = "('MTG','AUTO','PERS','CC','LOC','HELC')"
    _BUCKET_CASE = """
        case
            when days_past_due = 0 then 'Current'
            when days_past_due between 1 and 29 then '1-29'
            when days_past_due between 30 and 59 then '30-59'
            when days_past_due between 60 and 89 then '60-89'
            when days_past_due between 90 and 119 then '90-119'
            when days_past_due between 120 and 179 then '120-179'
            when days_past_due >= 180 then '180+'
            else 'Unknown'
        end
    """

    def check_delinquency_completeness(self):
        """mart_delinquency_aging must cover the credit-product population exactly."""
        expected = self._scalar(
            f"select count(*) from {self.intermediate}.int_account_metrics "
            f"where account_type in {self._DELINQ_TYPES}"
        )
        actual = self._scalar(
            f"select coalesce(sum(n_accounts), 0) from {self.marts}.mart_delinquency_aging"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "delinquency_completeness",
                "PASS" if ok else "FAIL",
                f"credit-product accounts = {expected}, mart accounts = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_delinquency_control_total(self):
        """total_balance and total_past_due must tie out to source."""
        exp_bal, exp_pd = self._rows(
            f"""
            select sum(current_balance), sum(coalesce(past_due_amount, 0))
            from {self.intermediate}.int_account_metrics
            where account_type in {self._DELINQ_TYPES}
            """
        )[0]
        act_bal, act_pd = self._rows(
            f"select coalesce(sum(total_balance), 0), coalesce(sum(total_past_due), 0) "
            f"from {self.marts}.mart_delinquency_aging"
        )[0]
        d_bal = abs(float(act_bal or 0) - float(exp_bal or 0))
        d_pd = abs(float(act_pd or 0) - float(exp_pd or 0))
        ok = d_bal <= 0.01 and d_pd <= 0.01
        self.results.append(
            CheckResult(
                "delinquency_control_total",
                "PASS" if ok else "FAIL",
                f"balance src={exp_bal} mart={act_bal} (diff {d_bal:.2f}); "
                f"past_due src={exp_pd} mart={act_pd} (diff {d_pd:.2f})",
                {"d_balance": d_bal, "d_past_due": d_pd},
            )
        )

    def check_delinquency_bucket_parity(self):
        """Per-value: every (account_type, region, bucket) count must match the SAS CASE."""
        expected = {
            (t, r, b): int(n)
            for t, r, b, n in self._rows(
                f"""
                select account_type, region_code, ({self._BUCKET_CASE}) as bucket, count(*)
                from {self.intermediate}.int_account_metrics
                where account_type in {self._DELINQ_TYPES}
                group by account_type, region_code, bucket
                """
            )
        }
        actual = {
            (t, r, b): int(n)
            for t, r, b, n in self._rows(
                f"select account_type, region_code, delinq_bucket, sum(n_accounts) "
                f"from {self.marts}.mart_delinquency_aging "
                f"group by account_type, region_code, delinq_bucket"
            )
        }
        bad = []
        for key in sorted(set(expected) | set(actual)):
            e, a = expected.get(key, 0), actual.get(key, 0)
            if e != a:
                bad.append(f"{key[0]}/{key[1]}/{key[2]}: expected {e}, actual {a}")
        ok = not bad
        self.results.append(
            CheckResult(
                "delinquency_bucket_parity",
                "PASS" if ok else "FAIL",
                "; ".join(bad) if bad else "all (type, region, bucket) counts match the SAS mapping",
                {"mismatches": bad},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        # monthly_regulatory_reporting.sas controls
        self.check_rwa_completeness()
        self.check_rwa_control_total()
        self.check_rwa_risk_weight_parity()
        self.check_delinquency_completeness()
        self.check_delinquency_control_total()
        self.check_delinquency_bucket_parity()
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
