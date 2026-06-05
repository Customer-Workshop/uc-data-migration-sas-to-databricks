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

    # ---------------------------------------------- policy valuation checks
    def check_policy_val_completeness(self):
        """int_policy_valuation row count equals in-scope raw policies."""
        expected = self._scalar(
            f"""
            select count(*)
            from {self.raw}.policies
            where policy_status = 'ACTIVE'
              and effective_date <= current_date()
              and expiry_date >= current_date()
            """
        )
        actual = self._scalar(
            f"select count(*) from {self.intermediate}.int_policy_valuation"
        )
        ok = expected == actual
        self.results.append(
            CheckResult(
                "policy_val_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope raw policies = {expected}, model rows = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_loss_ratio_parity(self):
        """agg_loss_ratio per policy_type matches independent raw calculation."""
        cur = self.con.cursor()
        try:
            cur.execute(
                f"""
                with raw_earned as (
                    select policy_type,
                           sum(annual_premium / 12 * least(12,
                               months_between(
                                   least(current_date(), expiry_date),
                                   greatest(effective_date, date_trunc('year', current_date()))
                               )
                           )) as total_earned
                    from {self.raw}.policies
                    where policy_status = 'ACTIVE'
                      and effective_date <= current_date()
                      and expiry_date >= current_date()
                    group by policy_type
                ),
                raw_incurred as (
                    select p.policy_type, sum(c.claimed_amount) as total_incurred
                    from {self.raw}.claims c
                    inner join {self.raw}.policies p on c.policy_id = p.policy_id
                    where p.policy_status = 'ACTIVE'
                      and p.effective_date <= current_date()
                      and p.expiry_date >= current_date()
                      and c.loss_date >= add_months(current_date(), -12)
                      and c.loss_date <= current_date()
                    group by p.policy_type
                ),
                expected as (
                    select e.policy_type,
                           case when e.total_earned > 0
                                then coalesce(i.total_incurred, 0) / e.total_earned
                                else null end as expected_lr
                    from raw_earned e
                    left join raw_incurred i on e.policy_type = i.policy_type
                ),
                mart as (
                    select policy_type, agg_loss_ratio
                    from {self.marts}.mart_loss_ratios
                )
                select e.policy_type, e.expected_lr, m.agg_loss_ratio
                from expected e
                full outer join mart m on e.policy_type = m.policy_type
                where abs(coalesce(e.expected_lr, 0) - coalesce(m.agg_loss_ratio, 0)) > 0.0001
                   or e.policy_type is null
                   or m.policy_type is null
                """
            )
            mismatches = cur.fetchall()
        finally:
            cur.close()
        ok = len(mismatches) == 0
        detail = "all policy types match" if ok else f"{len(mismatches)} type(s) diverge"
        self.results.append(
            CheckResult(
                "loss_ratio_parity",
                "PASS" if ok else "FAIL",
                detail,
                {"mismatches": len(mismatches)},
            )
        )

    def check_premium_adequacy_parity(self):
        """premium_adequate flag matches SAS rule value-for-value."""
        miscount = self._scalar(
            f"""
            select count(*) from (
                select policy_id,
                       premium_adequate as actual,
                       case when ytd_earned_premium > 0
                                 and coalesce(total_incurred, 0) / ytd_earned_premium + 0.30 <= 1.0
                            then 'Y' else 'N' end as expected
                from {self.intermediate}.int_policy_valuation
            ) t
            where actual <> expected
            """
        )
        ok = miscount == 0
        self.results.append(
            CheckResult(
                "premium_adequacy_parity",
                "PASS" if ok else "FAIL",
                f"{miscount} row(s) with flag mismatch",
                {"mismatches": miscount},
            )
        )

    def check_ibnr_control_total(self):
        """Total IBNR ties out to formula applied independently to raw data."""
        expected = self._scalar(
            f"""
            with raw_earned as (
                select policy_id,
                       annual_premium / 12 * least(12,
                           months_between(
                               least(current_date(), expiry_date),
                               greatest(effective_date, date_trunc('year', current_date()))
                           )
                       ) as ytd_ep
                from {self.raw}.policies
                where policy_status = 'ACTIVE'
                  and effective_date <= current_date()
                  and expiry_date >= current_date()
            ),
            raw_paid as (
                select policy_id,
                       sum(case when claim_status in ('CLOSED', 'SETTLED')
                                then claimed_amount else 0 end) as total_paid
                from {self.raw}.claims
                where loss_date >= add_months(current_date(), -12)
                  and loss_date <= current_date()
                group by policy_id
            )
            select sum(greatest(0, e.ytd_ep * 0.15 - coalesce(rp.total_paid, 0)))
            from raw_earned e
            left join raw_paid rp on e.policy_id = rp.policy_id
            """
        )
        actual = self._scalar(
            f"select sum(ibnr_estimate) from {self.intermediate}.int_policy_valuation"
        )
        diff = abs((expected or 0) - (actual or 0))
        ok = diff <= 0.01
        self.results.append(
            CheckResult(
                "ibnr_control_total",
                "PASS" if ok else "FAIL",
                f"expected = {expected}, model = {actual}, diff = {diff:.4f}",
                {"expected": expected, "actual": actual, "diff": diff},
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_policy_val_completeness()
        self.check_loss_ratio_parity()
        self.check_premium_adequacy_parity()
        self.check_ibnr_control_total()
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
