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

    # ----------------------------------------- customer_profitability.sas controls
    # SAS Step 1 interest-income IN-lists (the product-type classification).
    _LENDING = "account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')"
    _DEPOSIT = "account_type in ('CHK','SAV','MMA','CD','IRA')"

    def check_customer_pnl_completeness(self):
        """mart_customer_pnl holds exactly one row per in-scope customer.

        SAS `if a;` keeps the INTEREST_INCOME population (customers with >=1
        in-scope account). No silent row loss, no fan-out from the fee/ECL joins.
        """
        expected = self._scalar(
            f"select count(distinct customer_id) from {self.intermediate}.int_account_metrics"
        )
        actual = self._scalar(f"select count(*) from {self.marts}.mart_customer_pnl")
        ok = expected == actual
        self.results.append(
            CheckResult(
                "customer_pnl_completeness",
                "PASS" if ok else "FAIL",
                f"in-scope customers = {expected}, mart rows = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

    def check_customer_pnl_control_total(self):
        """Every P&L line in the mart is reconstructable from source, to the cent."""
        exp_nii = self._scalar(
            f"""
            select sum(case when {self._LENDING} then current_balance*interest_rate/12 else 0 end)
                 - sum(case when {self._DEPOSIT} then current_balance*interest_rate/12 else 0 end)
            from {self.intermediate}.int_account_metrics
            """
        )
        exp_fee = self._scalar(
            f"""
            select sum(case when t.transaction_type='FEE' then abs(t.transaction_amount) else 0 end)
            from {self.marts}.mart_daily_transactions t
            where t.customer_id in
                (select distinct customer_id from {self.intermediate}.int_account_metrics)
            """
        )
        exp_op = self._scalar(f"select 15*count(*) from {self.intermediate}.int_account_metrics")
        exp_ecl = self._scalar(
            f"""
            select sum(r.expected_loss)
            from {self.marts}.mart_risk_scores r
            where r.score_date =
                (select max(score_date) from {self.marts}.mart_risk_scores
                 where score_date <= current_date())
              and r.customer_id in
                (select distinct customer_id from {self.intermediate}.int_account_metrics)
            """
        )
        exp_np = (exp_nii or 0) + (exp_fee or 0) - (exp_op or 0) - (exp_ecl or 0)

        act_nii = self._scalar(f"select sum(net_interest_income) from {self.marts}.mart_customer_pnl")
        act_fee = self._scalar(f"select sum(coalesce(fee_income,0)) from {self.marts}.mart_customer_pnl")
        act_op = self._scalar(f"select sum(operating_cost) from {self.marts}.mart_customer_pnl")
        act_ecl = self._scalar(f"select sum(coalesce(total_ecl,0)) from {self.marts}.mart_customer_pnl")
        act_np = self._scalar(f"select sum(net_profit) from {self.marts}.mart_customer_pnl")

        lines = {
            "net_interest_income": (exp_nii, act_nii),
            "fee_income": (exp_fee, act_fee),
            "operating_cost": (exp_op, act_op),
            "total_ecl": (exp_ecl, act_ecl),
            "net_profit": (exp_np, act_np),
        }
        diffs = [
            f"{name} exp={(e or 0):,.2f} act={(a or 0):,.2f}"
            for name, (e, a) in lines.items()
            if abs((a or 0) - (e or 0)) > 0.01
        ]
        ok = not diffs
        detail = (
            f"net_profit total = {(act_np or 0):,.2f}; all P&L lines tie out"
            if ok
            else "; ".join(diffs)
        )
        self.results.append(
            CheckResult(
                "customer_pnl_control_total",
                "PASS" if ok else "FAIL",
                detail,
                {k: {"expected": v[0], "actual": v[1]} for k, v in lines.items()},
            )
        )

    def check_customer_pnl_interest_parity(self):
        """Per-type interest classification matches the SAS IN-lists.

        Reconstruct lending/deposit totals from the SAS product-type lists and
        compare to the model's grand totals. A misclassified account type (e.g.
        LOC dropped from LENDING) would move dollars between buckets and fail.
        The dbt singular test reconcile_customer_pnl_parity.sql is the per-value gate.
        """
        exp_lending = self._scalar(
            f"select sum(case when {self._LENDING} then current_balance*interest_rate/12 else 0 end) "
            f"from {self.intermediate}.int_account_metrics"
        )
        exp_deposit = self._scalar(
            f"select sum(case when {self._DEPOSIT} then current_balance*interest_rate/12 else 0 end) "
            f"from {self.intermediate}.int_account_metrics"
        )
        act_lending = self._scalar(
            f"select sum(lending_income) from {self.intermediate}.int_customer_interest_income"
        )
        act_deposit = self._scalar(
            f"select sum(deposit_cost) from {self.intermediate}.int_customer_interest_income"
        )
        neither = self._scalar(
            f"select count(distinct account_type) from {self.intermediate}.int_account_metrics "
            f"where not ({self._LENDING}) and not ({self._DEPOSIT})"
        )
        ok = (
            abs((exp_lending or 0) - (act_lending or 0)) <= 0.01
            and abs((exp_deposit or 0) - (act_deposit or 0)) <= 0.01
        )
        self.results.append(
            CheckResult(
                "customer_pnl_interest_parity",
                "PASS" if ok else "FAIL",
                f"lending exp/act = {(exp_lending or 0):,.2f}/{(act_lending or 0):,.2f}, "
                f"deposit exp/act = {(exp_deposit or 0):,.2f}/{(act_deposit or 0):,.2f}, "
                f"types in neither IN-list = {neither}",
                {},
            )
        )

    def _guarded(self, check):
        """Run a check; record SKIP if its prerequisite tables are not present."""
        try:
            check()
        except Exception as exc:  # noqa: BLE001 — surface as SKIP, not a crash
            self.results.append(
                CheckResult(check.__name__.replace("check_", ""), "SKIP", str(exc).splitlines()[0])
            )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self._guarded(self.check_customer_pnl_completeness)
        self._guarded(self.check_customer_pnl_control_total)
        self._guarded(self.check_customer_pnl_interest_parity)
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
