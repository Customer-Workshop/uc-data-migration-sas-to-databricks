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
import datetime as dt
import os
import sys
from dataclasses import dataclass, field

from databricks import sql


def _prev_ym() -> str:
    """Previous calendar month as YYYYMM (matches dbt var('prev_ym') / SAS PREV_YM)."""
    first_of_month = dt.date.today().replace(day=1)
    return (first_of_month - dt.timedelta(days=1)).strftime("%Y%m")


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

    def _table_exists(self, schema: str, table: str) -> bool:
        try:
            n = self._scalar(
                f"select count(*) from {self.catalog}.information_schema.tables "
                f"where table_schema = '{schema.split('.')[-1]}' "
                f"and table_name = '{table}'"
            )
            return bool(n)
        except Exception:
            return False

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

    # ----------------------------------------------- customer_profitability.sas
    def _customer_pnl_checks(self):
        """Controls for mart_customer_pnl (customer_profitability.sas).

        SKIP (not FAIL) when the mart has not been built into this namespace, so
        the report stays meaningful for namespaces that converted other programs.
        """
        marts = self.marts.split(".")[-1]
        if not self._table_exists(marts, "mart_customer_pnl"):
            for name in (
                "customer_pnl_completeness",
                "customer_pnl_control_total",
                "customer_pnl_account_type_parity",
                "customer_pnl_profit_tier_parity",
            ):
                self.results.append(
                    CheckResult(name, "SKIP", "mart_customer_pnl not built in this namespace")
                )
            return

        pnl = f"{self.marts}.mart_customer_pnl"
        accts = f"{self.intermediate}.int_account_metrics"
        txns = f"{self.marts}.mart_daily_transactions"
        ym = _prev_ym()

        # completeness — one P&L row per in-scope customer (SAS Step 4 "if a").
        expected = self._scalar(f"select count(distinct customer_id) from {accts}")
        actual = self._scalar(f"select count(*) from {pnl}")
        self.results.append(
            CheckResult(
                "customer_pnl_completeness",
                "PASS" if expected == actual else "FAIL",
                f"in-scope customers = {expected}, P&L rows = {actual}",
                {"expected": expected, "actual": actual},
            )
        )

        # control total — net interest income ties to the source account-type CASE.
        model_nii = self._scalar(f"select sum(net_interest_income) from {pnl}") or 0
        source_nii = self._scalar(
            f"""
            select sum(
                case when account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
                     then current_balance * interest_rate / 12 else 0 end
              - case when account_type in ('CHK','SAV','MMA','CD','IRA')
                     then current_balance * interest_rate / 12 else 0 end)
            from {accts}
            """
        ) or 0
        # fee income ties to the report-month FEE transactions (SAS Step 2 scope).
        model_fee = self._scalar(f"select sum(fee_income) from {pnl}") or 0
        source_fee = self._scalar(
            f"""
            select sum(case when transaction_type = 'FEE'
                            then abs(transaction_amount) else 0 end)
            from {txns}
            where date_format(transaction_date, 'yyyyMM') = '{ym}'
              and customer_id in (select customer_id from {accts})
            """
        ) or 0
        nii_ok = abs(float(model_nii) - float(source_nii)) <= 0.01
        fee_ok = abs(float(model_fee) - float(source_fee)) <= 0.01
        self.results.append(
            CheckResult(
                "customer_pnl_control_total",
                "PASS" if (nii_ok and fee_ok) else "FAIL",
                f"net_interest_income model={float(model_nii):.2f} source={float(source_nii):.2f}; "
                f"fee_income[{ym}] model={float(model_fee):.2f} source={float(source_fee):.2f}",
            )
        )

        # parity — every source account type is covered by the SAS income mapping.
        uncovered = self._scalar(
            f"""
            select count(*) from (select distinct account_type from {accts}) t
            where account_type not in ('MTG','AUTO','PERS','CC','LOC','HELC')
              and account_type not in ('CHK','SAV','MMA','CD','IRA')
            """
        )
        self.results.append(
            CheckResult(
                "customer_pnl_account_type_parity",
                "PASS" if uncovered == 0 else "FAIL",
                f"account types falling through the SAS CASE (silently zeroed) = {uncovered}",
            )
        )

        # parity — stored profit_tier matches the SAS threshold ladder per row.
        mismatches = self._scalar(
            f"""
            select count(*) from {pnl}
            where profit_tier is distinct from (
                case when net_profit >= 500 then 'Highly Profitable'
                     when net_profit >= 100 then 'Profitable'
                     when net_profit >= 0   then 'Marginal'
                     else 'Unprofitable' end)
            """
        )
        self.results.append(
            CheckResult(
                "customer_pnl_profit_tier_parity",
                "PASS" if mismatches == 0 else "FAIL",
                f"rows whose profit_tier diverges from the SAS mapping = {mismatches}",
            )
        )

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self._customer_pnl_checks()
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
