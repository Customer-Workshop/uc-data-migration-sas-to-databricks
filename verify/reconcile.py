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
PySpark checks) — see docs/CONVERSION_PLAYBOOK.md for the reconciliation contract.

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

    # ------------------------------------------------------------------ RWA
    def check_rwa_risk_weight_parity(self):
        """Every (account_type, risk_weight) pair in the mart must match the
        independently derived SAS CASE mapping applied to the source."""
        query = f"""
        with source_weights as (
            select
                a.account_type,
                case
                    when a.account_type in ('CHK','SAV','MMA') then 0.00
                    when a.account_type = 'CD'                 then 0.00
                    when a.account_type = 'MTG'
                         and c.collateral_value is not null
                         and c.collateral_value > 0
                         and (a.current_balance / c.collateral_value) <= 0.80
                        then 0.35
                    when a.account_type = 'MTG'
                         and c.collateral_value is not null
                         and c.collateral_value > 0
                         and (a.current_balance / c.collateral_value) > 0.80
                        then 0.50
                    when a.account_type = 'HELC'               then 0.50
                    when a.account_type in ('AUTO','PERS')     then 0.75
                    when a.account_type = 'CC'                 then 0.75
                    when a.account_type = 'LOC'                then 1.00
                    else 1.00
                end as risk_weight,
                count(*) as n
            from {self.intermediate}.int_account_metrics a
            left join {self.raw}.collateral c
                on a.account_id = c.account_id
            group by 1, 2
        ),
        mart_weights as (
            select account_type, risk_weight, sum(n_accounts) as n
            from {self.marts}.mart_regulatory_rwa
            group by 1, 2
        )
        select
            coalesce(s.account_type, m.account_type) as account_type,
            s.risk_weight as expected_weight,
            m.risk_weight as actual_weight,
            s.n as expected_n,
            m.n as actual_n
        from source_weights s
        full outer join mart_weights m
            on s.account_type = m.account_type
            and s.risk_weight = m.risk_weight
        where s.n is null or m.n is null or s.n <> m.n
        """
        cur = self.con.cursor()
        try:
            cur.execute(query)
            rows = cur.fetchall()
        finally:
            cur.close()
        ok = len(rows) == 0
        detail = "all (type, weight) pairs match" if ok else (
            "; ".join(f"{r[0]} expected {r[1]}×{r[3]} got {r[2]}×{r[4]}" for r in rows[:5])
        )
        self.results.append(CheckResult(
            "rwa_risk_weight_parity", "PASS" if ok else "FAIL", detail,
            {"mismatches": len(rows)},
        ))

    def check_rwa_exposure_control_total(self):
        """Total exposure in the mart must tie to sum(current_balance) from source."""
        expected = self._scalar(
            f"select sum(current_balance) from {self.intermediate}.int_account_metrics"
        )
        actual = self._scalar(
            f"select sum(total_exposure) from {self.marts}.mart_regulatory_rwa"
        )
        diff = abs((actual or 0) - (expected or 0))
        ok = diff <= 0.01
        self.results.append(CheckResult(
            "rwa_exposure_control_total", "PASS" if ok else "FAIL",
            f"source exposure = {expected}, mart exposure = {actual}, diff = {diff:.2f}",
            {"expected": expected, "actual": actual, "diff": diff},
        ))

    # -------------------------------------------------------------- delinquency
    def check_delinquency_completeness(self):
        """Sum of n_accounts in delinquency mart must equal lending accounts in source."""
        expected = self._scalar(f"""
            select count(*) from {self.intermediate}.int_account_metrics
            where account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
        """)
        actual = self._scalar(
            f"select sum(n_accounts) from {self.marts}.mart_delinquency_aging"
        )
        ok = expected == actual
        self.results.append(CheckResult(
            "delinquency_completeness", "PASS" if ok else "FAIL",
            f"lending accounts = {expected}, mart accounts = {actual}",
            {"expected": expected, "actual": actual},
        ))

    def check_delinquency_balance_control_total(self):
        """Total balance in the delinquency mart must tie to lending balances."""
        expected = self._scalar(f"""
            select sum(current_balance) from {self.intermediate}.int_account_metrics
            where account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
        """)
        actual = self._scalar(
            f"select sum(total_balance) from {self.marts}.mart_delinquency_aging"
        )
        diff = abs((actual or 0) - (expected or 0))
        ok = diff <= 0.01
        self.results.append(CheckResult(
            "delinquency_balance_control_total", "PASS" if ok else "FAIL",
            f"source balance = {expected}, mart balance = {actual}, diff = {diff:.2f}",
            {"expected": expected, "actual": actual, "diff": diff},
        ))

    def check_delinquency_bucket_parity(self):
        """Bucket assignments in the mart must match the SAS CASE re-derived from source."""
        query = f"""
        with source_buckets as (
            select
                a.account_type,
                case
                    when coalesce(p.max_days_past_due_ever, 0) = 0
                        then 'Current'
                    when coalesce(p.max_days_past_due_ever, 0) between 1 and 29
                        then '1-29'
                    when coalesce(p.max_days_past_due_ever, 0) between 30 and 59
                        then '30-59'
                    when coalesce(p.max_days_past_due_ever, 0) between 60 and 89
                        then '60-89'
                    when coalesce(p.max_days_past_due_ever, 0) between 90 and 119
                        then '90-119'
                    when coalesce(p.max_days_past_due_ever, 0) between 120 and 179
                        then '120-179'
                    when coalesce(p.max_days_past_due_ever, 0) >= 180
                        then '180+'
                    else 'Unknown'
                end as delinq_bucket,
                count(*) as n
            from {self.intermediate}.int_account_metrics a
            left join {self.raw}.payment_history p
                on a.account_id = p.account_id
            where a.account_type in ('MTG','AUTO','PERS','CC','LOC','HELC')
            group by 1, 2
        ),
        mart_buckets as (
            select account_type, delinq_bucket, sum(n_accounts) as n
            from {self.marts}.mart_delinquency_aging
            group by 1, 2
        )
        select
            coalesce(s.account_type, m.account_type) as account_type,
            coalesce(s.delinq_bucket, m.delinq_bucket) as delinq_bucket,
            s.n as expected_n,
            m.n as actual_n
        from source_buckets s
        full outer join mart_buckets m
            on s.account_type = m.account_type
            and s.delinq_bucket = m.delinq_bucket
        where s.n is null or m.n is null or s.n <> m.n
        """
        cur = self.con.cursor()
        try:
            cur.execute(query)
            rows = cur.fetchall()
        finally:
            cur.close()
        ok = len(rows) == 0
        detail = "all (type, bucket) pairs match" if ok else (
            "; ".join(f"{r[0]}/{r[1]} expected {r[2]} got {r[3]}" for r in rows[:5])
        )
        self.results.append(CheckResult(
            "delinquency_bucket_parity", "PASS" if ok else "FAIL", detail,
            {"mismatches": len(rows)},
        ))

    # ------------------------------------------------------------------- driver
    def run(self) -> bool:
        self.check_account_completeness()
        self.check_rwa_risk_weight_parity()
        self.check_rwa_exposure_control_total()
        self.check_delinquency_completeness()
        self.check_delinquency_balance_control_total()
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
